package main

import (
	"encoding/binary"
	"errors"
	"log"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SukkaW/hikyaku/proxy"
	"github.com/xtaci/smux"
)

const (
	dialTimeout  = 5 * time.Second
	minBackoff   = time.Second
	maxBackoff   = 5 * time.Second
	suspectAfter = 5 * time.Second
	healthCheck  = 500 * time.Millisecond
	carrierWait  = 10 * time.Second
	carrierPoll  = 50 * time.Millisecond
	slowStrikes  = 3
	slowEvery    = 2 * time.Second
	metricsEvery = 30 * time.Second
	probeWait    = 5 * time.Second
	// A ping or acknowledgement is overdue after TCP's retransmission timeout for the
	// carrier (RFC 6298: SRTT + 4·RTTVAR) but never under replyRoundTrips smoothed
	// round trips, kept within minReplyDeadline and maxReplyDeadline, and
	// maxReplyDeadline until a round trip is measured. The round-trip floor keeps GEO,
	// whose queues and loss recovery stretch replies well past RTTVAR, at the cap.
	minReplyDeadline = time.Second
	maxReplyDeadline = 3 * time.Second
	replyRoundTrips  = 5
	// Slowness is judged only from pongs to pings sent while a carrier carried no
	// data, so a busy carrier's own queue cannot make it look slow: the median of its
	// last quietSamples such round trips within quietWindow, needing minQuietSamples,
	// against the best other carrier's, with a margin of half the best or
	// minSlowMargin, whichever is larger.
	quietSamples    = 5
	minQuietSamples = 3
	quietWindow     = 10 * time.Second
	minSlowMargin   = 200 * time.Millisecond
	// Pings go out only when a carrier is quiet: often while it was in use recently,
	// which keeps the server's congestion window from shrinking during reading
	// pauses, and rarely once idle. A busy carrier still gets one every second, so its
	// expected wait stays current under load.
	quietAfter = 500 * time.Millisecond
	warmFor    = 30 * time.Second
	warmPing   = 500 * time.Millisecond
	coldPing   = 15 * time.Second
	busyPing   = time.Second
	// deadAfter closes a standby carrier whose ping has gone unanswered this long.
	deadAfter = 30 * time.Second
	// minWaitBand is the least a carrier's expected wait may exceed the best one's and
	// still count as tied; otherwise the band is a quarter of the best. A pool with one
	// server keeps the wider band: its carriers share a path, so a small difference is
	// noise. Across servers a smaller difference is a genuinely quicker route, and a
	// wrong pick costs the request that server's round trip.
	minWaitBand         = 20 * time.Millisecond
	multiServerWaitBand = 5 * time.Millisecond
)

var errNoCarrier = errors.New("no carrier is ready")

type carrier struct {
	slot    int
	server  string
	session *smux.Session
	conn    *proxy.Conn
	ready   time.Time
	latest  atomic.Int64 // the latest of those round trips in nanoseconds; 0 until the first

	lastStream atomic.Int64           // unix nanoseconds when a stream last closed
	goaway     atomic.Pointer[string] // the server's reason, once it sent GOAWAY
	pingNow    chan bool              // asks for a ping; true if the carrier is quiet

	mu         sync.Mutex
	unanswered map[*replayStream]struct{}
	waiting    map[*carrierStream]time.Time // streams still waiting for their acknowledgement
	streams    map[uint32]*carrierStream    // open streams by id, for the server's RESET
	pings      map[uint32]ping              // pings still waiting for their pong
	nextPing   uint32
	quiet      []sample      // round trips of pings sent while quiet, oldest first
	srtt       time.Duration // smoothed round trip from pongs and acknowledgements (RFC 6298); 0 until the first
	rttvar     time.Duration // its variation
}

type ping struct {
	sent  time.Time
	quiet bool // the carrier carried no data when it was sent
}

type sample struct {
	at  time.Time
	rtt time.Duration
}

// pool opens carriers on demand, up to a fixed number of slots; an opened slot is
// nil while its carrier is down and keeps being redialed. Silent carriers wait on
// standby until their slot has a replacement.
type pool struct {
	servers  []string
	states   []serverState
	psk      []byte
	waitBand time.Duration
	mu       sync.Mutex
	carriers []*carrier
	standby  []*carrier
	opened   int
	changed  chan struct{} // closed and replaced whenever a carrier becomes usable
	probing  atomic.Bool
	next     int // the server to try first on the next dial, rotating round-robin
}

// serverState tracks one server's consecutive failures and when it may be dialed
// again. A TCP dial that fails and a carrier the server never acknowledged share the
// same failure window: with 0-RTT a dial succeeds before the server answers, so only
// an acknowledgement proves the server is healthy. The window grows 1s, 2s, 4s, then
// 5s, and only one failure per window counts when carriers fail together.
type serverState struct {
	failures int
	nextTry  time.Time
}

func newPool(servers []string, psk []byte, carriers int) *pool {
	if len(servers) == 0 {
		panic("hikyaku-client: newPool needs at least one server")
	}
	// Keep the first occurrence of each address: duplicate entries would leave state
	// for only one of them reachable and make nextServer spin over identical servers.
	seen := make(map[string]struct{}, len(servers))
	unique := make([]string, 0, len(servers))
	for _, server := range servers {
		if _, ok := seen[server]; ok {
			continue
		}
		seen[server] = struct{}{}
		unique = append(unique, server)
	}
	waitBand := minWaitBand
	if len(unique) > 1 {
		waitBand = multiServerWaitBand
	}
	return &pool{
		servers:  unique,
		states:   make([]serverState, len(unique)),
		psk:      psk,
		waitBand: waitBand,
		carriers: make([]*carrier, carriers),
		changed:  make(chan struct{}),
	}
}

// warm opens the first carrier before any request, so one that arrives a round trip
// later finds it connected. Requests that arrive sooner wait for it as before and
// still ride its first flight.
func (p *pool) warm() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.opened == 0 {
		p.grow()
	}
}

// grow starts maintaining one more slot; the caller holds p.mu.
func (p *pool) grow() {
	if p.opened < len(p.carriers) {
		go p.maintain(p.opened)
		p.opened++
	}
}

func (p *pool) maintain(slot int) {
	for {
		server, wait := p.nextServer(time.Now())
		if wait > 0 {
			time.Sleep(wait)
			continue
		}
		c, err := p.dial(server)
		if err != nil {
			log.Printf("carrier %d: server %s: %v", slot, server, err)
			p.dialFailed(server)
			// Try another server at once; when every one is backing off, nextServer
			// hands back the earliest wait instead of spinning.
			continue
		}
		c.slot = slot
		p.set(slot, c)
		log.Printf("carrier %d: ready (server %s)", slot, server)
		v := p.watch(c)
		silent := v == silent || v == noAck
		switch {
		case v == goingAway:
			// The server keeps serving the carrier's streams, so they stay where they are.
			p.toStandby(c)
			log.Printf("carrier %d: server %s: %s after %s, standing by", slot, server, *c.goaway.Load(), c.lifetime())
			go p.retire(c)
		case v == slow:
			p.toStandby(c)
			c.moveUnanswered()
			log.Printf("carrier %d: server %s: slower than the best other carrier while quiet for %d checks after %s, standing by",
				slot, server, slowStrikes, c.lifetime())
			go p.retire(c)
		case silent && c.conn.Acked():
			p.toStandby(c)
			c.moveUnanswered()
			if v == noAck {
				log.Printf("carrier %d: server %s: acknowledgement overdue by %s after %s, standing by", slot, server, c.replyDeadline(), c.lifetime())
			} else {
				log.Printf("carrier %d: server %s: silent for %s after %s, standing by", slot, server, c.replyDeadline(), c.lifetime())
			}
			go p.retire(c)
		case silent:
			// Never acknowledged, so it has not proven it reaches the server; failing
			// its streams now lets apps retry instead of waiting on a dead standby.
			p.set(slot, nil)
			c.session.Close()
			log.Printf("carrier %d: server %s: no acknowledgement after %s, closing", slot, server, c.lifetime())
		default:
			p.set(slot, nil)
			log.Printf("carrier %d: server %s: closed after %s: %s", slot, server, c.lifetime(), c.closeReason())
		}
		// With 0-RTT a dial succeeds before the server answers, so a carrier it never
		// acknowledged, such as one with the wrong PSK, backs the server off as a failed
		// dial would. One sent away by a server that is shutting down is redialed at once,
		// as its successor may already be listening; the old server closed its listener
		// first, so it is not reached again, and a refused dial backs off.
		if c.conn.Acked() {
			p.serverAcked(server)
		} else {
			p.rejectFailed(server)
		}
	}
}

// nextServer returns the server to dial next, rotating the round-robin start after
// each choice. When every server is backing off, it returns the one available
// soonest and how long to wait for it, so maintain waits instead of polling.
func (p *pool) nextServer(now time.Time) (server string, wait time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := len(p.servers)
	if n == 0 {
		return "", 0
	}
	for i := range n {
		idx := (p.next + i) % n
		if !p.states[idx].nextTry.After(now) {
			p.next = (idx + 1) % n
			return p.servers[idx], 0
		}
	}
	earliest := 0
	for i := 1; i < n; i++ {
		if p.states[i].nextTry.Before(p.states[earliest].nextTry) {
			earliest = i
		}
	}
	p.next = earliest
	return p.servers[earliest], p.states[earliest].nextTry.Sub(now)
}

// state returns the state of server; the caller holds p.mu.
func (p *pool) state(server string) *serverState {
	for i, s := range p.servers {
		if s == server {
			return &p.states[i]
		}
	}
	return nil
}

// serverAcked resets a server's failure window; only a carrier the server
// acknowledged proves that it is healthy.
func (p *pool) serverAcked(server string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if st := p.state(server); st != nil {
		st.failures = 0
		st.nextTry = time.Time{}
	}
}

func (p *pool) dialFailed(server string) {
	p.failServer(server)
}

// rejectFailed records a carrier the server never acknowledged as a failure of that
// server, sharing its window with dial failures so consecutive rejections keep
// backing off instead of restarting whenever maintain switches servers.
func (p *pool) rejectFailed(server string) {
	p.failServer(server)
}

// failServer records one failure in server's current window.
func (p *pool) failServer(server string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	st := p.state(server)
	if st == nil {
		return
	}
	now := time.Now()
	// Carriers fail together when their server goes away, so only the first failure
	// after the previous backoff window opens the next one; the rest of that round
	// leaves the window alone instead of multiplying the wait by the carrier count.
	if st.nextTry.After(now) {
		return
	}
	st.failures++
	backoff := minBackoff
	for i := 1; i < st.failures && backoff < maxBackoff; i++ {
		backoff *= 2
	}
	st.nextTry = now.Add(min(backoff, maxBackoff))
}

func (p *pool) set(slot int, c *carrier) {
	p.mu.Lock()
	p.carriers[slot] = c
	if c != nil {
		p.wake()
	}
	p.mu.Unlock()
}

// wake releases requests waiting for a carrier; the caller holds p.mu.
func (p *pool) wake() {
	close(p.changed)
	p.changed = make(chan struct{})
}

func (p *pool) toStandby(c *carrier) {
	p.mu.Lock()
	p.carriers[c.slot] = nil
	p.standby = append(p.standby, c)
	p.mu.Unlock()
}

func (p *pool) replaced(slot int) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	c := p.carriers[slot]
	return c != nil && c.conn.Acked()
}

func (p *pool) dial(server string) (*carrier, error) {
	conn, err := net.DialTimeout("tcp", server, dialTimeout)
	if err != nil {
		return nil, err
	}
	pc, err := proxy.ClientHandshake(conn, p.psk)
	if err != nil {
		conn.Close()
		return nil, err
	}
	c := &carrier{server: server, conn: pc, pings: make(map[uint32]ping), pingNow: make(chan bool, 1)}
	pc.OnControl(c.control)
	session, err := smux.Client(pc, proxy.MuxConfig())
	if err != nil {
		conn.Close()
		return nil, err
	}
	c.session, c.ready = session, time.Now()
	go c.sendPings()
	return c, nil
}

func (c *carrier) lifetime() time.Duration {
	return time.Since(c.ready).Round(time.Second)
}

// closeReason explains why the session ended; one the pool closed itself reaches
// the reader as net.ErrClosed.
func (c *carrier) closeReason() string {
	if err := c.conn.ReadErr(); err != nil && !errors.Is(err, net.ErrClosed) {
		return err.Error()
	}
	return "closed"
}

// down reports whether c can carry no more streams. smux reports a failed connection
// to its streams but leaves the session open until watch closes it.
func (c *carrier) down() bool {
	return c.session.IsClosed() || c.conn.ReadErr() != nil
}

// usable reports whether c may take new streams: it is up and its server has not sent
// GOAWAY, after which no new stream may open on it.
func (c *carrier) usable() bool {
	return !c.down() && c.goaway.Load() == nil
}

// control handles the server's control messages on the carrier's reading goroutine.
func (c *carrier) control(cmd byte, body []byte) {
	switch cmd {
	case proxy.CtrlPong:
		if len(body) != 4 {
			return
		}
		id := binary.BigEndian.Uint32(body)
		c.mu.Lock()
		pg, ok := c.pings[id]
		delete(c.pings, id)
		// The server answers pings in order and drops them when it falls behind, so an
		// older ping still waiting will never be answered.
		for older := range c.pings {
			if older < id {
				delete(c.pings, older)
			}
		}
		if ok && pg.quiet {
			c.quiet = append(c.quiet, sample{time.Now(), time.Since(pg.sent)})
			if len(c.quiet) > quietSamples {
				c.quiet = c.quiet[1:]
			}
		}
		c.mu.Unlock()
		if ok {
			c.sample(time.Since(pg.sent))
		}
	case proxy.CtrlGoAway:
		reason := "server going away"
		if len(body) >= 2 && len(body) >= 2+int(body[1]) {
			reason = string(body[2 : 2+int(body[1])])
		}
		c.goaway.Store(&reason)
	case proxy.CtrlReset:
		if id, _, ok := proxy.ParseReset(body); ok {
			c.mu.Lock()
			s := c.streams[id]
			c.mu.Unlock()
			if s != nil {
				// Marked here, before smux reads the FIN that follows the RESET.
				s.reset.Store(true)
				// Closing writes a FIN, which must not block this reading goroutine.
				go s.Stream.Close()
			}
		}
	}
}

// sendPings writes the pings asked for on pingNow. It runs apart from the health
// checks, so a carrier whose writes are stuck cannot stall them: its ping is
// recorded before the write and simply goes overdue.
func (c *carrier) sendPings() {
	for {
		select {
		case quiet := <-c.pingNow:
			c.mu.Lock()
			id := c.nextPing
			c.nextPing++
			c.pings[id] = ping{sent: time.Now(), quiet: quiet}
			c.mu.Unlock()
			var body [4]byte
			binary.BigEndian.PutUint32(body[:], id)
			if c.conn.WriteControl(proxy.CtrlPing, body[:]) != nil {
				return
			}
		case <-c.session.CloseChan():
			return
		}
	}
}

func (c *carrier) ping(now time.Time) {
	quiet := !c.conn.ReceivingData() && now.Sub(c.conn.LastData()) >= quietAfter && now.Sub(c.conn.LastSent()) >= quietAfter
	select {
	case c.pingNow <- quiet:
	default:
	}
}

// quietMedian returns the median of c's recent quiet round trips, if it has enough.
func (c *carrier) quietMedian(now time.Time) (time.Duration, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var recent []time.Duration
	for _, s := range c.quiet {
		if now.Sub(s.at) <= quietWindow {
			recent = append(recent, s.rtt)
		}
	}
	if len(recent) < minQuietSamples {
		return 0, false
	}
	slices.Sort(recent)
	return recent[len(recent)/2], true
}

// pingInterval picks how often to ping a carrier from how long ago its last smux
// data arrived, how long ago its last stream closed, and how many it has open.
func pingInterval(sinceData, sinceStream time.Duration, streams int) time.Duration {
	switch {
	case sinceData < quietAfter:
		return busyPing
	case streams > 0 || sinceStream < warmFor:
		return warmPing
	default:
		return coldPing
	}
}

// pongOverdue reports whether a ping has waited at least d for its pong.
func (c *carrier) pongOverdue(d time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, pg := range c.pings {
		if time.Since(pg.sent) >= d {
			return true
		}
	}
	return false
}

// unresponsive reports whether c has left a ping unanswered for d and delivered no
// data for as long. A busy carrier's pong can wait behind its own queue while its
// data keeps arriving, which proves it alive.
func (c *carrier) unresponsive(d time.Duration) bool {
	return c.pongOverdue(d) && time.Since(c.conn.LastData()) >= d
}

// ackOverdue reports whether a stream has waited past c's reply deadline for its
// acknowledgement while no data arrived for as long either; like a pong, an
// acknowledgement can wait behind a busy carrier's own queue.
func (c *carrier) ackOverdue() bool {
	d := c.replyDeadline()
	if time.Since(c.conn.LastData()) < d {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, opened := range c.waiting {
		if time.Since(opened) >= d {
			return true
		}
	}
	return false
}

// replyDeadline is how long a ping or acknowledgement on c may take before c is
// suspect.
func (c *carrier) replyDeadline() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.srtt == 0 {
		return maxReplyDeadline
	}
	return min(max(c.srtt+4*c.rttvar, replyRoundTrips*c.srtt, minReplyDeadline), maxReplyDeadline)
}

type verdict int

const (
	closed verdict = iota
	silent
	noAck
	slow
	goingAway
)

// watch pings the carrier and reports why it should leave its slot: its session
// closed, the server is going away, a ping went unanswered, a stream's
// acknowledgement is overdue, or it stayed clearly slower than the best other carrier
// for slowStrikes checks in a row.
func (p *pool) watch(c *carrier) verdict {
	tick := time.NewTicker(healthCheck)
	defer tick.Stop()
	var strikes int
	var lastPing time.Time
	lastSlowCheck, lastLog := time.Now(), time.Now()
	for {
		select {
		case <-c.session.CloseChan():
			return closed
		case <-tick.C:
			now := time.Now()
			// smux reports a dead connection to its streams but leaves the session open.
			if c.conn.ReadErr() != nil {
				c.session.Close()
				return closed
			}
			if c.goaway.Load() != nil {
				return goingAway
			}
			sinceStream := now.Sub(time.Unix(0, c.lastStream.Load()))
			// Ticks are healthCheck apart, so half a tick of slack keeps a tick that lands
			// just short of the interval from pushing the ping to the next one, which would
			// turn the 0.5 s warm ping into one a second.
			interval := pingInterval(now.Sub(c.conn.LastData()), sinceStream, c.session.NumStreams())
			if now.Sub(lastPing) >= interval-healthCheck/2 {
				lastPing = now
				c.ping(now)
			}
			quiet := p.linkQuiet()
			if quiet {
				p.probeOutage()
			}
			// A link-wide outage looks the same on every carrier, so while it lasts only
			// acknowledged carriers go to standby, keeping their streams; new carriers
			// keep waiting for their acknowledgement and the other rules wait too.
			if c.conn.Acked() && c.unresponsive(c.replyDeadline()) {
				return silent
			}
			if !c.conn.Acked() && !quiet && now.Sub(c.ready) >= suspectAfter {
				return silent
			}
			if quiet {
				strikes = 0
				continue
			}
			if c.ackOverdue() {
				return noAck
			}
			if now.Sub(lastSlowCheck) >= slowEvery {
				lastSlowCheck = now
				if p.slower(c) {
					strikes++
				} else {
					strikes = 0
				}
				if strikes >= slowStrikes {
					return slow
				}
			}
			if now.Sub(lastLog) >= metricsEvery {
				lastLog = now
				c.mu.Lock()
				srtt := c.srtt
				c.mu.Unlock()
				log.Printf("carrier %d: server %s: rtt %s, reply deadline %s, %d streams", c.slot, c.server,
					srtt.Round(time.Millisecond), c.replyDeadline().Round(time.Millisecond), c.session.NumStreams())
			}
		}
	}
}

// slower reports whether c's quiet round trips sit clearly above the best other
// acknowledged carrier's on the same server; a carrier without enough of them, such
// as a busy one, is not judged, and with no other carrier on its server to compare
// against nothing is slow. Across servers quickest picks the better one, so a healthy
// but slower server is not retired and redialed over and over.
func (p *pool) slower(c *carrier) bool {
	now := time.Now()
	mine, ok := c.quietMedian(now)
	if !c.conn.Acked() || !ok {
		return false
	}
	p.mu.Lock()
	best := time.Duration(-1)
	for _, o := range p.carriers {
		if o == nil || o == c || o.server != c.server || !o.conn.Acked() || o.session.IsClosed() {
			continue
		}
		if m, ok := o.quietMedian(now); ok && (best < 0 || m < best) {
			best = m
		}
	}
	p.mu.Unlock()
	return best >= 0 && slowerThan(mine, best)
}

func slowerThan(mine, best time.Duration) bool {
	return mine-best >= max(best/2, minSlowMargin)
}

// linkQuiet reports a link-wide outage: some carriers have been acknowledged, yet
// every one of them has a ping unanswered past its reply deadline and no data for as
// long. A carrier that was never acknowledged has heard nothing real, so it counts
// neither way.
func (p *pool) linkQuiet() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	seen := false
	for _, c := range slices.Concat(p.carriers, p.standby) {
		if c == nil || !c.conn.Acked() || c.session.IsClosed() {
			continue
		}
		if !c.unresponsive(c.replyDeadline()) {
			return false
		}
		seen = true
	}
	return seen
}

// probeOutage dials one fresh carrier at a time while the link stays quiet.
// Carriers whose path state was lost may never hear back, so the first acknowledged
// probe is what ends the outage; it then serves from standby until slot 0 has an
// acknowledged carrier again.
func (p *pool) probeOutage() {
	if !p.probing.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer p.probing.Store(false)
		log.Printf("link: no carrier answered a ping in time, probing")
		for p.linkQuiet() {
			server, wait := p.nextServer(time.Now())
			if wait > 0 {
				time.Sleep(probeBackoff(wait))
				continue
			}
			c, err := p.dial(server)
			if err != nil {
				log.Printf("link: probe to server %s: %v", server, err)
				// Probing recovers from an outage; it must not drive the longer
				// per-server backoff that maintain uses. Retry at a short fixed pace.
				time.Sleep(minBackoff)
				continue
			}
			deadline := time.Now().Add(probeWait)
			for !c.conn.Acked() && !c.session.IsClosed() && time.Now().Before(deadline) {
				time.Sleep(carrierPoll)
			}
			if !c.conn.Acked() {
				c.session.Close()
				time.Sleep(minBackoff)
				continue
			}
			p.serverAcked(server)
			p.mu.Lock()
			p.standby = append(p.standby, c)
			p.wake()
			p.mu.Unlock()
			log.Printf("link: probe acknowledged, outage over")
			go p.retire(c)
			return
		}
	}()
}

// probeBackoff caps a per-server backoff wait for outage probing, so a long maintain
// backoff cannot stall the probe that must find a recovered path.
func probeBackoff(wait time.Duration) time.Duration {
	return min(wait, minBackoff)
}

// retire keeps a standby carrier for its existing streams, which survive a short
// outage, and closes it once its slot is replaced and those streams are done. It
// keeps pinging, so a recovery or an outage still shows; if the server never
// returns, it closes once a ping has gone unanswered for deadAfter.
func (p *pool) retire(c *carrier) {
	tick := time.NewTicker(healthCheck)
	defer tick.Stop()
	defer func() {
		p.mu.Lock()
		p.standby = slices.DeleteFunc(p.standby, func(s *carrier) bool { return s == c })
		p.mu.Unlock()
	}()
	for {
		select {
		case <-c.session.CloseChan():
			log.Printf("carrier %d: server %s: standby closed after %s: %s", c.slot, c.server, c.lifetime(), c.closeReason())
			return
		case <-tick.C:
			if c.conn.ReadErr() != nil {
				c.session.Close()
				continue
			}
			if c.session.NumStreams() == 0 && p.replaced(c.slot) {
				c.session.Close()
				log.Printf("carrier %d: server %s: standby retired after %s", c.slot, c.server, c.lifetime())
				return
			}
			if c.pongOverdue(deadAfter) {
				c.session.Close()
				log.Printf("carrier %d: server %s: standby closed after %s: no pong for %s", c.slot, c.server, c.lifetime(), deadAfter)
				return
			}
			c.ping(time.Now())
		}
	}
}

// openStream waits for a usable carrier when none is ready yet, such as right
// after start or an outage, so the stream rides the new carrier's first flight.
func (p *pool) openStream() (*carrier, *carrierStream, error) {
	deadline := time.NewTimer(carrierWait)
	defer deadline.Stop()
	for {
		// Taken before pick, so a carrier that appears in between still wakes us.
		p.mu.Lock()
		changed := p.changed
		p.mu.Unlock()
		if c := p.pick(); c != nil {
			// One that failed since it was picked waits for another, like one not ready yet.
			if s, err := c.session.OpenStream(); err == nil {
				return c, c.newStream(s), nil
			}
		}
		select {
		case <-changed:
		// A standby carrier that starts receiving again raises no signal.
		case <-time.After(healthCheck):
		case <-deadline.C:
			return nil, nil, errNoCarrier
		}
	}
}

func (p *pool) pick() *carrier {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.opened == 0 {
		p.grow()
	}
	best := quickest(p.carriers, p.waitBand)
	if best != nil && (best.session.NumStreams() > 0 || !p.spare(best)) {
		// Keep an idle carrier in reserve: open another in the background as soon as
		// this one takes a stream, so a request that arrives during a long download
		// gets a carrier of its own instead of queueing behind it.
		p.grow()
	}
	if best == nil {
		// A standby carrier takes new streams only once it answers pings or delivers
		// data again; that proves more than a new carrier still waiting for its
		// acknowledgement. One the server is closing never does.
		best = fewestStreams(p.standby, func(c *carrier) bool { return !c.unresponsive(c.replyDeadline()) })
	}
	if best == nil {
		best = fewestStreams(p.carriers, nil)
	}
	return best
}

// spare reports whether a slot other than c's has an idle carrier or is still
// opening one; the caller holds p.mu.
func (p *pool) spare(c *carrier) bool {
	for slot := range p.opened {
		if slot == c.slot {
			continue
		}
		if o := p.carriers[slot]; o == nil || !o.conn.Acked() || o.session.NumStreams() == 0 {
			return true
		}
	}
	return false
}

func fewestStreams(carriers []*carrier, eligible func(*carrier) bool) *carrier {
	var best *carrier
	for _, c := range carriers {
		if c == nil || !c.usable() || (eligible != nil && !eligible(c)) {
			continue
		}
		if best == nil || c.session.NumStreams() < best.session.NumStreams() {
			best = c
		}
	}
	return best
}

// quickest picks the acknowledged carrier where a new stream should hear back
// soonest, skipping any whose server is going away before watch moves it to standby.
func quickest(carriers []*carrier, band time.Duration) *carrier {
	now := time.Now()
	var acked []*carrier
	var waits []time.Duration
	var streams []int
	for _, c := range carriers {
		if c == nil || !c.usable() || !c.conn.Acked() {
			continue
		}
		acked = append(acked, c)
		waits = append(waits, c.expectedWait(now))
		streams = append(streams, c.session.NumStreams())
	}
	if len(acked) == 0 {
		return nil
	}
	return acked[choose(waits, streams, band)]
}

// choose returns which carrier to use from each one's expected wait, negative when
// unknown, and stream count. Waits within band of the best count as tied, and so do
// unknown ones; among the tied, the fewest streams wins.
func choose(waits []time.Duration, streams []int, band time.Duration) int {
	fastest := time.Duration(-1)
	for _, w := range waits {
		if w >= 0 && (fastest < 0 || w < fastest) {
			fastest = w
		}
	}
	limit := fastest + max(fastest/4, band)
	best := -1
	for i, w := range waits {
		if fastest >= 0 && w > limit {
			continue
		}
		if best < 0 || streams[i] < streams[best] {
			best = i
		}
	}
	return best
}

// expectedWait estimates how long a new stream's request would wait for its
// acknowledgement on c: the latest round trip, or longer while a ping or an
// acknowledgement has gone unanswered for longer than that, which shows a stall
// before any reply returns. It is negative while c has neither.
func (c *carrier) expectedWait(now time.Time) time.Duration {
	wait := time.Duration(-1)
	if latest := c.latest.Load(); latest != 0 {
		wait = time.Duration(latest)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, pg := range c.pings {
		wait = max(wait, now.Sub(pg.sent))
	}
	for _, opened := range c.waiting {
		wait = max(wait, now.Sub(opened))
	}
	return wait
}

// carrierStream strips the server's acknowledgement and result from the front of a
// stream: the acknowledgement becomes a round-trip sample for its carrier, and a
// failed result ends the stream with the server's reason.
type carrierStream struct {
	*smux.Stream
	c         *carrier
	opened    time.Time
	requested atomic.Int64 // unix nanoseconds of the first write, which carries the request
	answered  bool         // only touched by the single reader
	reset     atomic.Bool  // the server reset the stream; its reads and writes then fail with proxy.ErrReset
}

func (c *carrier) newStream(s *smux.Stream) *carrierStream {
	cs := &carrierStream{Stream: s, c: c, opened: time.Now()}
	c.mu.Lock()
	if c.waiting == nil {
		c.waiting = make(map[*carrierStream]time.Time)
	}
	c.waiting[cs] = cs.opened
	if c.streams == nil {
		c.streams = make(map[uint32]*carrierStream)
	}
	c.streams[s.ID()] = cs
	c.mu.Unlock()
	return cs
}

func (s *carrierStream) Write(b []byte) (int, error) {
	s.requested.CompareAndSwap(0, time.Now().UnixNano())
	n, err := s.Stream.Write(b)
	// Reported as the reset it is, so the relay does not send a RESET back.
	if err != nil && s.reset.Load() {
		err = proxy.ErrReset
	}
	return n, err
}

func (s *carrierStream) Read(b []byte) (n int, err error) {
	defer func() {
		if err != nil && s.reset.Load() {
			err = proxy.ErrReset
		}
	}()
	if !s.answered {
		if err := proxy.ReadAck(s.Stream); err != nil {
			return 0, err
		}
		s.c.acknowledged(s, time.Since(time.Unix(0, s.requested.Load())))
		if err := proxy.ReadResult(s.Stream); err != nil {
			return 0, err
		}
		s.answered = true
	}
	return s.Stream.Read(b)
}

func (s *carrierStream) Close() error {
	s.c.mu.Lock()
	delete(s.c.waiting, s)
	delete(s.c.streams, s.ID())
	s.c.mu.Unlock()
	s.c.lastStream.Store(time.Now().UnixNano())
	return s.Stream.Close()
}

// Abort resets the server's end of the stream too. It writes the RESET before the
// relay closes the stream, so it reaches the server ahead of the stream's FIN; a
// carrier whose writes are stuck holds the relay until the pool closes it.
func (s *carrierStream) Abort() {
	s.c.conn.WriteControl(proxy.CtrlReset, proxy.ResetBody(s.ID(), proxy.ResetAborted))
}

func (c *carrier) acknowledged(s *carrierStream, rtt time.Duration) {
	c.mu.Lock()
	delete(c.waiting, s)
	c.mu.Unlock()
	c.sample(rtt)
}

func (c *carrier) sample(rtt time.Duration) {
	c.latest.Store(int64(rtt))
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.srtt == 0 {
		c.srtt, c.rttvar = rtt, rtt/2
		return
	}
	diff := c.srtt - rtt
	if diff < 0 {
		diff = -diff
	}
	c.rttvar += (diff - c.rttvar) / 4
	c.srtt += (rtt - c.srtt) / 8
}
