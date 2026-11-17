package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SukkaW/hikyaku/proxy"
	"github.com/xtaci/smux"
)

func TestPoolKeepsSpare(t *testing.T) {
	psk := "spare-carrier-test"
	p := newPool([]string{startServer(t, psk)}, []byte(psk), 4)

	// The first stream rides the first carrier while it is still unacknowledged.
	_, first, err := p.openStream()
	if err != nil {
		t.Fatal(err)
	}
	first.Close()
	waitFor(t, "first carrier acknowledged", func() bool { return p.acked() >= 1 })

	_, second, err := p.openStream()
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	waitFor(t, "an idle spare beside the busy carrier", func() bool { return p.acked() >= 2 })
	p.mu.Lock()
	opened := p.opened
	p.mu.Unlock()
	if opened != 2 {
		t.Fatalf("opened %d slots, want 2: one busy carrier and one spare", opened)
	}
}

func TestQuickestSkipsGoingAway(t *testing.T) {
	psk := "goaway-pick-test"
	p := newPool([]string{startServer(t, psk)}, []byte(psk), 4)
	_, first, err := p.openStream()
	if err != nil {
		t.Fatal(err)
	}
	first.Close()
	waitFor(t, "first carrier acknowledged", func() bool { return p.acked() >= 1 })
	_, second, err := p.openStream()
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	waitFor(t, "two acknowledged carriers", func() bool { return p.acked() >= 2 })
	p.mu.Lock()
	carriers := append([]*carrier(nil), p.carriers...)
	p.mu.Unlock()
	// Between GOAWAY and the next health check the carrier is still in its slot; mark
	// the one that would be picked.
	going := quickest(carriers, p.waitBand)
	if going == nil {
		t.Fatal("no carrier picked")
	}
	reason := "test"
	going.goaway.Store(&reason)
	for range 20 {
		c := quickest(carriers, p.waitBand)
		if c == nil {
			t.Fatal("picked no carrier, want the one whose server is not going away")
		}
		if c.goaway.Load() != nil {
			t.Fatalf("picked carrier %d, whose server is going away", c.slot)
		}
	}
}

// A carrier whose server is going away takes no new stream even when nothing else is
// left, such as before its acknowledgement or before watch moves it to standby.
func TestPickSkipsGoingAway(t *testing.T) {
	psk := "goaway-fallback-test"
	p := newPool([]string{startServer(t, psk)}, []byte(psk), 1)
	p.opened = 1 // the test supplies the carrier itself, so no health check moves it
	c, err := p.dial(p.servers[0])
	if err != nil {
		t.Fatal(err)
	}
	defer c.session.Close()
	p.set(0, c)
	reason := "test"
	c.goaway.Store(&reason)
	if got := p.pick(); got != nil {
		t.Fatalf("picked carrier %d, whose server is going away", got.slot)
	}
}

func TestPoolWarm(t *testing.T) {
	psk := "warm-pool-test"
	p := newPool([]string{startServer(t, psk)}, []byte(psk), 4)
	p.warm()
	waitFor(t, "the first carrier acknowledged without any request", func() bool { return p.acked() == 1 })
	p.mu.Lock()
	opened := p.opened
	p.mu.Unlock()
	if opened != 1 {
		t.Fatalf("opened %d slots, want 1", opened)
	}
}

// Carriers are spread round-robin across the configured servers, not piled onto the
// first one.
func TestPoolSpreadsCarriersAcrossServers(t *testing.T) {
	psk := "multi-server-spread-test"
	a := fakeServer(t, psk, func(*proxy.Conn, *smux.Session) {})
	b := fakeServer(t, psk, func(*proxy.Conn, *smux.Session) {})
	p := newPool([]string{a, b}, []byte(psk), 2)
	p.warm()
	waitFor(t, "the first carrier acknowledged", func() bool { return p.acked() == 1 })
	if _, s, err := p.openStream(); err != nil {
		t.Fatal(err)
	} else {
		s.Close()
	}
	waitFor(t, "both carriers acknowledged", func() bool { return p.acked() == 2 })
	p.mu.Lock()
	servers := make(map[string]int)
	for _, c := range p.carriers {
		if c != nil {
			servers[c.server]++
		}
	}
	p.mu.Unlock()
	if servers[a] != 1 || servers[b] != 1 {
		t.Fatalf("carriers on %v, want one on each of %s and %s", servers, a, b)
	}
}

// A dead server is backed off without blocking a healthy one: the first carrier
// still comes up on the healthy server.
func TestPoolFailsOverToHealthyServer(t *testing.T) {
	psk := "multi-server-failover-test"
	dead, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadAddr := dead.Addr().String()
	dead.Close()
	healthy := fakeServer(t, psk, func(*proxy.Conn, *smux.Session) {})

	p := newPool([]string{deadAddr, healthy}, []byte(psk), 1)
	p.warm()
	waitFor(t, "a carrier on the healthy server", func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		c := p.carriers[0]
		return c != nil && c.server == healthy && c.conn.Acked()
	})

	p.mu.Lock()
	failures := p.states[0].failures
	p.mu.Unlock()
	if failures == 0 {
		t.Fatal("the dead server recorded no failure")
	}
}

// A carrier that fails to open a stream after it was picked hands the request to the
// next usable one instead of failing it.
func TestOpenStreamRetriesAnotherCarrier(t *testing.T) {
	psk := "open-retry-test"
	silent, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { silent.Close() })
	go func() {
		for {
			conn, err := silent.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { conn.Close() })
		}
	}()
	broken, err := newPool([]string{silent.Addr().String()}, []byte(psk), 1).dial(silent.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer broken.session.Close()
	// Its writes now fail while its reads, and so the carrier, stay up.
	broken.conn.Conn.(*net.TCPConn).CloseWrite()

	p := newPool([]string{startServer(t, psk)}, []byte(psk), 2)
	p.opened = 2 // the test supplies the carriers itself
	p.set(0, broken)
	type opened struct {
		c   *carrier
		err error
	}
	done := make(chan opened, 1)
	go func() {
		c, s, err := p.openStream()
		if err == nil {
			s.Close()
		}
		done <- opened{c, err}
	}()
	good, err := p.dial(p.servers[0])
	if err != nil {
		t.Fatal(err)
	}
	defer good.session.Close()
	good.slot = 1
	waitFor(t, "the good carrier acknowledged", good.conn.Acked)
	time.Sleep(100 * time.Millisecond) // so the broken carrier is tried first
	p.set(1, good)
	select {
	case o := <-done:
		if o.err != nil {
			t.Fatalf("openStream failed with %v, want the stream on the good carrier", o.err)
		}
		if o.c != good {
			t.Fatalf("stream opened on carrier %d, want the good one", o.c.slot)
		}
	case <-time.After(carrierWait):
		t.Fatal("openStream never returned")
	}
}

// A burst that arrives while the first carrier is still connecting, as when Surge
// starts the client and sends requests at once, waits for it instead of failing.
func TestPoolWarmBurst(t *testing.T) {
	psk := "warm-burst-test"
	p := newPool([]string{startServer(t, psk)}, []byte(psk), 4)
	p.warm()
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, s, err := p.openStream()
			if err != nil {
				t.Error(err)
				return
			}
			s.Close()
		}()
	}
	wg.Wait()
}

// Streams opened at once on one carrier reach the server in stream id order, which
// the server's handling of a RESET that overtakes its stream's SYN relies on.
func TestPoolWakesWaiters(t *testing.T) {
	psk := "wake-waiters-test"
	p := newPool([]string{startServer(t, psk)}, []byte(psk), 1)
	p.opened = 1 // the test supplies the carrier itself, so it decides when one appears
	done := make(chan error, 1)
	go func() {
		_, s, err := p.openStream()
		if err == nil {
			s.Close()
		}
		done <- err
	}()
	time.Sleep(100 * time.Millisecond)
	c, err := p.dial(p.servers[0])
	if err != nil {
		t.Fatal(err)
	}
	defer c.session.Close()
	appeared := time.Now()
	p.set(0, c)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
		if waited := time.Since(appeared); waited >= healthCheck/2 {
			t.Fatalf("waiter woke %s after the carrier appeared, so it relied on the %s fallback", waited, healthCheck)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter never woke")
	}
}

func (p *pool) acked() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, c := range p.carriers {
		if c != nil && c.conn.Acked() {
			n++
		}
	}
	return n
}

func TestPingInterval(t *testing.T) {
	cases := []struct {
		name                   string
		sinceData, sinceStream time.Duration
		streams                int
		want                   time.Duration
	}{
		{"data flowing", 0, time.Hour, 1, busyPing},
		{"quiet with a stream open", time.Second, time.Hour, 1, warmPing},
		{"quiet soon after a stream closed", time.Second, 10 * time.Second, 0, warmPing},
		{"idle", time.Minute, time.Minute, 0, coldPing},
	}
	for _, c := range cases {
		if got := pingInterval(c.sinceData, c.sinceStream, c.streams); got != c.want {
			t.Errorf("%s: %s, want %s", c.name, got, c.want)
		}
	}
}

// A write to a stream the server reset fails as a reset, so the relay ends the app's
// connection without sending a RESET back.
func TestWriteAfterResetIsReset(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	client, err := smux.Client(clientConn, proxy.MuxConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	server, err := smux.Server(serverConn, proxy.MuxConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	st, err := client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	go server.AcceptStream()
	c := &carrier{}
	s := c.newStream(st)
	c.control(proxy.CtrlReset, proxy.ResetBody(st.ID(), proxy.ResetAborted))
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := s.Write([]byte("late"))
		if errors.Is(err, proxy.ErrReset) {
			return
		}
		if err != nil || time.Now().After(deadline) {
			t.Fatalf("write after RESET: %v, want proxy.ErrReset", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestChoose(t *testing.T) {
	ms := time.Millisecond
	cases := []struct {
		name    string
		band    time.Duration
		waits   []time.Duration
		streams []int
		want    int
	}{
		// One server keeps the 20ms band, so small round-trip differences on its
		// shared path count as tied.
		{"single server: a clearly quicker carrier beats fewer streams", minWaitBand, []time.Duration{600 * ms, 1200 * ms}, []int{3, 0}, 0},
		{"single server: within the band, fewest streams wins", minWaitBand, []time.Duration{600 * ms, 700 * ms}, []int{3, 0}, 1},
		{"single server: the band is at least minWaitBand", minWaitBand, []time.Duration{30 * ms, 45 * ms}, []int{2, 0}, 1},
		{"single server: just outside the band", minWaitBand, []time.Duration{30 * ms, 60 * ms}, []int{2, 0}, 0},
		// Across servers the band is 5ms, so a few milliseconds pick the quicker route.
		{"multiple servers: within the band, fewest streams wins", multiServerWaitBand, []time.Duration{30 * ms, 32 * ms}, []int{2, 0}, 1},
		{"multiple servers: 18ms does not lose to 25ms", multiServerWaitBand, []time.Duration{18 * ms, 25 * ms}, []int{1, 0}, 0},
		{"an unknown wait counts as tied", minWaitBand, []time.Duration{-1, 600 * ms}, []int{0, 2}, 0},
		{"all unknown falls back to fewest streams", minWaitBand, []time.Duration{-1, -1}, []int{1, 0}, 1},
		{"equal carriers keep slot order", minWaitBand, []time.Duration{600 * ms, 600 * ms}, []int{1, 1}, 0},
	}
	for _, c := range cases {
		if got := choose(c.waits, c.streams, c.band); got != c.want {
			t.Errorf("%s: chose %d, want %d", c.name, got, c.want)
		}
	}
}

// A server that goes away makes every carrier fail in the same round; those failures
// share one backoff window instead of multiplying the wait by the carrier count.
func TestDialFailedBacksOffOncePerWindow(t *testing.T) {
	p := newPool([]string{"[2001:db8::a]:8444"}, nil, 4)
	server := p.servers[0]
	for range 4 {
		p.dialFailed(server)
	}
	p.mu.Lock()
	failures := p.states[0].failures
	wait := time.Until(p.states[0].nextTry)
	p.mu.Unlock()
	if failures != 1 {
		t.Fatalf("%d failures after four dials in one round, want 1", failures)
	}
	if wait < 900*time.Millisecond || wait > 1100*time.Millisecond {
		t.Fatalf("next try in %s, want about %s", wait, minBackoff)
	}

	p.mu.Lock()
	p.states[0].nextTry = time.Now().Add(-time.Second)
	p.mu.Unlock()
	p.dialFailed(server)
	p.mu.Lock()
	failures = p.states[0].failures
	wait = time.Until(p.states[0].nextTry)
	p.mu.Unlock()
	if failures != 2 {
		t.Fatalf("%d failures after the window passed, want 2", failures)
	}
	if wait < 1800*time.Millisecond || wait > 2200*time.Millisecond {
		t.Fatalf("next try in %s, want about %s", wait, 2*minBackoff)
	}
}

// Servers are tried round-robin, and when all of them are backing off the earliest
// one is returned with the time left to wait for it.
func TestNextServerRotatesAndWaits(t *testing.T) {
	p := newPool([]string{"[2001:db8::a]:8444", "[2001:db8::b]:8444"}, nil, 1)
	now := time.Now()
	if got, wait := p.nextServer(now); got != p.servers[0] || wait != 0 {
		t.Fatalf("first server %q after %s, want %q at once", got, wait, p.servers[0])
	}
	if got, wait := p.nextServer(now); got != p.servers[1] || wait != 0 {
		t.Fatalf("second server %q after %s, want %q at once", got, wait, p.servers[1])
	}
	p.dialFailed(p.servers[0])
	// Let the second failure of the first server count, so the two backoffs differ.
	p.mu.Lock()
	p.states[0].nextTry = time.Now().Add(-time.Second)
	p.mu.Unlock()
	p.dialFailed(p.servers[0])
	p.dialFailed(p.servers[1])
	got, wait := p.nextServer(time.Now())
	if got != p.servers[1] {
		t.Fatalf("waited for %q, want the server available soonest, %q", got, p.servers[1])
	}
	if wait <= 0 || wait > time.Second {
		t.Fatalf("wait %s, want the remaining part of the first backoff, up to %s", wait, time.Second)
	}
	p.dialSucceeded(p.servers[1])
	if got, wait := p.nextServer(time.Now()); got != p.servers[1] || wait != 0 {
		t.Fatalf("after the backoff reset: %q after %s, want %q at once", got, wait, p.servers[1])
	}
}

// quickest compares carriers from different servers on their round trips, not on
// which server they came from.
func TestQuickestAcrossServers(t *testing.T) {
	psk := "multi-server-quickest-test"
	a := fakeServer(t, psk, func(*proxy.Conn, *smux.Session) {})
	b := fakeServer(t, psk, func(*proxy.Conn, *smux.Session) {})
	p := newPool([]string{a, b}, []byte(psk), 2)
	slow, err := p.dial(a)
	if err != nil {
		t.Fatal(err)
	}
	defer slow.session.Close()
	fast, err := p.dial(b)
	if err != nil {
		t.Fatal(err)
	}
	defer fast.session.Close()
	waitFor(t, "both carriers acknowledged", func() bool { return slow.conn.Acked() && fast.conn.Acked() })
	slow.slot, fast.slot = 0, 1
	slow.sample(20 * time.Millisecond)
	fast.sample(5 * time.Millisecond)
	// More streams must not hide the round-trip difference between servers.
	for range 2 {
		if _, err := fast.session.OpenStream(); err != nil {
			t.Fatal(err)
		}
	}
	if got := quickest([]*carrier{slow, fast}, multiServerWaitBand); got != fast {
		t.Fatalf("multi-server pool picked carrier on %s with %d streams, want the faster %s", got.server, got.session.NumStreams(), fast.server)
	}
	// The 20ms single-server band ties 20ms with 5ms, so there the fewer streams win.
	if got := quickest([]*carrier{slow, fast}, minWaitBand); got != slow {
		t.Fatalf("single-server pool picked carrier on %s with %d streams, want the one with fewer streams on %s", got.server, got.session.NumStreams(), slow.server)
	}
}

// Slowness is judged within one server. A healthy but slower server is chosen against
// by quickest, not retired and redialed as a slow carrier.
func TestSlowerOnlyComparesWithinServer(t *testing.T) {
	psk := "slower-within-server-test"
	a := fakeServer(t, psk, func(*proxy.Conn, *smux.Session) {})
	b := fakeServer(t, psk, func(*proxy.Conn, *smux.Session) {})
	p := newPool([]string{a, b}, []byte(psk), 2)
	slow, err := p.dial(a)
	if err != nil {
		t.Fatal(err)
	}
	defer slow.session.Close()
	fast, err := p.dial(b)
	if err != nil {
		t.Fatal(err)
	}
	defer fast.session.Close()
	waitFor(t, "both carriers acknowledged", func() bool { return slow.conn.Acked() && fast.conn.Acked() })
	slow.slot, fast.slot = 0, 1
	p.set(0, slow)
	p.set(1, fast)
	now := time.Now()
	slow.quiet = []sample{{now, 900 * time.Millisecond}, {now, 900 * time.Millisecond}, {now, 900 * time.Millisecond}}
	fast.quiet = []sample{{now, 30 * time.Millisecond}, {now, 30 * time.Millisecond}, {now, 30 * time.Millisecond}}
	if p.slower(slow) {
		t.Fatalf("the only carrier on %s was judged slow against another server", slow.server)
	}

	sameServer, err := p.dial(a)
	if err != nil {
		t.Fatal(err)
	}
	defer sameServer.session.Close()
	waitFor(t, "the second carrier on the same server acknowledged", sameServer.conn.Acked)
	sameServer.slot = 1
	sameServer.quiet = fast.quiet
	p.set(1, sameServer)
	if !p.slower(slow) {
		t.Fatalf("carrier on %s was not judged slow against a quicker carrier on its own server", slow.server)
	}
}

// A pool needs a server; the CLI checks this, but library and test callers must not
// get an empty round-robin that spins at full speed.
func TestNewPoolNeedsServer(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("newPool with no servers did not panic")
		}
	}()
	newPool(nil, nil, 1)
}

// The wait band depends on how many servers the pool may use.
func TestNewPoolWaitBand(t *testing.T) {
	if got := newPool([]string{"[2001:db8::a]:8444"}, nil, 1).waitBand; got != minWaitBand {
		t.Fatalf("single-server wait band %s, want %s", got, minWaitBand)
	}
	if got := newPool([]string{"[2001:db8::a]:8444", "[2001:db8::b]:8444"}, nil, 1).waitBand; got != multiServerWaitBand {
		t.Fatalf("multi-server wait band %s, want %s", got, multiServerWaitBand)
	}
}

func TestExpectedWait(t *testing.T) {
	now := time.Now()
	c := &carrier{pings: make(map[uint32]ping), waiting: make(map[*carrierStream]time.Time)}
	if got := c.expectedWait(now); got >= 0 {
		t.Fatalf("no samples and nothing outstanding: %s, want unknown", got)
	}
	c.sample(600 * time.Millisecond)
	if got := c.expectedWait(now); got != 600*time.Millisecond {
		t.Fatalf("latest round trip only: %s, want 600ms", got)
	}
	c.pings[1] = ping{sent: now.Add(-2 * time.Second)}
	if got := c.expectedWait(now); got != 2*time.Second {
		t.Fatalf("with a ping unanswered for 2s: %s, want 2s", got)
	}
	delete(c.pings, 1)
	c.waiting[&carrierStream{}] = now.Add(-time.Second)
	if got := c.expectedWait(now); got != time.Second {
		t.Fatalf("with an acknowledgement 1s overdue: %s, want 1s", got)
	}
}

func TestQuietSamples(t *testing.T) {
	c := &carrier{pings: make(map[uint32]ping)}
	pong := func(id uint32) { c.control(proxy.CtrlPong, binary.BigEndian.AppendUint32(nil, id)) }
	c.pings[1] = ping{sent: time.Now(), quiet: true}
	c.pings[2] = ping{sent: time.Now(), quiet: false}
	pong(1)
	pong(2)
	if len(c.quiet) != 1 {
		t.Fatalf("%d quiet samples, want only the quiet ping's", len(c.quiet))
	}

	now := time.Now()
	ms := time.Millisecond
	c.quiet = []sample{{now.Add(-quietWindow - time.Second), 100 * ms}, {now, 900 * ms}, {now, 600 * ms}}
	if _, ok := c.quietMedian(now); ok {
		t.Fatal("judged from 2 recent samples; one is older than the window")
	}
	c.quiet = append(c.quiet, sample{now, 700 * ms})
	if got, ok := c.quietMedian(now); !ok || got != 700*ms {
		t.Fatalf("median %s, %v; want 700ms from the 3 recent samples", got, ok)
	}
}

func TestSlowerThan(t *testing.T) {
	ms := time.Millisecond
	cases := []struct {
		name       string
		mine, best time.Duration
		want       bool
	}{
		{"GEO, under half the best", 900 * ms, 620 * ms, false},
		{"GEO, over half the best", 1000 * ms, 620 * ms, true},
		{"Starlink, under the 200 ms floor", 220 * ms, 30 * ms, false},
		{"Starlink, over the 200 ms floor", 240 * ms, 30 * ms, true},
	}
	for _, c := range cases {
		if got := slowerThan(c.mine, c.best); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

func TestReplyDeadline(t *testing.T) {
	ms := time.Millisecond
	c := &carrier{}
	if got := c.replyDeadline(); got != maxReplyDeadline {
		t.Fatalf("before any round trip: %s, want %s", got, maxReplyDeadline)
	}
	c.sample(620 * ms)
	if got := c.replyDeadline(); got != maxReplyDeadline {
		t.Fatalf("after one 620ms round trip: %s, want the %s cap, which 5 round trips pass", got, maxReplyDeadline)
	}
	mid := &carrier{}
	mid.sample(300 * ms)
	if got := mid.replyDeadline(); got != 1500*ms {
		t.Fatalf("after one 300ms round trip: %s, want 5 round trips, above 300ms + 4 x 150ms", got)
	}
	steady := &carrier{}
	for range 50 {
		steady.sample(30 * ms)
	}
	if got := steady.replyDeadline(); got != minReplyDeadline {
		t.Fatalf("steady 30ms round trips: %s, want the %s floor", got, minReplyDeadline)
	}
	jumpy := &carrier{}
	for i := range 50 {
		jumpy.sample(time.Duration(500+2000*(i%2)) * ms)
	}
	if got := jumpy.replyDeadline(); got != maxReplyDeadline {
		t.Fatalf("round trips swinging 500-2500ms: %s, want the %s cap", got, maxReplyDeadline)
	}
}

// fakeServer accepts carriers and runs smux on them like the real server, but answers
// no pings; onCarrier runs for each one.
func fakeServer(t *testing.T, psk string, onCarrier func(*proxy.Conn, *smux.Session)) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { conn.Close() })
			go func() {
				c, err := proxy.ServerHandshake(conn, []byte(psk), proxy.NewReplayFilter())
				if err != nil {
					return
				}
				session, err := smux.Server(c, proxy.MuxConfig())
				if err != nil {
					return
				}
				onCarrier(c, session)
			}()
		}
	}()
	return ln.Addr().String()
}

func (p *pool) standing() []*carrier {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]*carrier(nil), p.standby...)
}

func TestUnansweredPingStandsBy(t *testing.T) {
	psk := "unanswered-ping-test"
	p := newPool([]string{fakeServer(t, psk, func(*proxy.Conn, *smux.Session) {})}, []byte(psk), 1)
	p.warm()
	waitFor(t, "the carrier acknowledged", func() bool { return p.acked() == 1 })
	deadline := time.Now().Add(healthCheck + maxReplyDeadline + 3*time.Second)
	for len(p.standing()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("a carrier whose pings go unanswered kept its slot")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestDataKeepsCarrierAlive(t *testing.T) {
	psk := "data-keeps-alive-test"
	stop := make(chan struct{})
	p := newPool([]string{fakeServer(t, psk, func(_ *proxy.Conn, s *smux.Session) {
		st, err := s.AcceptStream()
		if err != nil {
			return
		}
		tick := time.NewTicker(200 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				if _, err := st.Write(make([]byte, 64)); err != nil {
					return
				}
			}
		}
	})}, []byte(psk), 1)
	p.warm()
	waitFor(t, "the carrier acknowledged", func() bool { return p.acked() == 1 })
	// Opened through the pool, so its acknowledgement is awaited as well.
	if _, _, err := p.openStream(); err != nil {
		t.Fatal(err)
	}
	// The server answers no pings and acknowledges no stream but keeps sending data,
	// which proves it alive.
	time.Sleep(maxReplyDeadline + 2*healthCheck + time.Second)
	if len(p.standing()) != 0 {
		t.Fatal("a carrier still delivering data stood by because its pings or acknowledgement went unanswered")
	}
	close(stop)
	deadline := time.Now().Add(maxReplyDeadline + 3*time.Second)
	for len(p.standing()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the carrier kept its slot after its data stopped as well")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// pingServer answers pings like the real server, except the ones drop picks, which it
// discards as the real server does when its pong queue is full.
func pingServer(t *testing.T, psk string, drop func(id uint32) bool) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { conn.Close() })
			go func() {
				c, err := proxy.ServerHandshake(conn, []byte(psk), proxy.NewReplayFilter())
				if err != nil {
					return
				}
				pongs := make(chan []byte, 64)
				c.OnControl(func(cmd byte, body []byte) {
					if cmd == proxy.CtrlPing && !drop(binary.BigEndian.Uint32(body)) {
						pongs <- bytes.Clone(body)
					}
				})
				if _, err := smux.Server(c, proxy.MuxConfig()); err != nil {
					return
				}
				for id := range pongs {
					if c.WriteControl(proxy.CtrlPong, id) != nil {
						return
					}
				}
			}()
		}
	}()
	return ln.Addr().String()
}

func TestDroppedPingsKeepCarrier(t *testing.T) {
	psk := "dropped-pings-test"
	p := newPool([]string{pingServer(t, psk, func(id uint32) bool { return id < 3 })}, []byte(psk), 1)
	p.warm()
	waitFor(t, "the carrier acknowledged", func() bool { return p.acked() == 1 })
	p.mu.Lock()
	c := p.carriers[0]
	p.mu.Unlock()
	// An open stream keeps the carrier warm, so it is pinged every 0.5 s.
	if _, err := c.session.OpenStream(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(maxReplyDeadline + 2*healthCheck + time.Second)
	if n := len(p.standing()); n != 0 {
		t.Fatalf("%d carriers on standby; the server dropped three pings but answers the rest", n)
	}
	c.mu.Lock()
	outstanding := len(c.pings)
	c.mu.Unlock()
	if outstanding > 1 {
		t.Fatalf("%d pings still outstanding, want the dropped ones forgotten", outstanding)
	}
}

func TestGoAwayStandsBy(t *testing.T) {
	psk := "goaway-test"
	var carriers atomic.Int32
	p := newPool([]string{fakeServer(t, psk, func(c *proxy.Conn, _ *smux.Session) {
		carriers.Add(1)
		c.WriteControl(proxy.CtrlGoAway, []byte{proxy.GoAwayShutdown, 4, 't', 'e', 's', 't'})
	})}, []byte(psk), 1)
	p.warm()
	waitFor(t, "the carrier to stand by", func() bool { return len(p.standing()) > 0 })
	if reason := p.standing()[0].goaway.Load(); reason == nil || *reason != "test" {
		t.Fatalf("standby carrier's GOAWAY reason %v, want the server's", reason)
	}
	// The successor of a server that is shutting down may already be listening, so the
	// slot redials at once instead of after a back-off.
	deadline := time.Now().Add(minBackoff / 2)
	for carriers.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("no redial within %s of GOAWAY, want one at once", minBackoff/2)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A server that closes every carrier before acknowledging it, as for a client with the
// wrong PSK, gets backed-off redials instead of a dial every health check.
func TestRejectedCarriersBackOff(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	var accepted atomic.Int32
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			conn.Close()
		}
	}()
	p := newPool([]string{ln.Addr().String()}, []byte("wrong-psk"), 1)
	p.warm()
	time.Sleep(5 * time.Second)
	// Backing off 1, 2 and 4 s after each rejection allows about three dials in 5 s.
	if n := accepted.Load(); n > 4 {
		t.Fatalf("%d carriers dialed in 5 s to a server that rejects them all, want at most 4", n)
	}
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
