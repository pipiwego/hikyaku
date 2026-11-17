package main

import (
	"errors"
	"flag"
	"log"
	"net"
	"strings"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:1080", "SOCKS5 listen address")
	var serverFlags []string
	flag.Func("server", "server address (host:port); repeat it or separate addresses with commas", func(value string) error {
		serverFlags = append(serverFlags, value)
		return nil
	})
	psk := flag.String("psk", "", "pre-shared key, any string")
	carriers := flag.Int("carriers", 4, "maximum number of carriers across all servers")
	flag.Parse()

	servers, err := parseServers(serverFlags)
	if err != nil {
		log.Fatal(err)
	}
	if *psk == "" {
		log.Fatal("-psk is required")
	}
	if *carriers < 1 {
		log.Fatal("-carriers must be at least 1")
	}
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("SOCKS5 listening on %s", ln.Addr())
	p := newPool(servers, []byte(*psk), *carriers)
	p.warm()
	h := &inbound{pool: p}
	log.Fatal(h.serve(ln))
}

// parseServers splits repeated and comma-separated -server values, trims them,
// drops empty ones and removes duplicates while keeping the first-seen order.
func parseServers(values []string) ([]string, error) {
	var servers []string
	seen := make(map[string]struct{})
	for _, value := range values {
		for _, field := range strings.Split(value, ",") {
			addr := strings.TrimSpace(field)
			if addr == "" {
				continue
			}
			if _, ok := seen[addr]; ok {
				continue
			}
			seen[addr] = struct{}{}
			servers = append(servers, addr)
		}
	}
	if len(servers) == 0 {
		return nil, errors.New("-server is required")
	}
	return servers, nil
}
