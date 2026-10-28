package raftnode

import (
	"context"
	"errors"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

// ErrPartitioned is returned by Dial for a blocked peer.
var ErrPartitioned = errors.New("raftnode: peer is partitioned by chaos layer")

// ChaosLayer is a raft.StreamLayer that can refuse to talk to selected peers.
// Blocking a peer closes its live connections, refuses new dials to it and
// drops inbound connections from its IP. The two tests that need partitions
// (minority refuses writes, Porcupine nemesis) apply blocks on every node of
// a cut, so inbound filtering is only a second line of defence for the
// one-sided demo; it assumes one IP per node, which holds in Docker and in the
// in-process harness (which binds distinct loopback addresses).
type ChaosLayer struct {
	ln        net.Listener
	advertise net.Addr

	mu      sync.Mutex
	blocked map[raft.ServerAddress]string // address -> resolved IP ("" if unresolved)
	conns   map[*trackedConn]struct{}
	frozen  chan struct{} // non-nil while frozen; closed to thaw
}

var _ raft.StreamLayer = (*ChaosLayer)(nil)

type advertiseAddr string

func (a advertiseAddr) Network() string { return "tcp" }
func (a advertiseAddr) String() string  { return string(a) }

// NewChaosLayer wraps ln. advertise is the address peers use to reach this
// node and is reported as the transport's local address.
func NewChaosLayer(ln net.Listener, advertise string) *ChaosLayer {
	return &ChaosLayer{
		ln:        ln,
		advertise: advertiseAddr(advertise),
		blocked:   make(map[raft.ServerAddress]string),
		conns:     make(map[*trackedConn]struct{}),
	}
}

// Dial opens an outbound raft connection unless the peer is blocked.
func (c *ChaosLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	c.mu.Lock()
	_, blocked := c.blocked[address]
	c.mu.Unlock()
	if blocked {
		return nil, ErrPartitioned
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", string(address))
	if err != nil {
		return nil, err
	}
	return c.track(conn, address), nil
}

// Accept returns the next inbound connection, dropping those from blocked IPs.
func (c *ChaosLayer) Accept() (net.Conn, error) {
	for {
		conn, err := c.ln.Accept()
		if err != nil {
			return nil, err
		}
		if c.ipBlocked(remoteIP(conn)) {
			_ = conn.Close()
			continue
		}
		return c.track(conn, ""), nil
	}
}

// Close stops accepting connections.
func (c *ChaosLayer) Close() error { return c.ln.Close() }

// Addr reports the advertised address so raft announces a dialable leader.
func (c *ChaosLayer) Addr() net.Addr { return c.advertise }

// Block cuts the link to the given peers.
func (c *ChaosLayer) Block(addrs ...raft.ServerAddress) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, a := range addrs {
		c.blocked[a] = resolveIP(string(a))
	}
	for tc := range c.conns {
		if _, hit := c.blocked[tc.peer]; hit || (tc.ip != "" && c.ipBlockedLocked(tc.ip)) {
			_ = tc.Conn.Close()
			delete(c.conns, tc)
		}
	}
}

// Unblock restores the link to the given peers.
func (c *ChaosLayer) Unblock(addrs ...raft.ServerAddress) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, a := range addrs {
		delete(c.blocked, a)
	}
}

// Clear removes every block.
func (c *ChaosLayer) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.blocked = make(map[raft.ServerAddress]string)
}

// Freeze stalls every read and write on every raft connection until Thaw.
func (c *ChaosLayer) Freeze() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.frozen == nil {
		c.frozen = make(chan struct{})
	}
}

// Thaw releases a Freeze.
func (c *ChaosLayer) Thaw() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.frozen != nil {
		close(c.frozen)
		c.frozen = nil
	}
}

// Frozen reports whether traffic is stalled.
func (c *ChaosLayer) Frozen() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.frozen != nil
}

func (c *ChaosLayer) frozenCh() chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.frozen
}

// Blocked lists blocked peer addresses in a stable order.
func (c *ChaosLayer) Blocked() []raft.ServerAddress {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]raft.ServerAddress, 0, len(c.blocked))
	for a := range c.blocked {
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func (c *ChaosLayer) ipBlocked(ip string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ipBlockedLocked(ip)
}

func (c *ChaosLayer) ipBlockedLocked(ip string) bool {
	if ip == "" {
		return false
	}
	for _, blockedIP := range c.blocked {
		if blockedIP == ip {
			return true
		}
	}
	return false
}

func (c *ChaosLayer) track(conn net.Conn, peer raft.ServerAddress) *trackedConn {
	tc := &trackedConn{Conn: conn, layer: c, peer: peer, ip: remoteIP(conn)}
	c.mu.Lock()
	c.conns[tc] = struct{}{}
	c.mu.Unlock()
	return tc
}

type trackedConn struct {
	net.Conn
	layer *ChaosLayer
	peer  raft.ServerAddress // set for outbound connections
	ip    string
	once  sync.Once
}

func (t *trackedConn) Read(b []byte) (int, error) {
	if ch := t.layer.frozenCh(); ch != nil {
		<-ch
	}
	return t.Conn.Read(b)
}

func (t *trackedConn) Write(b []byte) (int, error) {
	if ch := t.layer.frozenCh(); ch != nil {
		<-ch
	}
	return t.Conn.Write(b)
}

func (t *trackedConn) Close() error {
	t.once.Do(func() {
		t.layer.mu.Lock()
		delete(t.layer.conns, t)
		t.layer.mu.Unlock()
	})
	return t.Conn.Close()
}

func remoteIP(conn net.Conn) string {
	host, _, err := net.SplitHostPort(conn.RemoteAddr().String())
	if err != nil {
		return ""
	}
	return host
}

func resolveIP(address string) string {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return ""
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.String()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil || len(ips) == 0 {
		return ""
	}
	return ips[0].IP.String()
}
