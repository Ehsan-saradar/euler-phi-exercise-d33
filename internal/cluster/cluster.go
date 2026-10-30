// Package cluster starts N real nodes inside one test process. Each node binds
// its own loopback address (127.0.1.N) so the chaos layer can tell peers apart
// by IP exactly as it does in Docker.
package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	kvv1 "github.com/Ehsan-saradar/euler-phi-exercise-d33/gen/kv/v1"
	"github.com/Ehsan-saradar/euler-phi-exercise-d33/internal/api"
	"github.com/Ehsan-saradar/euler-phi-exercise-d33/internal/raftnode"
	"github.com/Ehsan-saradar/euler-phi-exercise-d33/internal/server"
)

// Options tunes a test cluster.
type Options struct {
	Nodes             int
	InMemory          bool
	SnapshotThreshold uint64
	TrailingLogs      uint64
	ReadMode          raftnode.ReadMode
	LeaderWait        time.Duration
	LogLevel          slog.Level
}

// Member is one node plus its client connection.
type Member struct {
	ID      string
	Addr    string
	DataDir string

	c      *Cluster
	srv    *server.Server
	conn   *grpc.ClientConn
	peers  map[string]string
	joined bool
}

// Cluster is a set of in-process nodes.
type Cluster struct {
	t       testing.TB
	opts    Options
	Members []*Member
	logger  *slog.Logger
}

// New starts opts.Nodes nodes with a static bootstrap configuration and waits
// for a leader.
func New(t testing.TB, opts Options) *Cluster {
	t.Helper()
	if opts.Nodes == 0 {
		opts.Nodes = 3
	}
	if opts.LeaderWait == 0 {
		opts.LeaderWait = 500 * time.Millisecond
	}
	c := &Cluster{t: t, opts: opts, logger: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: opts.LogLevel}))}

	peers := make(map[string]string, opts.Nodes)
	listeners := make([]net.Listener, opts.Nodes)
	for i := range opts.Nodes {
		ln, err := listenAlias(i)
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = ln
		peers[nodeID(i)] = ln.Addr().String()
	}
	for i := range opts.Nodes {
		m := &Member{ID: nodeID(i), Addr: listeners[i].Addr().String(), c: c, peers: peers}
		if !opts.InMemory {
			m.DataDir = filepath.Join(t.TempDir(), m.ID)
		}
		m.start(listeners[i])
		c.Members = append(c.Members, m)
	}
	t.Cleanup(c.Close)
	c.Leader(5 * time.Second)
	return c
}

func nodeID(i int) string { return fmt.Sprintf("node%d", i+1) }

func listen(addr string) (net.Listener, error) {
	return (&net.ListenConfig{}).Listen(context.Background(), "tcp", addr)
}

// listenAlias binds node i to 127.0.1.(i+1). Linux routes all of 127/8 to the
// loopback interface; macOS only has 127.0.0.1 until aliases are added.
func listenAlias(i int) (net.Listener, error) {
	ln, err := listen(fmt.Sprintf("127.0.1.%d:0", i+1))
	if err != nil {
		return nil, fmt.Errorf("bind loopback alias: %w (on macOS run `make mac-aliases` once, or use `make test-docker`)", err)
	}
	return ln, nil
}

func (m *Member) config(ln net.Listener) server.Config {
	o := m.c.opts
	return server.Config{
		ID:        m.ID,
		Listener:  ln,
		Advertise: m.Addr,
		Peers:     m.peers,
		Bootstrap: !m.joined,
		DataDir:   m.DataDir,
		Raft: raftnode.Config{
			HeartbeatTimeout:   50 * time.Millisecond,
			ElectionTimeout:    100 * time.Millisecond,
			LeaderLeaseTimeout: 50 * time.Millisecond,
			CommitTimeout:      5 * time.Millisecond,
			SnapshotThreshold:  o.SnapshotThreshold,
			SnapshotInterval:   time.Hour,
			TrailingLogs:       o.TrailingLogs,
			ApplyTimeout:       2 * time.Second,
			ReadMode:           o.ReadMode,
		},
		API:    api.Config{LeaderWait: o.LeaderWait},
		Logger: m.c.logger,
	}
}

func (m *Member) start(ln net.Listener) {
	m.c.t.Helper()
	if ln == nil {
		var err error
		if ln, err = listen(m.Addr); err != nil {
			m.c.t.Fatalf("rebind %s: %v", m.Addr, err)
		}
	}
	srv, err := server.Start(m.config(ln))
	if err != nil {
		m.c.t.Fatalf("start %s: %v", m.ID, err)
	}
	m.srv = srv
	if m.conn == nil {
		conn, err := grpc.NewClient(m.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			m.c.t.Fatalf("dial %s: %v", m.ID, err)
		}
		m.conn = conn
	}
}

// Node exposes the raft node (nil while stopped).
func (m *Member) Node() *raftnode.Node {
	if m.srv == nil {
		return nil
	}
	return m.srv.Node()
}

// Running reports whether the node process is up.
func (m *Member) Running() bool { return m.srv != nil }

// KV returns a client bound to this member.
func (m *Member) KV() kvv1.KVServiceClient { return kvv1.NewKVServiceClient(m.conn) }

// Admin returns an admin client bound to this member.
func (m *Member) Admin() kvv1.AdminServiceClient { return kvv1.NewAdminServiceClient(m.conn) }

// Stop shuts the node down; its data directory survives for Restart.
func (m *Member) Stop() {
	m.c.t.Helper()
	if m.srv == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = m.srv.Stop(ctx)
	m.srv = nil
}

// Restart brings a stopped node back on the same address and data.
func (m *Member) Restart() {
	m.c.t.Helper()
	if m.srv != nil {
		m.c.t.Fatalf("%s is already running", m.ID)
	}
	m.start(nil)
}

// Block cuts this node's links to the given peers (one direction of a cut).
func (m *Member) Block(ids ...string) {
	m.c.t.Helper()
	if _, err := m.Node().Partition(ids, nil, false, nil); err != nil {
		m.c.t.Fatalf("block on %s: %v", m.ID, err)
	}
}

// Heal removes every block on this node and thaws it.
func (m *Member) Heal() {
	if n := m.Node(); n != nil {
		thaw := false
		_, _ = n.Partition(nil, nil, true, &thaw)
	}
}

// Freeze stalls all raft traffic on this node (simulated process pause).
func (m *Member) Freeze() {
	if n := m.Node(); n != nil {
		n.Chaos().Freeze()
	}
}

// Thaw releases Freeze.
func (m *Member) Thaw() {
	if n := m.Node(); n != nil {
		n.Chaos().Thaw()
	}
}

// Partition cuts the network between two groups of node ids, symmetrically.
func (c *Cluster) Partition(groupA, groupB []string) {
	c.t.Helper()
	for _, a := range groupA {
		c.Member(a).Block(groupB...)
	}
	for _, b := range groupB {
		c.Member(b).Block(groupA...)
	}
}

// Heal restores full connectivity.
func (c *Cluster) Heal() {
	for _, m := range c.Members {
		m.Heal()
	}
}

// Member looks up a node by id.
func (c *Cluster) Member(id string) *Member {
	for _, m := range c.Members {
		if m.ID == id {
			return m
		}
	}
	c.t.Fatalf("no member %s", id)
	return nil
}

// IDs returns the ids of all members, or of those matching keep.
func (c *Cluster) IDs(keep func(*Member) bool) []string {
	var ids []string
	for _, m := range c.Members {
		if keep == nil || keep(m) {
			ids = append(ids, m.ID)
		}
	}
	return ids
}

// Leader waits until some running member believes it is the leader.
func (c *Cluster) Leader(timeout time.Duration) *Member {
	c.t.Helper()
	m, err := c.LeaderAmong(timeout, c.Members)
	if err != nil {
		c.t.Fatal(err)
	}
	return m
}

// LeaderAmong waits for a leader among the given members.
func (c *Cluster) LeaderAmong(timeout time.Duration, members []*Member) (*Member, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, m := range members {
			if n := m.Node(); n != nil && n.IsLeader() {
				return m, nil
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil, fmt.Errorf("no leader among %d members within %s", len(members), timeout)
}

// Followers returns running members other than the leader.
func (c *Cluster) Followers(leader *Member) []*Member {
	var out []*Member
	for _, m := range c.Members {
		if m != leader && m.Running() {
			out = append(out, m)
		}
	}
	return out
}

// AddMember starts an extra node with no bootstrap config and joins it
// through the leader.
func (c *Cluster) AddMember() *Member {
	c.t.Helper()
	i := len(c.Members)
	ln, err := listenAlias(i)
	if err != nil {
		c.t.Fatal(err)
	}
	m := &Member{ID: nodeID(i), Addr: ln.Addr().String(), c: c, joined: true}
	if !c.opts.InMemory {
		m.DataDir = filepath.Join(c.t.TempDir(), m.ID)
	}
	m.start(ln)
	c.Members = append(c.Members, m)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.Leader(5*time.Second).Admin().Join(ctx, &kvv1.JoinRequest{Id: m.ID, Address: m.Addr}); err != nil {
		c.t.Fatalf("join %s: %v", m.ID, err)
	}
	return m
}

// Close stops everything.
func (c *Cluster) Close() {
	for _, m := range c.Members {
		m.Stop()
		if m.conn != nil {
			_ = m.conn.Close()
			m.conn = nil
		}
	}
}
