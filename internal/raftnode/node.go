// Package raftnode wires hashicorp/raft to the key-value FSM and exposes the
// two operations the API needs: Propose (write through the log) and Linearize
// (the read gate that makes local FSM reads linearizable).
package raftnode

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"google.golang.org/protobuf/proto"

	kvv1 "github.com/Ehsan-saradar/euler-phi-exercise-d33/gen/kv/v1"
	"github.com/Ehsan-saradar/euler-phi-exercise-d33/internal/fsm"
)

// ReadMode selects how linearizable reads are served on the leader.
type ReadMode string

const (
	// ReadModeReadIndex confirms leadership with one heartbeat round and waits
	// for the FSM to reach the commit index. No log entry is written.
	ReadModeReadIndex ReadMode = "read_index"
	// ReadModeBarrier appends a barrier entry per read. Trivially correct and
	// kept as a reference implementation for benchmarks and tests.
	ReadModeBarrier ReadMode = "barrier"
)

var (
	// ErrNotLeader means the caller should forward to the leader.
	ErrNotLeader = errors.New("raftnode: not the leader")
	// ErrNoLeader means no leader is known; the cluster may lack a quorum.
	ErrNoLeader = errors.New("raftnode: no leader")
	// ErrUnknownOutcome means a proposal may or may not have been committed.
	ErrUnknownOutcome = errors.New("raftnode: proposal outcome unknown")
	// ErrUnknownPeer means a membership or partition request named a peer
	// that is not in the configuration.
	ErrUnknownPeer = errors.New("raftnode: unknown peer")
)

// Config configures a Node.
type Config struct {
	ID        string
	Listener  net.Listener      // accepts raft traffic (typically a cmux branch)
	Advertise string            // address other nodes dial for raft and gRPC
	Peers     map[string]string // id -> advertise address; bootstrap config when non-empty
	Bootstrap bool              // call BootstrapCluster with Peers on first start
	DataDir   string            // empty means in-memory log, stable and snapshot stores

	HeartbeatTimeout   time.Duration
	ElectionTimeout    time.Duration
	LeaderLeaseTimeout time.Duration
	CommitTimeout      time.Duration
	SnapshotThreshold  uint64
	SnapshotInterval   time.Duration
	TrailingLogs       uint64
	ApplyTimeout       time.Duration
	ReadMode           ReadMode

	Logger *slog.Logger
}

func (c *Config) defaults() {
	if c.HeartbeatTimeout == 0 {
		c.HeartbeatTimeout = time.Second
	}
	if c.ElectionTimeout == 0 {
		c.ElectionTimeout = time.Second
	}
	if c.LeaderLeaseTimeout == 0 {
		c.LeaderLeaseTimeout = 500 * time.Millisecond
	}
	if c.CommitTimeout == 0 {
		c.CommitTimeout = 50 * time.Millisecond
	}
	if c.SnapshotThreshold == 0 {
		c.SnapshotThreshold = 8192
	}
	if c.SnapshotInterval == 0 {
		c.SnapshotInterval = 120 * time.Second
	}
	if c.TrailingLogs == 0 {
		c.TrailingLogs = 10240
	}
	if c.ApplyTimeout == 0 {
		c.ApplyTimeout = 5 * time.Second
	}
	if c.ReadMode == "" {
		c.ReadMode = ReadModeReadIndex
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

// Node is one member of the replicated store.
type Node struct {
	cfg       Config
	raft      *raft.Raft
	store     *fsm.Store
	chaos     *ChaosLayer
	transport *raft.NetworkTransport
	logs      raft.LogStore
	snaps     raft.SnapshotStore
	closer    func() error

	// termBarrier is the highest term in which this node, as leader, has
	// committed a barrier. Raft §8: a leader may only serve reads after it has
	// committed an entry from its own term.
	termBarrier atomic.Uint64
}

// New starts raft on cfg.Listener and bootstraps the cluster when asked.
func New(cfg Config) (*Node, error) {
	cfg.defaults()
	if cfg.ID == "" || cfg.Listener == nil || cfg.Advertise == "" {
		return nil, errors.New("raftnode: ID, Listener and Advertise are required")
	}

	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(cfg.ID)
	rc.HeartbeatTimeout = cfg.HeartbeatTimeout
	rc.ElectionTimeout = cfg.ElectionTimeout
	rc.LeaderLeaseTimeout = cfg.LeaderLeaseTimeout
	rc.CommitTimeout = cfg.CommitTimeout
	rc.SnapshotThreshold = cfg.SnapshotThreshold
	rc.SnapshotInterval = cfg.SnapshotInterval
	rc.TrailingLogs = cfg.TrailingLogs
	rc.Logger = hclog.New(&hclog.LoggerOptions{
		Name:   "raft." + cfg.ID,
		Level:  hclogLevel(cfg.Logger),
		Output: os.Stderr,
	})
	if err := raft.ValidateConfig(rc); err != nil {
		return nil, fmt.Errorf("raftnode: invalid raft config: %w", err)
	}

	logs, stable, snaps, closer, err := openStores(cfg)
	if err != nil {
		return nil, err
	}

	chaos := NewChaosLayer(cfg.Listener, cfg.Advertise)
	transport := raft.NewNetworkTransportWithLogger(chaos, 3, 10*time.Second, rc.Logger.Named("transport"))

	store := fsm.New()
	r, err := raft.NewRaft(rc, store, logs, stable, snaps, transport)
	if err != nil {
		_ = closer()
		return nil, fmt.Errorf("raftnode: start raft: %w", err)
	}

	n := &Node{cfg: cfg, raft: r, store: store, chaos: chaos, transport: transport, logs: logs, snaps: snaps, closer: closer}
	if cfg.Bootstrap && len(cfg.Peers) > 0 {
		if err := n.bootstrap(); err != nil {
			_ = n.Shutdown()
			return nil, err
		}
	}
	return n, nil
}

// bootstrap seeds the configuration. hashicorp/raft documents that calling
// BootstrapCluster with the same configuration on every server is safe, so no
// coordinator is needed; a node that already has state reports
// ErrCantBootstrap, which is the normal restart path.
func (n *Node) bootstrap() error {
	servers := make([]raft.Server, 0, len(n.cfg.Peers))
	for id, addr := range n.cfg.Peers {
		servers = append(servers, raft.Server{ID: raft.ServerID(id), Address: raft.ServerAddress(addr), Suffrage: raft.Voter})
	}
	err := n.raft.BootstrapCluster(raft.Configuration{Servers: servers}).Error()
	if err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
		return fmt.Errorf("raftnode: bootstrap: %w", err)
	}
	return nil
}

func openStores(cfg Config) (raft.LogStore, raft.StableStore, raft.SnapshotStore, func() error, error) {
	if cfg.DataDir == "" {
		mem := raft.NewInmemStore()
		return mem, mem, raft.NewInmemSnapshotStore(), func() error { return nil }, nil
	}
	if err := os.MkdirAll(cfg.DataDir, 0o750); err != nil {
		return nil, nil, nil, nil, fmt.Errorf("raftnode: create data dir: %w", err)
	}
	bolt, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft.db"))
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("raftnode: open bolt store: %w", err)
	}
	snaps, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, os.Stderr)
	if err != nil {
		_ = bolt.Close()
		return nil, nil, nil, nil, fmt.Errorf("raftnode: open snapshot store: %w", err)
	}
	return bolt, bolt, snaps, bolt.Close, nil
}

// Store exposes the FSM for local reads.
func (n *Node) Store() *fsm.Store { return n.store }

// Chaos exposes the partition switch.
func (n *Node) Chaos() *ChaosLayer { return n.chaos }

// ID returns the node id.
func (n *Node) ID() string { return n.cfg.ID }

// Raft exposes the underlying raft instance for tests and status.
func (n *Node) Raft() *raft.Raft { return n.raft }

// IsLeader reports whether this node currently believes it is the leader.
func (n *Node) IsLeader() bool { return n.raft.State() == raft.Leader }

// Leader returns the known leader's address and id, both empty if unknown.
func (n *Node) Leader() (addr, id string) {
	a, i := n.raft.LeaderWithID()
	return string(a), string(i)
}

// WaitForLeader blocks until a leader is known or ctx ends.
func (n *Node) WaitForLeader(ctx context.Context) (addr, id string, err error) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if addr, id = n.Leader(); addr != "" {
			return addr, id, nil
		}
		select {
		case <-ctx.Done():
			return "", "", ErrNoLeader
		case <-ticker.C:
		}
	}
}

// Propose replicates cmd through the log and returns the FSM result and the
// entry's index. It only succeeds on the leader; followers get ErrNotLeader.
func (n *Node) Propose(ctx context.Context, cmd *kvv1.Command) (*fsm.Result, uint64, error) {
	data, err := proto.Marshal(cmd)
	if err != nil {
		return nil, 0, err
	}
	f := n.raft.Apply(data, n.cfg.ApplyTimeout)
	if err := await(ctx, f); err != nil {
		return nil, 0, mapRaftErr(err)
	}
	res, ok := f.Response().(*fsm.Result)
	if !ok {
		return nil, 0, fmt.Errorf("raftnode: unexpected FSM response %T", f.Response())
	}
	if res.Err != nil {
		return nil, 0, res.Err
	}
	return res, f.Index(), nil
}

// Linearize is the read gate. On return the local FSM reflects every write
// that was acknowledged before Linearize was called, so a subsequent local
// read is linearizable. The returned index is the commit index observed.
func (n *Node) Linearize(ctx context.Context) (uint64, error) {
	if !n.IsLeader() {
		return 0, ErrNotLeader
	}
	if n.cfg.ReadMode == ReadModeBarrier {
		return n.barrier(ctx)
	}

	// Raft §8: serve reads only after committing an entry in the current term,
	// otherwise CommitIndex may lag entries the previous leader acknowledged.
	term := n.raft.CurrentTerm()
	if n.termBarrier.Load() < term {
		if _, err := n.barrier(ctx); err != nil {
			return 0, err
		}
		n.termBarrier.Store(term)
	}

	// ReadIndex: confirm we are still the leader with a heartbeat quorum.
	if err := await(ctx, n.raft.VerifyLeader()); err != nil {
		return 0, mapRaftErr(err)
	}
	if n.raft.CurrentTerm() != term {
		return 0, ErrNotLeader // term moved under us; caller retries or forwards
	}
	commit := n.raft.CommitIndex()
	return commit, n.waitApplied(ctx, commit)
}

// barrier appends a barrier entry and waits for the FSM to apply everything
// before it.
func (n *Node) barrier(ctx context.Context) (uint64, error) {
	f := n.raft.Barrier(n.cfg.ApplyTimeout)
	if err := await(ctx, f); err != nil {
		return 0, mapRaftErr(err)
	}
	if idx, ok := f.(raft.IndexFuture); ok {
		return idx.Index(), nil
	}
	return n.raft.CommitIndex(), nil
}

// waitApplied blocks until the FSM reflects every command at or below index.
// The FSM never sees noop, barrier or configuration entries, so when it lags
// the target we look at the log to find the last command entry in the gap;
// if the gap holds no command the FSM is already current.
func (n *Node) waitApplied(ctx context.Context, index uint64) error {
	applied := n.store.Applied()
	if applied >= index {
		return nil
	}
	const maxScan = 1024
	target := applied
	for i := index; i > applied && index-i < maxScan; i-- {
		var entry raft.Log
		if err := n.logs.GetLog(i, &entry); err != nil {
			// Compacted or missing: fall back to the always-correct barrier.
			_, berr := n.barrier(ctx)
			return berr
		}
		if entry.Type == raft.LogCommand {
			target = i
			break
		}
	}
	if index-target >= maxScan && target == applied {
		_, err := n.barrier(ctx)
		return err
	}
	return n.store.WaitApplied(ctx, target)
}

// AddVoter adds a server to the configuration. Leader only.
func (n *Node) AddVoter(ctx context.Context, id, addr string) error {
	f := n.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, n.cfg.ApplyTimeout)
	return mapRaftErr(await(ctx, f))
}

// RemoveServer removes a server from the configuration. Leader only.
func (n *Node) RemoveServer(ctx context.Context, id string) error {
	f := n.raft.RemoveServer(raft.ServerID(id), 0, n.cfg.ApplyTimeout)
	return mapRaftErr(await(ctx, f))
}

// Snapshot forces a snapshot and returns its last index.
func (n *Node) Snapshot() (uint64, error) {
	f := n.raft.Snapshot()
	if err := f.Error(); err != nil {
		return 0, mapRaftErr(err)
	}
	meta, rc, err := f.Open()
	if err != nil {
		return 0, err
	}
	_ = rc.Close()
	return meta.Index, nil
}

// Partition updates the chaos blocklist by peer id and returns the blocked ids.
// frozen, when non-nil, freezes or thaws all raft traffic on this node.
func (n *Node) Partition(block, unblock []string, clearAll bool, frozen *bool) ([]string, error) {
	addrOf, idOf, err := n.peerTable()
	if err != nil {
		return nil, err
	}
	if clearAll {
		n.chaos.Clear()
	}
	if frozen != nil {
		if *frozen {
			n.chaos.Freeze()
		} else {
			n.chaos.Thaw()
		}
	}
	for _, id := range block {
		addr, ok := addrOf[id]
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrUnknownPeer, id)
		}
		n.chaos.Block(raft.ServerAddress(addr))
	}
	for _, id := range unblock {
		addr, ok := addrOf[id]
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrUnknownPeer, id)
		}
		n.chaos.Unblock(raft.ServerAddress(addr))
	}
	return n.blockedIDs(idOf), nil
}

func (n *Node) blockedIDs(idOf map[string]string) []string {
	out := make([]string, 0)
	for _, a := range n.chaos.Blocked() {
		if id, ok := idOf[string(a)]; ok {
			out = append(out, id)
		} else {
			out = append(out, string(a))
		}
	}
	return out
}

func (n *Node) peerTable() (addrOf, idOf map[string]string, err error) {
	cf := n.raft.GetConfiguration()
	if err := cf.Error(); err != nil {
		return nil, nil, mapRaftErr(err)
	}
	addrOf, idOf = map[string]string{}, map[string]string{}
	for _, s := range cf.Configuration().Servers {
		addrOf[string(s.ID)] = string(s.Address)
		idOf[string(s.Address)] = string(s.ID)
	}
	return addrOf, idOf, nil
}

// Status reports the raft view of this node.
func (n *Node) Status() *kvv1.StatusResponse {
	leaderAddr, leaderID := n.Leader()
	stats := n.raft.Stats()
	st := &kvv1.StatusResponse{
		NodeId:        n.cfg.ID,
		State:         n.raft.State().String(),
		LeaderId:      leaderID,
		LeaderAddress: leaderAddr,
		Term:          n.raft.CurrentTerm(),
		CommitIndex:   n.raft.CommitIndex(),
		AppliedIndex:  n.store.Applied(),
		LastLogIndex:  n.raft.LastIndex(),
		NumKeys:       uint64(n.store.Len()),
	}
	st.LastSnapshotIndex, _ = strconv.ParseUint(stats["last_snapshot_index"], 10, 64)
	if cf := n.raft.GetConfiguration(); cf.Error() == nil {
		idOf := map[string]string{}
		for _, s := range cf.Configuration().Servers {
			st.Peers = append(st.Peers, &kvv1.Peer{Id: string(s.ID), Address: string(s.Address), Voter: s.Suffrage == raft.Voter})
			idOf[string(s.Address)] = string(s.ID)
		}
		st.BlockedPeers = n.blockedIDs(idOf)
	}
	st.Frozen = n.chaos.Frozen()
	return st
}

// Shutdown stops raft and closes the stores.
func (n *Node) Shutdown() error {
	err := n.raft.Shutdown().Error()
	if cerr := n.transport.Close(); err == nil {
		err = cerr
	}
	if cerr := n.closer(); err == nil {
		err = cerr
	}
	return err
}

// await waits for a raft future while honouring ctx. A ctx expiry after the
// proposal was enqueued means the outcome is unknown, never "failed".
func await(ctx context.Context, f raft.Future) error {
	done := make(chan error, 1)
	go func() { done <- f.Error() }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ErrUnknownOutcome
	}
}

func mapRaftErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, raft.ErrNotLeader):
		return ErrNotLeader
	case errors.Is(err, raft.ErrLeadershipLost), errors.Is(err, raft.ErrLeadershipTransferInProgress):
		return fmt.Errorf("%w: %w", ErrUnknownOutcome, err)
	case errors.Is(err, raft.ErrEnqueueTimeout):
		return fmt.Errorf("%w: %w", ErrNoLeader, err)
	default:
		return err
	}
}

func hclogLevel(l *slog.Logger) hclog.Level {
	ctx := context.Background()
	switch {
	case l.Enabled(ctx, slog.LevelDebug):
		return hclog.Debug
	case l.Enabled(ctx, slog.LevelInfo):
		return hclog.Info
	case l.Enabled(ctx, slog.LevelWarn):
		return hclog.Warn
	default:
		return hclog.Error
	}
}
