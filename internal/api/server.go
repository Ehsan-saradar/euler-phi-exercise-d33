// Package api implements the gRPC KV and Admin services on top of a raft node.
// Any node accepts any request; writes and linearizable reads that land on a
// follower are forwarded to the leader exactly once.
package api

import (
	"context"
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	kvv1 "github.com/Ehsan-saradar/euler-phi-exercise-d33/gen/kv/v1"
	"github.com/Ehsan-saradar/euler-phi-exercise-d33/internal/fsm"
	"github.com/Ehsan-saradar/euler-phi-exercise-d33/internal/raftnode"
)

type fsmResult = fsm.Result

// Config tunes the API layer.
type Config struct {
	LeaderWait        time.Duration // how long a follower waits for a leader before answering Unavailable
	DefaultRangeLimit int
	MaxRangeLimit     int
	MaxKeyBytes       int
	MaxValueBytes     int
	Registry          prometheus.Registerer // nil = private registry
}

func (c *Config) defaults() {
	if c.LeaderWait == 0 {
		c.LeaderWait = 2 * time.Second
	}
	if c.DefaultRangeLimit == 0 {
		c.DefaultRangeLimit = 100
	}
	if c.MaxRangeLimit == 0 {
		c.MaxRangeLimit = 1000
	}
	if c.MaxKeyBytes == 0 {
		c.MaxKeyBytes = 1024
	}
	if c.MaxValueBytes == 0 {
		c.MaxValueBytes = 1 << 20
	}
}

// Server implements KVService and AdminService.
type Server struct {
	kvv1.UnimplementedKVServiceServer
	kvv1.UnimplementedAdminServiceServer

	cfg  Config
	node *raftnode.Node
	fwd  *forwarder
	m    *Metrics
}

// New builds the API for node.
func New(node *raftnode.Node, cfg Config) *Server {
	cfg.defaults()
	return &Server{cfg: cfg, node: node, fwd: newForwarder(node, cfg.LeaderWait), m: NewMetrics(cfg.Registry)}
}

// Register attaches the services to a gRPC server.
func (s *Server) Register(g *grpc.Server) {
	kvv1.RegisterKVServiceServer(g, s)
	kvv1.RegisterAdminServiceServer(g, s)
}

// Close releases forwarding connections.
func (s *Server) Close() { s.fwd.close() }

func (s *Server) header(index uint64) *kvv1.ResponseHeader {
	_, leaderID := s.node.Leader()
	return &kvv1.ResponseHeader{
		NodeId:    s.node.ID(),
		LeaderId:  leaderID,
		RaftTerm:  s.node.Raft().CurrentTerm(),
		RaftIndex: index,
	}
}

func (s *Server) validate(key, value []byte) error {
	switch {
	case len(key) == 0:
		return status.Error(codes.InvalidArgument, "key must not be empty")
	case len(key) > s.cfg.MaxKeyBytes:
		return status.Errorf(codes.InvalidArgument, "key exceeds %d bytes", s.cfg.MaxKeyBytes)
	case len(value) > s.cfg.MaxValueBytes:
		return status.Errorf(codes.InvalidArgument, "value exceeds %d bytes", s.cfg.MaxValueBytes)
	}
	return nil
}

// forwardKV runs call against the leader's KV service.
func forwardKV[R any](ctx context.Context, s *Server, rpc string, call func(context.Context, kvv1.KVServiceClient) (R, error)) (R, error) {
	s.m.Forwarded.WithLabelValues(rpc).Inc()
	conn, fctx, err := s.fwd.leader(ctx)
	if err != nil {
		var zero R
		return zero, err
	}
	start := time.Now()
	resp, err := call(fctx, kvv1.NewKVServiceClient(conn))
	s.m.observeSince(s.m.ForwardTime, start)
	return resp, fromLeader(err)
}

// forwardAdmin runs call against the leader's Admin service.
func forwardAdmin[R any](ctx context.Context, s *Server, rpc string, call func(context.Context, kvv1.AdminServiceClient) (R, error)) (R, error) {
	s.m.Forwarded.WithLabelValues(rpc).Inc()
	conn, fctx, err := s.fwd.leader(ctx)
	if err != nil {
		var zero R
		return zero, err
	}
	resp, err := call(fctx, kvv1.NewAdminServiceClient(conn))
	return resp, fromLeader(err)
}

// propose wraps Node.Propose with metrics.
func (s *Server) propose(ctx context.Context, cmd *kvv1.Command) (*fsmResult, uint64, error) {
	start := time.Now()
	res, idx, err := s.node.Propose(ctx, cmd)
	switch {
	case err == nil:
		s.m.observeSince(s.m.ProposeTime, start)
		s.m.Proposals.WithLabelValues("ok").Inc()
	case errors.Is(err, raftnode.ErrNotLeader):
		s.m.Proposals.WithLabelValues("not_leader").Inc()
	case errors.Is(err, raftnode.ErrNoLeader):
		s.m.Proposals.WithLabelValues("not_applied").Inc()
	case errors.Is(err, raftnode.ErrUnknownOutcome):
		s.m.Proposals.WithLabelValues("unknown").Inc()
	default:
		s.m.Proposals.WithLabelValues("error").Inc()
	}
	return res, idx, err
}

// --- KVService ---

// Put writes a key through the raft log.
func (s *Server) Put(ctx context.Context, req *kvv1.PutRequest) (*kvv1.PutResponse, error) {
	if err := s.validate(req.GetKey(), req.GetValue()); err != nil {
		return nil, err
	}
	cmd := &kvv1.Command{Op: &kvv1.Command_Put{Put: &kvv1.PutOp{Key: req.GetKey(), Value: req.GetValue()}}}
	res, idx, err := s.propose(ctx, cmd)
	if errors.Is(err, raftnode.ErrNotLeader) {
		return forwardKV(ctx, s, "Put", func(c context.Context, cl kvv1.KVServiceClient) (*kvv1.PutResponse, error) {
			return cl.Put(c, req)
		})
	}
	if err != nil {
		return nil, toStatus(err)
	}
	return &kvv1.PutResponse{Header: s.header(idx), Kv: res.KV}, nil
}

// Delete removes a key through the raft log.
func (s *Server) Delete(ctx context.Context, req *kvv1.DeleteRequest) (*kvv1.DeleteResponse, error) {
	if err := s.validate(req.GetKey(), nil); err != nil {
		return nil, err
	}
	cmd := &kvv1.Command{Op: &kvv1.Command_Delete{Delete: &kvv1.DeleteOp{Key: req.GetKey()}}}
	res, idx, err := s.propose(ctx, cmd)
	if errors.Is(err, raftnode.ErrNotLeader) {
		return forwardKV(ctx, s, "Delete", func(c context.Context, cl kvv1.KVServiceClient) (*kvv1.DeleteResponse, error) {
			return cl.Delete(c, req)
		})
	}
	if err != nil {
		return nil, toStatus(err)
	}
	return &kvv1.DeleteResponse{Header: s.header(idx), Deleted: res.Deleted}, nil
}

// CompareAndSwap writes only if the key is at the expected version (0 = absent).
func (s *Server) CompareAndSwap(ctx context.Context, req *kvv1.CompareAndSwapRequest) (*kvv1.CompareAndSwapResponse, error) {
	if err := s.validate(req.GetKey(), req.GetValue()); err != nil {
		return nil, err
	}
	cmd := &kvv1.Command{Op: &kvv1.Command_Cas{Cas: &kvv1.CompareAndSwapOp{
		Key: req.GetKey(), ExpectedVersion: req.GetExpectedVersion(), Value: req.GetValue(),
	}}}
	res, idx, err := s.propose(ctx, cmd)
	if errors.Is(err, raftnode.ErrNotLeader) {
		return forwardKV(ctx, s, "CompareAndSwap", func(c context.Context, cl kvv1.KVServiceClient) (*kvv1.CompareAndSwapResponse, error) {
			return cl.CompareAndSwap(c, req)
		})
	}
	if err != nil {
		return nil, toStatus(err)
	}
	return &kvv1.CompareAndSwapResponse{Header: s.header(idx), Succeeded: res.Succeeded, Current: res.KV}, nil
}

// readIndex returns the index a read may be served at. Stale reads are local;
// linearizable reads pass the leader's read gate or are forwarded.
func (s *Server) readIndex(ctx context.Context, c kvv1.Consistency) (idx uint64, forward bool, err error) {
	if c == kvv1.Consistency_CONSISTENCY_STALE {
		s.m.Reads.WithLabelValues("stale", "ok").Inc()
		return s.node.Store().Applied(), false, nil
	}
	start := time.Now()
	idx, err = s.node.Linearize(ctx)
	switch {
	case errors.Is(err, raftnode.ErrNotLeader):
		s.m.Reads.WithLabelValues("linearizable", "forwarded").Inc()
		return 0, true, nil
	case err != nil:
		s.m.Reads.WithLabelValues("linearizable", "error").Inc()
		return 0, false, toStatus(err)
	}
	s.m.observeSince(s.m.LinearTime, start)
	s.m.Reads.WithLabelValues("linearizable", "ok").Inc()
	return idx, false, nil
}

// Get reads one key at the requested consistency.
func (s *Server) Get(ctx context.Context, req *kvv1.GetRequest) (*kvv1.GetResponse, error) {
	if err := s.validate(req.GetKey(), nil); err != nil {
		return nil, err
	}
	idx, forward, err := s.readIndex(ctx, req.GetConsistency())
	if err != nil {
		return nil, err
	}
	if forward {
		return forwardKV(ctx, s, "Get", func(c context.Context, cl kvv1.KVServiceClient) (*kvv1.GetResponse, error) {
			return cl.Get(c, req)
		})
	}
	kv := s.node.Store().Get(req.GetKey())
	return &kvv1.GetResponse{Header: s.header(idx), Found: kv != nil, Kv: kv}, nil
}

// Range lists keys by prefix with cursor pagination.
func (s *Server) Range(ctx context.Context, req *kvv1.RangeRequest) (*kvv1.RangeResponse, error) {
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = s.cfg.DefaultRangeLimit
	}
	if limit > s.cfg.MaxRangeLimit {
		limit = s.cfg.MaxRangeLimit
	}
	idx, forward, err := s.readIndex(ctx, req.GetConsistency())
	if err != nil {
		return nil, err
	}
	if forward {
		return forwardKV(ctx, s, "Range", func(c context.Context, cl kvv1.KVServiceClient) (*kvv1.RangeResponse, error) {
			return cl.Range(c, req)
		})
	}
	kvs, more := s.node.Store().Range(req.GetPrefix(), req.GetStartAfter(), limit)
	return &kvv1.RangeResponse{Header: s.header(idx), Kvs: kvs, More: more}, nil
}

// --- AdminService ---

// Status reports this node's raft view.
func (s *Server) Status(context.Context, *kvv1.StatusRequest) (*kvv1.StatusResponse, error) {
	return s.node.Status(), nil
}

// Join adds a voter; forwarded to the leader when needed.
func (s *Server) Join(ctx context.Context, req *kvv1.JoinRequest) (*kvv1.JoinResponse, error) {
	if req.GetId() == "" || req.GetAddress() == "" {
		return nil, status.Error(codes.InvalidArgument, "id and address are required")
	}
	err := s.node.AddVoter(ctx, req.GetId(), req.GetAddress())
	if errors.Is(err, raftnode.ErrNotLeader) {
		return forwardAdmin(ctx, s, "Join", func(c context.Context, cl kvv1.AdminServiceClient) (*kvv1.JoinResponse, error) {
			return cl.Join(c, req)
		})
	}
	if err != nil {
		return nil, toStatus(err)
	}
	return &kvv1.JoinResponse{}, nil
}

// Leave removes a server; forwarded to the leader when needed.
func (s *Server) Leave(ctx context.Context, req *kvv1.LeaveRequest) (*kvv1.LeaveResponse, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	err := s.node.RemoveServer(ctx, req.GetId())
	if errors.Is(err, raftnode.ErrNotLeader) {
		return forwardAdmin(ctx, s, "Leave", func(c context.Context, cl kvv1.AdminServiceClient) (*kvv1.LeaveResponse, error) {
			return cl.Leave(c, req)
		})
	}
	if err != nil {
		return nil, toStatus(err)
	}
	return &kvv1.LeaveResponse{}, nil
}

// Partition is deliberately local: it configures this node's own links.
func (s *Server) Partition(_ context.Context, req *kvv1.PartitionRequest) (*kvv1.PartitionResponse, error) {
	var frozen *bool
	if req.Frozen != nil {
		frozen = req.Frozen
	}
	blocked, err := s.node.Partition(req.GetBlock(), req.GetUnblock(), req.GetClear(), frozen)
	if err != nil {
		return nil, toStatus(err)
	}
	return &kvv1.PartitionResponse{BlockedPeers: blocked, Frozen: s.node.Chaos().Frozen()}, nil
}

// Snapshot forces a snapshot on this node.
func (s *Server) Snapshot(context.Context, *kvv1.SnapshotRequest) (*kvv1.SnapshotResponse, error) {
	idx, err := s.node.Snapshot()
	if err != nil {
		return nil, toStatus(err)
	}
	return &kvv1.SnapshotResponse{Index: idx}, nil
}
