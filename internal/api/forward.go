package api

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/Ehsan-saradar/euler-phi-exercise-d33/internal/raftnode"
)

// forwardedKey marks a request that already hopped once. A node receiving it
// never forwards again, so a leadership flap cannot bounce a request around.
const forwardedKey = "kv-forwarded"

// forwarder keeps one client connection per peer address.
type forwarder struct {
	node        *raftnode.Node
	leaderWait  time.Duration
	mu          sync.Mutex
	conns       map[string]*grpc.ClientConn
	dialOptions []grpc.DialOption
}

func newForwarder(node *raftnode.Node, leaderWait time.Duration) *forwarder {
	return &forwarder{
		node:        node,
		leaderWait:  leaderWait,
		conns:       make(map[string]*grpc.ClientConn),
		dialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	}
}

func isForwarded(ctx context.Context) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	return ok && len(md.Get(forwardedKey)) > 0
}

// leader resolves the leader connection or returns Unavailable.
func (f *forwarder) leader(ctx context.Context) (*grpc.ClientConn, context.Context, error) {
	if isForwarded(ctx) {
		return nil, nil, notApplied("not the leader (request already forwarded once)")
	}
	waitCtx, cancel := context.WithTimeout(ctx, f.leaderWait)
	defer cancel()
	addr, _, err := f.node.WaitForLeader(waitCtx)
	if err != nil {
		return nil, nil, notApplied("no leader known; cluster may lack a quorum")
	}
	conn, err := f.conn(addr)
	if err != nil {
		return nil, nil, notApplied(fmt.Sprintf("dial leader %s: %v", addr, err))
	}
	return conn, metadata.AppendToOutgoingContext(ctx, forwardedKey, "1"), nil
}

func (f *forwarder) conn(addr string) (*grpc.ClientConn, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if c, ok := f.conns[addr]; ok {
		return c, nil
	}
	c, err := grpc.NewClient(addr, f.dialOptions...)
	if err != nil {
		return nil, err
	}
	f.conns[addr] = c
	return c, nil
}

func (f *forwarder) close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for addr, c := range f.conns {
		_ = c.Close()
		delete(f.conns, addr)
	}
}

// fromLeader normalises an error returned by the forwarded call. A status the
// leader produced passes through untouched (it carries NOT_APPLIED when it
// applies). A transport failure means the leader may have committed the entry
// before dying, so it is reported as an unknown outcome.
func fromLeader(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return status.Error(codes.DeadlineExceeded, "leader unreachable after forwarding; outcome unknown: "+err.Error())
	}
	switch st.Code() {
	case codes.Unavailable:
		if IsNotApplied(err) {
			return err
		}
		return status.Error(codes.DeadlineExceeded, "leader unreachable after forwarding; outcome unknown: "+st.Message())
	case codes.Canceled:
		return status.Error(codes.DeadlineExceeded, "forwarded call canceled; outcome unknown: "+st.Message())
	default:
		return err
	}
}
