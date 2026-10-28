package linearizability

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	kvv1 "github.com/Ehsan-saradar/euler-phi-exercise-d33/gen/kv/v1"
	"github.com/Ehsan-saradar/euler-phi-exercise-d33/internal/api"
)

// infinity is the Return timestamp of an operation with an unknown outcome.
const infinity = int64(1) << 62

// Stats summarises a recorded history.
type Stats struct {
	Ok, NotApplied, Unknown int
	Errors                  map[string]int // error class -> count
}

// Recorder runs client goroutines and collects a Porcupine history.
type Recorder struct {
	start   time.Time
	mu      sync.Mutex
	history []porcupine.Operation
	stats   Stats
}

// NewRecorder creates an empty recorder.
func NewRecorder() *Recorder {
	return &Recorder{start: time.Now(), stats: Stats{Errors: map[string]int{}}}
}

func (r *Recorder) now() int64 { return time.Since(r.start).Nanoseconds() }

// Record appends one operation.
func (r *Recorder) record(client int, in Input, out Output, call, ret int64, notApplied bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if out.err != "" {
		r.stats.Errors[out.err]++
	}
	switch {
	case notApplied:
		r.stats.NotApplied++
		return // no effect, nothing to check
	case out.Unknown:
		r.stats.Unknown++
		if in.Kind == KindGet {
			return // a read with no answer carries no information
		}
		ret = infinity
	default:
		r.stats.Ok++
	}
	r.history = append(r.history, porcupine.Operation{ClientId: client, Input: in, Output: out, Call: call, Return: ret})
}

// History returns the recorded operations.
func (r *Recorder) History() []porcupine.Operation {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]porcupine.Operation(nil), r.history...)
}

// Stats returns counters.
func (r *Recorder) Stats() Stats {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.stats
	st.Errors = make(map[string]int, len(r.stats.Errors))
	for k, v := range r.stats.Errors {
		st.Errors[k] = v
	}
	return st
}

// ClientConfig tunes one client goroutine.
type ClientConfig struct {
	ID        int
	Keys      []string
	Pick      func() kvv1.KVServiceClient // chooses a node per operation
	Rand      *rand.Rand
	OpTimeout time.Duration
	Think     time.Duration
}

// RunClient issues random operations until ctx ends.
func (r *Recorder) RunClient(ctx context.Context, cfg ClientConfig) {
	seq := 0
	for ctx.Err() == nil {
		seq++
		key := cfg.Keys[cfg.Rand.Intn(len(cfg.Keys))]
		var in Input
		switch n := cfg.Rand.Intn(10); {
		case n < 4:
			in = Input{Kind: KindGet, Key: key}
		case n < 8:
			in = Input{Kind: KindPut, Key: key, Value: fmt.Sprintf("c%d-%d", cfg.ID, seq)}
		default:
			in = Input{Kind: KindCas, Key: key, Value: fmt.Sprintf("c%d-%d", cfg.ID, seq), Expected: uint64(cfg.Rand.Intn(4))}
		}
		call := r.now()
		out, notApplied := r.invoke(ctx, cfg, in)
		r.record(cfg.ID, in, out, call, r.now(), notApplied)
		if cfg.Think > 0 {
			time.Sleep(time.Duration(cfg.Rand.Int63n(int64(cfg.Think))))
		}
	}
}

func (r *Recorder) invoke(ctx context.Context, cfg ClientConfig, in Input) (out Output, notApplied bool) {
	opCtx, cancel := context.WithTimeout(ctx, cfg.OpTimeout)
	defer cancel()
	client := cfg.Pick()
	var err error
	switch in.Kind {
	case KindGet:
		var resp *kvv1.GetResponse
		resp, err = client.Get(opCtx, &kvv1.GetRequest{Key: []byte(in.Key), Consistency: kvv1.Consistency_CONSISTENCY_LINEARIZABLE})
		if err == nil {
			out = Output{Found: resp.GetFound(), Value: string(resp.GetKv().GetValue()), Version: resp.GetKv().GetVersion()}
		}
	case KindPut:
		var resp *kvv1.PutResponse
		resp, err = client.Put(opCtx, &kvv1.PutRequest{Key: []byte(in.Key), Value: []byte(in.Value)})
		if err == nil {
			out = Output{Version: resp.GetKv().GetVersion()}
		}
	default:
		var resp *kvv1.CompareAndSwapResponse
		resp, err = client.CompareAndSwap(opCtx, &kvv1.CompareAndSwapRequest{Key: []byte(in.Key), ExpectedVersion: in.Expected, Value: []byte(in.Value)})
		if err == nil {
			out = Output{Succeeded: resp.GetSucceeded()}
			if resp.GetSucceeded() {
				out.Version = resp.GetCurrent().GetVersion()
			}
		}
	}
	if err == nil {
		return out, false
	}
	class := classify(err)
	if api.IsNotApplied(err) || status.Code(err) == codes.InvalidArgument || class == "unavailable/connection-refused" {
		return Output{err: class}, true
	}
	return Output{Unknown: true, err: class}, false
}

// classify buckets an error for the statistics and the not-applied rule.
// A "connection refused" (or a subchannel in TRANSIENT_FAILURE) means the RPC
// never left the client, so like a NOT_APPLIED status it had no effect; any
// other transport failure happened after the request may have been sent.
func classify(err error) string {
	st, ok := status.FromError(err)
	if !ok {
		return "non-grpc"
	}
	msg := st.Message()
	switch {
	case api.IsNotApplied(err):
		return "unavailable/not-applied"
	case st.Code() == codes.Unavailable && (strings.Contains(msg, "connection refused") || strings.Contains(msg, "connection error")):
		return "unavailable/connection-refused"
	case st.Code() == codes.Unavailable:
		return "unavailable/transport"
	case st.Code() == codes.DeadlineExceeded && strings.Contains(msg, "outcome unknown"):
		return "deadline/server-reported-unknown"
	case st.Code() == codes.DeadlineExceeded:
		return "deadline/client-timeout"
	default:
		return st.Code().String()
	}
}
