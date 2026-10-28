package linearizability

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	kvv1 "github.com/Ehsan-saradar/euler-phi-exercise-d33/gen/kv/v1"
	"github.com/Ehsan-saradar/euler-phi-exercise-d33/internal/cluster"
	"github.com/Ehsan-saradar/euler-phi-exercise-d33/internal/raftnode"
)

func envInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return def
}

// TestLinearizability runs clients against a 3-node cluster while the
// nemesis kills, partitions and freezes nodes, then checks the history.
//
//	CHAOS_SEED      seed to reproduce a run (default: time-based, printed)
//	CHAOS_SECONDS   wall time of the fault phase (default 20)
//	CHAOS_CLIENTS   concurrent clients (default 6)
//	CHAOS_KEYS      distinct keys (default 8)
//	CHAOS_OUT       directory for the Porcupine visualization (default docs/chaos)
func TestLinearizability(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos run skipped in -short mode")
	}
	seed := int64(envInt("CHAOS_SEED", int(time.Now().UnixNano()%1_000_000_000)))
	seconds := envInt("CHAOS_SECONDS", 20)
	clients := envInt("CHAOS_CLIENTS", 6)
	numKeys := envInt("CHAOS_KEYS", 8)
	outDir := os.Getenv("CHAOS_OUT")
	if outDir == "" {
		outDir = filepath.Join("..", "..", "docs", "chaos")
	}
	t.Logf("seed=%d seconds=%d clients=%d keys=%d", seed, seconds, clients, numKeys)

	for _, mode := range []raftnode.ReadMode{raftnode.ReadModeReadIndex} {
		t.Run(string(mode), func(t *testing.T) {
			runChaos(t, seed, seconds, clients, numKeys, mode, outDir)
		})
	}
}

func runChaos(t *testing.T, seed int64, seconds, clients, numKeys int, mode raftnode.ReadMode, outDir string) {
	c := cluster.New(t, cluster.Options{
		ReadMode:          mode,
		SnapshotThreshold: 256,
		TrailingLogs:      64,
		LeaderWait:        300 * time.Millisecond,
		LogLevel:          slog.LevelError,
	})
	rec := NewRecorder()
	root := rand.New(rand.NewSource(seed))
	keys := make([]string, numKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%d", i)
	}
	conns := make([]kvv1.KVServiceClient, len(c.Members))
	for i, m := range c.Members {
		conns[i] = m.KV()
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(seconds)*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for i := range clients {
		r := rand.New(rand.NewSource(root.Int63()))
		cfg := ClientConfig{
			ID:        i,
			Keys:      keys,
			Rand:      r,
			OpTimeout: 3 * time.Second,
			Think:     20 * time.Millisecond,
			Pick:      func() kvv1.KVServiceClient { return conns[r.Intn(len(conns))] },
		}
		wg.Add(1)
		go func() { defer wg.Done(); rec.RunClient(ctx, cfg) }()
	}
	nemesis := NewNemesis(t, c, rand.New(rand.NewSource(root.Int63())), rec.start)
	wg.Add(1)
	go func() { defer wg.Done(); nemesis.Run(ctx) }()
	wg.Wait()

	// Quiet tail: with faults healed, every outstanding write must settle and
	// every node must answer linearizable reads again.
	c.Leader(10 * time.Second)
	tail, cancelTail := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelTail()
	for i, m := range c.Members {
		cfg := ClientConfig{ID: clients + i, Keys: keys, Rand: rand.New(rand.NewSource(int64(i))), OpTimeout: 3 * time.Second, Pick: func() kvv1.KVServiceClient { return m.KV() }}
		for _, k := range keys {
			in := Input{Kind: KindGet, Key: k}
			call := rec.now()
			out, na := rec.invoke(tail, cfg, in)
			rec.record(cfg.ID, in, out, call, rec.now(), na)
		}
	}

	history := rec.History()
	st := rec.Stats()
	t.Logf("history: %d ops (ok=%d not_applied=%d unknown=%d), %d faults", len(history), st.Ok, st.NotApplied, st.Unknown, len(nemesis.Faults))
	t.Logf("errors by class: %v", st.Errors)
	if st.Ok < 50 {
		t.Fatalf("too few successful operations (%d) for a meaningful check", st.Ok)
	}
	if len(nemesis.Faults) < 2 {
		t.Fatalf("only %d faults injected; increase CHAOS_SECONDS", len(nemesis.Faults))
	}

	res, info := porcupine.CheckOperationsVerbose(Model, history, 3*time.Minute)
	_ = os.MkdirAll(outDir, 0o750)
	file := filepath.Join(outDir, fmt.Sprintf("run-%s-seed%d-%s.html", mode, seed, res))
	if err := porcupine.VisualizePath(Model, info, file); err != nil {
		t.Logf("visualization failed: %v", err)
	} else {
		t.Logf("visualization: %s", file)
	}
	writeSummary(t, outDir, seed, mode, res, st, len(history), nemesis.Faults)
	switch res {
	case porcupine.Ok:
	case porcupine.Unknown:
		t.Fatalf("checker timed out; rerun with CHAOS_SEED=%d and fewer ops", seed)
	default:
		t.Fatalf("history is NOT linearizable (seed %d); see %s", seed, file)
	}
}

func writeSummary(t *testing.T, dir string, seed int64, mode raftnode.ReadMode, res porcupine.CheckResult, st Stats, ops int, faults []Fault) {
	f, err := os.OpenFile(filepath.Join(dir, "RESULTS.md"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o640)
	if err != nil {
		t.Logf("results file: %v", err)
		return
	}
	defer func() { _ = f.Close() }()
	kinds := map[string]int{}
	for _, fl := range faults {
		kinds[fl.Kind]++
	}
	_, _ = fmt.Fprintf(f, "| %s | %d | %s | %s | %d | %d | %d | %d | %v |\n",
		time.Now().UTC().Format(time.RFC3339), seed, mode, res, ops, st.Ok, st.NotApplied, st.Unknown, kinds)
}
