// Command kvbench is a small closed-loop load generator reporting throughput
// and latency percentiles for writes, linearizable reads and stale reads.
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	kvv1 "github.com/Ehsan-saradar/euler-phi-exercise-d33/gen/kv/v1"
)

func main() {
	addrs := flag.String("addrs", "localhost:7001,localhost:7002,localhost:7003", "comma-separated node addresses")
	mode := flag.String("mode", "put", "put | get | stale | mixed (50% put, 50% get)")
	clients := flag.Int("clients", 16, "concurrent closed-loop clients")
	duration := flag.Duration("duration", 10*time.Second, "run length")
	keys := flag.Int("keys", 1000, "key space")
	valueSize := flag.Int("value", 256, "value bytes")
	markdown := flag.Bool("md", false, "print a markdown table row")
	verbose := flag.Bool("v", false, "print every error with its timing")
	flag.Parse()

	var conns []kvv1.KVServiceClient
	for _, a := range strings.Split(*addrs, ",") {
		c, err := grpc.NewClient(a, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer func() { _ = c.Close() }()
		conns = append(conns, kvv1.NewKVServiceClient(c))
	}
	value := make([]byte, *valueSize)
	for i := range value {
		value[i] = byte('a' + i%26)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()
	runStart := time.Now()
	deadline := runStart.Add(*duration)
	var wg sync.WaitGroup
	var ops, errs atomic.Int64
	var errMu sync.Mutex
	errByCode := map[codes.Code]int{}
	lat := make([][]time.Duration, *clients)
	for i := range *clients {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(id)))
			for ctx.Err() == nil {
				ni := r.Intn(len(conns))
				cl := conns[ni]
				key := []byte(fmt.Sprintf("bench/%d", r.Intn(*keys)))
				op := *mode
				if op == "mixed" {
					if r.Intn(2) == 0 {
						op = "put"
					} else {
						op = "get"
					}
				}
				opCtx, opCancel := context.WithTimeout(ctx, 5*time.Second)
				start := time.Now()
				var err error
				switch op {
				case "put":
					_, err = cl.Put(opCtx, &kvv1.PutRequest{Key: key, Value: value})
				case "get":
					_, err = cl.Get(opCtx, &kvv1.GetRequest{Key: key, Consistency: kvv1.Consistency_CONSISTENCY_LINEARIZABLE})
				default:
					_, err = cl.Get(opCtx, &kvv1.GetRequest{Key: key, Consistency: kvv1.Consistency_CONSISTENCY_STALE})
				}
				opCancel()
				// gRPC rejects calls at the deadline slightly before the context's
				// timer fires, so anything finishing after the deadline is end-of-run.
				if ctx.Err() != nil || !time.Now().Before(deadline) {
					break
				}
				if err != nil {
					errs.Add(1)
					errMu.Lock()
					errByCode[status.Code(err)]++
					errMu.Unlock()
					if *verbose {
						fmt.Fprintf(os.Stderr, "t=%s client=%d node=%d op=%s took=%s err=%v\n", time.Since(runStart).Round(time.Millisecond), id, ni, op, time.Since(start).Round(time.Millisecond), err)
					}
					continue
				}
				ops.Add(1)
				lat[id] = append(lat[id], time.Since(start))
			}
		}(i)
	}
	wg.Wait()

	var all []time.Duration
	for _, l := range lat {
		all = append(all, l...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	pct := func(p float64) time.Duration {
		if len(all) == 0 {
			return 0
		}
		return all[int(float64(len(all)-1)*p)]
	}
	rate := float64(ops.Load()) / duration.Seconds()
	errText := fmt.Sprintf("%d", errs.Load())
	if errs.Load() > 0 {
		errText += fmt.Sprintf(" %v", errByCode)
	}
	if *markdown {
		fmt.Printf("| %s | %d | %d B | %.0f | %s | %s | %s | %s | %s |\n", *mode, *clients, *valueSize, rate,
			pct(0.5).Round(10*time.Microsecond), pct(0.95).Round(10*time.Microsecond), pct(0.99).Round(10*time.Microsecond), pct(1).Round(10*time.Microsecond), errText)
		return
	}
	fmt.Printf("mode=%s clients=%d value=%dB duration=%s\n", *mode, *clients, *valueSize, *duration)
	fmt.Printf("ops=%d errors=%s throughput=%.0f ops/s\n", ops.Load(), errText, rate)
	fmt.Printf("p50=%s p95=%s p99=%s max=%s\n", pct(0.5), pct(0.95), pct(0.99), pct(1))
}
