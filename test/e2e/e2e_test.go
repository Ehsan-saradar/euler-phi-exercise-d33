//go:build e2e

// Package e2e drives the docker compose cluster from the outside: real
// container kills and pauses, plus app-level partitions. Run with
//
//	docker compose up -d --build --wait && go test -tags e2e ./test/e2e/
package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	kvv1 "github.com/Ehsan-saradar/euler-phi-exercise-d33/gen/kv/v1"
)

type node struct {
	id    string
	addr  string
	kv    kvv1.KVServiceClient
	admin kvv1.AdminServiceClient
}

func compose(t *testing.T, args ...string) {
	t.Helper()
	cmd := exec.Command("docker", append([]string{"compose"}, args...)...)
	cmd.Dir = filepath.Join("..", "..")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("docker compose %v: %v\n%s", args, err, out)
	}
}

func nodes(t *testing.T) []*node {
	t.Helper()
	addrs := os.Getenv("E2E_ADDRS")
	if addrs == "" {
		addrs = "localhost:7001,localhost:7002,localhost:7003"
	}
	var out []*node
	for i, a := range strings.Split(addrs, ",") {
		conn, err := grpc.NewClient(a, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		out = append(out, &node{id: fmt.Sprintf("node%d", i+1), addr: a, kv: kvv1.NewKVServiceClient(conn), admin: kvv1.NewAdminServiceClient(conn)})
	}
	return out
}

func leader(t *testing.T, ns []*node, timeout time.Duration) *node {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range ns {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			st, err := n.admin.Status(ctx, &kvv1.StatusRequest{})
			cancel()
			if err == nil && st.GetState() == "Leader" {
				return n
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("no leader")
	return nil
}

func put(t *testing.T, n *node, k, v string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := n.kv.Put(ctx, &kvv1.PutRequest{Key: []byte(k), Value: []byte(v)}); err != nil {
		t.Fatalf("put %s on %s: %v", k, n.id, err)
	}
}

func expect(t *testing.T, n *node, k, v string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		resp, err := n.kv.Get(ctx, &kvv1.GetRequest{Key: []byte(k)})
		cancel()
		if err == nil {
			if string(resp.GetKv().GetValue()) != v {
				t.Fatalf("on %s key %s = %q, want %q", n.id, k, resp.GetKv().GetValue(), v)
			}
			return
		}
		last = err
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("get %s on %s kept failing: %v", k, n.id, last)
}

func others(ns []*node, l *node) []*node {
	var out []*node
	for _, n := range ns {
		if n != l {
			out = append(out, n)
		}
	}
	return out
}

func TestE2E(t *testing.T) {
	ns := nodes(t)
	leader(t, ns, 30*time.Second)

	t.Run("write anywhere read anywhere", func(t *testing.T) {
		put(t, ns[0], "e2e/a", "1")
		for _, n := range ns {
			expect(t, n, "e2e/a", "1")
		}
	})

	t.Run("kill leader keeps acknowledged writes", func(t *testing.T) {
		l := leader(t, ns, 10*time.Second)
		for i := range 20 {
			put(t, l, fmt.Sprintf("e2e/k%d", i), "v")
		}
		compose(t, "kill", l.id)
		rest := others(ns, l)
		nl := leader(t, rest, 20*time.Second)
		for i := range 20 {
			expect(t, nl, fmt.Sprintf("e2e/k%d", i), "v")
		}
		put(t, rest[1], "e2e/after", "ok")
		compose(t, "start", l.id)
		expect(t, l, "e2e/after", "ok")
	})

	t.Run("minority partition refuses writes", func(t *testing.T) {
		l := leader(t, ns, 10*time.Second)
		rest := others(ns, l)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := l.admin.Partition(ctx, &kvv1.PartitionRequest{Block: []string{rest[0].id, rest[1].id}}); err != nil {
			t.Fatal(err)
		}
		for _, r := range rest {
			if _, err := r.admin.Partition(ctx, &kvv1.PartitionRequest{Block: []string{l.id}}); err != nil {
				t.Fatal(err)
			}
		}
		wctx, wcancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, err := l.kv.Put(wctx, &kvv1.PutRequest{Key: []byte("e2e/minority"), Value: []byte("x")})
		wcancel()
		if c := status.Code(err); c != codes.Unavailable && c != codes.DeadlineExceeded {
			t.Fatalf("minority write returned %v", err)
		}
		nl := leader(t, rest, 20*time.Second)
		put(t, nl, "e2e/majority", "yes")
		for _, n := range ns {
			hctx, hcancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, _ = n.admin.Partition(hctx, &kvv1.PartitionRequest{Clear: true})
			hcancel()
		}
		expect(t, l, "e2e/majority", "yes")
		gctx, gcancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer gcancel()
		if resp, err := l.kv.Get(gctx, &kvv1.GetRequest{Key: []byte("e2e/minority")}); err != nil || resp.GetFound() {
			t.Fatalf("refused write visible after heal: %v %v", resp, err)
		}
	})

	t.Run("paused follower catches up", func(t *testing.T) {
		l := leader(t, ns, 10*time.Second)
		f := others(ns, l)[0]
		compose(t, "pause", f.id)
		for i := range 10 {
			put(t, l, fmt.Sprintf("e2e/p%d", i), "v")
		}
		compose(t, "unpause", f.id)
		for i := range 10 {
			expect(t, f, fmt.Sprintf("e2e/p%d", i), "v")
		}
	})
}
