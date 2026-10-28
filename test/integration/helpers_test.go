package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	kvv1 "github.com/Ehsan-saradar/euler-phi-exercise-d33/gen/kv/v1"
	"github.com/Ehsan-saradar/euler-phi-exercise-d33/internal/cluster"
)

func ctxT(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}

func put(t *testing.T, m *cluster.Member, key, value string) {
	t.Helper()
	ctx, cancel := ctxT(5 * time.Second)
	defer cancel()
	if _, err := m.KV().Put(ctx, &kvv1.PutRequest{Key: []byte(key), Value: []byte(value)}); err != nil {
		t.Fatalf("put %s on %s: %v", key, m.ID, err)
	}
}

func get(t *testing.T, m *cluster.Member, key string, c kvv1.Consistency) *kvv1.GetResponse {
	t.Helper()
	ctx, cancel := ctxT(5 * time.Second)
	defer cancel()
	resp, err := m.KV().Get(ctx, &kvv1.GetRequest{Key: []byte(key), Consistency: c})
	if err != nil {
		t.Fatalf("get %s on %s: %v", key, m.ID, err)
	}
	return resp
}

func expectValue(t *testing.T, m *cluster.Member, key, want string) {
	t.Helper()
	resp := get(t, m, key, kvv1.Consistency_CONSISTENCY_LINEARIZABLE)
	if !resp.GetFound() || string(resp.GetKv().GetValue()) != want {
		t.Fatalf("on %s key %s = (%v, %q), want %q", m.ID, key, resp.GetFound(), resp.GetKv().GetValue(), want)
	}
}

func writeKeys(t *testing.T, m *cluster.Member, prefix string, n int) {
	t.Helper()
	for i := range n {
		put(t, m, fmt.Sprintf("%s/%04d", prefix, i), fmt.Sprintf("v%d", i))
	}
}

func expectKeys(t *testing.T, m *cluster.Member, prefix string, n int) {
	t.Helper()
	for i := range n {
		expectValue(t, m, fmt.Sprintf("%s/%04d", prefix, i), fmt.Sprintf("v%d", i))
	}
}

func code(err error) codes.Code { return status.Code(err) }
