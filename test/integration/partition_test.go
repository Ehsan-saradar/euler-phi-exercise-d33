package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	kvv1 "github.com/Ehsan-saradar/euler-phi-exercise-d33/gen/kv/v1"
	"github.com/Ehsan-saradar/euler-phi-exercise-d33/internal/cluster"
)

// Senior: the minority side of a partition refuses writes with an explicit
// error, the majority keeps working, and after healing the minority converges
// without any of its refused writes appearing.
func TestMinorityPartitionRefusesWrites(t *testing.T) {
	c := cluster.New(t, cluster.Options{InMemory: true, LeaderWait: 200 * time.Millisecond})
	oldLeader := c.Leader(time.Second)
	majority := c.Followers(oldLeader)
	put(t, oldLeader, "shared", "v0")

	c.Partition([]string{oldLeader.ID}, []string{majority[0].ID, majority[1].ID})

	// Minority: every write attempt must fail; none may be acknowledged.
	for i := range 10 {
		ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
		_, err := oldLeader.KV().Put(ctx, &kvv1.PutRequest{Key: []byte(fmt.Sprintf("minority/%d", i)), Value: []byte("x")})
		cancel()
		switch code(err) {
		case codes.Unavailable, codes.DeadlineExceeded:
		default:
			t.Fatalf("write %d on the minority returned %v, want Unavailable or DeadlineExceeded", i, err)
		}
	}
	// Minority: linearizable reads must fail too, stale reads still answer.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	_, err := oldLeader.KV().Get(ctx, &kvv1.GetRequest{Key: []byte("shared")})
	cancel()
	if code(err) != codes.Unavailable && code(err) != codes.DeadlineExceeded {
		t.Fatalf("linearizable read on the minority returned %v", err)
	}
	if resp := get(t, oldLeader, "shared", kvv1.Consistency_CONSISTENCY_STALE); string(resp.GetKv().GetValue()) != "v0" {
		t.Fatalf("stale read on minority = %v", resp)
	}
	if oldLeader.Node().IsLeader() {
		t.Fatal("isolated leader did not step down after losing its lease")
	}

	// Majority: elects a new leader and serves reads and writes.
	newLeader, err := c.LeaderAmong(5*time.Second, majority)
	if err != nil {
		t.Fatal(err)
	}
	put(t, newLeader, "shared", "v1")
	writeKeys(t, majority[1], "majority", 20)
	expectValue(t, majority[0], "shared", "v1")

	// Heal: the old leader rejoins as a follower and converges.
	c.Heal()
	expectValue(t, oldLeader, "shared", "v1")
	expectKeys(t, oldLeader, "majority", 20)
	for i := range 10 {
		if resp := get(t, oldLeader, fmt.Sprintf("minority/%d", i), kvv1.Consistency_CONSISTENCY_LINEARIZABLE); resp.GetFound() {
			t.Fatalf("refused minority write %d became visible after heal", i)
		}
	}
}

// A follower cut off from the leader forwards to nobody and answers
// Unavailable instead of hanging or serving a possibly stale value.
func TestIsolatedFollowerAnswersUnavailable(t *testing.T) {
	c := cluster.New(t, cluster.Options{InMemory: true, LeaderWait: 200 * time.Millisecond})
	leader := c.Leader(time.Second)
	isolated := c.Followers(leader)[0]
	others := c.IDs(func(m *cluster.Member) bool { return m.ID != isolated.ID })
	put(t, leader, "k", "v0")

	c.Partition([]string{isolated.ID}, others)
	deadline := time.Now().Add(3 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := isolated.KV().Get(ctx, &kvv1.GetRequest{Key: []byte("k")})
		cancel()
		if code(err) == codes.Unavailable {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("isolated follower kept answering linearizable reads: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	put(t, leader, "k", "v1")
	c.Heal()
	expectValue(t, isolated, "k", "v1")
}
