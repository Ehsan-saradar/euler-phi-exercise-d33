package integration

import (
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	kvv1 "github.com/Ehsan-saradar/euler-phi-exercise-d33/gen/kv/v1"
	"github.com/Ehsan-saradar/euler-phi-exercise-d33/internal/cluster"
	"github.com/Ehsan-saradar/euler-phi-exercise-d33/internal/raftnode"
)

// Core: a 3-node cluster accepts writes on any node and linearizable reads on
// any node see them.
func TestWriteAnywhereReadAnywhere(t *testing.T) {
	c := cluster.New(t, cluster.Options{InMemory: true})
	for i, m := range c.Members {
		put(t, m, "k", "v"+m.ID)
		for _, r := range c.Members {
			expectValue(t, r, "k", "v"+m.ID)
		}
		if i == 0 {
			resp := get(t, m, "k", kvv1.Consistency_CONSISTENCY_LINEARIZABLE)
			if resp.GetHeader().GetLeaderId() == "" || resp.GetHeader().GetRaftIndex() == 0 {
				t.Fatalf("header not populated: %v", resp.GetHeader())
			}
		}
	}

	// Stale reads are served locally and eventually converge.
	deadline := time.Now().Add(2 * time.Second)
	for _, m := range c.Members {
		for {
			resp := get(t, m, "k", kvv1.Consistency_CONSISTENCY_STALE)
			if string(resp.GetKv().GetValue()) == "vnode3" {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("stale read on %s never converged", m.ID)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func TestDeleteAndCompareAndSwap(t *testing.T) {
	c := cluster.New(t, cluster.Options{InMemory: true})
	f := c.Followers(c.Leader(time.Second))[0] // exercise forwarding
	ctx, cancel := ctxT(5 * time.Second)
	defer cancel()

	created, err := f.KV().CompareAndSwap(ctx, &kvv1.CompareAndSwapRequest{Key: []byte("cfg"), ExpectedVersion: 0, Value: []byte("a")})
	if err != nil || !created.GetSucceeded() {
		t.Fatalf("create-only cas: %v %v", created, err)
	}
	stale, err := f.KV().CompareAndSwap(ctx, &kvv1.CompareAndSwapRequest{Key: []byte("cfg"), ExpectedVersion: 7, Value: []byte("b")})
	if err != nil || stale.GetSucceeded() || string(stale.GetCurrent().GetValue()) != "a" {
		t.Fatalf("stale cas: %v %v", stale, err)
	}
	ok, err := f.KV().CompareAndSwap(ctx, &kvv1.CompareAndSwapRequest{Key: []byte("cfg"), ExpectedVersion: 1, Value: []byte("b")})
	if err != nil || !ok.GetSucceeded() || ok.GetCurrent().GetVersion() != 2 {
		t.Fatalf("cas: %v %v", ok, err)
	}
	del, err := f.KV().Delete(ctx, &kvv1.DeleteRequest{Key: []byte("cfg")})
	if err != nil || !del.GetDeleted() {
		t.Fatalf("delete: %v %v", del, err)
	}
	if resp := get(t, f, "cfg", kvv1.Consistency_CONSISTENCY_LINEARIZABLE); resp.GetFound() {
		t.Fatal("key visible after delete")
	}
}

func TestRangePaginationThroughFollower(t *testing.T) {
	c := cluster.New(t, cluster.Options{InMemory: true})
	f := c.Followers(c.Leader(time.Second))[0]
	writeKeys(t, f, "svc", 25)
	put(t, f, "other", "x")

	ctx, cancel := ctxT(5 * time.Second)
	defer cancel()
	var seen []string
	var after []byte
	for {
		resp, err := f.KV().Range(ctx, &kvv1.RangeRequest{Prefix: []byte("svc/"), StartAfter: after, Limit: 10})
		if err != nil {
			t.Fatal(err)
		}
		for _, kv := range resp.GetKvs() {
			seen = append(seen, string(kv.GetKey()))
		}
		if !resp.GetMore() {
			break
		}
		after = resp.GetKvs()[len(resp.GetKvs())-1].GetKey()
	}
	if len(seen) != 25 || seen[0] != "svc/0000" || seen[24] != "svc/0024" {
		t.Fatalf("paged keys = %v", seen)
	}
}

func TestInvalidArguments(t *testing.T) {
	c := cluster.New(t, cluster.Options{InMemory: true, Nodes: 1})
	ctx, cancel := ctxT(2 * time.Second)
	defer cancel()
	_, err := c.Members[0].KV().Put(ctx, &kvv1.PutRequest{Key: nil, Value: []byte("v")})
	if code(err) != codes.InvalidArgument {
		t.Fatalf("empty key: %v", err)
	}
}

// Senior: killing the leader loses no acknowledged write.
func TestLeaderFailoverKeepsAcknowledgedWrites(t *testing.T) {
	for _, mode := range []raftnode.ReadMode{raftnode.ReadModeReadIndex, raftnode.ReadModeBarrier} {
		t.Run(string(mode), func(t *testing.T) {
			c := cluster.New(t, cluster.Options{ReadMode: mode})
			leader := c.Leader(time.Second)
			writeKeys(t, leader, "before", 50)

			leader.Stop()
			survivors := c.Followers(leader)
			newLeader, err := c.LeaderAmong(5*time.Second, survivors)
			if err != nil {
				t.Fatal(err)
			}
			// The very first linearizable read on the new leader must already
			// reflect every write the old leader acknowledged (Raft §8).
			expectKeys(t, newLeader, "before", 50)
			for _, s := range survivors {
				expectKeys(t, s, "before", 50)
			}
			writeKeys(t, survivors[1], "after", 20)

			leader.Restart()
			c.Leader(5 * time.Second)
			expectKeys(t, leader, "before", 50)
			expectKeys(t, leader, "after", 20)
		})
	}
}

// Senior: a node restarted from disk comes back with all committed data.
func TestRestartFromDisk(t *testing.T) {
	c := cluster.New(t, cluster.Options{})
	writeKeys(t, c.Members[0], "persist", 30)
	for _, m := range c.Members {
		m.Stop()
	}
	for _, m := range c.Members {
		m.Restart()
	}
	c.Leader(5 * time.Second)
	expectKeys(t, c.Members[2], "persist", 30)
}
