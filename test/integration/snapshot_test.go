package integration

import (
	"context"
	"testing"
	"time"

	kvv1 "github.com/Ehsan-saradar/euler-phi-exercise-d33/gen/kv/v1"
	"github.com/Ehsan-saradar/euler-phi-exercise-d33/internal/cluster"
)

func nodeStatus(t *testing.T, m *cluster.Member) *kvv1.StatusResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	st, err := m.Admin().Status(ctx, &kvv1.StatusRequest{})
	if err != nil {
		t.Fatal(err)
	}
	return st
}

// Senior: snapshots are taken past the threshold, the log is compacted, and a
// node that fell behind the compaction point is restored from a snapshot.
func TestSnapshotCompactionAndCatchUp(t *testing.T) {
	c := cluster.New(t, cluster.Options{SnapshotThreshold: 64, TrailingLogs: 8})
	leader := c.Leader(time.Second)
	lagging := c.Followers(leader)[0]

	lagging.Stop()
	writeKeys(t, leader, "bulk", 200) // well past the threshold while one node is down

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	snap, err := leader.Admin().Snapshot(ctx, &kvv1.SnapshotRequest{})
	if err != nil {
		t.Fatalf("force snapshot: %v", err)
	}
	st := nodeStatus(t, leader)
	if st.GetLastSnapshotIndex() == 0 || snap.GetIndex() == 0 {
		t.Fatalf("no snapshot recorded: %v", st)
	}
	// Compaction keeps only TrailingLogs entries behind the snapshot, so the
	// lagging node's next index was discarded and must come from a snapshot.
	lagging.Restart()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if s := nodeStatus(t, lagging); s.GetLastSnapshotIndex() >= snap.GetIndex()-8 && s.GetNumKeys() == 200 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("lagging node did not restore from snapshot: %v", nodeStatus(t, lagging))
		}
		time.Sleep(50 * time.Millisecond)
	}
	expectKeys(t, lagging, "bulk", 200)

	// A restart from disk after compaction rebuilds state from snapshot + log tail.
	writeKeys(t, leader, "tail", 5)
	lagging.Stop()
	lagging.Restart()
	c.Leader(5 * time.Second)
	expectKeys(t, lagging, "bulk", 200)
	expectKeys(t, lagging, "tail", 5)
}

// Membership: a fourth node joins at runtime and serves reads.
func TestJoinNewNode(t *testing.T) {
	c := cluster.New(t, cluster.Options{InMemory: true})
	writeKeys(t, c.Members[0], "pre", 10)
	n4 := c.AddMember()
	expectKeys(t, n4, "pre", 10)
	put(t, n4, "via4", "ok")
	expectValue(t, c.Members[1], "via4", "ok")
	if st := nodeStatus(t, n4); len(st.GetPeers()) != 4 {
		t.Fatalf("peers = %v", st.GetPeers())
	}
}
