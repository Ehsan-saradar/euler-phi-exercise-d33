package linearizability

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/Ehsan-saradar/euler-phi-exercise-d33/internal/cluster"
)

// Fault is one injected failure.
type Fault struct {
	Kind   string
	Target []string
	Start  time.Duration // since recorder start
	End    time.Duration
}

// Nemesis injects random faults into an in-process cluster.
type Nemesis struct {
	c      *cluster.Cluster
	r      *rand.Rand
	t      testing.TB
	Faults []Fault
	since  time.Time
	Hold   time.Duration // how long a fault stays active
	Quiet  time.Duration // pause between faults
}

// NewNemesis builds a nemesis over c seeded with r.
func NewNemesis(t testing.TB, c *cluster.Cluster, r *rand.Rand, since time.Time) *Nemesis {
	return &Nemesis{c: c, r: r, t: t, since: since, Hold: 1500 * time.Millisecond, Quiet: 1000 * time.Millisecond}
}

// Run injects faults until ctx ends, then heals everything.
func (n *Nemesis) Run(ctx context.Context) {
	defer n.healAll()
	kinds := []string{"kill-leader", "kill-random", "isolate-leader", "split-random", "freeze-leader", "freeze-random"}
	for ctx.Err() == nil {
		kind := kinds[n.r.Intn(len(kinds))]
		f := Fault{Kind: kind, Start: time.Since(n.since)}
		heal := n.inject(kind, &f)
		n.sleep(ctx, n.Hold)
		heal()
		f.End = time.Since(n.since)
		n.Faults = append(n.Faults, f)
		n.t.Logf("nemesis: %-14s %v  (%s..%s)", f.Kind, f.Target, f.Start.Round(time.Millisecond), f.End.Round(time.Millisecond))
		n.sleep(ctx, n.Quiet)
	}
}

func (n *Nemesis) sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

func (n *Nemesis) running() []*cluster.Member {
	var out []*cluster.Member
	for _, m := range n.c.Members {
		if m.Running() {
			out = append(out, m)
		}
	}
	return out
}

func (n *Nemesis) leaderOrRandom(wantLeader bool) *cluster.Member {
	if wantLeader {
		if m, err := n.c.LeaderAmong(2*time.Second, n.running()); err == nil {
			return m
		}
	}
	running := n.running()
	return running[n.r.Intn(len(running))]
}

func (n *Nemesis) inject(kind string, f *Fault) (heal func()) {
	switch kind {
	case "kill-leader", "kill-random":
		m := n.leaderOrRandom(kind == "kill-leader")
		f.Target = []string{m.ID}
		m.Stop()
		return m.Restart
	case "isolate-leader":
		m := n.leaderOrRandom(true)
		f.Target = []string{m.ID}
		others := n.c.IDs(func(x *cluster.Member) bool { return x.ID != m.ID })
		n.c.Partition([]string{m.ID}, others)
		return n.c.Heal
	case "split-random":
		ids := n.c.IDs(nil)
		n.r.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
		cut := 1 + n.r.Intn(len(ids)-1)
		f.Target = []string{fmt.Sprintf("%v|%v", ids[:cut], ids[cut:])}
		n.c.Partition(ids[:cut], ids[cut:])
		return n.c.Heal
	default: // freeze-leader, freeze-random
		m := n.leaderOrRandom(kind == "freeze-leader")
		f.Target = []string{m.ID}
		m.Freeze()
		return m.Thaw
	}
}

func (n *Nemesis) healAll() {
	n.c.Heal()
	for _, m := range n.c.Members {
		if !m.Running() {
			m.Restart()
		}
	}
}
