package raftnode

import (
	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus"
)

// Collector exposes the raft view of the node as gauges. It reads live state
// on every scrape instead of keeping counters in sync.
func (n *Node) Collector() prometheus.Collector {
	return &collector{n: n,
		term:         prometheus.NewDesc("kv_raft_term", "Current raft term.", nil, nil),
		commit:       prometheus.NewDesc("kv_raft_commit_index", "Raft commit index.", nil, nil),
		applied:      prometheus.NewDesc("kv_fsm_applied_index", "Index applied by the key-value state machine.", nil, nil),
		lastLog:      prometheus.NewDesc("kv_raft_last_log_index", "Last index in the raft log.", nil, nil),
		lastSnapshot: prometheus.NewDesc("kv_raft_last_snapshot_index", "Index of the last snapshot.", nil, nil),
		isLeader:     prometheus.NewDesc("kv_raft_is_leader", "1 when this node is the leader.", nil, nil),
		state:        prometheus.NewDesc("kv_raft_state", "1 for the current raft state.", []string{"state"}, nil),
		keys:         prometheus.NewDesc("kv_fsm_keys", "Keys in the state machine.", nil, nil),
		blocked:      prometheus.NewDesc("kv_chaos_blocked_peers", "Peers blocked by the chaos layer.", nil, nil),
		frozen:       prometheus.NewDesc("kv_chaos_frozen", "1 when raft traffic is frozen by the chaos layer.", nil, nil),
	}
}

type collector struct {
	n                                                                                    *Node
	term, commit, applied, lastLog, lastSnapshot, isLeader, state, keys, blocked, frozen *prometheus.Desc
}

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{c.term, c.commit, c.applied, c.lastLog, c.lastSnapshot, c.isLeader, c.state, c.keys, c.blocked, c.frozen} {
		ch <- d
	}
}

func (c *collector) Collect(ch chan<- prometheus.Metric) {
	st := c.n.Status()
	gauge := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
	}
	gauge(c.term, float64(st.GetTerm()))
	gauge(c.commit, float64(st.GetCommitIndex()))
	gauge(c.applied, float64(st.GetAppliedIndex()))
	gauge(c.lastLog, float64(st.GetLastLogIndex()))
	gauge(c.lastSnapshot, float64(st.GetLastSnapshotIndex()))
	gauge(c.isLeader, b2f(st.GetState() == raft.Leader.String()))
	for _, s := range []raft.RaftState{raft.Follower, raft.Candidate, raft.Leader, raft.Shutdown} {
		gauge(c.state, b2f(st.GetState() == s.String()), s.String())
	}
	gauge(c.keys, float64(st.GetNumKeys()))
	gauge(c.blocked, float64(len(st.GetBlockedPeers())))
	gauge(c.frozen, b2f(st.GetFrozen()))
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
