package api

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics are the API-level counters and histograms. They live on a registry
// owned by the server so several nodes can share one process in tests.
type Metrics struct {
	Proposals   *prometheus.CounterVec // result: ok | not_leader | not_applied | unknown | error
	ProposeTime prometheus.Histogram   // seconds from Apply to FSM response on the leader
	Reads       *prometheus.CounterVec // consistency, result
	LinearTime  prometheus.Histogram   // seconds spent in the read gate on the leader
	Forwarded   *prometheus.CounterVec // rpc
	ForwardTime prometheus.Histogram   // seconds for the forwarded call
}

// NewMetrics registers the API metrics on reg (nil = a throwaway registry).
func NewMetrics(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	buckets := prometheus.ExponentialBuckets(0.0002, 2, 16) // 200µs .. ~6.5s
	m := &Metrics{
		Proposals: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kv_proposals_total", Help: "Write proposals by result.",
		}, []string{"result"}),
		ProposeTime: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "kv_propose_duration_seconds", Help: "Leader-side time from Apply to FSM response.", Buckets: buckets,
		}),
		Reads: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kv_reads_total", Help: "Reads by consistency and result.",
		}, []string{"consistency", "result"}),
		LinearTime: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "kv_linearize_duration_seconds", Help: "Leader-side time in the linearizable read gate.", Buckets: buckets,
		}),
		Forwarded: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kv_forwarded_total", Help: "Requests forwarded to the leader by RPC.",
		}, []string{"rpc"}),
		ForwardTime: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "kv_forward_duration_seconds", Help: "Round trip of a forwarded call.", Buckets: buckets,
		}),
	}
	reg.MustRegister(m.Proposals, m.ProposeTime, m.Reads, m.LinearTime, m.Forwarded, m.ForwardTime)
	return m
}

func (m *Metrics) observeSince(h prometheus.Histogram, start time.Time) {
	h.Observe(time.Since(start).Seconds())
}
