package server

import (
	"time"

	"github.com/XUanhoa04/async-trace-doctor/internal/model"
	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	Audited         prometheus.Counter
	Violations      *prometheus.CounterVec
	Broken          prometheus.Counter
	OrphanProducers prometheus.Counter
	OrphanConsumers prometheus.Counter
	Completeness    prometheus.Gauge
	QueueLatency    prometheus.Histogram
	StateSpans      prometheus.Gauge
	Evictions       *prometheus.CounterVec
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Audited:         prometheus.NewCounter(prometheus.CounterOpts{Name: "async_trace_audited_spans_total", Help: "Total spans processed by completed audit windows."}),
		Violations:      prometheus.NewCounterVec(prometheus.CounterOpts{Name: "async_trace_violations_total", Help: "Detected violations by bounded rule and severity."}, []string{"rule_id", "severity"}),
		Broken:          prometheus.NewCounter(prometheus.CounterOpts{Name: "async_trace_broken_links_total", Help: "Consumer spans without strong producer context."}),
		OrphanProducers: prometheus.NewCounter(prometheus.CounterOpts{Name: "async_trace_orphan_producers_total", Help: "Producer spans without a correlated consumer."}),
		OrphanConsumers: prometheus.NewCounter(prometheus.CounterOpts{Name: "async_trace_orphan_consumers_total", Help: "Consumer spans without a correlated producer."}),
		Completeness:    prometheus.NewGauge(prometheus.GaugeOpts{Name: "async_trace_context_completeness_ratio", Help: "Ratio of consumer spans correlated by link or parent."}),
		QueueLatency:    prometheus.NewHistogram(prometheus.HistogramOpts{Name: "async_trace_queue_latency_seconds", Help: "Observed correlated producer-to-consumer latency.", Buckets: prometheus.ExponentialBuckets(0.001, 4, 10)}),
		StateSpans:      prometheus.NewGauge(prometheus.GaugeOpts{Name: "async_trace_state_spans", Help: "Current spans retained in bounded state."}),
		Evictions:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: "async_trace_state_evictions_total", Help: "State evictions by reason."}, []string{"reason"}),
	}
	reg.MustRegister(m.Audited, m.Violations, m.Broken, m.OrphanProducers, m.OrphanConsumers, m.Completeness, m.QueueLatency, m.StateSpans, m.Evictions)
	return m
}
func (m *Metrics) Observe(r model.Report) {
	m.Audited.Add(float64(r.Summary.AuditedSpans))
	m.Broken.Add(float64(r.Summary.BrokenLinks))
	m.OrphanProducers.Add(float64(r.Summary.OrphanProducers))
	m.OrphanConsumers.Add(float64(r.Summary.OrphanConsumers))
	m.Completeness.Set(r.Summary.ContextCompletenessRatio)
	for _, f := range r.Findings {
		m.Violations.WithLabelValues(f.RuleID, f.Severity).Inc()
		if raw, ok := f.Evidence["queue_latency"].(string); ok {
			if d, err := time.ParseDuration(raw); err == nil {
				m.QueueLatency.Observe(d.Seconds())
			}
		}
	}
}
