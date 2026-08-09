package server

import (
	"fmt"
	"github.com/XUanhoa04/async-trace-doctor/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	"strings"
	"sync"
)

type Metrics struct {
	Audited               prometheus.Counter
	Violations            *prometheus.CounterVec
	Broken                prometheus.Counter
	OrphanProducers       prometheus.Counter
	OrphanConsumers       prometheus.Counter
	Completeness          prometheus.Gauge
	QueueLatency          prometheus.Histogram
	StateSpans            prometheus.Gauge
	Evictions             *prometheus.CounterVec
	Rejected              *prometheus.CounterVec
	DuplicateExports      prometheus.Counter
	ConflictingDuplicates prometheus.Counter
	AuditRuns             prometheus.Counter
	ActiveFindings        *prometheus.GaugeVec
	mu                    sync.Mutex
	activeFingerprints    map[string]bool
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Audited:               prometheus.NewCounter(prometheus.CounterOpts{Name: "async_trace_audited_spans_total", Help: "Unique spans admitted to live audit state."}),
		Violations:            prometheus.NewCounterVec(prometheus.CounterOpts{Name: "async_trace_violations_total", Help: "Unique finding fingerprints first observed by rule and severity."}, []string{"rule_id", "severity"}),
		Broken:                prometheus.NewCounter(prometheus.CounterOpts{Name: "async_trace_broken_links_total", Help: "Consumer spans without strong producer context."}),
		OrphanProducers:       prometheus.NewCounter(prometheus.CounterOpts{Name: "async_trace_orphan_producers_total", Help: "Producer spans without a correlated consumer."}),
		OrphanConsumers:       prometheus.NewCounter(prometheus.CounterOpts{Name: "async_trace_orphan_consumers_total", Help: "Consumer spans without a correlated producer."}),
		Completeness:          prometheus.NewGauge(prometheus.GaugeOpts{Name: "async_trace_context_completeness_ratio", Help: "Ratio of consumer spans correlated by link or parent."}),
		QueueLatency:          prometheus.NewHistogram(prometheus.HistogramOpts{Name: "async_trace_queue_latency_seconds", Help: "Observed correlated producer-to-consumer latency.", Buckets: prometheus.ExponentialBuckets(0.001, 4, 10)}),
		StateSpans:            prometheus.NewGauge(prometheus.GaugeOpts{Name: "async_trace_state_spans", Help: "Current spans retained in bounded state."}),
		Evictions:             prometheus.NewCounterVec(prometheus.CounterOpts{Name: "async_trace_state_evictions_total", Help: "State evictions by reason."}, []string{"reason"}),
		Rejected:              prometheus.NewCounterVec(prometheus.CounterOpts{Name: "async_trace_ingest_rejected_spans_total", Help: "OTLP spans rejected by bounded reason."}, []string{"reason"}),
		DuplicateExports:      prometheus.NewCounter(prometheus.CounterOpts{Name: "async_trace_duplicate_exports_total", Help: "Repeated exports of an identical trace/span identity."}),
		ConflictingDuplicates: prometheus.NewCounter(prometheus.CounterOpts{Name: "async_trace_conflicting_duplicates_total", Help: "Repeated trace/span identities with conflicting content."}),
		AuditRuns:             prometheus.NewCounter(prometheus.CounterOpts{Name: "async_trace_audit_runs_total", Help: "Completed audit snapshots."}),
		ActiveFindings:        prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "async_trace_active_findings", Help: "Findings in the latest audit snapshot by rule and severity."}, []string{"rule_id", "severity"}),
		activeFingerprints:    map[string]bool{},
	}
	reg.MustRegister(m.Audited, m.Violations, m.Broken, m.OrphanProducers, m.OrphanConsumers, m.Completeness, m.QueueLatency, m.StateSpans, m.Evictions, m.Rejected, m.DuplicateExports, m.ConflictingDuplicates, m.AuditRuns, m.ActiveFindings)
	return m
}
func (m *Metrics) Observe(r model.Report) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.AuditRuns.Inc()
	m.Completeness.Set(r.Summary.ContextCompletenessRatio)
	m.ActiveFindings.Reset()
	active := map[string]float64{}
	currentFingerprints := map[string]bool{}
	for _, f := range r.Findings {
		labelKey := f.RuleID + "\x00" + f.Severity
		active[labelKey]++
		fingerprint := fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s|%s|%s", f.RuleID, strings.Join(f.TraceIDs, ","), strings.Join(f.SpanIDs, ","), f.ProducerService, f.ConsumerService, f.MessagingSystem, f.Destination, f.ConsumerGroup, f.Subscription)
		currentFingerprints[fingerprint] = true
		if !m.activeFingerprints[fingerprint] {
			m.Violations.WithLabelValues(f.RuleID, f.Severity).Inc()
			switch f.RuleID {
			case "ATD-CTX-001":
				m.Broken.Inc()
			case "ATD-COR-001":
				m.OrphanProducers.Inc()
			case "ATD-COR-002":
				m.OrphanConsumers.Inc()
			}
		}
	}
	m.activeFingerprints = currentFingerprints
	for key, count := range active {
		parts := strings.Split(key, "\x00")
		m.ActiveFindings.WithLabelValues(parts[0], parts[1]).Set(count)
	}
	for _, seconds := range r.QueueLatencySamples {
		if seconds >= 0 {
			m.QueueLatency.Observe(seconds)
		}
	}
}
