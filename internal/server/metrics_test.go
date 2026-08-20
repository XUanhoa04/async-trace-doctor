package server

import (
	"testing"

	"github.com/XUanhoa04/async-trace-doctor/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestNewMetricsRegistration(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	if m == nil {
		t.Fatal("NewMetrics returned nil")
	}

	// Verify all metrics are registered by gathering them
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather registered metrics: %v", err)
	}
	if len(mfs) == 0 {
		t.Fatal("expected registered metrics in registry, got none")
	}
}

func TestMetricsObserveCalculations(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	r1 := model.Report{
		Summary: model.Summary{
			ContextCompletenessRatio: 0.75,
		},
		Findings: []model.Finding{
			{
				RuleID:          "ATD-CTX-001",
				Severity:        "error",
				ProducerService: "pub",
				ConsumerService: "sub",
				MessagingSystem: "kafka",
				Destination:     "orders",
				TraceIDs:        []string{"t1"},
				SpanIDs:         []string{"s1"},
			},
			{
				RuleID:          "ATD-COR-001",
				Severity:        "warning",
				ProducerService: "pub",
				MessagingSystem: "kafka",
				Destination:     "orders",
				TraceIDs:        []string{"t2"},
				SpanIDs:         []string{"s2"},
			},
			{
				RuleID:          "ATD-COR-002",
				Severity:        "warning",
				ConsumerService: "sub",
				MessagingSystem: "kafka",
				Destination:     "orders",
				TraceIDs:        []string{"t3"},
				SpanIDs:         []string{"s3"},
			},
		},
		QueueLatencySamples: []float64{0.125, -0.05, 0.450}, // Negative should be ignored
	}

	m.Observe(r1)

	// Verify Completeness gauge
	if val := getGaugeValue(t, m.Completeness); val != 0.75 {
		t.Errorf("Completeness = %v, want 0.75", val)
	}

	// Verify Broken counter for ATD-CTX-001
	if val := getCounterValue(t, m.Broken); val != 1 {
		t.Errorf("Broken = %v, want 1", val)
	}

	// Verify OrphanProducers for ATD-COR-001
	if val := getCounterValue(t, m.OrphanProducers); val != 1 {
		t.Errorf("OrphanProducers = %v, want 1", val)
	}

	// Verify OrphanConsumers for ATD-COR-002
	if val := getCounterValue(t, m.OrphanConsumers); val != 1 {
		t.Errorf("OrphanConsumers = %v, want 1", val)
	}

	// Verify AuditRuns incremented
	if val := getCounterValue(t, m.AuditRuns); val != 1 {
		t.Errorf("AuditRuns = %v, want 1", val)
	}

	// Verify ActiveFindings gauge
	activeGauge, err := m.ActiveFindings.GetMetricWithLabelValues("ATD-CTX-001", "error")
	if err != nil {
		t.Fatalf("failed to get active findings metric: %v", err)
	}
	if val := getGaugeValue(t, activeGauge); val != 1 {
		t.Errorf("ActiveFindings(ATD-CTX-001, error) = %v, want 1", val)
	}

	// Test second observation with identical findings: violations should not re-increment
	m.Observe(r1)
	if val := getCounterValue(t, m.Broken); val != 1 {
		t.Errorf("Broken after repeated observation = %v, want 1 (deduplicated)", val)
	}
	if val := getCounterValue(t, m.AuditRuns); val != 2 {
		t.Errorf("AuditRuns = %v, want 2", val)
	}

	// Test third observation with resolved findings (empty findings): active findings should reset
	rEmpty := model.Report{
		Summary: model.Summary{
			ContextCompletenessRatio: 1.0,
		},
		Findings: []model.Finding{},
	}
	m.Observe(rEmpty)
	if val := getGaugeValue(t, m.Completeness); val != 1.0 {
		t.Errorf("Completeness = %v, want 1.0", val)
	}
}

func getGaugeValue(t *testing.T, g prometheus.Gauge) float64 {
	t.Helper()
	var metric dto.Metric
	if err := g.Write(&metric); err != nil {
		t.Fatalf("failed to read gauge metric: %v", err)
	}
	return metric.GetGauge().GetValue()
}

func getCounterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var metric dto.Metric
	if err := c.Write(&metric); err != nil {
		t.Fatalf("failed to read counter metric: %v", err)
	}
	return metric.GetCounter().GetValue()
}
