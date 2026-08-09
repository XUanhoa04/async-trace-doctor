package rules

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/XUanhoa04/async-trace-doctor/internal/config"
	"github.com/XUanhoa04/async-trace-doctor/internal/ingest"
	"github.com/XUanhoa04/async-trace-doctor/internal/model"
)

func TestGoldenScenarios(t *testing.T) {
	tests := []struct {
		name, file string
		want       map[string]bool
	}{{"normal", "normal.json", map[string]bool{}}, {"broken", "broken-context.json", map[string]bool{"ATD-CTX-001": true}}, {"batch", "batch-incomplete.json", map[string]bool{"ATD-BAT-001": true, "ATD-COR-001": true}}, {"duplicate", "duplicate.json", map[string]bool{"ATD-DUP-001": true}}}
	cfg, err := config.Load(filepath.Join("..", "..", "config", "rules.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spans, err := ingest.ReadPath(filepath.Join("..", "..", "testdata", "core", tt.file), ingest.Limits{MaxBytes: 1 << 20, MaxSpans: 100}, cfg.RedactAttributes)
			if err != nil {
				t.Fatal(err)
			}
			r := Engine{Config: cfg}.Audit(spans)
			got := map[string]bool{}
			for _, f := range r.Findings {
				got[f.RuleID] = true
			}
			for id := range tt.want {
				if !got[id] {
					t.Errorf("missing %s; got %#v", id, got)
				}
			}
			if tt.name == "normal" && len(r.Findings) != 0 {
				t.Errorf("normal traffic findings: %#v", r.Findings)
			}
		})
	}
}

func TestFailedRetriesAreNotDuplicateProcessing(t *testing.T) {
	cfg := testConfig(t)
	now := time.Now()
	spans := []model.Span{
		processSpan("retry-1", "a", "workers", "ERROR", now),
		processSpan("retry-1", "b", "workers", "ERROR", now.Add(time.Second)),
		processSpan("retry-1", "c", "workers", "UNSET", now.Add(2*time.Second)),
	}
	report := Engine{Config: cfg}.Audit(spans)
	assertNoRule(t, report, "ATD-DUP-001")
}

func TestConsumerGroupsAreIndependentForDuplicates(t *testing.T) {
	cfg := testConfig(t)
	now := time.Now()
	spans := []model.Span{
		processSpan("fanout-1", "a1", "group-a", "UNSET", now),
		processSpan("fanout-1", "a2", "group-a", "UNSET", now.Add(time.Second)),
		processSpan("fanout-1", "b1", "group-b", "UNSET", now.Add(2*time.Second)),
		processSpan("fanout-1", "b2", "group-b", "UNSET", now.Add(3*time.Second)),
	}
	report := Engine{Config: cfg}.Audit(spans)
	assertNoRule(t, report, "ATD-DUP-001")
}

func TestDuplicateExportOfSameSpanIsNotDuplicateProcessing(t *testing.T) {
	cfg := testConfig(t)
	span := processSpan("export-1", "same", "workers", "UNSET", time.Now())
	report := Engine{Config: cfg}.Audit([]model.Span{span, span, span})
	assertNoRule(t, report, "ATD-DUP-001")
}

func TestClockSkewFindingAndSignedEvidence(t *testing.T) {
	cfg := testConfig(t)
	now := time.Now()
	p := messagingSpan("p", "producer", "send", now, now.Add(10*time.Second))
	c := messagingSpan("c", "consumer", "process", now.Add(2*time.Second), now.Add(3*time.Second))
	c.Links = []model.Link{{TraceID: p.TraceID, SpanID: p.SpanID}}
	report := Engine{Config: cfg}.Audit([]model.Span{p, c})
	if finding := findRule(report, "ATD-TIM-001"); finding == nil || finding.Evidence["observed_latency"] != "-8s" {
		t.Fatalf("missing signed clock-skew finding: %#v", report.Findings)
	}
}

func TestNonMessagingNameDoesNotTriggerMissingSystem(t *testing.T) {
	cfg := testConfig(t)
	spans := []model.Span{{TraceID: "http", SpanID: "http", Name: "publish_article", Kind: "CLIENT", Service: "web"}, {TraceID: "db", SpanID: "db", Name: "process_payment_row", Kind: "INTERNAL", Service: "billing"}}
	report := Engine{Config: cfg}.Audit(spans)
	assertNoRule(t, report, "ATD-SEM-002")
}

func TestLiveAuditUsesEventWatermarkWhileOfflineIsFinalized(t *testing.T) {
	cfg := testConfig(t)
	old := time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)
	p := messagingSpan("p", "producer", "send", old, old.Add(time.Second))
	engine := Engine{Config: cfg}
	assertNoRule(t, engine.AuditWindow([]model.Span{p}), "ATD-COR-001")
	if findRule(engine.Audit([]model.Span{p}), "ATD-COR-001") == nil {
		t.Fatal("finalized offline input must close unmatched spans")
	}
	watermark := model.Span{TraceID: "later", SpanID: "later", End: p.End.Add(cfg.Settings.CorrelationWindow.Duration)}
	if findRule(engine.AuditWindow([]model.Span{p, watermark}), "ATD-COR-001") == nil {
		t.Fatal("event watermark should close the producer window")
	}
}

func TestBatchCoverageFromSharedProducerContext(t *testing.T) {
	cfg := testConfig(t)
	now := time.Now()
	p := messagingSpan("p", "producer", "send", now, now.Add(time.Second))
	p.TraceID = strings.Repeat("a", 32)
	p.SpanID = strings.Repeat("b", 16)
	p.Attributes["messaging.batch.message_count"] = 5
	c := messagingSpan("c", "consumer", "process", now.Add(2*time.Second), now.Add(3*time.Second))
	c.Attributes["messaging.batch.message_count"] = 5
	for i := 0; i < 5; i++ {
		c.Links = append(c.Links, model.Link{TraceID: p.TraceID, SpanID: p.SpanID})
	}
	report := Engine{Config: cfg}.Audit([]model.Span{p, c})
	assertNoRule(t, report, "ATD-BAT-001")
	c.Links = c.Links[:1]
	if findRule(Engine{Config: cfg}.Audit([]model.Span{p, c}), "ATD-BAT-001") == nil {
		t.Fatal("one shared-context link must not claim coverage for five messages")
	}
}

func TestRequiredConsumerGroupDetectsMissingFanoutDelivery(t *testing.T) {
	cfg := testConfig(t)
	cfg.Topology.ExpectedEdges = []config.ExpectedEdge{
		{Producer: "producer", System: "kafka", Destination: "orders", Consumer: "worker", ConsumerGroup: "group-a", RequirePerMessage: true},
		{Producer: "producer", System: "kafka", Destination: "orders", Consumer: "worker", ConsumerGroup: "group-b", RequirePerMessage: true},
	}
	now := time.Now()
	p := messagingSpan("p", "producer", "send", now, now.Add(time.Second))
	p.Attributes["messaging.message.id"] = "order-1"
	c := messagingSpan("c", "worker", "process", now.Add(2*time.Second), now.Add(3*time.Second))
	c.Attributes["messaging.consumer.group.name"] = "group-a"
	c.Links = []model.Link{{TraceID: p.TraceID, SpanID: p.SpanID}}
	report := Engine{Config: cfg}.Audit([]model.Span{p, c})
	f := findRuleEvidence(report, "ATD-TOP-001", "missing_required_consumer")
	if f == nil || f.ConsumerGroup != "group-b" {
		t.Fatalf("missing group-b delivery was not detected: %#v", report.Findings)
	}
}

func TestDeniedAndIgnoredTopologyEdges(t *testing.T) {
	cfg := testConfig(t)
	cfg.Topology.DeniedEdges = []config.ExpectedEdge{{Producer: "billing", System: "kafka", Destination: "orders", Consumer: "fraud"}}
	cfg.Topology.IgnoredEdges = []config.ExpectedEdge{{Producer: "billing", System: "kafka", Destination: "orders.dlq", Consumer: "replayer"}}
	now := time.Now()
	p1 := messagingSpan("p1", "billing", "send", now, now.Add(time.Second))
	c1 := messagingSpan("c1", "fraud", "process", now.Add(2*time.Second), now.Add(3*time.Second))
	c1.Links = []model.Link{{TraceID: p1.TraceID, SpanID: p1.SpanID}}
	p2 := messagingSpan("p2", "billing", "send", now, now.Add(time.Second))
	p2.Attributes["messaging.destination.name"] = "orders.dlq"
	c2 := messagingSpan("c2", "replayer", "process", now.Add(2*time.Second), now.Add(3*time.Second))
	c2.Attributes["messaging.destination.name"] = "orders.dlq"
	c2.Links = []model.Link{{TraceID: p2.TraceID, SpanID: p2.SpanID}}
	report := Engine{Config: cfg}.Audit([]model.Span{p1, c1, p2, c2})
	if findRuleEvidence(report, "ATD-TOP-001", "denied_edge") == nil {
		t.Fatalf("denied edge was not detected: %#v", report.Findings)
	}
	for _, f := range report.Findings {
		if f.RuleID == "ATD-TOP-001" && f.Destination == "orders.dlq" {
			t.Fatalf("ignored DLQ edge produced topology finding: %#v", f)
		}
	}
}

func TestOverlappingExpectedEdgesAllMatchOneObservedEdge(t *testing.T) {
	cfg := testConfig(t)
	cfg.Topology.ExpectedEdges = []config.ExpectedEdge{
		{Producer: "billing", System: "kafka", Destination: "orders", Consumer: "worker"},
		{Producer: "*", System: "kafka", Destination: "orders", Consumer: "*"},
	}
	now := time.Now()
	p := messagingSpan("p", "billing", "send", now, now.Add(time.Second))
	c := messagingSpan("c", "worker", "process", now.Add(2*time.Second), now.Add(3*time.Second))
	c.Links = []model.Link{{TraceID: p.TraceID, SpanID: p.SpanID}}
	report := Engine{Config: cfg}.Audit([]model.Span{p, c})
	assertNoRule(t, report, "ATD-TOP-001")
}

func testConfig(t *testing.T) config.Config {
	t.Helper()
	cfg, err := config.Load(filepath.Join("..", "..", "config", "rules.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func messagingSpan(id, service, operation string, start, end time.Time) model.Span {
	kind := "PRODUCER"
	if operation == "process" {
		kind = "CONSUMER"
	}
	sum := sha256.Sum256([]byte(id))
	return model.Span{TraceID: hex.EncodeToString(sum[:16]), SpanID: hex.EncodeToString(sum[16:24]), Name: operation + " orders", Kind: kind, Service: service, Start: start, End: end, Attributes: map[string]any{"messaging.system": "kafka", "messaging.destination.name": "orders", "messaging.operation.type": operation}}
}

func processSpan(messageID, spanID, group, status string, start time.Time) model.Span {
	s := messagingSpan(spanID, "worker", "process", start, start.Add(time.Millisecond))
	s.StatusCode = status
	s.Attributes["messaging.message.id"] = messageID
	s.Attributes["messaging.consumer.group.name"] = group
	return s
}

func findRule(report model.Report, id string) *model.Finding {
	for i := range report.Findings {
		if report.Findings[i].RuleID == id {
			return &report.Findings[i]
		}
	}
	return nil
}

func findRuleEvidence(report model.Report, id, evidenceType string) *model.Finding {
	for i := range report.Findings {
		if report.Findings[i].RuleID == id && report.Findings[i].Evidence["type"] == evidenceType {
			return &report.Findings[i]
		}
	}
	return nil
}

func assertNoRule(t *testing.T, report model.Report, id string) {
	t.Helper()
	if f := findRule(report, id); f != nil {
		t.Fatalf("unexpected %s finding: %#v", id, *f)
	}
}
func TestMessageIDIsNotRequired(t *testing.T) {
	cfg, err := config.Load(filepath.Join("..", "..", "config", "rules.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	spans, err := ingest.ReadPath(filepath.Join("..", "..", "testdata", "holdout", "orphan-producer.json"), ingest.Limits{MaxBytes: 1 << 20, MaxSpans: 10}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r := Engine{Config: cfg}.Audit(spans)
	for _, f := range r.Findings {
		if f.RuleID == "ATD-SEM-004" {
			t.Fatal("message ID absence must not invalidate operation")
		}
		if f.RuleID == "ATD-COR-001" && f.Confidence != "low" {
			t.Fatalf("expected downgraded confidence, got %s", f.Confidence)
		}
	}
}

func TestUnresolvedValidLinkIsCoverageGapNotBrokenPropagation(t *testing.T) {
	cfg := testConfig(t)
	now := time.Now()
	c := messagingSpan("c", "consumer", "process", now, now.Add(time.Second))
	c.Links = []model.Link{{TraceID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SpanID: "aaaaaaaaaaaaaaaa"}}
	report := Engine{Config: cfg}.Audit([]model.Span{c})
	assertNoRule(t, report, "ATD-CTX-001")
	assertNoRule(t, report, "ATD-COR-002")
	f := findRule(report, "ATD-COV-001")
	if f == nil || f.EvidenceState != model.EvidenceInsufficient || report.Summary.BrokenLinks != 0 {
		t.Fatalf("unresolved link must be explicit insufficient evidence: %#v", report)
	}
}

func TestLiveAbsenceFindingDoesNotFailPolicyWithoutCoverage(t *testing.T) {
	cfg := testConfig(t)
	now := time.Now()
	p := messagingSpan("p", "producer", "send", now, now.Add(time.Second))
	p.TraceID = strings.Repeat("a", 32)
	p.SpanID = strings.Repeat("b", 16)
	watermark := model.Span{TraceID: "later", SpanID: "later", End: p.End.Add(cfg.Settings.CorrelationWindow.Duration)}
	engine := Engine{Config: cfg}
	report := engine.AuditWindow([]model.Span{p, watermark})
	f := findRule(report, "ATD-COR-001")
	if f == nil || f.EvidenceState != model.EvidenceInsufficient {
		t.Fatalf("live absence must expose insufficient coverage: %#v", report.Findings)
	}
	if engine.ViolatesPolicy(report) {
		t.Fatal("insufficient-evidence absence finding must not fail policy by default")
	}
}

func TestRuleApplicabilityCanScopeBySystemAndEnvironment(t *testing.T) {
	cfg := testConfig(t)
	for i := range cfg.Rules {
		if cfg.Rules[i].ID == "ATD-CTX-001" {
			cfg.Rules[i].AppliesTo.Systems = []string{"rabbitmq"}
			cfg.Rules[i].AppliesTo.Environments = []string{"prod"}
		}
	}
	now := time.Now()
	c := messagingSpan("c", "consumer", "process", now, now.Add(time.Second))
	c.ResourceAttributes = map[string]any{"deployment.environment.name": "staging"}
	assertNoRule(t, Engine{Config: cfg}.Audit([]model.Span{c}), "ATD-CTX-001")
}

func TestInvalidIdentityAndTimestampsAreExplicitFindings(t *testing.T) {
	cfg := testConfig(t)
	span := messagingSpan("bad", "producer", "send", time.Time{}, time.Time{})
	span.TraceID = strings.Repeat("0", 32)
	span.SpanID = "short"
	report := Engine{Config: cfg}.Audit([]model.Span{span})
	if findRule(report, "ATD-SEM-006") == nil || findRule(report, "ATD-SEM-007") == nil {
		t.Fatalf("invalid telemetry was not surfaced: %#v", report.Findings)
	}
}
