package report

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/XUanhoa04/async-trace-doctor/internal/model"
)

func TestTrimPreservesUTF8(t *testing.T) {
	got := trim("dịch-vụ-thanh-toán", 10)
	if !utf8.ValidString(got) || utf8.RuneCountInString(got) != 10 || !strings.HasSuffix(got, "…") {
		t.Fatalf("invalid UTF-8 trim %q", got)
	}
}

func TestTrimNonPositiveLimit(t *testing.T) {
	if got := trim("hello", 0); got != "" {
		t.Fatalf("trim limit zero = %q", got)
	}
	if got := trim("hello", -5); got != "" {
		t.Fatalf("trim limit negative = %q", got)
	}
}

func TestTrimEdgeCases(t *testing.T) {
	if got := trim("hello", 5); got != "hello" {
		t.Fatalf("trim exact length = %q, want %q", got, "hello")
	}
	if got := trim("hello", 6); got != "hello" {
		t.Fatalf("trim greater length = %q, want %q", got, "hello")
	}
	if got := trim("hello", 1); got != "h" {
		t.Fatalf("trim length 1 = %q, want %q", got, "h")
	}
}

func TestWriteJSON(t *testing.T) {
	r := model.Report{
		SchemaVersion: "1.1",
		Summary: model.Summary{
			AuditedSpans: 5,
		},
	}

	var compactBuf bytes.Buffer
	if err := WriteJSON(&compactBuf, r, false); err != nil {
		t.Fatalf("WriteJSON(compact) error: %v", err)
	}
	var decodedCompact model.Report
	if err := json.Unmarshal(compactBuf.Bytes(), &decodedCompact); err != nil {
		t.Fatalf("failed to decode compact json: %v", err)
	}
	if decodedCompact.Summary.AuditedSpans != 5 {
		t.Errorf("decoded audited spans = %d, want 5", decodedCompact.Summary.AuditedSpans)
	}

	var prettyBuf bytes.Buffer
	if err := WriteJSON(&prettyBuf, r, true); err != nil {
		t.Fatalf("WriteJSON(pretty) error: %v", err)
	}
	if !strings.Contains(prettyBuf.String(), "\n  \"schema_version\"") {
		t.Errorf("expected pretty printed json to contain indented lines, got:\n%s", prettyBuf.String())
	}
}

func TestWriteTableEmptyFindings(t *testing.T) {
	r := model.Report{
		Summary: model.Summary{
			AuditedSpans:   10,
			MessagingSpans: 4,
			Violations:     0,
		},
	}
	var buf bytes.Buffer
	if err := WriteTable(&buf, r); err != nil {
		t.Fatalf("WriteTable error: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "No policy violations detected.") {
		t.Errorf("expected 'No policy violations detected.', got: %s", out)
	}
	if !strings.Contains(out, "coverage unknown") {
		t.Errorf("expected fallback 'coverage unknown', got: %s", out)
	}
}

func TestWriteTableWithFindings(t *testing.T) {
	r := model.Report{
		Coverage: model.Coverage{
			Status: "complete",
		},
		Summary: model.Summary{
			AuditedSpans:             20,
			MessagingSpans:           8,
			Violations:               2,
			ContextCompletenessRatio: 0.85,
		},
		Findings: []model.Finding{
			{
				RuleID:            "ATD-SEM-001",
				Severity:          "warning",
				ProducerService:   "pub",
				ConsumerService:   "sub",
				ConsumerGroup:     "billing",
				Subscription:      "sub-1",
				MessagingSystem:   "kafka",
				Destination:       "orders-topic",
				CorrelationMethod: "messaging_attributes",
				Confidence:        model.ConfidenceHigh,
				EvidenceState:     model.EvidenceSufficient,
				Message:           "missing message identity",
			},
			{
				RuleID:            "ATD-TOP-002",
				Severity:          "error",
				ProducerService:   "extremely-long-producer-service-name-that-gets-truncated-by-the-table-formatter",
				ConsumerService:   "billing-consumer",
				MessagingSystem:   "rabbitmq",
				Destination:       "events",
				CorrelationMethod: "span_link",
				Confidence:        model.ConfidenceMedium,
				EvidenceState:     model.EvidenceInsufficient,
				Message:           "unexpected topology edge",
			},
		},
	}

	var buf bytes.Buffer
	if err := WriteTable(&buf, r); err != nil {
		t.Fatalf("WriteTable error: %v", err)
	}
	out := buf.String()

	if !strings.Contains(out, "WARNING") || !strings.Contains(out, "ERROR") {
		t.Errorf("expected uppercased severities WARNING and ERROR, got:\n%s", out)
	}
	if !strings.Contains(out, "ATD-SEM-001") || !strings.Contains(out, "ATD-TOP-002") {
		t.Errorf("expected rule IDs, got:\n%s", out)
	}
	if !strings.Contains(out, "sub[billing]{sub-1}") {
		t.Errorf("expected formatted consumer identity, got:\n%s", out)
	}
	if !strings.Contains(out, "context completeness 85.0%") {
		t.Errorf("expected formatted context completeness ratio, got:\n%s", out)
	}
	if !strings.Contains(out, "…") {
		t.Errorf("expected ellipsis in truncated edge, got:\n%s", out)
	}
}
