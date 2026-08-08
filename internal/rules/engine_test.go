package rules

import (
	"path/filepath"
	"testing"

	"github.com/XUanhoa04/async-trace-doctor/internal/config"
	"github.com/XUanhoa04/async-trace-doctor/internal/ingest"
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
