package report_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/XUanhoa04/async-trace-doctor/internal/config"
	"github.com/XUanhoa04/async-trace-doctor/internal/ingest"
	"github.com/XUanhoa04/async-trace-doctor/internal/report"
	"github.com/XUanhoa04/async-trace-doctor/internal/rules"
)

func TestNormalJSONGolden(t *testing.T) {
	cfg, err := config.Load(filepath.Join("..", "..", "config", "rules.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	spans, err := ingest.ReadPath(filepath.Join("..", "..", "testdata", "core", "normal.json"), ingest.Limits{MaxBytes: 1 << 20, MaxSpans: 10}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r := rules.Engine{Config: cfg}.Audit(spans)
	r.GeneratedAt = time.Time{}
	r.Summary.ProcessingMillis = 0
	var got bytes.Buffer
	if err := report.WriteJSON(&got, r, true); err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile(filepath.Join("..", "..", "testdata", "golden", "normal-report.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(bytes.TrimSpace(got.Bytes()), bytes.TrimSpace(want)) {
		t.Fatalf("golden mismatch\n--- got ---\n%s\n--- want ---\n%s", got.String(), want)
	}
}
