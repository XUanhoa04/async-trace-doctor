package ingest

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/XUanhoa04/async-trace-doctor/internal/model"
)

func TestDecodeCollectorOTLPJSON(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "testdata", "core", "normal.json"))
	if err != nil {
		t.Fatal(err)
	}
	spans, err := DecodeJSON(b, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(spans) != 2 {
		t.Fatalf("got %d spans", len(spans))
	}
	if spans[0].Service != "checkout" || spans[1].Links[0].SpanID != "1111111111111111" {
		t.Fatalf("unexpected decoded spans: %#v", spans)
	}
}
func TestReadPathJSONLAndRedaction(t *testing.T) {
	spans, err := ReadPath(filepath.Join("..", "..", "testdata", "holdout", "orphan-consumer.jsonl"), Limits{MaxBytes: 1 << 20, MaxSpans: 10}, []string{"messaging.destination.name"})
	if err != nil {
		t.Fatal(err)
	}
	if spans[0].Destination() != "[REDACTED]" {
		t.Fatalf("redaction not applied: %q", spans[0].Destination())
	}
}
func TestMalformedOTLP(t *testing.T) {
	_, err := ReadPath(filepath.Join("..", "..", "testdata", "malformed.json"), Limits{MaxBytes: 1 << 20, MaxSpans: 10}, nil)
	if err == nil {
		t.Fatal("expected malformed input error")
	}
}
func TestInputLimits(t *testing.T) {
	_, err := ReadPath(filepath.Join("..", "..", "testdata", "core", "normal.json"), Limits{MaxBytes: 10, MaxSpans: 10}, nil)
	if err == nil {
		t.Fatal("expected byte limit error")
	}
}

func TestDecodeSpanStatus(t *testing.T) {
	raw := []byte(`{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"11111111111111111111111111111111","spanId":"1111111111111111","name":"process q","startTimeUnixNano":"1","endTimeUnixNano":"2","status":{"code":2}}]}]}]}`)
	spans, err := DecodeJSON(raw, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(spans) != 1 || spans[0].StatusCode != "ERROR" {
		t.Fatalf("status was not preserved: %#v", spans)
	}
}

func TestOfflineInputDeduplicatesAndRejectsConflictingIdentity(t *testing.T) {
	span := model.Span{TraceID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SpanID: "aaaaaaaaaaaaaaaa", Name: "one"}
	got, err := deduplicateSpans([]model.Span{span, span})
	if err != nil || len(got) != 1 {
		t.Fatalf("identical duplicate was not removed: %#v, %v", got, err)
	}
	conflict := span
	conflict.Name = "different"
	if _, err := deduplicateSpans([]model.Span{span, conflict}); err == nil {
		t.Fatal("conflicting duplicate identity must fail offline audit")
	}
}
