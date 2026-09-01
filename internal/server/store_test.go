package server

import (
	"fmt"
	"testing"
	"time"

	"github.com/XUanhoa04/async-trace-doctor/internal/model"
	"github.com/prometheus/client_golang/prometheus"
)

func TestStoreCapacityAndTTLEviction(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	s := NewStore(2, time.Minute, m)
	now := time.Now().UTC()
	result := s.Add([]model.Span{{SpanID: "old", End: now.Add(-2 * time.Minute)}, {SpanID: "new1", End: now}, {SpanID: "new2", End: now}, {SpanID: "new3", End: now}})
	got := s.Snapshot()
	if len(got) != 2 || got[0].SpanID != "new1" || got[1].SpanID != "new2" || result.Rejected != 2 {
		t.Fatalf("unexpected bounded state: %#v", got)
	}
}

func TestStoreDeduplicatesAndRejectsConflicts(t *testing.T) {
	reg := prometheus.NewRegistry()
	s := NewStore(4, time.Minute, NewMetrics(reg))
	now := time.Now().UTC()
	span := model.Span{TraceID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SpanID: "aaaaaaaaaaaaaaaa", Name: "one", End: now}
	result := s.Add([]model.Span{span, span})
	if result.Accepted != 1 || result.DuplicateExports != 1 || s.Len() != 1 {
		t.Fatalf("identical export was not deduplicated: %#v", result)
	}
	conflict := span
	conflict.Name = "different"
	result = s.Add([]model.Span{conflict})
	if result.ConflictingDuplicates != 1 || result.Rejected != 1 || s.Len() != 1 {
		t.Fatalf("conflicting identity was not rejected: %#v", result)
	}
}

func TestStoreRejectsMemoryBeforeSpanCapacity(t *testing.T) {
	reg := prometheus.NewRegistry()
	s := NewStore(100, time.Minute, NewMetrics(reg), 512)
	span := model.Span{TraceID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SpanID: "aaaaaaaaaaaaaaaa", End: time.Now().UTC(), Attributes: map[string]any{"large": string(make([]byte, 1024))}}
	result := s.Add([]model.Span{span})
	if result.RejectedMemory != 1 || result.Rejected != 1 || s.Len() != 0 {
		t.Fatalf("memory admission result: %#v", result)
	}
}

func TestStoreStatsAndByteEstimation(t *testing.T) {
	reg := prometheus.NewRegistry()
	s := NewStore(10, time.Minute, NewMetrics(reg))
	now := time.Now().UTC()
	span := model.Span{
		TraceID: "0123456789abcdef0123456789abcdef",
		SpanID:  "0123456789abcdef",
		Name:    "process",
		End:     now,
		Attributes: map[string]any{
			"str_key":   "hello",
			"int_key":   100,
			"int64_key": int64(200),
			"bool_key":  true,
			"float_key": 3.14,
			"nil_key":   nil,
		},
	}
	res := s.Add([]model.Span{span})
	if res.Accepted != 1 {
		t.Fatalf("expected 1 accepted span, got: %#v", res)
	}
	if s.CurrentBytes() <= 0 {
		t.Errorf("expected CurrentBytes > 0, got: %d", s.CurrentBytes())
	}
	stats := s.Stats()
	if stats.RejectedSpans != 0 || stats.DuplicateExports != 0 {
		t.Errorf("expected clean stats, got: %#v", stats)
	}
}

func BenchmarkStoreAddWithDuplicates(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s := NewStore(2, time.Hour, NewMetrics(prometheus.NewRegistry()), 1<<20)
		span := model.Span{TraceID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SpanID: "aaaaaaaaaaaaaaaa", Name: "send", End: time.Now().UTC(), Attributes: map[string]any{"messaging.system": "kafka", "key": "value"}}
		s.Add([]model.Span{span, span})
	}
}

func BenchmarkStoreSnapshotCached(b *testing.B) {
	s := NewStore(50000, time.Hour, NewMetrics(prometheus.NewRegistry()), 256<<20)
	now := time.Now().UTC()
	spans := make([]model.Span, 10000)
	for i := range spans {
		spans[i] = model.Span{TraceID: fmt.Sprintf("%032x", i), SpanID: fmt.Sprintf("%016x", i), End: now}
	}
	s.Add(spans)
	_ = s.Snapshot()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Snapshot()
	}
}
