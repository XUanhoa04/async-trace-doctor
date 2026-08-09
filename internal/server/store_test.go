package server

import (
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
