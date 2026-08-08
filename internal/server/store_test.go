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
	s.Add([]model.Span{{SpanID: "old", End: now.Add(-2 * time.Minute)}, {SpanID: "new1", End: now}, {SpanID: "new2", End: now}, {SpanID: "new3", End: now}})
	got := s.Snapshot()
	if len(got) != 2 || got[0].SpanID != "new2" || got[1].SpanID != "new3" {
		t.Fatalf("unexpected bounded state: %#v", got)
	}
}
