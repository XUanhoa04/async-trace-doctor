package server

import (
	"sync"
	"time"

	"github.com/XUanhoa04/async-trace-doctor/internal/model"
)

type Store struct {
	mu      sync.RWMutex
	spans   []model.Span
	max     int
	ttl     time.Duration
	metrics *Metrics
}

func NewStore(max int, ttl time.Duration, m *Metrics) *Store {
	return &Store{max: max, ttl: ttl, metrics: m}
}
func (s *Store) Add(in []model.Span) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.spans = append(s.spans, in...)
	s.evictLocked(time.Now().UTC())
	if len(s.spans) > s.max {
		n := len(s.spans) - s.max
		s.spans = append([]model.Span(nil), s.spans[n:]...)
		s.metrics.Evictions.WithLabelValues("capacity").Add(float64(n))
	}
	s.metrics.StateSpans.Set(float64(len(s.spans)))
}
func (s *Store) Snapshot() []model.Span {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.evictLocked(time.Now().UTC())
	out := make([]model.Span, len(s.spans))
	copy(out, s.spans)
	return out
}
func (s *Store) evictLocked(now time.Time) {
	cut := now.Add(-s.ttl)
	kept := s.spans[:0]
	n := 0
	for _, span := range s.spans {
		if span.End.Before(cut) {
			n++
			continue
		}
		kept = append(kept, span)
	}
	if n > 0 {
		s.spans = append([]model.Span(nil), kept...)
		s.metrics.Evictions.WithLabelValues("ttl").Add(float64(n))
	}
	s.metrics.StateSpans.Set(float64(len(s.spans)))
}
func (s *Store) Len() int { s.mu.RLock(); defer s.mu.RUnlock(); return len(s.spans) }
