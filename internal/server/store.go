package server

import (
	"reflect"
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
	index   map[string]int
	stats   StoreStats
}

type AddResult struct {
	Accepted              int
	Rejected              int
	DuplicateExports      int
	ConflictingDuplicates int
}

type StoreStats struct {
	RejectedSpans         uint64
	DuplicateExports      uint64
	ConflictingDuplicates uint64
	TTLEvictions          uint64
}

func NewStore(max int, ttl time.Duration, m *Metrics) *Store {
	return &Store{max: max, ttl: ttl, metrics: m, index: map[string]int{}}
}
func (s *Store) Add(in []model.Span) AddResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result AddResult
	now := time.Now().UTC()
	s.evictLocked(now)
	for _, span := range in {
		if !span.End.IsZero() && span.End.Before(now.Add(-s.ttl)) {
			result.Rejected++
			s.stats.RejectedSpans++
			s.stats.TTLEvictions++
			s.metrics.Evictions.WithLabelValues("ttl").Inc()
			s.metrics.Rejected.WithLabelValues("stale").Inc()
			continue
		}
		identity := spanIdentity(span)
		if existing, ok := s.index[identity]; identity != "" && ok {
			if reflect.DeepEqual(s.spans[existing], span) {
				result.DuplicateExports++
				s.stats.DuplicateExports++
				s.metrics.DuplicateExports.Inc()
			} else {
				result.ConflictingDuplicates++
				result.Rejected++
				s.stats.ConflictingDuplicates++
				s.stats.RejectedSpans++
				s.metrics.ConflictingDuplicates.Inc()
				s.metrics.Rejected.WithLabelValues("conflicting_duplicate").Inc()
			}
			continue
		}
		if len(s.spans) >= s.max {
			result.Rejected++
			s.stats.RejectedSpans++
			s.metrics.Rejected.WithLabelValues("capacity").Inc()
			continue
		}
		s.spans = append(s.spans, span)
		if identity != "" {
			s.index[identity] = len(s.spans) - 1
		}
		result.Accepted++
		s.metrics.Audited.Inc()
	}
	s.metrics.StateSpans.Set(float64(len(s.spans)))
	return result
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
		s.stats.TTLEvictions += uint64(n)
		s.rebuildIndexLocked()
	}
	s.metrics.StateSpans.Set(float64(len(s.spans)))
}
func (s *Store) Len() int { s.mu.RLock(); defer s.mu.RUnlock(); return len(s.spans) }

func (s *Store) Stats() StoreStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.stats
}

func (s *Store) rebuildIndexLocked() {
	s.index = make(map[string]int, len(s.spans))
	for i, span := range s.spans {
		if identity := spanIdentity(span); identity != "" {
			s.index[identity] = i
		}
	}
}

func spanIdentity(span model.Span) string {
	if span.TraceID == "" || span.SpanID == "" {
		return ""
	}
	return span.TraceID + "/" + span.SpanID
}
