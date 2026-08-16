package server

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"log/slog"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/XUanhoa04/async-trace-doctor/internal/model"
)

type storeEntry struct {
	index       int
	contentHash uint64
}

type Store struct {
	mu                     sync.RWMutex
	spans                  []model.Span
	max                    int
	maxBytes               int64
	currentBytes           int64
	ttl                    time.Duration
	metrics                *Metrics
	logger                 *slog.Logger
	index                  map[string]storeEntry
	stats                  StoreStats
	generation             uint64
	lastSnapshotGeneration uint64
	cachedSnapshot         []model.Span
}

type AddResult struct {
	Accepted              int
	Rejected              int
	DuplicateExports      int
	ConflictingDuplicates int
	RejectedMemory        int
}

type StoreStats struct {
	RejectedSpans         uint64
	DuplicateExports      uint64
	ConflictingDuplicates uint64
	TTLEvictions          uint64
}

// NewStore keeps maxBytes optional for source compatibility with embedders.
// A non-positive byte limit disables byte-based admission.
func NewStore(max int, ttl time.Duration, m *Metrics, maxBytes ...int64) *Store {
	var byteLimit int64
	if len(maxBytes) > 0 {
		byteLimit = maxBytes[0]
	}
	return &Store{max: max, maxBytes: byteLimit, ttl: ttl, metrics: m, index: map[string]storeEntry{}}
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
		hash := spanContentHash(span)
		if existing, ok := s.index[identity]; identity != "" && ok {
			if existing.contentHash == hash && reflect.DeepEqual(s.spans[existing.index], span) {
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
		estimated := estimateSpanBytes(span)
		if s.maxBytes > 0 && s.currentBytes+estimated > s.maxBytes {
			result.Rejected++
			result.RejectedMemory++
			s.stats.RejectedSpans++
			s.metrics.Rejected.WithLabelValues("memory").Inc()
			continue
		}
		s.spans = append(s.spans, span)
		s.currentBytes += estimated
		s.generation++
		if identity != "" {
			s.index[identity] = storeEntry{index: len(s.spans) - 1, contentHash: hash}
		}
		result.Accepted++
		s.metrics.Audited.Inc()
	}
	s.updateGaugesLocked()
	if result.Rejected > 0 && s.logger != nil {
		s.logger.Warn("spans rejected by bounded admission", "rejected", result.Rejected, "memory_rejected", result.RejectedMemory, "conflicting_duplicates", result.ConflictingDuplicates, "retained_spans", len(s.spans), "state_bytes", s.currentBytes)
	}
	return result
}

func (s *Store) Snapshot() []model.Span {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.evictLocked(time.Now().UTC())
	if s.cachedSnapshot != nil && s.generation == s.lastSnapshotGeneration {
		return s.cachedSnapshot
	}
	out := make([]model.Span, len(s.spans))
	copy(out, s.spans)
	s.cachedSnapshot = out
	s.lastSnapshotGeneration = s.generation
	return out
}

func (s *Store) evictLocked(now time.Time) {
	cut := now.Add(-s.ttl)
	kept := s.spans[:0]
	n := 0
	var keptBytes int64
	for _, span := range s.spans {
		if !span.End.IsZero() && span.End.Before(cut) {
			n++
			continue
		}
		kept = append(kept, span)
		keptBytes += estimateSpanBytes(span)
	}
	if n > 0 {
		s.spans = append([]model.Span(nil), kept...)
		s.currentBytes = keptBytes
		s.generation++
		s.metrics.Evictions.WithLabelValues("ttl").Add(float64(n))
		s.stats.TTLEvictions += uint64(n)
		s.rebuildIndexLocked()
		if s.logger != nil {
			s.logger.Info("expired spans evicted", "count", n, "retained_spans", len(s.spans), "state_bytes", s.currentBytes)
		}
	}
	s.updateGaugesLocked()
}

func (s *Store) Len() int { s.mu.RLock(); defer s.mu.RUnlock(); return len(s.spans) }

func (s *Store) CurrentBytes() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentBytes
}

func (s *Store) Stats() StoreStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.stats
}

func (s *Store) rebuildIndexLocked() {
	s.index = make(map[string]storeEntry, len(s.spans))
	for i, span := range s.spans {
		if identity := spanIdentity(span); identity != "" {
			s.index[identity] = storeEntry{index: i, contentHash: spanContentHash(span)}
		}
	}
}

func (s *Store) updateGaugesLocked() {
	s.metrics.StateSpans.Set(float64(len(s.spans)))
	s.metrics.StateBytes.Set(float64(s.currentBytes))
}

func spanIdentity(span model.Span) string {
	if span.TraceID == "" || span.SpanID == "" {
		return ""
	}
	return span.TraceID + "/" + span.SpanID
}

func spanContentHash(span model.Span) uint64 {
	h := fnv.New64a()
	for _, value := range []string{span.TraceID, span.SpanID, span.ParentSpanID, span.Name, span.Kind, span.Service, span.StatusCode} {
		_, _ = h.Write([]byte(value))
		_, _ = h.Write([]byte{0})
	}
	var times [16]byte
	binary.LittleEndian.PutUint64(times[:8], uint64(span.Start.UnixNano()))
	binary.LittleEndian.PutUint64(times[8:], uint64(span.End.UnixNano()))
	_, _ = h.Write(times[:])
	hashKeys := func(m map[string]any) {
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			_, _ = h.Write([]byte(k))
			_, _ = h.Write([]byte{0})
		}
	}
	hashKeys(span.Attributes)
	hashKeys(span.ResourceAttributes)
	return h.Sum64()
}

func estimateSpanBytes(span model.Span) int64 {
	n := int64(192 + len(span.TraceID) + len(span.SpanID) + len(span.ParentSpanID) + len(span.Name) + len(span.Kind) + len(span.Service) + len(span.StatusCode))
	n += estimateMapBytes(span.Attributes) + estimateMapBytes(span.ResourceAttributes)
	for _, link := range span.Links {
		n += int64(64+len(link.TraceID)+len(link.SpanID)) + estimateMapBytes(link.Attributes)
	}
	return n
}

func estimateMapBytes(values map[string]any) int64 {
	var n int64
	for key, value := range values {
		if value != nil {
			n += int64(64 + len(key) + len(fmt.Sprint(value)))
		} else {
			n += int64(64 + len(key))
		}
	}
	return n
}
