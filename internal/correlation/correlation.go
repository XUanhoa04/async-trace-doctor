package correlation

import (
	"fmt"
	"sort"
	"time"

	"github.com/XUanhoa04/async-trace-doctor/internal/model"
)

type Result struct {
	Correlations    []model.Correlation
	ProducerMatched map[int]bool
	ConsumerMatched map[int]bool
}

// Correlate follows the semantic-convention preference order: links, direct
// parent context, messaging identity, then a bounded low-confidence heuristic.
func Correlate(spans []model.Span, window time.Duration) Result {
	res := Result{ProducerMatched: map[int]bool{}, ConsumerMatched: map[int]bool{}}
	producersByContext := map[string]int{}
	var producers, consumers []int
	for i, s := range spans {
		if s.IsProducer() {
			producers = append(producers, i)
			producersByContext[key(s.TraceID, s.SpanID)] = i
		}
		if s.IsConsumer() {
			consumers = append(consumers, i)
		}
	}
	for _, ci := range consumers {
		c := spans[ci]
		linked := map[int]bool{}
		for _, l := range c.Links {
			if pi, ok := producersByContext[key(l.TraceID, l.SpanID)]; ok && contextCompatible(spans[pi], c) {
				linked[pi] = true
			}
		}
		if len(linked) > 0 {
			pis := sortedKeys(linked)
			for _, pi := range pis {
				add(&res, spans, pi, ci, "span_link", model.ConfidenceHigh)
			}
			continue
		}
		if c.ParentSpanID != "" {
			if pi, ok := producersByContext[key(c.TraceID, c.ParentSpanID)]; ok && contextCompatible(spans[pi], c) {
				add(&res, spans, pi, ci, "parent_context", model.ConfidenceHigh)
				continue
			}
		}
		if c.MessageID() != "" {
			candidates := make([]candidate, 0)
			for _, pi := range producers {
				p := spans[pi]
				if p.MessageID() == c.MessageID() && compatible(p, c) {
					d := delta(p, c)
					if d <= window {
						candidates = append(candidates, candidate{index: pi, delta: d})
					}
				}
			}
			sortCandidates(candidates)
			if len(candidates) > 0 {
				add(&res, spans, candidates[0].index, ci, "messaging_attributes", model.ConfidenceMedium)
				continue
			}
		}
		candidates := make([]candidate, 0)
		for _, pi := range producers {
			p := spans[pi]
			// A context-free heuristic must not invent causality with a producer
			// that started after the consumer. Exact IDs and links may still expose
			// clock skew and are handled by the stronger branches above.
			if !compatible(p, c) || (!p.Start.IsZero() && !c.Start.IsZero() && p.Start.After(c.Start)) {
				continue
			}
			d := delta(p, c)
			if d <= window {
				candidates = append(candidates, candidate{index: pi, delta: d})
			}
		}
		sortCandidates(candidates)
		limit := 1
		if count, ok := c.AttrInt("messaging.batch.message_count"); ok && count > limit {
			limit = count
		}
		if limit > len(candidates) {
			limit = len(candidates)
		}
		for _, candidate := range candidates[:limit] {
			add(&res, spans, candidate.index, ci, "time_window_heuristic", model.ConfidenceLow)
		}
	}
	return res
}

func add(r *Result, spans []model.Span, pi, ci int, method string, confidence model.Confidence) {
	p, c := spans[pi], spans[ci]
	lat := c.Start.Sub(p.End)
	r.Correlations = append(r.Correlations, model.Correlation{ProducerIndex: pi, ConsumerIndex: ci, Producer: model.Ref(p), Consumer: model.Ref(c), Method: method, Confidence: confidence, QueueLatency: lat})
	r.ProducerMatched[pi] = true
	r.ConsumerMatched[ci] = true
}

type candidate struct {
	index int
	delta time.Duration
}

func sortCandidates(candidates []candidate) {
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].delta != candidates[j].delta {
			return candidates[i].delta < candidates[j].delta
		}
		return candidates[i].index < candidates[j].index
	})
}
func compatible(p, c model.Span) bool {
	return p.System() != "" && p.System() == c.System() && p.Destination() != "" && p.Destination() == c.Destination()
}
func contextCompatible(p, c model.Span) bool {
	return (p.System() == "" || c.System() == "" || p.System() == c.System()) &&
		(p.Destination() == "" || c.Destination() == "" || p.Destination() == c.Destination())
}
func delta(p, c model.Span) time.Duration {
	d := c.Start.Sub(p.End)
	if d < 0 {
		d = -d
	}
	return d
}
func key(t, s string) string { return fmt.Sprintf("%s/%s", t, s) }
func sortedKeys(m map[int]bool) []int {
	out := make([]int, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Ints(out)
	return out
}
