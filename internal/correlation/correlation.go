package correlation

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/XUanhoa04/async-trace-doctor/internal/model"
)

type Result struct {
	Correlations         []model.Correlation
	ProducerMatched      map[int]bool
	ConsumerMatched      map[int]bool
	ContextReferences    map[int]int
	UnresolvedReferences map[int]int
}

// Correlate follows the semantic-convention preference order: links, direct
// parent context, messaging identity, then a bounded low-confidence heuristic.
func Correlate(spans []model.Span, window time.Duration) Result {
	res := Result{ProducerMatched: map[int]bool{}, ConsumerMatched: map[int]bool{}, ContextReferences: map[int]int{}, UnresolvedReferences: map[int]int{}}
	producersByContext := map[string][]int{}
	producersByMessage := map[string][]int{}
	producersByRoute := map[string][]int{}
	var consumers []int
	for i, s := range spans {
		if s.IsProducer() {
			producersByContext[key(s.TraceID, s.SpanID)] = append(producersByContext[key(s.TraceID, s.SpanID)], i)
			if route := routeLookupKey(s); route != "" {
				producersByRoute[route] = append(producersByRoute[route], i)
				if _, identity := s.MessageIdentity(); identity != "" {
					mk := route + "\x00" + identity
					producersByMessage[mk] = append(producersByMessage[mk], i)
				}
			}
		}
		if s.IsConsumer() {
			consumers = append(consumers, i)
		}
	}
	for route := range producersByRoute {
		sort.Slice(producersByRoute[route], func(i, j int) bool {
			left, right := spans[producersByRoute[route][i]], spans[producersByRoute[route][j]]
			if !left.End.Equal(right.End) {
				return left.End.Before(right.End)
			}
			return producersByRoute[route][i] < producersByRoute[route][j]
		})
	}
	for _, ci := range consumers {
		c := spans[ci]
		linked := map[int]bool{}
		for _, l := range c.Links {
			if !l.HasValidContext() {
				continue
			}
			res.ContextReferences[ci]++
			matched := false
			for _, pi := range producersByContext[key(l.TraceID, l.SpanID)] {
				if strongScopeCompatible(spans[pi], c) {
					linked[pi] = true
					matched = true
				}
			}
			if !matched {
				res.UnresolvedReferences[ci]++
			}
		}
		if len(linked) > 0 {
			pis := sortedKeys(linked)
			for _, pi := range pis {
				add(&res, spans, pi, ci, "span_link", model.ConfidenceHigh)
			}
			continue
		}
		if validParentContext(c) {
			res.ContextReferences[ci]++
			matched := false
			for _, pi := range producersByContext[key(c.TraceID, c.ParentSpanID)] {
				if strongScopeCompatible(spans[pi], c) {
					add(&res, spans, pi, ci, "parent_context", model.ConfidenceHigh)
					matched = true
					break
				}
			}
			if matched {
				continue
			}
			res.UnresolvedReferences[ci]++
		}
		if identityKind, identity := c.MessageIdentity(); identity != "" {
			candidates := make([]candidate, 0)
			mk := routeLookupKey(c) + "\x00" + identity
			for _, pi := range producersByMessage[mk] {
				p := spans[pi]
				_, producerIdentity := p.MessageIdentity()
				if producerIdentity == identity && compatible(p, c) {
					d := delta(p, c)
					if d <= window {
						candidates = append(candidates, candidate{index: pi, delta: d})
					}
				}
			}
			sortCandidates(candidates)
			if len(candidates) > 0 {
				method := "messaging_attributes"
				if identityKind == "kafka_partition_offset" {
					method = "kafka_partition_offset"
				}
				add(&res, spans, candidates[0].index, ci, method, model.ConfidenceMedium)
				continue
			}
		}
		limit := 1
		if count, ok := c.AttrInt("messaging.batch.message_count"); ok && count > limit {
			limit = count
		}
		candidates := nearestRouteCandidates(producersByRoute[routeLookupKey(c)], spans, c, window, limit)
		for _, candidate := range candidates {
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

func nearestRouteCandidates(indices []int, spans []model.Span, consumer model.Span, window time.Duration, limit int) []candidate {
	if len(indices) == 0 || limit <= 0 {
		return nil
	}
	position := sort.Search(len(indices), func(i int) bool { return !spans[indices[i]].End.Before(consumer.Start) })
	left, right := position-1, position
	out := make([]candidate, 0, limit)
	for len(out) < limit && (left >= 0 || right < len(indices)) {
		chooseLeft := false
		leftDelta, rightDelta := time.Duration(1<<63-1), time.Duration(1<<63-1)
		if left >= 0 {
			leftDelta = delta(spans[indices[left]], consumer)
		}
		if right < len(indices) {
			rightDelta = delta(spans[indices[right]], consumer)
		}
		if leftDelta <= rightDelta {
			chooseLeft = true
		}
		var pi int
		var distance time.Duration
		if chooseLeft {
			pi, distance = indices[left], leftDelta
			left--
		} else {
			pi, distance = indices[right], rightDelta
			right++
		}
		if distance > window {
			if leftDelta > window && rightDelta > window {
				break
			}
			continue
		}
		producer := spans[pi]
		if (!producer.Start.IsZero() && !consumer.Start.IsZero() && producer.Start.After(consumer.Start)) || !compatible(producer, consumer) {
			continue
		}
		out = append(out, candidate{index: pi, delta: distance})
	}
	return out
}
func compatible(p, c model.Span) bool {
	return p.System() != "" && p.System() == c.System() && destinationCompatible(p, c) && scopeCompatible(p, c)
}
func strongScopeCompatible(p, c model.Span) bool {
	return true // exact context identity is causal evidence; isolation belongs at ingestion/sharding boundaries
}
func scopeCompatible(p, c model.Span) bool {
	return optionalEqual(p.Environment(), c.Environment()) && optionalEqual(p.ServiceNamespace(), c.ServiceNamespace()) && optionalEqual(p.DestinationNamespace(), c.DestinationNamespace()) && optionalEqual(p.ServerAddress(), c.ServerAddress())
}
func optionalEqual(a, b string) bool { return a == "" || b == "" || a == b }
func destinationCompatible(p, c model.Span) bool {
	pd, cd := p.Destination(), c.Destination()
	if pd == "" || cd == "" {
		return false
	}
	if !strings.EqualFold(p.System(), "rabbitmq") {
		return pd == cd && optionalEqual(p.Partition(), c.Partition())
	}
	return pd == cd || strings.HasPrefix(cd, pd+":") || strings.HasPrefix(pd, cd+":")
}
func routeLookupKey(s model.Span) string {
	if s.System() == "" || s.Destination() == "" {
		return ""
	}
	destination := s.Destination()
	if strings.EqualFold(s.System(), "rabbitmq") {
		destination = strings.Split(destination, ":")[0]
	}
	return strings.Join([]string{strings.ToLower(s.System()), destination}, "\x00")
}
func validParentContext(span model.Span) bool {
	return len(span.TraceID) == 32 && len(span.ParentSpanID) == 16 && strings.Trim(span.TraceID, "0") != "" && strings.Trim(span.ParentSpanID, "0") != ""
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
