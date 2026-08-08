package rules

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/XUanhoa04/async-trace-doctor/internal/config"
	"github.com/XUanhoa04/async-trace-doctor/internal/correlation"
	"github.com/XUanhoa04/async-trace-doctor/internal/model"
)

type Engine struct{ Config config.Config }

func (e Engine) Audit(spans []model.Span) model.Report {
	started := time.Now()
	corr := correlation.Correlate(spans, e.Config.Settings.CorrelationWindow.Duration)
	report := model.Report{SchemaVersion: "1.0", SemanticConventionVersion: e.Config.SemanticConventionVersion, GeneratedAt: time.Now().UTC(), Findings: []model.Finding{}, Topology: []model.Edge{}}
	for _, r := range e.Config.Rules {
		if !r.Enabled {
			continue
		}
		report.Findings = append(report.Findings, e.run(r, spans, corr)...)
	}
	report.Topology = buildTopology(spans, corr)
	model.SortFindings(report.Findings)
	report.Summary = summary(spans, corr, report.Findings, time.Since(started), e.Config.Settings.CorrelationWindow.Duration)
	return report
}

func (e Engine) ViolatesPolicy(report model.Report) bool {
	for _, f := range report.Findings {
		if e.Config.ViolatesPolicy(f.Severity) {
			return true
		}
	}
	return false
}

func (e Engine) run(r config.Rule, spans []model.Span, corr correlation.Result) []model.Finding {
	var out []model.Finding
	switch r.Check {
	case "missing_service_name", "missing_messaging_system", "missing_destination", "invalid_operation", "invalid_span_kind", "missing_consumer_context":
		for i, s := range spans {
			if !isMessaging(s) && r.Check != "missing_messaging_system" {
				continue
			}
			if !applies(r, s) {
				continue
			}
			var bad bool
			var ev map[string]any
			switch r.Check {
			case "missing_service_name":
				bad = s.Service == ""
				ev = map[string]any{"attribute": "service.name"}
			case "missing_messaging_system":
				bad = looksMessaging(s) && s.System() == ""
				ev = map[string]any{"attribute": "messaging.system"}
			case "missing_destination":
				bad = needsDestination(s.Operation()) && s.Destination() == ""
				ev = map[string]any{"attribute": "messaging.destination.name", "operation": s.Operation()}
			case "invalid_operation":
				bad = !validOperation(s.Operation())
				ev = map[string]any{"value": s.Operation(), "allowed": []string{"create", "send", "receive", "process", "settle"}}
			case "invalid_span_kind":
				expected := expectedKinds(s.Operation())
				bad = len(expected) > 0 && !contains(expected, s.Kind)
				ev = map[string]any{"operation": s.Operation(), "actual_kind": s.Kind, "expected_kinds": expected}
			case "missing_consumer_context":
				bad = s.Operation() == "process" && !hasStrongConsumerContext(i, corr)
				ev = map[string]any{"links": len(s.Links), "parent_span_id_present": s.ParentSpanID != ""}
			}
			if bad {
				out = append(out, finding(r, s, nil, "semantic_validation", confidenceFor(r.Check), ev))
			}
		}
	case "orphan_producer":
		for i, s := range spans {
			if s.IsProducer() && applies(r, s) && !corr.ProducerMatched[i] && correlationWindowClosed(s, e.Config.Settings.CorrelationWindow.Duration) {
				conf := model.ConfidenceMedium
				if s.MessageID() == "" {
					conf = model.ConfidenceLow
				}
				out = append(out, finding(r, s, nil, "none", conf, map[string]any{"correlation_window": e.Config.Settings.CorrelationWindow.Duration.String(), "message_id_present": s.MessageID() != ""}))
			}
		}
	case "orphan_consumer":
		for i, s := range spans {
			if s.IsConsumer() && applies(r, s) && !corr.ConsumerMatched[i] && correlationWindowClosed(s, e.Config.Settings.CorrelationWindow.Duration) {
				conf := model.ConfidenceMedium
				if s.MessageID() == "" {
					conf = model.ConfidenceLow
				}
				out = append(out, finding(r, model.Span{}, &s, "none", conf, map[string]any{"correlation_window": e.Config.Settings.CorrelationWindow.Duration.String(), "message_id_present": s.MessageID() != ""}))
			}
		}
	case "batch_links_incomplete":
		for _, s := range spans {
			if !s.IsConsumer() || !applies(r, s) {
				continue
			}
			count, ok := s.AttrInt("messaging.batch.message_count")
			if !ok || count < 2 {
				continue
			}
			contexts := uniqueContexts(s.Links)
			if len(contexts) < count {
				out = append(out, finding(r, model.Span{}, &s, "span_link", model.ConfidenceHigh, map[string]any{"batch_message_count": count, "unique_link_count": len(contexts)}))
			}
		}
	case "duplicate_processing":
		groups := map[string][]model.Span{}
		for _, s := range spans {
			if s.Operation() == "process" && s.MessageID() != "" && applies(r, s) {
				k := strings.Join([]string{s.Service, s.System(), s.Destination(), s.MessageID()}, "\x00")
				groups[k] = append(groups[k], s)
			}
		}
		for _, g := range groups {
			if len(g) > e.Config.Settings.DuplicateThreshold {
				s := g[0]
				out = append(out, finding(r, model.Span{}, &s, "messaging_attributes", model.ConfidenceHigh, map[string]any{"processing_count": len(g), "threshold": e.Config.Settings.DuplicateThreshold, "message_id": s.MessageID()}))
			}
		}
	case "queue_latency_high":
		for _, c := range corr.Correlations {
			if c.QueueLatency > e.Config.Settings.QueueLatency.Duration {
				p, co := spans[c.ProducerIndex], spans[c.ConsumerIndex]
				out = append(out, finding(r, p, &co, c.Method, c.Confidence, map[string]any{"queue_latency": c.QueueLatency.String(), "threshold": e.Config.Settings.QueueLatency.Duration.String()}))
			}
		}
	case "runtime_topology_mismatch":
		out = append(out, e.topologyFindings(r, spans, corr)...)
	}
	return out
}

func finding(r config.Rule, p model.Span, c *model.Span, method string, confidence model.Confidence, evidence map[string]any) model.Finding {
	base := p
	if c != nil && base.SpanID == "" {
		base = *c
	}
	f := model.Finding{RuleID: r.ID, Severity: r.Severity, MessagingSystem: base.System(), Destination: base.Destination(), CorrelationMethod: method, Confidence: confidence, Evidence: evidence, Message: r.Message, SuggestedFix: r.SuggestedFix}
	if p.SpanID != "" {
		if p.IsConsumer() || p.Kind == "CONSUMER" {
			f.ConsumerService = p.Service
		} else {
			f.ProducerService = p.Service
		}
		f.TraceIDs = appendUnique(f.TraceIDs, p.TraceID)
		f.SpanIDs = appendUnique(f.SpanIDs, p.SpanID)
	}
	if c != nil {
		f.ConsumerService = c.Service
		f.TraceIDs = appendUnique(f.TraceIDs, c.TraceID)
		f.SpanIDs = appendUnique(f.SpanIDs, c.SpanID)
	}
	return f
}
func (e Engine) topologyFindings(r config.Rule, spans []model.Span, corr correlation.Result) []model.Finding {
	if len(e.Config.Topology.ExpectedEdges) == 0 {
		return nil
	}
	observed := buildTopology(spans, corr)
	var out []model.Finding
	matched := make([]bool, len(e.Config.Topology.ExpectedEdges))
	for _, o := range observed {
		idx := -1
		for i, x := range e.Config.Topology.ExpectedEdges {
			if edgeMatches(x, o) {
				idx = i
				break
			}
		}
		if idx >= 0 {
			matched[idx] = true
			continue
		}
		out = append(out, model.Finding{RuleID: r.ID, Severity: r.Severity, ProducerService: o.Producer, ConsumerService: o.Consumer, MessagingSystem: o.System, Destination: o.Destination, CorrelationMethod: "correlated_topology", Confidence: model.ConfidenceHigh, Evidence: map[string]any{"type": "unexpected_edge", "count": o.Count}, Message: r.Message, SuggestedFix: r.SuggestedFix})
	}
	for i, x := range e.Config.Topology.ExpectedEdges {
		if !matched[i] {
			out = append(out, model.Finding{RuleID: r.ID, Severity: r.Severity, ProducerService: x.Producer, ConsumerService: x.Consumer, MessagingSystem: x.System, Destination: x.Destination, CorrelationMethod: "expected_topology", Confidence: model.ConfidenceMedium, Evidence: map[string]any{"type": "missing_edge"}, Message: r.Message, SuggestedFix: r.SuggestedFix})
		}
	}
	return out
}
func edgeMatches(x config.ExpectedEdge, o model.Edge) bool {
	return (x.Producer == "*" || x.Producer == o.Producer) && (x.Consumer == "*" || x.Consumer == o.Consumer) && (x.System == "*" || x.System == o.System) && (x.Destination == "*" || x.Destination == o.Destination)
}
func buildTopology(spans []model.Span, c correlation.Result) []model.Edge {
	m := map[string]*model.Edge{}
	for _, x := range c.Correlations {
		p, co := spans[x.ProducerIndex], spans[x.ConsumerIndex]
		k := strings.Join([]string{p.Service, p.System(), p.Destination(), co.Service}, "\x00")
		if m[k] == nil {
			m[k] = &model.Edge{Producer: p.Service, System: p.System(), Destination: p.Destination(), Consumer: co.Service}
		}
		m[k].Count++
	}
	out := make([]model.Edge, 0, len(m))
	for _, e := range m {
		out = append(out, *e)
	}
	sort.Slice(out, func(i, j int) bool { return fmt.Sprint(out[i]) < fmt.Sprint(out[j]) })
	return out
}
func summary(spans []model.Span, c correlation.Result, findings []model.Finding, elapsed, window time.Duration) model.Summary {
	s := model.Summary{AuditedSpans: len(spans), Violations: len(findings), ProcessingMillis: elapsed.Milliseconds()}
	consumer := 0
	strong := 0
	for i, sp := range spans {
		if isMessaging(sp) {
			s.MessagingSpans++
		}
		if sp.IsConsumer() {
			consumer++
			if hasStrongConsumerContext(i, c) {
				strong++
			} else {
				s.BrokenLinks++
			}
		}
		if sp.IsProducer() && !c.ProducerMatched[i] && correlationWindowClosed(sp, window) {
			s.OrphanProducers++
		}
		if sp.IsConsumer() && !c.ConsumerMatched[i] && correlationWindowClosed(sp, window) {
			s.OrphanConsumers++
		}
	}
	if consumer > 0 {
		s.ContextCompletenessRatio = float64(strong) / float64(consumer)
	}
	return s
}
func isMessaging(s model.Span) bool { return s.System() != "" || s.Operation() != "" }
func looksMessaging(s model.Span) bool {
	return s.Operation() != "" || strings.Contains(strings.ToLower(s.Name), "publish") || strings.Contains(strings.ToLower(s.Name), "send") || strings.Contains(strings.ToLower(s.Name), "process") || s.Kind == "PRODUCER" || s.Kind == "CONSUMER"
}
func validOperation(x string) bool {
	return contains([]string{"create", "send", "receive", "process", "settle"}, x)
}
func needsDestination(x string) bool {
	return x == "create" || x == "send" || x == "receive" || x == "process" || x == "settle"
}
func expectedKinds(op string) []string {
	switch op {
	case "create":
		return []string{"PRODUCER"}
	case "send":
		return []string{"PRODUCER", "CLIENT"}
	case "receive", "settle":
		return []string{"CLIENT"}
	case "process":
		return []string{"CONSUMER"}
	default:
		return nil
	}
}
func applies(r config.Rule, s model.Span) bool {
	return len(r.AppliesTo.Operations) == 0 || contains(r.AppliesTo.Operations, s.Operation())
}
func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
func hasStrongConsumerContext(i int, c correlation.Result) bool {
	for _, x := range c.Correlations {
		if x.ConsumerIndex == i && (x.Method == "span_link" || x.Method == "parent_context") {
			return true
		}
	}
	return false
}
func confidenceFor(check string) model.Confidence {
	if check == "missing_consumer_context" {
		return model.ConfidenceHigh
	}
	return model.ConfidenceHigh
}
func uniqueContexts(links []model.Link) map[string]bool {
	m := map[string]bool{}
	for _, l := range links {
		if l.TraceID != "" && l.SpanID != "" {
			m[l.TraceID+"/"+l.SpanID] = true
		}
	}
	return m
}
func appendUnique(xs []string, v string) []string {
	if v == "" {
		return xs
	}
	for _, x := range xs {
		if x == v {
			return xs
		}
	}
	return append(xs, v)
}

func correlationWindowClosed(s model.Span, window time.Duration) bool {
	return !s.End.IsZero() && time.Since(s.End) >= window
}
