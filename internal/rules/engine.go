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

type auditContext struct {
	finalized bool
	watermark time.Time
}

// Audit treats the supplied offline input as complete. An unmatched span is
// therefore eligible for an orphan finding after correlation has considered
// every span in the input, regardless of the machine's wall clock.
func (e Engine) Audit(spans []model.Span) model.Report {
	return e.audit(spans, auditContext{finalized: true, watermark: eventWatermark(spans)})
}

// AuditWindow treats spans as an open live snapshot. Event time, rather than
// time.Now, closes correlation windows so historical or clock-shifted data is
// not judged against the auditor host clock.
func (e Engine) AuditWindow(spans []model.Span) model.Report {
	return e.audit(spans, auditContext{watermark: eventWatermark(spans)})
}

func (e Engine) audit(spans []model.Span, audit auditContext) model.Report {
	started := time.Now()
	corr := correlation.Correlate(spans, e.Config.Settings.CorrelationWindow.Duration)
	report := model.Report{SchemaVersion: "1.0", SemanticConventionVersion: e.Config.SemanticConventionVersion, GeneratedAt: time.Now().UTC(), Findings: []model.Finding{}, Topology: []model.Edge{}}
	for _, r := range e.Config.Rules {
		if !r.Enabled {
			continue
		}
		report.Findings = append(report.Findings, e.run(r, spans, corr, audit)...)
	}
	report.Topology = buildTopology(spans, corr)
	model.SortFindings(report.Findings)
	report.Summary = summary(spans, corr, report.Findings, time.Since(started), e.Config.Settings.CorrelationWindow.Duration, audit)
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

func (e Engine) run(r config.Rule, spans []model.Span, corr correlation.Result, audit auditContext) []model.Finding {
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
			if s.IsProducer() && applies(r, s) && !corr.ProducerMatched[i] && correlationWindowClosed(s, e.Config.Settings.CorrelationWindow.Duration, audit) {
				conf := model.ConfidenceMedium
				if s.MessageID() == "" {
					conf = model.ConfidenceLow
				}
				out = append(out, finding(r, s, nil, "none", conf, map[string]any{"correlation_window": e.Config.Settings.CorrelationWindow.Duration.String(), "message_id_present": s.MessageID() != ""}))
			}
		}
	case "orphan_consumer":
		for i, s := range spans {
			if s.IsConsumer() && applies(r, s) && !corr.ConsumerMatched[i] && correlationWindowClosed(s, e.Config.Settings.CorrelationWindow.Duration, audit) {
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
			coverage, validLinks, contexts := batchLinkCoverage(s, spans)
			if coverage < count {
				out = append(out, finding(r, model.Span{}, &s, "span_link", model.ConfidenceHigh, map[string]any{"batch_message_count": count, "covered_message_count": coverage, "valid_link_count": validLinks, "unique_context_count": contexts}))
			}
		}
	case "duplicate_processing":
		groups := map[string][]model.Span{}
		seenSpans := map[string]bool{}
		for _, s := range spans {
			if s.Operation() == "process" && s.MessageID() != "" && applies(r, s) {
				spanKey := s.TraceID + "\x00" + s.SpanID
				if s.TraceID != "" && s.SpanID != "" && seenSpans[spanKey] {
					continue // repeated OTLP export of the same span is not reprocessing
				}
				seenSpans[spanKey] = true
				k := strings.Join([]string{s.Service, s.System(), s.Destination(), s.ConsumerGroup(), s.Subscription(), s.MessageID()}, "\x00")
				groups[k] = append(groups[k], s)
			}
		}
		for _, g := range groups {
			for _, attempts := range splitAttemptWindows(g, e.Config.Settings.CorrelationWindow.Duration) {
				successes := make([]model.Span, 0, len(attempts))
				failed := 0
				for _, attempt := range attempts {
					if attempt.Failed() {
						failed++
						continue
					}
					successes = append(successes, attempt)
				}
				if len(successes) > e.Config.Settings.DuplicateThreshold {
					s := successes[0]
					out = append(out, finding(r, model.Span{}, &s, "messaging_attributes", model.ConfidenceHigh, map[string]any{"attempt_count": len(attempts), "successful_processing_count": len(successes), "failed_retry_count": failed, "threshold": e.Config.Settings.DuplicateThreshold, "message_id_present": true, "consumer_group": s.ConsumerGroup()}))
				}
			}
		}
	case "queue_latency_high":
		for _, c := range corr.Correlations {
			if c.QueueLatency > e.Config.Settings.QueueLatency.Duration {
				p, co := spans[c.ProducerIndex], spans[c.ConsumerIndex]
				out = append(out, finding(r, p, &co, c.Method, c.Confidence, map[string]any{"queue_latency": c.QueueLatency.String(), "threshold": e.Config.Settings.QueueLatency.Duration.String()}))
			}
		}
	case "clock_skew":
		for _, c := range corr.Correlations {
			if c.QueueLatency < -e.Config.Settings.ClockSkewTolerance.Duration {
				p, co := spans[c.ProducerIndex], spans[c.ConsumerIndex]
				out = append(out, finding(r, p, &co, c.Method, c.Confidence, map[string]any{"observed_latency": c.QueueLatency.String(), "clock_skew_tolerance": e.Config.Settings.ClockSkewTolerance.Duration.String()}))
			}
		}
	case "runtime_topology_mismatch":
		out = append(out, e.topologyFindings(r, spans, corr, audit)...)
	}
	return out
}

func finding(r config.Rule, p model.Span, c *model.Span, method string, confidence model.Confidence, evidence map[string]any) model.Finding {
	base := p
	if c != nil && base.SpanID == "" {
		base = *c
	}
	f := model.Finding{RuleID: r.ID, Severity: r.Severity, MessagingSystem: base.System(), Destination: base.Destination(), ConsumerGroup: base.ConsumerGroup(), Subscription: base.Subscription(), CorrelationMethod: method, Confidence: confidence, Evidence: evidence, Message: r.Message, SuggestedFix: r.SuggestedFix}
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
		f.ConsumerGroup = c.ConsumerGroup()
		f.Subscription = c.Subscription()
		f.TraceIDs = appendUnique(f.TraceIDs, c.TraceID)
		f.SpanIDs = appendUnique(f.SpanIDs, c.SpanID)
	}
	return f
}
func (e Engine) topologyFindings(r config.Rule, spans []model.Span, corr correlation.Result, audit auditContext) []model.Finding {
	observed := buildTopology(spans, corr)
	var out []model.Finding
	matched := make([]bool, len(e.Config.Topology.ExpectedEdges))
	for _, o := range observed {
		if matchingEdge(e.Config.Topology.DeniedEdges, o) >= 0 {
			out = append(out, topologyFinding(r, o, "denied_edge", model.ConfidenceHigh, map[string]any{"type": "denied_edge", "count": o.Count}))
			continue
		}
		if matchingEdge(e.Config.Topology.IgnoredEdges, o) >= 0 {
			continue
		}
		if len(e.Config.Topology.ExpectedEdges) == 0 {
			continue
		}
		matchCount := 0
		for i, expected := range e.Config.Topology.ExpectedEdges {
			if edgeMatches(expected, o) {
				matched[i] = true
				matchCount++
			}
		}
		if matchCount > 0 {
			continue
		}
		out = append(out, topologyFinding(r, o, "unexpected_edge", model.ConfidenceHigh, map[string]any{"type": "unexpected_edge", "count": o.Count}))
	}
	for i, x := range e.Config.Topology.ExpectedEdges {
		if x.RequirePerMessage {
			out = append(out, e.missingRequiredConsumers(r, x, spans, corr, audit)...)
			continue
		}
		// Absence of traffic is not topology drift. Report a missing edge only
		// after an eligible producer has had a complete correlation window.
		if !matched[i] && hasEligibleProducer(x, spans, e.Config.Settings.CorrelationWindow.Duration, audit) {
			o := model.Edge{Producer: x.Producer, Consumer: x.Consumer, System: x.System, Destination: x.Destination, ConsumerGroup: x.ConsumerGroup, Subscription: x.Subscription}
			out = append(out, topologyFinding(r, o, "missing_edge", model.ConfidenceMedium, map[string]any{"type": "missing_edge"}))
		}
	}
	return out
}

func topologyFinding(r config.Rule, edge model.Edge, kind string, confidence model.Confidence, evidence map[string]any) model.Finding {
	method := "correlated_topology"
	if kind == "missing_edge" {
		method = "expected_topology"
	}
	return model.Finding{RuleID: r.ID, Severity: r.Severity, ProducerService: edge.Producer, ConsumerService: edge.Consumer, MessagingSystem: edge.System, Destination: edge.Destination, ConsumerGroup: edge.ConsumerGroup, Subscription: edge.Subscription, CorrelationMethod: method, Confidence: confidence, Evidence: evidence, Message: r.Message, SuggestedFix: r.SuggestedFix}
}

func matchingEdge(edges []config.ExpectedEdge, observed model.Edge) int {
	for i, edge := range edges {
		if edgeMatches(edge, observed) {
			return i
		}
	}
	return -1
}

func edgeMatches(x config.ExpectedEdge, o model.Edge) bool {
	return wildcardMatch(x.Producer, o.Producer) && wildcardMatch(x.Consumer, o.Consumer) && wildcardMatch(x.System, o.System) && wildcardMatch(x.Destination, o.Destination) && optionalMatch(x.ConsumerGroup, o.ConsumerGroup) && optionalMatch(x.Subscription, o.Subscription)
}
func wildcardMatch(expected, actual string) bool { return expected == "*" || expected == actual }
func optionalMatch(expected, actual string) bool {
	return expected == "" || expected == "*" || expected == actual
}
func buildTopology(spans []model.Span, c correlation.Result) []model.Edge {
	m := map[string]*model.Edge{}
	for _, x := range c.Correlations {
		p, co := spans[x.ProducerIndex], spans[x.ConsumerIndex]
		k := strings.Join([]string{p.Service, p.System(), p.Destination(), co.Service, co.ConsumerGroup(), co.Subscription()}, "\x00")
		if m[k] == nil {
			m[k] = &model.Edge{Producer: p.Service, System: p.System(), Destination: p.Destination(), Consumer: co.Service, ConsumerGroup: co.ConsumerGroup(), Subscription: co.Subscription()}
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
func (e Engine) missingRequiredConsumers(r config.Rule, expected config.ExpectedEdge, spans []model.Span, corr correlation.Result, audit auditContext) []model.Finding {
	var out []model.Finding
	for pi, producer := range spans {
		if !producerMatches(expected, producer) || !correlationWindowClosed(producer, e.Config.Settings.CorrelationWindow.Duration, audit) {
			continue
		}
		batchCount, batch := producer.AttrInt("messaging.batch.message_count")
		if batch && batchCount > 1 {
			// A batch Send span does not identify its individual messages. Per-message
			// delivery must be verified from Create spans or message-level link data.
			continue
		}
		found := false
		for _, correlated := range corr.Correlations {
			if correlated.ProducerIndex != pi {
				continue
			}
			consumer := spans[correlated.ConsumerIndex]
			edge := model.Edge{Producer: producer.Service, System: producer.System(), Destination: producer.Destination(), Consumer: consumer.Service, ConsumerGroup: consumer.ConsumerGroup(), Subscription: consumer.Subscription()}
			if edgeMatches(expected, edge) {
				found = true
				break
			}
		}
		if !found {
			f := finding(r, producer, nil, "expected_delivery", model.ConfidenceMedium, map[string]any{"type": "missing_required_consumer", "consumer": expected.Consumer, "consumer_group": expected.ConsumerGroup, "subscription": expected.Subscription, "message_id_present": producer.MessageID() != ""})
			f.ConsumerService = expected.Consumer
			f.ConsumerGroup = expected.ConsumerGroup
			f.Subscription = expected.Subscription
			out = append(out, f)
		}
	}
	return out
}

func hasEligibleProducer(expected config.ExpectedEdge, spans []model.Span, window time.Duration, audit auditContext) bool {
	for _, span := range spans {
		if producerMatches(expected, span) && correlationWindowClosed(span, window, audit) {
			return true
		}
	}
	return false
}

func producerMatches(expected config.ExpectedEdge, span model.Span) bool {
	return span.IsProducer() && wildcardMatch(expected.Producer, span.Service) && wildcardMatch(expected.System, span.System()) && wildcardMatch(expected.Destination, span.Destination())
}

func summary(spans []model.Span, c correlation.Result, findings []model.Finding, elapsed, window time.Duration, audit auditContext) model.Summary {
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
		if sp.IsProducer() && !c.ProducerMatched[i] && correlationWindowClosed(sp, window, audit) {
			s.OrphanProducers++
		}
		if sp.IsConsumer() && !c.ConsumerMatched[i] && correlationWindowClosed(sp, window, audit) {
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
	return s.Operation() != "" || s.Kind == "PRODUCER" || s.Kind == "CONSUMER"
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
func batchLinkCoverage(consumer model.Span, spans []model.Span) (coverage, validLinks, uniqueContexts int) {
	producers := map[string]model.Span{}
	for _, span := range spans {
		if span.IsProducer() {
			producers[span.TraceID+"/"+span.SpanID] = span
		}
	}
	contexts := map[string]bool{}
	anonymousLinks := map[string]int{}
	messages := map[string]map[string]bool{}
	for _, link := range consumer.Links {
		if link.TraceID == "" || link.SpanID == "" {
			continue
		}
		validLinks++
		contextKey := link.TraceID + "/" + link.SpanID
		if messageID := fmt.Sprint(link.Attributes["messaging.message.id"]); messageID != "" && messageID != "<nil>" {
			if messages[contextKey] == nil {
				messages[contextKey] = map[string]bool{}
			}
			messages[contextKey][messageID] = true
			contexts[contextKey] = true
			continue
		}
		contexts[contextKey] = true
		anonymousLinks[contextKey]++
	}
	for contextKey := range contexts {
		messageCount := len(messages[contextKey])
		budget := 1
		if producer, ok := producers[contextKey]; ok {
			if count, ok := producer.AttrInt("messaging.batch.message_count"); ok && count > budget {
				budget = count
			}
		}
		if messageCount > budget {
			budget = messageCount
		}
		covered := messageCount + anonymousLinks[contextKey]
		if covered > budget {
			covered = budget
		}
		coverage += covered
	}
	return coverage, validLinks, len(contexts)
}

func splitAttemptWindows(spans []model.Span, window time.Duration) [][]model.Span {
	if len(spans) == 0 {
		return nil
	}
	sort.Slice(spans, func(i, j int) bool { return spans[i].Start.Before(spans[j].Start) })
	groups := [][]model.Span{{spans[0]}}
	for _, span := range spans[1:] {
		lastGroup := groups[len(groups)-1]
		previous := lastGroup[len(lastGroup)-1]
		if !span.Start.IsZero() && !previous.Start.IsZero() && span.Start.Sub(previous.Start) > window {
			groups = append(groups, []model.Span{span})
			continue
		}
		groups[len(groups)-1] = append(groups[len(groups)-1], span)
	}
	return groups
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

func eventWatermark(spans []model.Span) time.Time {
	var watermark time.Time
	for _, span := range spans {
		if span.End.After(watermark) {
			watermark = span.End
		}
	}
	return watermark
}

func correlationWindowClosed(s model.Span, window time.Duration, audit auditContext) bool {
	if s.End.IsZero() {
		return false
	}
	if audit.finalized {
		return true
	}
	return !audit.watermark.IsZero() && !audit.watermark.Before(s.End.Add(window))
}
