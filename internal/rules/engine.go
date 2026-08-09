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
	finalized        bool
	coverageComplete bool
	watermark        time.Time
}

// Audit treats the supplied offline input as complete. An unmatched span is
// therefore eligible for an orphan finding after correlation has considered
// every span in the input, regardless of the machine's wall clock.
func (e Engine) Audit(spans []model.Span) model.Report {
	return e.audit(spans, auditContext{finalized: true, coverageComplete: e.Config.Settings.InputCompleteness == "complete", watermark: eventWatermark(spans)})
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
	report := model.Report{SchemaVersion: "1.1", SemanticConventionVersion: e.Config.SemanticConventionVersion, GeneratedAt: time.Now().UTC(), Findings: []model.Finding{}, Topology: []model.Edge{}, Coverage: baseCoverage(spans, audit)}
	for _, r := range e.Config.Rules {
		if !r.Enabled {
			continue
		}
		report.Findings = append(report.Findings, e.run(r, spans, corr, audit)...)
	}
	if report.Coverage.Status == "degraded" {
		for i := range report.Findings {
			if report.Findings[i].EvidenceState == model.EvidenceInsufficient {
				report.Findings[i].EvidenceState = model.EvidenceDegraded
			}
		}
	}
	report.Topology = buildTopology(spans, corr)
	for _, c := range corr.Correlations {
		report.QueueLatencySamples = append(report.QueueLatencySamples, c.QueueLatency.Seconds())
	}
	model.SortFindings(report.Findings)
	report.Summary = summary(spans, corr, report.Findings, time.Since(started), e.Config.Settings.CorrelationWindow.Duration, audit)
	return report
}

func (e Engine) ViolatesPolicy(report model.Report) bool {
	for _, f := range report.Findings {
		if f.EvidenceState != model.EvidenceSufficient && !e.Config.Settings.FailOnInsufficientEvidence {
			continue
		}
		if e.Config.ViolatesPolicy(f.Severity) {
			return true
		}
	}
	return false
}

func (e Engine) run(r config.Rule, spans []model.Span, corr correlation.Result, audit auditContext) []model.Finding {
	var out []model.Finding
	switch r.Check {
	case "missing_service_name", "missing_messaging_system", "missing_destination", "invalid_operation", "invalid_span_kind", "invalid_span_identity", "invalid_timestamps", "invalid_context_reference", "missing_consumer_context", "unresolved_consumer_context":
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
			case "invalid_span_identity":
				bad = len(s.TraceID) != 32 || len(s.SpanID) != 16 || allZeroHex(s.TraceID) || allZeroHex(s.SpanID)
				ev = map[string]any{"trace_id_length": len(s.TraceID), "span_id_length": len(s.SpanID), "trace_id_zero": allZeroHex(s.TraceID), "span_id_zero": allZeroHex(s.SpanID)}
			case "invalid_timestamps":
				bad = timestampMissing(s.Start) || timestampMissing(s.End) || s.End.Before(s.Start)
				ev = map[string]any{"start_time": s.Start.UTC().Format(time.RFC3339Nano), "end_time": s.End.UTC().Format(time.RFC3339Nano), "end_before_start": s.End.Before(s.Start)}
			case "invalid_context_reference":
				invalidLinks := 0
				for _, link := range s.Links {
					if !link.HasValidContext() {
						invalidLinks++
					}
				}
				invalidParent := s.ParentSpanID != "" && (len(s.TraceID) != 32 || len(s.ParentSpanID) != 16 || allZeroHex(s.TraceID) || allZeroHex(s.ParentSpanID))
				bad = invalidLinks > 0 || invalidParent
				ev = map[string]any{"invalid_link_count": invalidLinks, "invalid_parent_context": invalidParent}
			case "missing_consumer_context":
				bad = s.Operation() == "process" && corr.ContextReferences[i] == 0
				ev = map[string]any{"context_references": corr.ContextReferences[i], "links": len(s.Links), "parent_span_id_present": s.ParentSpanID != ""}
			case "unresolved_consumer_context":
				bad = s.IsConsumer() && corr.UnresolvedReferences[i] > 0
				ev = map[string]any{"context_references": corr.ContextReferences[i], "unresolved_references": corr.UnresolvedReferences[i], "links": len(s.Links), "parent_span_id_present": s.ParentSpanID != ""}
			}
			if bad {
				f := finding(r, s, nil, "semantic_validation", confidenceFor(r.Check), ev)
				if r.Check == "missing_consumer_context" {
					if candidate := correlationForConsumer(i, corr); candidate != nil {
						producer := spans[candidate.ProducerIndex]
						ev["candidate_correlation_method"] = candidate.Method
						ev["candidate_confidence"] = candidate.Confidence
						ev["candidate_queue_latency"] = candidate.QueueLatency.String()
						method := "identity_candidate"
						if candidate.Method == "time_window_heuristic" {
							method = "time_candidate"
						}
						f = finding(r, producer, &s, method, model.ConfidenceHigh, ev)
					}
				}
				if r.Check == "unresolved_consumer_context" {
					f.CorrelationMethod = "unresolved_context_reference"
					f.Confidence = model.ConfidenceLow
					f.EvidenceState = model.EvidenceInsufficient
					unresolved := []string{}
					for _, link := range s.Links {
						if link.TraceID != "" && link.SpanID != "" {
							unresolved = append(unresolved, link.TraceID+"/"+link.SpanID)
							f.TraceIDs = appendUnique(f.TraceIDs, link.TraceID)
							f.SpanIDs = appendUnique(f.SpanIDs, link.SpanID)
						}
					}
					if s.ParentSpanID != "" {
						unresolved = append(unresolved, s.TraceID+"/"+s.ParentSpanID)
						f.SpanIDs = appendUnique(f.SpanIDs, s.ParentSpanID)
					}
					f.Evidence["unresolved_contexts"] = unresolved
				}
				out = append(out, f)
			}
		}
	case "orphan_producer":
		for i, s := range spans {
			if s.IsProducer() && applies(r, s) && !corr.ProducerMatched[i] && correlationWindowClosed(s, e.Config.Settings.CorrelationWindow.Duration, audit) {
				conf := model.ConfidenceLow
				identityKind, identity := s.MessageIdentity()
				if audit.coverageComplete && identity != "" {
					conf = model.ConfidenceMedium
				}
				f := finding(r, s, nil, "none", conf, map[string]any{"correlation_window": e.Config.Settings.CorrelationWindow.Duration.String(), "identity_kind": identityKind, "identity_present": identity != "", "coverage_complete": audit.coverageComplete})
				markAbsenceEvidence(&f, audit)
				out = append(out, f)
			}
		}
	case "orphan_consumer":
		for i, s := range spans {
			if s.IsConsumer() && applies(r, s) && !corr.ConsumerMatched[i] && corr.ContextReferences[i] == 0 && correlationWindowClosed(s, e.Config.Settings.CorrelationWindow.Duration, audit) {
				conf := model.ConfidenceLow
				identityKind, identity := s.MessageIdentity()
				if audit.coverageComplete && identity != "" {
					conf = model.ConfidenceMedium
				}
				f := finding(r, model.Span{}, &s, "none", conf, map[string]any{"correlation_window": e.Config.Settings.CorrelationWindow.Duration.String(), "identity_kind": identityKind, "identity_present": identity != "", "coverage_complete": audit.coverageComplete})
				markAbsenceEvidence(&f, audit)
				out = append(out, f)
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
			identityKind, identity := s.MessageIdentity()
			if s.Operation() == "process" && identity != "" && applies(r, s) {
				spanKey := s.TraceID + "\x00" + s.SpanID
				if s.TraceID != "" && s.SpanID != "" && seenSpans[spanKey] {
					continue // repeated OTLP export of the same span is not reprocessing
				}
				seenSpans[spanKey] = true
				k := strings.Join([]string{s.Service, s.System(), s.Destination(), s.ConsumerGroup(), s.Subscription(), identityKind, identity}, "\x00")
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
					identityKind, _ := s.MessageIdentity()
					out = append(out, finding(r, model.Span{}, &s, "messaging_attributes", model.ConfidenceHigh, map[string]any{"attempt_count": len(attempts), "successful_processing_count": len(successes), "failed_retry_count": failed, "threshold": e.Config.Settings.DuplicateThreshold, "identity_kind": identityKind, "consumer_group": s.ConsumerGroup()}))
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
	f := model.Finding{RuleID: r.ID, Severity: r.Severity, MessagingSystem: base.System(), Destination: base.Destination(), ConsumerGroup: base.ConsumerGroup(), Subscription: base.Subscription(), CorrelationMethod: method, Confidence: confidence, EvidenceState: model.EvidenceSufficient, Evidence: evidence, Message: r.Message, SuggestedFix: r.SuggestedFix}
	if identity := brokerIdentityEvidence(base); len(identity) > 0 {
		f.Evidence["broker_identity"] = identity
	}
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
			o := model.Edge{Producer: x.Producer, Consumer: x.Consumer, System: x.System, Destination: x.Destination, ConsumerGroup: x.ConsumerGroup, Subscription: x.Subscription, Environment: x.Environment, ServiceNamespace: x.ServiceNamespace, DestinationNamespace: x.DestinationNamespace, BrokerAddress: x.BrokerAddress}
			confidence := model.ConfidenceLow
			if audit.coverageComplete {
				confidence = model.ConfidenceMedium
			}
			f := topologyFinding(r, o, "missing_edge", confidence, map[string]any{"type": "missing_edge", "coverage_complete": audit.coverageComplete})
			markAbsenceEvidence(&f, audit)
			out = append(out, f)
		}
	}
	return out
}

func topologyFinding(r config.Rule, edge model.Edge, kind string, confidence model.Confidence, evidence map[string]any) model.Finding {
	method := "correlated_topology"
	if kind == "missing_edge" {
		method = "expected_topology"
	}
	isolation := map[string]any{}
	for key, value := range map[string]string{"environment": edge.Environment, "service_namespace": edge.ServiceNamespace, "destination_namespace": edge.DestinationNamespace, "broker_address": edge.BrokerAddress} {
		if value != "" {
			isolation[key] = value
		}
	}
	if len(isolation) > 0 {
		evidence["isolation_scope"] = isolation
	}
	return model.Finding{RuleID: r.ID, Severity: r.Severity, ProducerService: edge.Producer, ConsumerService: edge.Consumer, MessagingSystem: edge.System, Destination: edge.Destination, ConsumerGroup: edge.ConsumerGroup, Subscription: edge.Subscription, CorrelationMethod: method, Confidence: confidence, EvidenceState: model.EvidenceSufficient, Evidence: evidence, Message: r.Message, SuggestedFix: r.SuggestedFix}
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
	return wildcardMatch(x.Producer, o.Producer) && wildcardMatch(x.Consumer, o.Consumer) && wildcardMatch(x.System, o.System) && wildcardMatch(x.Destination, o.Destination) && optionalMatch(x.ConsumerGroup, o.ConsumerGroup) && optionalMatch(x.Subscription, o.Subscription) && optionalMatch(x.Environment, o.Environment) && optionalMatch(x.ServiceNamespace, o.ServiceNamespace) && optionalMatch(x.DestinationNamespace, o.DestinationNamespace) && optionalMatch(x.BrokerAddress, o.BrokerAddress)
}
func wildcardMatch(expected, actual string) bool { return expected == "*" || expected == actual }
func optionalMatch(expected, actual string) bool {
	return expected == "" || expected == "*" || expected == actual
}
func buildTopology(spans []model.Span, c correlation.Result) []model.Edge {
	m := map[string]*model.Edge{}
	for _, x := range c.Correlations {
		p, co := spans[x.ProducerIndex], spans[x.ConsumerIndex]
		k := strings.Join([]string{p.Service, p.System(), p.Destination(), co.Service, co.ConsumerGroup(), co.Subscription(), p.Environment(), p.ServiceNamespace(), p.DestinationNamespace(), p.ServerAddress()}, "\x00")
		if m[k] == nil {
			m[k] = &model.Edge{Producer: p.Service, System: p.System(), Destination: p.Destination(), Consumer: co.Service, ConsumerGroup: co.ConsumerGroup(), Subscription: co.Subscription(), Environment: p.Environment(), ServiceNamespace: p.ServiceNamespace(), DestinationNamespace: p.DestinationNamespace(), BrokerAddress: p.ServerAddress()}
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
			edge := model.Edge{Producer: producer.Service, System: producer.System(), Destination: producer.Destination(), Consumer: consumer.Service, ConsumerGroup: consumer.ConsumerGroup(), Subscription: consumer.Subscription(), Environment: producer.Environment(), ServiceNamespace: producer.ServiceNamespace(), DestinationNamespace: producer.DestinationNamespace(), BrokerAddress: producer.ServerAddress()}
			if edgeMatches(expected, edge) {
				found = true
				break
			}
		}
		if !found {
			identityKind, identity := producer.MessageIdentity()
			f := finding(r, producer, nil, "expected_delivery", model.ConfidenceMedium, map[string]any{"type": "missing_required_consumer", "consumer": expected.Consumer, "consumer_group": expected.ConsumerGroup, "subscription": expected.Subscription, "identity_kind": identityKind, "identity_present": identity != ""})
			markAbsenceEvidence(&f, audit)
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
	return span.IsProducer() && wildcardMatch(expected.Producer, span.Service) && wildcardMatch(expected.System, span.System()) && wildcardMatch(expected.Destination, span.Destination()) && optionalMatch(expected.Environment, span.Environment()) && optionalMatch(expected.ServiceNamespace, span.ServiceNamespace()) && optionalMatch(expected.DestinationNamespace, span.DestinationNamespace()) && optionalMatch(expected.BrokerAddress, span.ServerAddress())
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
			if c.ContextReferences[i] > 0 {
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
	return matchesScope(r.AppliesTo.Operations, s.Operation()) &&
		matchesScope(r.AppliesTo.Systems, s.System()) &&
		matchesScope(r.AppliesTo.Services, s.Service) &&
		matchesScope(r.AppliesTo.Destinations, s.Destination()) &&
		matchesScope(r.AppliesTo.Environments, s.Environment())
}
func matchesScope(values []string, actual string) bool {
	if len(values) == 0 {
		return true
	}
	for _, value := range values {
		if value == "*" || value == actual {
			return true
		}
	}
	return false
}
func allZeroHex(value string) bool {
	return value != "" && strings.Trim(value, "0") == ""
}
func timestampMissing(value time.Time) bool {
	return value.IsZero() || value.Equal(time.Unix(0, 0).UTC())
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
	if check == "unresolved_consumer_context" {
		return model.ConfidenceLow
	}
	return model.ConfidenceHigh
}

func markAbsenceEvidence(f *model.Finding, audit auditContext) {
	if !audit.coverageComplete {
		f.EvidenceState = model.EvidenceInsufficient
		f.Confidence = model.ConfidenceLow
	}
}

func correlationForConsumer(index int, result correlation.Result) *model.Correlation {
	for i := range result.Correlations {
		if result.Correlations[i].ConsumerIndex == index {
			return &result.Correlations[i]
		}
	}
	return nil
}

func baseCoverage(spans []model.Span, audit auditContext) model.Coverage {
	coverage := model.Coverage{Status: "unknown", InputCompleteness: "unknown", RetainedSpans: len(spans)}
	if audit.coverageComplete {
		coverage.Status = "complete"
		coverage.InputCompleteness = "complete"
	} else {
		coverage.Limitations = append(coverage.Limitations, "absence-based conclusions are unsafe without telemetry completeness evidence")
	}
	for _, span := range spans {
		coverage.DroppedAttributes += uint64(span.DroppedAttributesCount)
		coverage.DroppedLinks += uint64(span.DroppedLinksCount)
		for _, link := range span.Links {
			coverage.DroppedAttributes += uint64(link.DroppedAttributesCount)
		}
	}
	if coverage.DroppedAttributes > 0 || coverage.DroppedLinks > 0 {
		coverage.Status = "degraded"
		coverage.Limitations = append(coverage.Limitations, "OTLP reported dropped attributes or links")
	}
	return coverage
}

func brokerIdentityEvidence(span model.Span) map[string]any {
	identity := map[string]any{}
	values := map[string]string{
		"environment":           span.Environment(),
		"service_namespace":     span.ServiceNamespace(),
		"destination_namespace": span.DestinationNamespace(),
		"server_address":        span.ServerAddress(),
		"partition":             span.Partition(),
		"kafka_offset":          span.KafkaOffset(),
		"consumer_group":        span.ConsumerGroup(),
		"subscription":          span.Subscription(),
	}
	for key, value := range values {
		if value != "" {
			identity[key] = value
		}
	}
	return identity
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
		if !link.HasValidContext() {
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
