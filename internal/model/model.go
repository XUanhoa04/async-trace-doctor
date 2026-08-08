package model

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

type Link struct {
	TraceID    string         `json:"trace_id"`
	SpanID     string         `json:"span_id"`
	Attributes map[string]any `json:"attributes,omitempty"`
}

type Span struct {
	TraceID      string         `json:"trace_id"`
	SpanID       string         `json:"span_id"`
	ParentSpanID string         `json:"parent_span_id,omitempty"`
	Name         string         `json:"name"`
	Kind         string         `json:"kind"`
	Service      string         `json:"service_name,omitempty"`
	Start        time.Time      `json:"start_time"`
	End          time.Time      `json:"end_time"`
	Attributes   map[string]any `json:"attributes,omitempty"`
	Links        []Link         `json:"links,omitempty"`
}

func (s Span) AttrString(key string) string {
	v, ok := s.Attributes[key]
	if !ok {
		return ""
	}
	return fmt.Sprint(v)
}

func (s Span) AttrInt(key string) (int, bool) {
	v, ok := s.Attributes[key]
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case int:
		return n, true
	case int64:
		return int(n), true
	case float64:
		return int(n), true
	case json.Number:
		i, err := n.Int64()
		return int(i), err == nil
	default:
		var i int
		_, err := fmt.Sscan(fmt.Sprint(v), &i)
		return i, err == nil
	}
}

func (s Span) Operation() string   { return strings.ToLower(s.AttrString("messaging.operation.type")) }
func (s Span) System() string      { return s.AttrString("messaging.system") }
func (s Span) Destination() string { return s.AttrString("messaging.destination.name") }
func (s Span) MessageID() string   { return s.AttrString("messaging.message.id") }
func (s Span) IsProducer() bool    { return s.Operation() == "create" || s.Operation() == "send" }
func (s Span) IsConsumer() bool    { return s.Operation() == "process" || s.Operation() == "receive" }

type Confidence string

const (
	ConfidenceHigh   Confidence = "high"
	ConfidenceMedium Confidence = "medium"
	ConfidenceLow    Confidence = "low"
)

type Correlation struct {
	ProducerIndex int           `json:"-"`
	ConsumerIndex int           `json:"-"`
	Producer      SpanRef       `json:"producer"`
	Consumer      SpanRef       `json:"consumer"`
	Method        string        `json:"method"`
	Confidence    Confidence    `json:"confidence"`
	QueueLatency  time.Duration `json:"queue_latency_ns"`
}

type SpanRef struct {
	TraceID string `json:"trace_id,omitempty"`
	SpanID  string `json:"span_id,omitempty"`
	Service string `json:"service,omitempty"`
}

func Ref(s Span) SpanRef { return SpanRef{TraceID: s.TraceID, SpanID: s.SpanID, Service: s.Service} }

type Finding struct {
	RuleID            string         `json:"rule_id"`
	Severity          string         `json:"severity"`
	ProducerService   string         `json:"producer_service,omitempty"`
	ConsumerService   string         `json:"consumer_service,omitempty"`
	MessagingSystem   string         `json:"messaging_system,omitempty"`
	Destination       string         `json:"destination,omitempty"`
	TraceIDs          []string       `json:"trace_ids,omitempty"`
	SpanIDs           []string       `json:"span_ids,omitempty"`
	CorrelationMethod string         `json:"correlation_method,omitempty"`
	Confidence        Confidence     `json:"confidence"`
	Evidence          map[string]any `json:"evidence"`
	Message           string         `json:"message"`
	SuggestedFix      string         `json:"suggested_fix"`
}

type Edge struct {
	Producer    string `json:"producer"`
	System      string `json:"system"`
	Destination string `json:"destination"`
	Consumer    string `json:"consumer"`
	Count       int    `json:"count"`
}

type Summary struct {
	AuditedSpans             int     `json:"audited_spans"`
	MessagingSpans           int     `json:"messaging_spans"`
	Violations               int     `json:"violations"`
	BrokenLinks              int     `json:"broken_links"`
	OrphanProducers          int     `json:"orphan_producers"`
	OrphanConsumers          int     `json:"orphan_consumers"`
	ContextCompletenessRatio float64 `json:"context_completeness_ratio"`
	ProcessingMillis         int64   `json:"processing_millis"`
}

type Report struct {
	SchemaVersion             string    `json:"schema_version"`
	SemanticConventionVersion string    `json:"semantic_convention_version"`
	GeneratedAt               time.Time `json:"generated_at"`
	Summary                   Summary   `json:"summary"`
	Findings                  []Finding `json:"findings"`
	Topology                  []Edge    `json:"topology"`
}

func SortFindings(findings []Finding) {
	sort.Slice(findings, func(i, j int) bool {
		if findings[i].RuleID != findings[j].RuleID {
			return findings[i].RuleID < findings[j].RuleID
		}
		if strings.Join(findings[i].SpanIDs, ",") != strings.Join(findings[j].SpanIDs, ",") {
			return strings.Join(findings[i].SpanIDs, ",") < strings.Join(findings[j].SpanIDs, ",")
		}
		return findings[i].Message < findings[j].Message
	})
}
