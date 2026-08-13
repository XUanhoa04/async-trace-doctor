package model

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

type Link struct {
	TraceID                string         `json:"trace_id"`
	SpanID                 string         `json:"span_id"`
	Attributes             map[string]any `json:"attributes,omitempty"`
	Flags                  uint32         `json:"flags,omitempty"`
	DroppedAttributesCount uint32         `json:"dropped_attributes_count,omitempty"`
}

func (l Link) HasValidContext() bool {
	return len(l.TraceID) == 32 && len(l.SpanID) == 16 && strings.Trim(l.TraceID, "0") != "" && strings.Trim(l.SpanID, "0") != ""
}

type Span struct {
	TraceID                string         `json:"trace_id"`
	SpanID                 string         `json:"span_id"`
	ParentSpanID           string         `json:"parent_span_id,omitempty"`
	Name                   string         `json:"name"`
	Kind                   string         `json:"kind"`
	Service                string         `json:"service_name,omitempty"`
	Start                  time.Time      `json:"start_time"`
	End                    time.Time      `json:"end_time"`
	Attributes             map[string]any `json:"attributes,omitempty"`
	ResourceAttributes     map[string]any `json:"resource_attributes,omitempty"`
	Links                  []Link         `json:"links,omitempty"`
	StatusCode             string         `json:"status_code,omitempty"`
	Flags                  uint32         `json:"flags,omitempty"`
	DroppedAttributesCount uint32         `json:"dropped_attributes_count,omitempty"`
	DroppedLinksCount      uint32         `json:"dropped_links_count,omitempty"`
}

func (s Span) AttrString(key string) string {
	v, ok := s.Attributes[key]
	if !ok {
		return ""
	}
	return fmt.Sprint(v)
}

func (s Span) ResourceAttrString(key string) string {
	v, ok := s.ResourceAttributes[key]
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
func (s Span) ConsumerGroup() string {
	return s.AttrString("messaging.consumer.group.name")
}
func (s Span) Subscription() string {
	return s.AttrString("messaging.destination.subscription.name")
}
func (s Span) Environment() string {
	if v := s.ResourceAttrString("deployment.environment.name"); v != "" {
		return v
	}
	return s.ResourceAttrString("deployment.environment")
}
func (s Span) ServiceNamespace() string { return s.ResourceAttrString("service.namespace") }
func (s Span) DestinationNamespace() string {
	return s.AttrString("messaging.destination.namespace")
}
func (s Span) ServerAddress() string { return s.AttrString("server.address") }
func (s Span) Partition() string     { return s.AttrString("messaging.destination.partition.id") }
func (s Span) KafkaOffset() string   { return s.AttrString("messaging.kafka.offset") }
func (s Span) MessageIdentity() (string, string) {
	if s.MessageID() != "" {
		return "message_id", s.MessageID()
	}
	if strings.EqualFold(s.System(), "kafka") && s.Partition() != "" && s.KafkaOffset() != "" {
		return MethodKafkaPartOffset, s.Partition() + "/" + s.KafkaOffset()
	}
	return "", ""
}
func (s Span) Failed() bool {
	return strings.EqualFold(s.StatusCode, "ERROR") || s.AttrString("error.type") != ""
}
func (s Span) IsProducer() bool { return s.Operation() == "create" || s.Operation() == "send" }
func (s Span) IsConsumer() bool { return s.Operation() == "process" || s.Operation() == "receive" }

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
	ConsumerGroup     string         `json:"consumer_group,omitempty"`
	Subscription      string         `json:"subscription,omitempty"`
	TraceIDs          []string       `json:"trace_ids,omitempty"`
	SpanIDs           []string       `json:"span_ids,omitempty"`
	CorrelationMethod string         `json:"correlation_method,omitempty"`
	Confidence        Confidence     `json:"confidence"`
	EvidenceState     string         `json:"evidence_state"`
	Evidence          map[string]any `json:"evidence"`
	Message           string         `json:"message"`
	SuggestedFix      string         `json:"suggested_fix"`
}

const (
	EvidenceSufficient   = "sufficient"
	EvidenceInsufficient = "insufficient"
	EvidenceDegraded     = "degraded"
)

type Edge struct {
	Producer             string `json:"producer"`
	System               string `json:"system"`
	Destination          string `json:"destination"`
	Consumer             string `json:"consumer"`
	ConsumerGroup        string `json:"consumer_group,omitempty"`
	Subscription         string `json:"subscription,omitempty"`
	Environment          string `json:"environment,omitempty"`
	ServiceNamespace     string `json:"service_namespace,omitempty"`
	DestinationNamespace string `json:"destination_namespace,omitempty"`
	BrokerAddress        string `json:"broker_address,omitempty"`
	Count                int    `json:"count"`
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

type Coverage struct {
	Status                string   `json:"status"`
	InputCompleteness     string   `json:"input_completeness"`
	RetainedSpans         int      `json:"retained_spans,omitempty"`
	RejectedSpans         uint64   `json:"rejected_spans,omitempty"`
	DuplicateExports      uint64   `json:"duplicate_exports,omitempty"`
	ConflictingDuplicates uint64   `json:"conflicting_duplicates,omitempty"`
	TTLEvictions          uint64   `json:"ttl_evictions,omitempty"`
	DroppedAttributes     uint64   `json:"dropped_attributes,omitempty"`
	DroppedLinks          uint64   `json:"dropped_links,omitempty"`
	Limitations           []string `json:"limitations,omitempty"`
}

type Report struct {
	SchemaVersion             string    `json:"schema_version"`
	SemanticConventionVersion string    `json:"semantic_convention_version"`
	GeneratedAt               time.Time `json:"generated_at"`
	Summary                   Summary   `json:"summary"`
	Coverage                  Coverage  `json:"coverage"`
	Findings                  []Finding `json:"findings"`
	Topology                  []Edge    `json:"topology"`
	QueueLatencySamples       []float64 `json:"-"`
}

func SortFindings(findings []Finding) {
	keys := make([]string, len(findings))
	for i := range findings {
		keys[i] = findings[i].RuleID + "\x00" + strings.Join(findings[i].SpanIDs, ",") + "\x00" + findings[i].Message
	}
	sort.Sort(findingSorter{findings: findings, keys: keys})
}

type findingSorter struct {
	findings []Finding
	keys     []string
}

func (s findingSorter) Len() int           { return len(s.findings) }
func (s findingSorter) Less(i, j int) bool { return s.keys[i] < s.keys[j] }
func (s findingSorter) Swap(i, j int) {
	s.findings[i], s.findings[j] = s.findings[j], s.findings[i]
	s.keys[i], s.keys[j] = s.keys[j], s.keys[i]
}
