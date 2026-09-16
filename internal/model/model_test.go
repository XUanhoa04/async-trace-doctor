package model

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func TestSortFindingsUsesStablePrecomputedKeys(t *testing.T) {
	findings := []Finding{{RuleID: "B", SpanIDs: []string{"2"}}, {RuleID: "A", SpanIDs: []string{"9"}}, {RuleID: "A", SpanIDs: []string{"1"}}}
	SortFindings(findings)
	if findings[0].SpanIDs[0] != "1" || findings[1].SpanIDs[0] != "9" || findings[2].RuleID != "B" {
		t.Fatalf("unexpected order: %#v", findings)
	}
}

func TestLinkAttrStringAndValidContext(t *testing.T) {
	link := Link{
		TraceID: "0123456789abcdef0123456789abcdef",
		SpanID:  "0123456789abcdef",
		Attributes: map[string]any{
			"messaging.message.id": "msg-123",
			"nil_value":            nil,
		},
	}
	if !link.HasValidContext() {
		t.Errorf("expected link to have valid context")
	}
	if got := link.AttrString("messaging.message.id"); got != "msg-123" {
		t.Errorf("link.AttrString() = %q, want %q", got, "msg-123")
	}
	if got := link.AttrString("nil_value"); got != "" {
		t.Errorf("link.AttrString(nil_value) = %q, want empty string", got)
	}
	if got := link.AttrString("non_existent"); got != "" {
		t.Errorf("link.AttrString(non_existent) = %q, want empty string", got)
	}

	invalidLinks := []Link{
		{TraceID: "00000000000000000000000000000000", SpanID: "0123456789abcdef"},
		{TraceID: "0123456789abcdef0123456789abcdef", SpanID: "0000000000000000"},
		{TraceID: "short", SpanID: "0123456789abcdef"},
		{TraceID: "0123456789abcdef0123456789abcdef", SpanID: "short"},
		{TraceID: "0123456789abcdef0123456789abcdeg", SpanID: "0123456789abcdef"}, // 'g' is non-hex
		{TraceID: "0123456789abcdef0123456789abcdef", SpanID: "0123456789abcdeg"}, // 'g' is non-hex
	}
	for _, invalid := range invalidLinks {
		if invalid.HasValidContext() {
			t.Errorf("expected invalid context for link: %#v", invalid)
		}
	}
}

func TestSpanAttrStringAndResourceAttrString(t *testing.T) {
	span := Span{
		Attributes: map[string]any{
			"string_val": "hello",
			"nil_val":    nil,
			"num_val":    42,
		},
		ResourceAttributes: map[string]any{
			"service.name": "order-service",
			"nil_res":      nil,
		},
	}

	if got := span.AttrString("string_val"); got != "hello" {
		t.Errorf("span.AttrString(string_val) = %q, want %q", got, "hello")
	}
	if got := span.AttrString("num_val"); got != "42" {
		t.Errorf("span.AttrString(num_val) = %q, want %q", got, "42")
	}
	if got := span.AttrString("nil_val"); got != "" {
		t.Errorf("span.AttrString(nil_val) = %q, want empty string", got)
	}
	if got := span.AttrString("missing"); got != "" {
		t.Errorf("span.AttrString(missing) = %q, want empty string", got)
	}

	if got := span.ResourceAttrString("service.name"); got != "order-service" {
		t.Errorf("span.ResourceAttrString(service.name) = %q, want %q", got, "order-service")
	}
	if got := span.ResourceAttrString("nil_res"); got != "" {
		t.Errorf("span.ResourceAttrString(nil_res) = %q, want empty string", got)
	}
	if got := span.ResourceAttrString("missing"); got != "" {
		t.Errorf("span.ResourceAttrString(missing) = %q, want empty string", got)
	}
}

func TestSpanAttrIntTypes(t *testing.T) {
	span := Span{
		Attributes: map[string]any{
			"int":         int(10),
			"int64":       int64(20),
			"float64":     float64(30.0),
			"json_number": json.Number("40"),
			"string_num":  "50",
			"invalid_str": "not-a-number",
			"nil_val":     nil,
		},
	}

	testCases := []struct {
		key      string
		expected int
		ok       bool
	}{
		{"int", 10, true},
		{"int64", 20, true},
		{"float64", 30, true},
		{"json_number", 40, true},
		{"string_num", 50, true},
		{"invalid_str", 0, false},
		{"nil_val", 0, false},
		{"missing", 0, false},
	}

	for _, tc := range testCases {
		got, ok := span.AttrInt(tc.key)
		if ok != tc.ok || got != tc.expected {
			t.Errorf("span.AttrInt(%q) = (%d, %v), want (%d, %v)", tc.key, got, ok, tc.expected, tc.ok)
		}
	}
}

func TestSpanHelpers(t *testing.T) {
	s1 := Span{
		Attributes: map[string]any{
			"messaging.operation.type": "SEND",
			"messaging.message.id":     "msg-01",
		},
	}
	if !s1.IsProducer() || s1.IsConsumer() {
		t.Errorf("expected s1 to be producer")
	}
	kind, id := s1.MessageIdentity()
	if kind != "message_id" || id != "msg-01" {
		t.Errorf("MessageIdentity() = (%s, %s), want (message_id, msg-01)", kind, id)
	}

	s2 := Span{
		Attributes: map[string]any{
			"messaging.system":                   "kafka",
			"messaging.destination.partition.id": "3",
			"messaging.kafka.offset":             "100",
		},
		StatusCode: "ERROR",
	}
	kind, id = s2.MessageIdentity()
	if kind != MethodKafkaPartOffset || id != "3/100" {
		t.Errorf("MessageIdentity() = (%s, %s), want (%s, 3/100)", kind, id, MethodKafkaPartOffset)
	}
	if !s2.Failed() {
		t.Errorf("expected s2.Failed() to be true")
	}
}

func TestSpanValidationAndDuration(t *testing.T) {
	now := time.Now()
	s := Span{
		TraceID:      "0123456789abcdef0123456789abcdef",
		SpanID:       "0123456789abcdef",
		ParentSpanID: "fedcba9876543210",
		Start:        now,
		End:          now.Add(250 * time.Millisecond),
	}
	if !s.HasValidContext() {
		t.Errorf("expected span to have valid context")
	}
	if !s.HasValidParentContext() {
		t.Errorf("expected span to have valid parent context")
	}
	if d := s.Duration(); d != 250*time.Millisecond {
		t.Errorf("span.Duration() = %v, want 250ms", d)
	}

	invalidSpan := Span{
		TraceID:      "invalid-trace-id",
		SpanID:       "invalid-span-id",
		ParentSpanID: "0000000000000000",
		Start:        now.Add(time.Second),
		End:          now, // end before start
	}
	if invalidSpan.HasValidContext() {
		t.Errorf("expected invalid context for invalid span")
	}
	if invalidSpan.HasValidParentContext() {
		t.Errorf("expected invalid parent context for zero parent span ID")
	}
	if d := invalidSpan.Duration(); d != 0 {
		t.Errorf("expected duration 0 for end before start, got %v", d)
	}
}

func TestHexAndIDHelpers(t *testing.T) {
	if !IsHex("0123456789abcdefABCDEF") {
		t.Errorf("expected valid hex")
	}
	if IsHex("0123456789abcdefg") {
		t.Errorf("expected non-hex to fail")
	}
	if !HasValidTraceID("0123456789abcdef0123456789abcdef") {
		t.Errorf("expected valid trace ID")
	}
	if HasValidTraceID("00000000000000000000000000000000") {
		t.Errorf("expected all zero trace ID to fail")
	}
	if HasValidTraceID("0123456789abcdef") {
		t.Errorf("expected short trace ID to fail")
	}
	if !HasValidSpanID("0123456789abcdef") {
		t.Errorf("expected valid span ID")
	}
	if HasValidSpanID("0000000000000000") {
		t.Errorf("expected all zero span ID to fail")
	}
	if HasValidSpanID("0123456789abcdef0123456789abcdef") {
		t.Errorf("expected long span ID to fail")
	}
}

func TestConfidenceString(t *testing.T) {
	for _, tc := range []struct {
		c    Confidence
		want string
	}{
		{ConfidenceHigh, "high"},
		{ConfidenceMedium, "medium"},
		{ConfidenceLow, "low"},
	} {
		if got := tc.c.String(); got != tc.want {
			t.Errorf("Confidence.String() = %q, want %q", got, tc.want)
		}
		// Verify it integrates with fmt.Sprintf without explicit cast.
		if got := fmt.Sprintf("%s", tc.c); got != tc.want {
			t.Errorf("fmt.Sprintf(%%s, Confidence) = %q, want %q", got, tc.want)
		}
	}
}

func TestSpanAttrFloat64(t *testing.T) {
	span := Span{
		Attributes: map[string]any{
			"float64":     float64(3.14),
			"float32":     float32(2.71),
			"int":         int(42),
			"int64":       int64(100),
			"json_number": json.Number("99.5"),
			"string_num":  "1.23",
			"invalid_str": "not-a-number",
			"nil_val":     nil,
		},
	}
	testCases := []struct {
		key      string
		expected float64
		ok       bool
	}{
		{"float64", 3.14, true},
		{"float32", 2.71, true},
		{"int", 42, true},
		{"int64", 100, true},
		{"json_number", 99.5, true},
		{"string_num", 1.23, true},
		{"invalid_str", 0, false},
		{"nil_val", 0, false},
		{"missing", 0, false},
	}
	for _, tc := range testCases {
		got, ok := span.AttrFloat64(tc.key)
		if ok != tc.ok {
			t.Errorf("AttrFloat64(%q) ok = %v, want %v", tc.key, ok, tc.ok)
			continue
		}
		if ok && (got-tc.expected) > 0.01 {
			t.Errorf("AttrFloat64(%q) = %v, want %v", tc.key, got, tc.expected)
		}
	}
}

func BenchmarkSortFindings(b *testing.B) {
	template := make([]Finding, 1000)
	for i := range template {
		template[i] = Finding{RuleID: fmt.Sprintf("ATD-%03d", 999-i), SpanIDs: []string{fmt.Sprintf("%016d", i)}, Message: "finding"}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		findings := append([]Finding(nil), template...)
		SortFindings(findings)
	}
}
