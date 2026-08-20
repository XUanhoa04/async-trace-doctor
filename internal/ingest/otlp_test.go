package ingest

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/XUanhoa04/async-trace-doctor/internal/model"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/protobuf/proto"
	"time"
)

func TestDecodeCollectorOTLPJSON(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "testdata", "core", "normal.json"))
	if err != nil {
		t.Fatal(err)
	}
	spans, err := DecodeJSON(b, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(spans) != 2 {
		t.Fatalf("got %d spans", len(spans))
	}
	if spans[0].Service != "checkout" || spans[1].Links[0].SpanID != "1111111111111111" {
		t.Fatalf("unexpected decoded spans: %#v", spans)
	}
}
func TestReadPathJSONLAndRedaction(t *testing.T) {
	spans, err := ReadPath(filepath.Join("..", "..", "testdata", "holdout", "orphan-consumer.jsonl"), Limits{MaxBytes: 1 << 20, MaxSpans: 10}, []string{"messaging.destination.name"})
	if err != nil {
		t.Fatal(err)
	}
	if spans[0].Destination() != "[REDACTED]" {
		t.Fatalf("redaction not applied: %q", spans[0].Destination())
	}
}
func TestMalformedOTLP(t *testing.T) {
	_, err := ReadPath(filepath.Join("..", "..", "testdata", "malformed.json"), Limits{MaxBytes: 1 << 20, MaxSpans: 10}, nil)
	if err == nil {
		t.Fatal("expected malformed input error")
	}
}
func TestInputLimits(t *testing.T) {
	_, err := ReadPath(filepath.Join("..", "..", "testdata", "core", "normal.json"), Limits{MaxBytes: 10, MaxSpans: 10}, nil)
	if err == nil {
		t.Fatal("expected byte limit error")
	}
}

func TestDecodeSpanStatus(t *testing.T) {
	raw := []byte(`{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"11111111111111111111111111111111","spanId":"1111111111111111","name":"process q","startTimeUnixNano":"1","endTimeUnixNano":"2","status":{"code":2}}]}]}]}`)
	spans, err := DecodeJSON(raw, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(spans) != 1 || spans[0].StatusCode != "ERROR" {
		t.Fatalf("status was not preserved: %#v", spans)
	}
}

func TestOfflineInputDeduplicatesAndRejectsConflictingIdentity(t *testing.T) {
	span := model.Span{TraceID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SpanID: "aaaaaaaaaaaaaaaa", Name: "one"}
	got, err := deduplicateSpans([]model.Span{span, span})
	if err != nil || len(got) != 1 {
		t.Fatalf("identical duplicate was not removed: %#v, %v", got, err)
	}
	conflict := span
	conflict.Name = "different"
	if _, err := deduplicateSpans([]model.Span{span, conflict}); err == nil {
		t.Fatal("conflicting duplicate identity must fail offline audit")
	}
}

func TestDecodeJSONRejectsMalformedIDs(t *testing.T) {
	for _, tc := range []struct{ name, traceID string }{
		{name: "odd length", traceID: strings.Repeat("a", 31)},
		{name: "non hex", traceID: strings.Repeat("z", 32)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := []byte(`{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"` + tc.traceID + `","spanId":"1111111111111111"}]}]}]}`)
			if _, err := DecodeJSON(raw, nil); err == nil {
				t.Fatal("expected malformed ID error")
			}
		})
	}
}

func TestDecodeJSONIDEdgeCasesAndNestedLinks(t *testing.T) {
	raw := []byte(`{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","spanId":"BBBBBBBBBBBBBBBB","parentSpanId":"","links":[{"traceId":"CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC","spanId":"DDDDDDDDDDDDDDDD"}]}]}]}]}`)
	spans, err := DecodeJSON(raw, nil)
	if err != nil {
		t.Fatal(err)
	}
	if spans[0].TraceID != strings.Repeat("a", 32) || spans[0].ParentSpanID != "" || spans[0].Links[0].SpanID != strings.Repeat("d", 16) {
		t.Fatalf("IDs were not normalized: %#v", spans[0])
	}
}

func TestDecodeJSONAttributesAndDroppedCounts(t *testing.T) {
	raw := []byte(`{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"11111111111111111111111111111111","spanId":"2222222222222222","droppedAttributesCount":7,"droppedLinksCount":3,"attributes":[{"key":"message.payload","value":{"stringValue":"secret"}},{"key":"array","value":{"arrayValue":{"values":[{"stringValue":"a"},{"stringValue":"b"}]}}},{"key":"kv","value":{"kvlistValue":{"values":[{"key":"a","value":{"stringValue":"b"}}]}}}]}]}]}]}`)
	spans, err := DecodeJSON(raw, nil)
	if err != nil {
		t.Fatal(err)
	}
	span := spans[0]
	if span.Attributes["message.payload"] != "[REDACTED]" || span.Attributes["array"] != "[2 values]" || span.Attributes["kv"] != "{1 attributes}" || span.DroppedAttributesCount != 7 || span.DroppedLinksCount != 3 {
		t.Fatalf("attributes or dropped counts not preserved: %#v", span)
	}
}

func TestDecodeJSONAndProtobufParity(t *testing.T) {
	raw := []byte(`{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"checkout"}}]},"scopeSpans":[{"spans":[{"traceId":"11111111111111111111111111111111","spanId":"2222222222222222","name":"send","kind":4,"startTimeUnixNano":"1","endTimeUnixNano":"2","attributes":[{"key":"messaging.system","value":{"stringValue":"kafka"}}]}]}]}]}`)
	jsonSpans, err := DecodeJSON(raw, nil)
	if err != nil {
		t.Fatal(err)
	}
	req, err := DecodeRequestJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	var decoded collectortrace.ExportTraceServiceRequest
	if err = proto.Unmarshal(wire, &decoded); err != nil {
		t.Fatal(err)
	}
	protoSpans := FromProto(decoded.ResourceSpans, nil)
	if !reflect.DeepEqual(jsonSpans, protoSpans) {
		t.Fatalf("JSON/protobuf mismatch:\njson=%#v\nprotobuf=%#v", jsonSpans, protoSpans)
	}
}

func TestAnyValueDecoder(t *testing.T) {
	if got := value(nil); got != nil {
		t.Errorf("value(nil) = %v, want nil", got)
	}

	strVal := &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "hello"}}
	if got := value(strVal); got != "hello" {
		t.Errorf("value(StringValue) = %v, want hello", got)
	}

	boolVal := &commonv1.AnyValue{Value: &commonv1.AnyValue_BoolValue{BoolValue: true}}
	if got := value(boolVal); got != true {
		t.Errorf("value(BoolValue) = %v, want true", got)
	}

	intVal := &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: 12345}}
	if got := value(intVal); got != int64(12345) {
		t.Errorf("value(IntValue) = %v, want 12345", got)
	}

	doubleVal := &commonv1.AnyValue{Value: &commonv1.AnyValue_DoubleValue{DoubleValue: 3.14}}
	if got := value(doubleVal); got != 3.14 {
		t.Errorf("value(DoubleValue) = %v, want 3.14", got)
	}

	bytesVal := &commonv1.AnyValue{Value: &commonv1.AnyValue_BytesValue{BytesValue: []byte("abc")}}
	if got := value(bytesVal); got != "[3 bytes]" {
		t.Errorf("value(BytesValue) = %v, want [3 bytes]", got)
	}

	arrVal := &commonv1.AnyValue{Value: &commonv1.AnyValue_ArrayValue{ArrayValue: &commonv1.ArrayValue{Values: []*commonv1.AnyValue{strVal, intVal}}}}
	if got := value(arrVal); got != "[2 values]" {
		t.Errorf("value(ArrayValue) = %v, want [2 values]", got)
	}

	kvVal := &commonv1.AnyValue{Value: &commonv1.AnyValue_KvlistValue{KvlistValue: &commonv1.KeyValueList{Values: []*commonv1.KeyValue{{Key: "k", Value: strVal}}}}}
	if got := value(kvVal); got != "{1 attributes}" {
		t.Errorf("value(KvlistValue) = %v, want {1 attributes}", got)
	}
}

func TestIsPayloadAttribute(t *testing.T) {
	payloadKeys := []string{
		"message.payload",
		"messaging.payload.size",
		"http.request.body",
		"http.response.body.content",
		"PAYLOAD",
	}
	for _, key := range payloadKeys {
		if !isPayloadAttribute(key) {
			t.Errorf("expected isPayloadAttribute(%q) to be true", key)
		}
	}

	nonPayloadKeys := []string{
		"messaging.system",
		"messaging.destination.name",
		"service.name",
		"http.status_code",
	}
	for _, key := range nonPayloadKeys {
		if isPayloadAttribute(key) {
			t.Errorf("expected isPayloadAttribute(%q) to be false", key)
		}
	}
}

func TestReadPathErrors(t *testing.T) {
	// Non-existent path
	_, err := ReadPath(filepath.Join("..", "..", "testdata", "non_existent_file.json"), Limits{}, nil)
	if err == nil {
		t.Fatal("expected error for non-existent path, got nil")
	}

	// Empty directory (create temp dir with no json files)
	tempDir := t.TempDir()
	_, err = ReadPath(tempDir, Limits{}, nil)
	if err == nil {
		t.Fatal("expected error for directory with no json files, got nil")
	}
}

func TestDedupContentHashDifferences(t *testing.T) {
	now := time.Now()
	base := model.Span{
		TraceID: "0123456789abcdef0123456789abcdef",
		SpanID:  "0123456789abcdef",
		Name:    "process",
		Kind:    "CONSUMER",
		Service: "order-worker",
		Start:   now,
		End:     now.Add(time.Second),
	}

	baseHash := dedupContentHash(base)

	// Differing service
	diffService := base
	diffService.Service = "different-worker"
	if dedupContentHash(diffService) == baseHash {
		t.Errorf("expected different hash for differing service")
	}

	// Differing status
	diffStatus := base
	diffStatus.StatusCode = "ERROR"
	if dedupContentHash(diffStatus) == baseHash {
		t.Errorf("expected different hash for differing status code")
	}

	// Differing time
	diffTime := base
	diffTime.End = now.Add(2 * time.Second)
	if dedupContentHash(diffTime) == baseHash {
		t.Errorf("expected different hash for differing end time")
	}
}

func BenchmarkDeduplicateSpans(b *testing.B) {
	spans := make([]model.Span, 10000)
	for i := range spans {
		identity := i / 2
		spans[i] = model.Span{TraceID: fmt.Sprintf("%032x", identity), SpanID: fmt.Sprintf("%016x", identity), Name: "span", Attributes: map[string]any{"key": "value"}}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := deduplicateSpans(spans); err != nil {
			b.Fatal(err)
		}
	}
}
