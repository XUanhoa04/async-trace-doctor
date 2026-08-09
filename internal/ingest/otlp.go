package ingest

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/XUanhoa04/async-trace-doctor/internal/model"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

type Limits struct {
	MaxBytes int64
	MaxSpans int
}

func ReadPath(path string, limits Limits, redact []string) ([]model.Span, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat input: %w", err)
	}
	var files []string
	if info.IsDir() {
		err = filepath.WalkDir(path, func(p string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if !d.IsDir() && (strings.HasSuffix(strings.ToLower(p), ".json") || strings.HasSuffix(strings.ToLower(p), ".jsonl")) {
				files = append(files, p)
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("walk input: %w", err)
		}
		sort.Strings(files)
	} else {
		files = []string{path}
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no .json or .jsonl files found in %s", path)
	}
	var spans []model.Span
	var consumed int64
	for _, f := range files {
		st, err := os.Stat(f)
		if err != nil {
			return nil, err
		}
		consumed += st.Size()
		if limits.MaxBytes > 0 && consumed > limits.MaxBytes {
			return nil, fmt.Errorf("input exceeds max bytes (%d)", limits.MaxBytes)
		}
		parsed, err := readFile(f, limits.MaxBytes, redact)
		if err != nil {
			return nil, err
		}
		spans = append(spans, parsed...)
		if limits.MaxSpans > 0 && len(spans) > limits.MaxSpans {
			return nil, fmt.Errorf("input exceeds max spans (%d)", limits.MaxSpans)
		}
	}
	return spans, nil
}

func readFile(path string, maxBytes int64, redact []string) ([]model.Span, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	r := io.Reader(f)
	if maxBytes > 0 {
		r = io.LimitReader(f, maxBytes+1)
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	trim := bytes.TrimSpace(b)
	if len(trim) == 0 {
		return nil, fmt.Errorf("%s: empty input", path)
	}
	if spans, err := DecodeJSON(trim, redact); err == nil {
		return spans, nil
	}
	var all []model.Span
	scanner := bufio.NewScanner(bytes.NewReader(trim))
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	line := 0
	for scanner.Scan() {
		line++
		raw := bytes.TrimSpace(scanner.Bytes())
		if len(raw) == 0 {
			continue
		}
		spans, err := DecodeJSON(raw, redact)
		if err != nil {
			return nil, fmt.Errorf("%s line %d: malformed OTLP JSON: %w", path, line, err)
		}
		all = append(all, spans...)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan %s: %w", path, err)
	}
	return all, nil
}

func DecodeJSON(data []byte, redact []string) ([]model.Span, error) {
	req, err := DecodeRequestJSON(data)
	if err != nil {
		return nil, err
	}
	return FromProto(req.ResourceSpans, redact), nil
}

// DecodeRequestJSON implements the OTLP/JSON deviation from protobuf JSON:
// traceId, spanId, and parentSpanId use hexadecimal rather than base64 bytes.
func DecodeRequestJSON(data []byte) (*collectortrace.ExportTraceServiceRequest, error) {
	var document any
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&document); err != nil {
		return nil, err
	}
	if err := normalizeIDs(document); err != nil {
		return nil, err
	}
	normalized, err := json.Marshal(document)
	if err != nil {
		return nil, err
	}
	var req collectortrace.ExportTraceServiceRequest
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(normalized, &req); err != nil {
		return nil, err
	}
	return &req, nil
}

func normalizeIDs(v any) error {
	switch x := v.(type) {
	case map[string]any:
		for k, child := range x {
			if k == "traceId" || k == "spanId" || k == "parentSpanId" {
				s, ok := child.(string)
				if !ok {
					return fmt.Errorf("%s must be a hexadecimal string", k)
				}
				if s == "" && k == "parentSpanId" {
					continue
				}
				want := 16
				if k == "traceId" {
					want = 32
				}
				if len(s) != want {
					return fmt.Errorf("%s must contain %d hexadecimal characters", k, want)
				}
				decoded, err := hex.DecodeString(s)
				if err != nil {
					return fmt.Errorf("invalid %s: %w", k, err)
				}
				x[k] = base64.StdEncoding.EncodeToString(decoded)
				continue
			}
			if err := normalizeIDs(child); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range x {
			if err := normalizeIDs(child); err != nil {
				return err
			}
		}
	}
	return nil
}

func FromProto(resources []*tracev1.ResourceSpans, redact []string) []model.Span {
	redacted := map[string]bool{}
	for _, k := range redact {
		redacted[k] = true
	}
	var out []model.Span
	for _, rs := range resources {
		resourceAttrs := attrs(rs.GetResource().GetAttributes(), redacted)
		service := stringAttr(resourceAttrs, "service.name")
		for _, ss := range rs.GetScopeSpans() {
			for _, s := range ss.GetSpans() {
				a := attrs(s.GetAttributes(), redacted)
				links := make([]model.Link, 0, len(s.GetLinks()))
				for _, l := range s.GetLinks() {
					links = append(links, model.Link{TraceID: hex.EncodeToString(l.GetTraceId()), SpanID: hex.EncodeToString(l.GetSpanId()), Attributes: attrs(l.GetAttributes(), redacted)})
				}
				out = append(out, model.Span{TraceID: hex.EncodeToString(s.GetTraceId()), SpanID: hex.EncodeToString(s.GetSpanId()), ParentSpanID: hex.EncodeToString(s.GetParentSpanId()), Name: s.GetName(), Kind: kind(s.GetKind()), Service: service, Start: time.Unix(0, int64(s.GetStartTimeUnixNano())).UTC(), End: time.Unix(0, int64(s.GetEndTimeUnixNano())).UTC(), Attributes: a, Links: links, StatusCode: statusCode(s.GetStatus().GetCode())})
			}
		}
	}
	return out
}

func attrs(kvs []*commonv1.KeyValue, redacted map[string]bool) map[string]any {
	out := make(map[string]any, len(kvs))
	for _, kv := range kvs {
		if redacted[kv.Key] || isPayloadAttribute(kv.Key) {
			out[kv.Key] = "[REDACTED]"
		} else {
			out[kv.Key] = value(kv.Value)
		}
	}
	return out
}

func isPayloadAttribute(key string) bool {
	k := strings.ToLower(key)
	return strings.Contains(k, "payload") || strings.HasSuffix(k, ".body") || strings.HasSuffix(k, ".body.content")
}
func value(v *commonv1.AnyValue) any {
	if v == nil {
		return nil
	}
	switch x := v.Value.(type) {
	case *commonv1.AnyValue_StringValue:
		return x.StringValue
	case *commonv1.AnyValue_BoolValue:
		return x.BoolValue
	case *commonv1.AnyValue_IntValue:
		return x.IntValue
	case *commonv1.AnyValue_DoubleValue:
		return x.DoubleValue
	case *commonv1.AnyValue_BytesValue:
		return fmt.Sprintf("[%d bytes]", len(x.BytesValue))
	case *commonv1.AnyValue_ArrayValue:
		return fmt.Sprintf("[%d values]", len(x.ArrayValue.Values))
	case *commonv1.AnyValue_KvlistValue:
		return fmt.Sprintf("{%d attributes}", len(x.KvlistValue.Values))
	default:
		return nil
	}
}
func stringAttr(m map[string]any, k string) string {
	if v, ok := m[k]; ok {
		return fmt.Sprint(v)
	}
	return ""
}
func kind(k tracev1.Span_SpanKind) string {
	switch k {
	case tracev1.Span_SPAN_KIND_INTERNAL:
		return "INTERNAL"
	case tracev1.Span_SPAN_KIND_SERVER:
		return "SERVER"
	case tracev1.Span_SPAN_KIND_CLIENT:
		return "CLIENT"
	case tracev1.Span_SPAN_KIND_PRODUCER:
		return "PRODUCER"
	case tracev1.Span_SPAN_KIND_CONSUMER:
		return "CONSUMER"
	default:
		return "UNSPECIFIED"
	}
}

func statusCode(code tracev1.Status_StatusCode) string {
	switch code {
	case tracev1.Status_STATUS_CODE_OK:
		return "OK"
	case tracev1.Status_STATUS_CODE_ERROR:
		return "ERROR"
	default:
		return "UNSET"
	}
}
