package server

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/XUanhoa04/async-trace-doctor/internal/config"
	"github.com/XUanhoa04/async-trace-doctor/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

func TestOTLPHTTPAcceptsGzipProtobuf(t *testing.T) {
	raw, err := proto.Marshal(&collectortrace.ExportTraceServiceRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var compressed bytes.Buffer
	zw := gzip.NewWriter(&compressed)
	if _, err = zw.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err = zw.Close(); err != nil {
		t.Fatal(err)
	}
	reg := prometheus.NewRegistry()
	svc := New(config.Config{}, Options{MaxRequestBytes: 1024, MaxSpans: 10, TTL: time.Minute}, reg)
	req := httptest.NewRequest(http.MethodPost, "/v1/traces", bytes.NewReader(compressed.Bytes()))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "gzip")
	rr := httptest.NewRecorder()
	svc.httpExport(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("got %d: %s", rr.Code, rr.Body.String())
	}
}
func TestDecompressedLimit(t *testing.T) {
	var compressed bytes.Buffer
	zw := gzip.NewWriter(&compressed)
	_, _ = zw.Write(bytes.Repeat([]byte("x"), 100))
	_ = zw.Close()
	if _, err := decompress(compressed.Bytes(), "gzip", 10); err == nil {
		t.Fatal("expected decompressed size error")
	}
}

func TestSuspiciousGzipCompressionRatio(t *testing.T) {
	var compressed bytes.Buffer
	zw := gzip.NewWriter(&compressed)
	_, _ = zw.Write(bytes.Repeat([]byte{0}, 1<<20))
	_ = zw.Close()
	if _, err := decompress(compressed.Bytes(), "gzip", 2<<20); err == nil || !strings.Contains(err.Error(), "compression ratio") {
		t.Fatalf("expected compression ratio rejection, got %v", err)
	}
}

func TestHTTPErrorResponseIsJSON(t *testing.T) {
	reg := prometheus.NewRegistry()
	svc := New(config.Config{}, Options{MaxRequestBytes: 1024, MaxSpans: 10, TTL: time.Minute}, reg)
	req := httptest.NewRequest(http.MethodPost, "/v1/traces", strings.NewReader("x"))
	req.Header.Set("Content-Type", "text/plain")
	rr := httptest.NewRecorder()
	svc.httpExport(rr, req)
	if rr.Code != http.StatusUnsupportedMediaType || !strings.Contains(rr.Header().Get("Content-Type"), "application/json") || !strings.Contains(rr.Body.String(), `"code":415`) {
		t.Fatalf("unexpected error response: %d %s %s", rr.Code, rr.Header(), rr.Body.String())
	}
}

func TestRateLimitMiddlewareRejectsBurst(t *testing.T) {
	metrics := NewMetrics(prometheus.NewRegistry())
	h := rateLimitMiddleware(newTokenBucket(0.001, 1), metrics)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	for i, want := range []int{http.StatusNoContent, http.StatusTooManyRequests} {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/v1/traces", nil))
		if rr.Code != want {
			t.Fatalf("request %d status = %d, want %d", i, rr.Code, want)
		}
	}
}

func TestRedactedReportHidesServiceAndTopology(t *testing.T) {
	svc := New(config.Config{}, Options{MaxSpans: 10, TTL: time.Minute, RedactReport: true}, prometheus.NewRegistry())
	svc.last = model.Report{Topology: []model.Edge{{Producer: "payments"}}, Findings: []model.Finding{{ProducerService: "payments", ConsumerService: "billing"}}}
	rr := httptest.NewRecorder()
	svc.handleReport(rr, httptest.NewRequest(http.MethodGet, "/report", nil))
	if strings.Contains(rr.Body.String(), "payments") || strings.Contains(rr.Body.String(), "billing") || strings.Contains(rr.Body.String(), `"topology"`) {
		t.Fatalf("sensitive report data leaked: %s", rr.Body.String())
	}
}

func TestPartialSuccessReportsRejectedSpans(t *testing.T) {
	response := exportResponse(AddResult{Accepted: 1, Rejected: 2})
	if response.GetPartialSuccess().GetRejectedSpans() != 2 || response.GetPartialSuccess().GetErrorMessage() == "" {
		t.Fatalf("missing OTLP partial success: %#v", response)
	}
}

func TestOTLPHTTPRejectsStaleSpanWithPartialSuccess(t *testing.T) {
	reg := prometheus.NewRegistry()
	svc := New(config.Config{}, Options{MaxRequestBytes: 4096, MaxSpans: 10, MaxRetainedBytes: 1 << 20, TTL: time.Minute}, reg)
	stale := time.Now().Add(-2 * time.Minute).UnixNano()
	raw := fmt.Sprintf(`{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","spanId":"bbbbbbbbbbbbbbbb","startTimeUnixNano":"%d","endTimeUnixNano":"%d"}]}]}]}`, stale-1, stale)
	req := httptest.NewRequest(http.MethodPost, "/v1/traces", strings.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	svc.httpExport(rr, req)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"rejectedSpans":"1"`) {
		t.Fatalf("missing stale partial success: %d %s", rr.Code, rr.Body.String())
	}
}
