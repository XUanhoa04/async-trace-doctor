package server

import (
	"bytes"
	"compress/gzip"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/XUanhoa04/async-trace-doctor/internal/config"
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
