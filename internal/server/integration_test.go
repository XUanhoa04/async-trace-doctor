package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/XUanhoa04/async-trace-doctor/internal/config"
	"github.com/XUanhoa04/async-trace-doctor/internal/ingest"
	"github.com/prometheus/client_golang/prometheus"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestServerStartStopReadyAndMetrics(t *testing.T) {
	cfg, err := config.Load(filepath.Join("..", "..", "config", "rules.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	httpAddr, grpcAddr, adminAddr := freeAddress(t), freeAddress(t), freeAddress(t)
	reg := prometheus.NewRegistry()
	svc := New(cfg, Options{HTTPAddress: httpAddr, GRPCAddress: grpcAddr, AdminAddress: adminAddr, MaxRequestBytes: 1 << 20, MaxRetainedBytes: 1 << 20, MaxSpans: 100, TTL: time.Minute, AuditInterval: 20 * time.Millisecond}, reg)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx, reg) }()
	waitHTTP(t, "http://"+adminAddr+"/health", http.StatusOK)
	now := time.Now().UnixNano()
	raw := fmt.Sprintf(`{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"%s","spanId":"%s","name":"invalid","startTimeUnixNano":"%d","endTimeUnixNano":"%d"}]}]}]}`, strings.Repeat("a", 32), strings.Repeat("b", 16), now, now+1)
	resp, err := http.Post("http://"+httpAddr+"/v1/traces", "application/json", bytes.NewBufferString(raw))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	waitHTTP(t, "http://"+adminAddr+"/ready", http.StatusOK)
	waitHTTPBody(t, "http://"+adminAddr+"/report", `"audited_spans":1`)
	reportResp, err := http.Get("http://" + adminAddr + "/report")
	if err != nil {
		t.Fatal(err)
	}
	reportBody, _ := io.ReadAll(reportResp.Body)
	_ = reportResp.Body.Close()
	if !bytes.Contains(reportBody, []byte(`"audited_spans":1`)) {
		t.Fatalf("report did not include ingested span: %s", reportBody)
	}
	metricsResp, err := http.Get("http://" + adminAddr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	metricsBody, _ := io.ReadAll(metricsResp.Body)
	_ = metricsResp.Body.Close()
	if !bytes.Contains(metricsBody, []byte("async_trace_audited_spans_total 1")) {
		t.Fatalf("metrics did not expose audited span: %s", metricsBody)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not shut down")
	}
}

func TestServerGRPCExportAndFinalDrain(t *testing.T) {
	cfg, err := config.Load(filepath.Join("..", "..", "config", "rules.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	httpAddr, grpcAddr, adminAddr := freeAddress(t), freeAddress(t), freeAddress(t)
	reg := prometheus.NewRegistry()
	svc := New(cfg, Options{HTTPAddress: httpAddr, GRPCAddress: grpcAddr, AdminAddress: adminAddr, MaxRequestBytes: 1 << 20, MaxRetainedBytes: 1 << 20, MaxSpans: 100, TTL: time.Minute, AuditInterval: time.Hour}, reg)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx, reg) }()
	waitHTTP(t, "http://"+adminAddr+"/health", http.StatusOK)
	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	now := time.Now().UnixNano()
	raw := []byte(fmt.Sprintf(`{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","spanId":"bbbbbbbbbbbbbbbb","name":"grpc","startTimeUnixNano":"%d","endTimeUnixNano":"%d"}]}]}]}`, now, now+1))
	req, err := ingest.DecodeRequestJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = collectortrace.NewTraceServiceClient(conn).Export(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	svc.mu.RLock()
	audited := svc.last.Summary.AuditedSpans
	svc.mu.RUnlock()
	if audited != 1 {
		t.Fatalf("final drain audited %d spans, want 1", audited)
	}
}

func freeAddress(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func waitHTTP(t *testing.T, url string, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == want {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s did not return %d", url, want)
}

func waitHTTPBody(t *testing.T, url, fragment string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if bytes.Contains(body, []byte(fragment)) {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s did not contain %q", url, fragment)
}
