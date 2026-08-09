package server

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/XUanhoa04/async-trace-doctor/internal/config"
	"github.com/XUanhoa04/async-trace-doctor/internal/ingest"
	"github.com/XUanhoa04/async-trace-doctor/internal/model"
	"github.com/XUanhoa04/async-trace-doctor/internal/rules"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type Options struct {
	HTTPAddress, GRPCAddress, AdminAddress string
	MaxRequestBytes                        int64
	MaxSpans                               int
	TTL, AuditInterval                     time.Duration
}
type Service struct {
	collectortrace.UnimplementedTraceServiceServer
	cfg     config.Config
	engine  rules.Engine
	store   *Store
	metrics *Metrics
	opts    Options
	mu      sync.RWMutex
	last    model.Report
	ready   bool
}

func New(cfg config.Config, opts Options, reg *prometheus.Registry) *Service {
	m := NewMetrics(reg)
	for _, rule := range cfg.Rules {
		m.Violations.WithLabelValues(rule.ID, rule.Severity).Add(0)
	}
	return &Service{cfg: cfg, engine: rules.Engine{Config: cfg}, store: NewStore(opts.MaxSpans, opts.TTL, m), metrics: m, opts: opts}
}
func (s *Service) Export(_ context.Context, req *collectortrace.ExportTraceServiceRequest) (*collectortrace.ExportTraceServiceResponse, error) {
	s.store.Add(ingest.FromProto(req.ResourceSpans, s.cfg.RedactAttributes))
	return &collectortrace.ExportTraceServiceResponse{}, nil
}
func (s *Service) Run(ctx context.Context, reg *prometheus.Registry) error {
	grpcLis, err := net.Listen("tcp", s.opts.GRPCAddress)
	if err != nil {
		return fmt.Errorf("listen gRPC: %w", err)
	}
	grpcServer := grpc.NewServer(grpc.MaxRecvMsgSize(int(s.opts.MaxRequestBytes)))
	collectortrace.RegisterTraceServiceServer(grpcServer, s)
	otlpMux := http.NewServeMux()
	otlpMux.HandleFunc("/v1/traces", s.httpExport)
	otlpServer := &http.Server{Addr: s.opts.HTTPAddress, Handler: otlpMux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, MaxHeaderBytes: 16 << 10}
	adminMux := http.NewServeMux()
	adminMux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "healthy"})
	})
	adminMux.HandleFunc("/ready", s.handleReady)
	adminMux.HandleFunc("/report", s.handleReport)
	adminMux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	adminServer := &http.Server{Addr: s.opts.AdminAddress, Handler: adminMux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, MaxHeaderBytes: 16 << 10}
	errCh := make(chan error, 3)
	go func() {
		if e := grpcServer.Serve(grpcLis); e != nil {
			errCh <- e
		}
	}()
	go func() {
		if e := otlpServer.ListenAndServe(); e != nil && e != http.ErrServerClosed {
			errCh <- e
		}
	}()
	go func() {
		if e := adminServer.ListenAndServe(); e != nil && e != http.ErrServerClosed {
			errCh <- e
		}
	}()
	s.mu.Lock()
	s.ready = true
	s.mu.Unlock()
	ticker := time.NewTicker(s.opts.AuditInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.audit()
		case err := <-errCh:
			return err
		case <-ctx.Done():
			s.mu.Lock()
			s.ready = false
			s.mu.Unlock()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			grpcServer.GracefulStop()
			_ = otlpServer.Shutdown(shutdownCtx)
			_ = adminServer.Shutdown(shutdownCtx)
			return nil
		}
	}
}
func (s *Service) audit() {
	spans := s.store.Snapshot()
	if len(spans) == 0 {
		return
	}
	r := s.engine.AuditWindow(spans)
	s.metrics.Observe(r)
	s.mu.Lock()
	s.last = r
	s.mu.Unlock()
}
func (s *Service) httpExport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, s.opts.MaxRequestBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "request body too large or unreadable", http.StatusRequestEntityTooLarge)
		return
	}
	body, err = decompress(body, r.Header.Get("Content-Encoding"), s.opts.MaxRequestBytes)
	if err != nil {
		http.Error(w, "unsupported or invalid content encoding", http.StatusBadRequest)
		return
	}
	var req *collectortrace.ExportTraceServiceRequest
	ct := strings.Split(r.Header.Get("Content-Type"), ";")[0]
	switch ct {
	case "application/json":
		req, err = ingest.DecodeRequestJSON(body)
	case "application/x-protobuf", "application/protobuf", "":
		req = &collectortrace.ExportTraceServiceRequest{}
		err = proto.Unmarshal(body, req)
	default:
		http.Error(w, "unsupported Content-Type", http.StatusUnsupportedMediaType)
		return
	}
	if err != nil {
		http.Error(w, "malformed OTLP payload", http.StatusBadRequest)
		return
	}
	s.store.Add(ingest.FromProto(req.ResourceSpans, s.cfg.RedactAttributes))
	resp := &collectortrace.ExportTraceServiceResponse{}
	if ct == "application/json" {
		b, _ := protojson.Marshal(resp)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(b)
	} else {
		b, _ := proto.Marshal(resp)
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(b)
	}
}

func decompress(body []byte, encoding string, maxBytes int64) ([]byte, error) {
	if encoding == "" || encoding == "identity" {
		return body, nil
	}
	if encoding != "gzip" {
		return nil, fmt.Errorf("unsupported content encoding %q", encoding)
	}
	zr, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	decoded, err := io.ReadAll(io.LimitReader(zr, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(decoded)) > maxBytes {
		return nil, fmt.Errorf("decompressed request exceeds %d bytes", maxBytes)
	}
	return decoded, nil
}
func (s *Service) handleReady(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	ready := s.ready
	s.mu.RUnlock()
	if !ready {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ready", "retained_spans": s.store.Len()})
}
func (s *Service) handleReport(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	r := s.last
	s.mu.RUnlock()
	writeJSON(w, http.StatusOK, r)
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
