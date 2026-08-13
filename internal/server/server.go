package server

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
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
	MaxRetainedBytes                       int64
	MaxSpans                               int
	TTL, AuditInterval                     time.Duration
	AuthToken                              string
	RedactReport                           bool
	RateLimit                              float64
	RateBurst                              int
	ConfigPath                             string
	StartupProbePath                       string
	Logger                                 *slog.Logger
}

type Service struct {
	collectortrace.UnimplementedTraceServiceServer
	cfg         config.Config
	engine      rules.Engine
	store       *Store
	metrics     *Metrics
	opts        Options
	logger      *slog.Logger
	mu          sync.RWMutex
	last        model.Report
	ready       bool
	startup     bool
	lastAuditAt time.Time
}

func New(cfg config.Config, opts Options, reg *prometheus.Registry) *Service {
	m := NewMetrics(reg)
	for _, rule := range cfg.Rules {
		m.Violations.WithLabelValues(rule.ID, rule.Severity).Add(0)
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	if opts.StartupProbePath == "" {
		opts.StartupProbePath = "/startup"
	}
	store := NewStore(opts.MaxSpans, opts.TTL, m, opts.MaxRetainedBytes)
	store.logger = logger
	return &Service{cfg: cfg, engine: rules.Engine{Config: cfg}, store: store, metrics: m, opts: opts, logger: logger}
}

func (s *Service) Export(_ context.Context, req *collectortrace.ExportTraceServiceRequest) (*collectortrace.ExportTraceServiceResponse, error) {
	s.mu.RLock()
	redact := append([]string(nil), s.cfg.RedactAttributes...)
	s.mu.RUnlock()
	result := s.store.Add(ingest.FromProto(req.ResourceSpans, redact))
	return exportResponse(result), nil
}

func (s *Service) Run(ctx context.Context, reg *prometheus.Registry) error {
	if err := s.validateOptions(); err != nil {
		return err
	}
	grpcLis, err := net.Listen("tcp", s.opts.GRPCAddress)
	if err != nil {
		return fmt.Errorf("listen gRPC: %w", err)
	}
	otlpLis, err := net.Listen("tcp", s.opts.HTTPAddress)
	if err != nil {
		_ = grpcLis.Close()
		return fmt.Errorf("listen OTLP HTTP: %w", err)
	}
	adminLis, err := net.Listen("tcp", s.opts.AdminAddress)
	if err != nil {
		_ = grpcLis.Close()
		_ = otlpLis.Close()
		return fmt.Errorf("listen admin HTTP: %w", err)
	}
	httpBucket := newTokenBucket(s.opts.RateLimit, s.opts.RateBurst)
	grpcBucket := newTokenBucket(s.opts.RateLimit, s.opts.RateBurst)
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(int(s.opts.MaxRequestBytes)),
		grpc.ChainUnaryInterceptor(BearerAuthInterceptor(s.opts.AuthToken), rateLimitInterceptor(grpcBucket, s.metrics)),
	)
	collectortrace.RegisterTraceServiceServer(grpcServer, s)

	otlpMux := http.NewServeMux()
	otlpMux.Handle("/v1/traces", rateLimitMiddleware(httpBucket, s.metrics)(http.HandlerFunc(s.httpExport)))
	otlpHandler := BearerAuthMiddleware(s.opts.AuthToken)(otlpMux)
	otlpServer := &http.Server{Addr: s.opts.HTTPAddress, Handler: otlpHandler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, MaxHeaderBytes: 16 << 10}

	adminMux := http.NewServeMux()
	adminMux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "healthy"})
	})
	adminMux.HandleFunc(s.opts.StartupProbePath, s.handleStartup)
	adminMux.HandleFunc("/ready", s.handleReady)
	adminMux.HandleFunc("/report", s.handleReport)
	adminMux.HandleFunc("/admin/reload", s.handleReload)
	adminMux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	adminHandler := BearerAuthMiddleware(s.opts.AuthToken)(adminMux)
	adminServer := &http.Server{Addr: s.opts.AdminAddress, Handler: adminHandler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, MaxHeaderBytes: 16 << 10}

	errCh := make(chan error, 3)
	go func() {
		if serveErr := grpcServer.Serve(grpcLis); serveErr != nil {
			errCh <- fmt.Errorf("gRPC server: %w", serveErr)
		}
	}()
	go func() {
		if serveErr := otlpServer.Serve(otlpLis); serveErr != nil && serveErr != http.ErrServerClosed {
			errCh <- fmt.Errorf("OTLP HTTP server: %w", serveErr)
		}
	}()
	go func() {
		if serveErr := adminServer.Serve(adminLis); serveErr != nil && serveErr != http.ErrServerClosed {
			errCh <- fmt.Errorf("admin HTTP server: %w", serveErr)
		}
	}()
	s.mu.Lock()
	s.startup = true
	s.ready = true
	s.mu.Unlock()
	s.logger.Info("receiver started", "otlp_http", s.opts.HTTPAddress, "otlp_grpc", s.opts.GRPCAddress, "admin", s.opts.AdminAddress)

	ticker := time.NewTicker(s.opts.AuditInterval)
	defer ticker.Stop()
	var runErr error
	for runErr == nil {
		select {
		case <-ticker.C:
			s.audit()
		case runErr = <-errCh:
		case <-ctx.Done():
			s.audit()
			s.logger.Info("final audit completed, shutting down")
			runErr = context.Canceled
		}
	}
	s.mu.Lock()
	s.ready = false
	s.mu.Unlock()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	grpcDone := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(grpcDone)
	}()
	select {
	case <-grpcDone:
	case <-shutdownCtx.Done():
		grpcServer.Stop()
	}
	_ = otlpServer.Shutdown(shutdownCtx)
	_ = adminServer.Shutdown(shutdownCtx)
	_ = grpcLis.Close()
	_ = otlpLis.Close()
	_ = adminLis.Close()
	if runErr == context.Canceled {
		return nil
	}
	return runErr
}

func (s *Service) validateOptions() error {
	if s.opts.MaxRequestBytes <= 0 || s.opts.MaxSpans <= 0 || s.opts.TTL <= 0 || s.opts.AuditInterval <= 0 {
		return fmt.Errorf("request bytes, span count, TTL, and audit interval must be positive")
	}
	if !strings.HasPrefix(s.opts.StartupProbePath, "/") {
		return fmt.Errorf("startup probe path must begin with /")
	}
	for _, reserved := range []string{"/health", "/ready", "/report", "/metrics", "/admin/reload", "/v1/traces"} {
		if s.opts.StartupProbePath == reserved {
			return fmt.Errorf("startup probe path conflicts with reserved path %s", reserved)
		}
	}
	return nil
}

func (s *Service) audit() {
	spans := s.store.Snapshot()
	s.mu.RLock()
	engine := s.engine
	s.mu.RUnlock()
	r := engine.AuditWindow(spans)
	s.enrichCoverage(&r)
	s.metrics.Observe(r)
	s.mu.Lock()
	s.last = r
	s.lastAuditAt = time.Now().UTC()
	s.mu.Unlock()
	s.logger.Debug("audit completed", "spans", len(spans), "findings", len(r.Findings), "coverage", r.Coverage.Status)
}

func (s *Service) httpExport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErrorJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, s.opts.MaxRequestBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		s.logRequestError(r, "request body too large or unreadable", err)
		writeErrorJSON(w, http.StatusRequestEntityTooLarge, "request body too large or unreadable")
		return
	}
	decompressCtx, cancelDecompress := context.WithTimeout(r.Context(), 5*time.Second)
	body, err = decompressContext(decompressCtx, body, r.Header.Get("Content-Encoding"), s.opts.MaxRequestBytes)
	cancelDecompress()
	if err != nil {
		s.logRequestError(r, "invalid content encoding", err)
		writeErrorJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	var req *collectortrace.ExportTraceServiceRequest
	ct := strings.ToLower(strings.TrimSpace(strings.Split(r.Header.Get("Content-Type"), ";")[0]))
	switch ct {
	case "application/json":
		req, err = ingest.DecodeRequestJSON(body)
	case "application/x-protobuf", "application/protobuf", "":
		req = &collectortrace.ExportTraceServiceRequest{}
		err = proto.Unmarshal(body, req)
	default:
		writeErrorJSON(w, http.StatusUnsupportedMediaType, "unsupported Content-Type: "+ct)
		return
	}
	if err != nil {
		s.logRequestError(r, "malformed OTLP payload", err)
		writeErrorJSON(w, http.StatusBadRequest, "malformed OTLP payload")
		return
	}
	s.mu.RLock()
	redact := append([]string(nil), s.cfg.RedactAttributes...)
	s.mu.RUnlock()
	result := s.store.Add(ingest.FromProto(req.ResourceSpans, redact))
	resp := exportResponse(result)
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
	return decompressContext(context.Background(), body, encoding, maxBytes)
}

func decompressContext(ctx context.Context, body []byte, encoding string, maxBytes int64) ([]byte, error) {
	if encoding == "" || strings.EqualFold(encoding, "identity") {
		return body, nil
	}
	if !strings.EqualFold(encoding, "gzip") {
		return nil, fmt.Errorf("unsupported content encoding %q", encoding)
	}
	zr, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	deadline := time.Now().Add(5 * time.Second)
	decoded := bytes.NewBuffer(make([]byte, 0, minInt64(maxBytes, 64<<10)))
	chunk := make([]byte, 64<<10)
	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("gzip decompression canceled: %w", ctx.Err())
		default:
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("gzip decompression timed out")
		}
		n, readErr := zr.Read(chunk)
		if n > 0 {
			if int64(decoded.Len()+n) > maxBytes {
				return nil, fmt.Errorf("decompressed request exceeds %d bytes", maxBytes)
			}
			if len(body) > 0 && decoded.Len()+n > len(body)*100 {
				return nil, fmt.Errorf("suspicious compression ratio")
			}
			_, _ = decoded.Write(chunk[:n])
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return nil, readErr
		}
	}
	return decoded.Bytes(), nil
}

func minInt64(a int64, b int) int {
	if a <= 0 || a > int64(b) {
		return b
	}
	return int(a)
}

func (s *Service) handleStartup(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	startup := s.startup
	s.mu.RUnlock()
	if !startup {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "starting"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "started"})
}

func (s *Service) handleReady(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	ready := s.ready
	lastAuditAt := s.lastAuditAt
	coverage := s.last.Coverage.Status
	s.mu.RUnlock()
	if !ready || lastAuditAt.IsZero() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "not_ready", "reason": "waiting_for_successful_audit"})
		return
	}
	stats := s.store.Stats()
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ready", "last_audit_at": lastAuditAt, "audit_lag_seconds": time.Since(lastAuditAt).Seconds(),
		"retained_spans": s.store.Len(), "retained_bytes": s.store.CurrentBytes(), "rejected_spans": stats.RejectedSpans,
		"duplicate_exports": stats.DuplicateExports, "coverage_status": coverage,
	})
}

func (s *Service) handleReport(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	r := s.last
	s.mu.RUnlock()
	if s.opts.RedactReport {
		r.Findings = append([]model.Finding(nil), r.Findings...)
		for i := range r.Findings {
			r.Findings[i].ProducerService = redactServiceName(r.Findings[i].ProducerService)
			r.Findings[i].ConsumerService = redactServiceName(r.Findings[i].ConsumerService)
		}
		encoded, err := json.Marshal(r)
		if err == nil {
			var redacted map[string]any
			if json.Unmarshal(encoded, &redacted) == nil {
				delete(redacted, "topology")
				writeJSON(w, http.StatusOK, redacted)
				return
			}
		}
	}
	writeJSON(w, http.StatusOK, r)
}

func redactServiceName(name string) string {
	if name == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(name))
	return "service-" + hex.EncodeToString(sum[:4])
}

func (s *Service) handleReload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErrorJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if err := s.ReloadConfig(); err != nil {
		writeErrorJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "reloaded"})
}

func (s *Service) ReloadConfig() error {
	if s.opts.ConfigPath == "" {
		s.metrics.ConfigReloads.WithLabelValues("failed").Inc()
		return fmt.Errorf("configuration reload is disabled: no config path")
	}
	cfg, err := config.Load(s.opts.ConfigPath)
	if err != nil {
		s.metrics.ConfigReloads.WithLabelValues("failed").Inc()
		s.logger.Error("configuration reload failed", "error", err)
		return fmt.Errorf("reload config: %w", err)
	}
	s.mu.Lock()
	s.cfg = cfg
	s.engine = rules.Engine{Config: cfg}
	s.mu.Unlock()
	for _, rule := range cfg.Rules {
		s.metrics.Violations.WithLabelValues(rule.ID, rule.Severity).Add(0)
	}
	s.metrics.ConfigReloads.WithLabelValues("success").Inc()
	s.logger.Info("configuration reloaded", "path", s.opts.ConfigPath, "rules", len(cfg.Rules))
	return nil
}

func (s *Service) logRequestError(r *http.Request, message string, err error) {
	s.logger.Warn(message, "error", err, "content_type", r.Header.Get("Content-Type"), "content_length", r.ContentLength, "source_ip", r.RemoteAddr)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErrorJSON(w http.ResponseWriter, statusCode int, message string) {
	writeJSON(w, statusCode, map[string]any{"code": statusCode, "message": message})
}

func exportResponse(result AddResult) *collectortrace.ExportTraceServiceResponse {
	response := &collectortrace.ExportTraceServiceResponse{}
	if result.Rejected > 0 {
		reason := "capacity, stale data, or conflicting trace/span identity"
		if result.RejectedMemory > 0 {
			reason = "retained-state memory limit"
		}
		response.PartialSuccess = &collectortrace.ExportTracePartialSuccess{RejectedSpans: int64(result.Rejected), ErrorMessage: "spans rejected by bounded state admission: " + reason}
	}
	return response
}

func (s *Service) enrichCoverage(report *model.Report) {
	stats := s.store.Stats()
	report.Coverage.RetainedSpans = s.store.Len()
	report.Coverage.RejectedSpans = stats.RejectedSpans
	report.Coverage.DuplicateExports = stats.DuplicateExports
	report.Coverage.ConflictingDuplicates = stats.ConflictingDuplicates
	report.Coverage.TTLEvictions = stats.TTLEvictions
	if stats.RejectedSpans > 0 || stats.ConflictingDuplicates > 0 || stats.TTLEvictions > 0 {
		report.Coverage.Status = model.CoverageDegraded
		report.Coverage.Limitations = append(report.Coverage.Limitations, "receiver rejection or state eviction may hide correlation evidence")
		for i := range report.Findings {
			if report.Findings[i].EvidenceState == model.EvidenceInsufficient {
				report.Findings[i].EvidenceState = model.EvidenceDegraded
			}
		}
	}
}
