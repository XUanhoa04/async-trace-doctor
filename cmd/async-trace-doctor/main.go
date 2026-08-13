package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/XUanhoa04/async-trace-doctor/internal/config"
	"github.com/XUanhoa04/async-trace-doctor/internal/ingest"
	"github.com/XUanhoa04/async-trace-doctor/internal/logging"
	"github.com/XUanhoa04/async-trace-doctor/internal/report"
	"github.com/XUanhoa04/async-trace-doctor/internal/rules"
	"github.com/XUanhoa04/async-trace-doctor/internal/server"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
)

var errPolicyViolation = errors.New("policy violation")

func main() {
	if err := root().Execute(); err != nil {
		if errors.Is(err, errPolicyViolation) {
			os.Exit(2)
		}
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
func root() *cobra.Command {
	cmd := &cobra.Command{Use: "async-trace-doctor", Short: "Audit OpenTelemetry messaging trace quality", SilenceErrors: true, SilenceUsage: true}
	cmd.AddCommand(auditCmd(), serveCmd())
	return cmd
}
func auditCmd() *cobra.Command {
	var input, rulesPath, jsonPath string
	var maxBytes int64
	var maxSpans int
	c := &cobra.Command{Use: "audit", Short: "Audit OTLP JSON or JSONL from a file or directory", RunE: func(_ *cobra.Command, _ []string) error {
		cfg, err := config.Load(rulesPath)
		if err != nil {
			return err
		}
		spans, err := ingest.ReadPath(input, ingest.Limits{MaxBytes: maxBytes, MaxSpans: maxSpans}, cfg.RedactAttributes)
		if err != nil {
			return err
		}
		r := rules.Engine{Config: cfg}.Audit(spans)
		if err := report.WriteTable(os.Stdout, r); err != nil {
			return err
		}
		if jsonPath != "" {
			f, err := os.Create(jsonPath)
			if err != nil {
				return fmt.Errorf("create JSON report: %w", err)
			}
			if err = report.WriteJSON(f, r, true); err != nil {
				_ = f.Close()
				return err
			}
			if err = f.Close(); err != nil {
				return err
			}
		}
		if (rules.Engine{Config: cfg}).ViolatesPolicy(r) {
			return errPolicyViolation
		}
		return nil
	}}
	c.Flags().StringVarP(&input, "input", "i", "", "OTLP JSON/JSONL file or directory (required)")
	c.Flags().StringVar(&rulesPath, "rules", "config/rules.yaml", "versioned rule config")
	c.Flags().StringVarP(&jsonPath, "json", "j", "", "write JSON report to this path")
	c.Flags().Int64Var(&maxBytes, "max-bytes", 64<<20, "maximum aggregate input bytes")
	c.Flags().IntVar(&maxSpans, "max-spans", 100000, "maximum input spans")
	_ = c.MarkFlagRequired("input")
	return c
}
func serveCmd() *cobra.Command {
	var rulesPath, httpAddr, grpcAddr, adminAddr, authToken, logLevel, logFormat, startupProbePath string
	var maxBytes, maxRetainedBytes int64
	var maxSpans int
	var ttl, interval time.Duration
	var redactReport bool
	var rateLimit float64
	var rateBurst int
	c := &cobra.Command{Use: "serve", Short: "Receive OTLP and audit bounded windows", RunE: func(_ *cobra.Command, _ []string) error {
		cfg, err := config.Load(rulesPath)
		if err != nil {
			return err
		}
		if ttl <= 0 || interval <= 0 || maxBytes <= 0 || maxRetainedBytes <= 0 || maxSpans <= 0 {
			return fmt.Errorf("limits, TTL, and interval must be positive")
		}
		if rateLimit < 0 || rateBurst < 0 || (rateLimit > 0 && rateBurst == 0) {
			return fmt.Errorf("rate limit must be non-negative and requires a positive burst")
		}
		if ttl < cfg.Settings.CorrelationWindow.Duration {
			return fmt.Errorf("state TTL %s must be at least the correlation window %s", ttl, cfg.Settings.CorrelationWindow.Duration)
		}
		reg := prometheus.NewRegistry()
		logger := logging.NewLogger(logLevel, logFormat)
		svc := server.New(cfg, server.Options{HTTPAddress: httpAddr, GRPCAddress: grpcAddr, AdminAddress: adminAddr, MaxRequestBytes: maxBytes, MaxRetainedBytes: maxRetainedBytes, MaxSpans: maxSpans, TTL: ttl, AuditInterval: interval, AuthToken: authToken, RedactReport: redactReport, RateLimit: rateLimit, RateBurst: rateBurst, ConfigPath: rulesPath, StartupProbePath: startupProbePath, Logger: logger}, reg)
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		hup := make(chan os.Signal, 1)
		signal.Notify(hup, syscall.Signal(1))
		defer signal.Stop(hup)
		go func() {
			for range hup {
				if reloadErr := svc.ReloadConfig(); reloadErr != nil {
					logger.Error("SIGHUP configuration reload failed", "error", reloadErr)
				}
			}
		}()
		logger.Info("starting AsyncTraceDoctor", "otlp_http", httpAddr, "otlp_grpc", grpcAddr, "admin", adminAddr, "auth_enabled", authToken != "", "redact_report", redactReport)
		return svc.Run(ctx, reg)
	}}
	c.Flags().StringVar(&rulesPath, "rules", "config/rules.yaml", "versioned rule config")
	c.Flags().StringVar(&httpAddr, "http-address", ":4318", "OTLP/HTTP listen address")
	c.Flags().StringVar(&grpcAddr, "grpc-address", ":4317", "OTLP/gRPC listen address")
	c.Flags().StringVar(&adminAddr, "admin-address", ":8080", "health, readiness, report, and metrics address")
	c.Flags().Int64Var(&maxBytes, "max-request-bytes", 8<<20, "maximum OTLP HTTP/gRPC request bytes")
	c.Flags().Int64Var(&maxRetainedBytes, "max-retained-bytes", 256<<20, "maximum estimated bytes retained in live state")
	c.Flags().IntVar(&maxSpans, "max-spans", 50000, "maximum spans retained")
	c.Flags().DurationVar(&ttl, "state-ttl", 10*time.Minute, "retained span TTL")
	c.Flags().DurationVar(&interval, "audit-interval", 10*time.Second, "bounded audit interval")
	c.Flags().StringVar(&authToken, "auth-token", os.Getenv("ATD_AUTH_TOKEN"), "bearer token for OTLP and admin endpoints (or ATD_AUTH_TOKEN)")
	c.Flags().BoolVar(&redactReport, "redact-report", false, "remove topology and hash service names in admin reports")
	c.Flags().Float64Var(&rateLimit, "rate-limit", 1000, "maximum OTLP requests per second (0 disables)")
	c.Flags().IntVar(&rateBurst, "rate-burst", 100, "OTLP rate-limit burst size")
	c.Flags().StringVar(&logLevel, "log-level", "info", "log level: debug, info, warn, error")
	c.Flags().StringVar(&logFormat, "log-format", "text", "log format: text or json")
	c.Flags().StringVar(&startupProbePath, "startup-probe-path", "/startup", "startup probe path on the admin server")
	return c
}
