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
	var rulesPath, httpAddr, grpcAddr, adminAddr string
	var maxBytes int64
	var maxSpans int
	var ttl, interval time.Duration
	c := &cobra.Command{Use: "serve", Short: "Receive OTLP and audit bounded windows", RunE: func(_ *cobra.Command, _ []string) error {
		cfg, err := config.Load(rulesPath)
		if err != nil {
			return err
		}
		if ttl <= 0 || interval <= 0 || maxBytes <= 0 || maxSpans <= 0 {
			return fmt.Errorf("limits, TTL, and interval must be positive")
		}
		if ttl < cfg.Settings.CorrelationWindow.Duration {
			return fmt.Errorf("state TTL %s must be at least the correlation window %s", ttl, cfg.Settings.CorrelationWindow.Duration)
		}
		reg := prometheus.NewRegistry()
		svc := server.New(cfg, server.Options{HTTPAddress: httpAddr, GRPCAddress: grpcAddr, AdminAddress: adminAddr, MaxRequestBytes: maxBytes, MaxSpans: maxSpans, TTL: ttl, AuditInterval: interval}, reg)
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		fmt.Fprintf(os.Stdout, "AsyncTraceDoctor ready: OTLP/HTTP %s, OTLP/gRPC %s, admin %s\n", httpAddr, grpcAddr, adminAddr)
		return svc.Run(ctx, reg)
	}}
	c.Flags().StringVar(&rulesPath, "rules", "config/rules.yaml", "versioned rule config")
	c.Flags().StringVar(&httpAddr, "http-address", ":4318", "OTLP/HTTP listen address")
	c.Flags().StringVar(&grpcAddr, "grpc-address", ":4317", "OTLP/gRPC listen address")
	c.Flags().StringVar(&adminAddr, "admin-address", ":8080", "health, readiness, report, and metrics address")
	c.Flags().Int64Var(&maxBytes, "max-request-bytes", 8<<20, "maximum OTLP HTTP/gRPC request bytes")
	c.Flags().IntVar(&maxSpans, "max-spans", 50000, "maximum spans retained")
	c.Flags().DurationVar(&ttl, "state-ttl", 10*time.Minute, "retained span TTL")
	c.Flags().DurationVar(&interval, "audit-interval", 10*time.Second, "bounded audit interval")
	return c
}
