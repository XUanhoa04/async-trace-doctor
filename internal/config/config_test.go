package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func validConfig() Config {
	return Config{
		APIVersion:                "asynctracedoctor.io/v1alpha1",
		SemanticConventionVersion: "1.43.0",
		Settings: Settings{
			CorrelationWindow:  Duration{10 * time.Minute},
			QueueLatency:       Duration{30 * time.Second},
			ClockSkewTolerance: Duration{5 * time.Second},
			DuplicateThreshold: 1,
			FailOnSeverity:     "error",
			InputCompleteness:  "complete",
		},
		Rules: []Rule{
			{
				ID:           "ATD-001",
				Check:        "missing_service_name",
				Enabled:      true,
				Severity:     "error",
				Message:      "service name is required",
				SuggestedFix: "set service.name attribute",
			},
		},
		Topology: Topology{
			ExpectedEdges: []ExpectedEdge{
				{
					Producer:          "order-service",
					System:            "kafka",
					Destination:       "orders",
					Consumer:          "billing-service",
					RequirePerMessage: true,
				},
			},
			DeniedEdges: []ExpectedEdge{
				{
					Producer:    "order-service",
					System:      "kafka",
					Destination: "orders",
					Consumer:    "unknown-service",
				},
			},
			IgnoredEdges: []ExpectedEdge{
				{
					Producer:    "debug-service",
					System:      "kafka",
					Destination: "debug",
					Consumer:    "debug-consumer",
				},
			},
		},
	}
}

func TestLoadRules(t *testing.T) {
	c, err := Load(filepath.Join("..", "..", "config", "rules.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Rules) < 11 || c.SemanticConventionVersion != "1.43.0" {
		t.Fatalf("unexpected config: %#v", c)
	}
}

func TestRejectUnknownYAMLAndCheck(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rules.yaml")
	raw := "apiVersion: asynctracedoctor.io/v1alpha1\nsemanticConventionVersion: 1.43.0\nunknown: true\n"
	if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected strict YAML failure")
	}
}

func TestLoadNonExistentFile(t *testing.T) {
	if _, err := Load("non-existent-rules-file.yaml"); err == nil {
		t.Fatal("expected error for non-existent file")
	}
}

func TestDurationUnmarshalYAML(t *testing.T) {
	var d Duration
	node := &yaml.Node{Value: "15s"}
	if err := d.UnmarshalYAML(node); err != nil {
		t.Fatalf("unexpected error unmarshaling duration: %v", err)
	}
	if d.Duration != 15*time.Second {
		t.Fatalf("expected 15s, got %v", d.Duration)
	}

	invalidNode := &yaml.Node{Value: "invalid-duration"}
	if err := d.UnmarshalYAML(invalidNode); err == nil {
		t.Fatal("expected error unmarshaling invalid duration")
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*Config)
		wantError string
	}{
		{
			name:      "valid base config",
			mutate:    func(c *Config) {},
			wantError: "",
		},
		{
			name: "invalid apiVersion",
			mutate: func(c *Config) {
				c.APIVersion = "v1"
			},
			wantError: "unsupported apiVersion",
		},
		{
			name: "missing semanticConventionVersion",
			mutate: func(c *Config) {
				c.SemanticConventionVersion = ""
			},
			wantError: "semanticConventionVersion is required",
		},
		{
			name: "non-positive correlationWindow",
			mutate: func(c *Config) {
				c.Settings.CorrelationWindow = Duration{0}
			},
			wantError: "correlationWindow must be positive",
		},
		{
			name: "non-positive queueLatency",
			mutate: func(c *Config) {
				c.Settings.QueueLatency = Duration{-1 * time.Second}
			},
			wantError: "queueLatencyThreshold must be positive",
		},
		{
			name: "negative clockSkewTolerance",
			mutate: func(c *Config) {
				c.Settings.ClockSkewTolerance = Duration{-1 * time.Second}
			},
			wantError: "clockSkewTolerance must not be negative",
		},
		{
			name: "duplicateThreshold less than 1",
			mutate: func(c *Config) {
				c.Settings.DuplicateThreshold = 0
			},
			wantError: "duplicateThreshold must be at least 1",
		},
		{
			name: "invalid failOnSeverity",
			mutate: func(c *Config) {
				c.Settings.FailOnSeverity = "fatal"
			},
			wantError: "invalid failOnSeverity",
		},
		{
			name: "invalid inputCompleteness",
			mutate: func(c *Config) {
				c.Settings.InputCompleteness = "partial"
			},
			wantError: "inputCompleteness must be complete or unknown",
		},
		{
			name: "empty rule ID",
			mutate: func(c *Config) {
				c.Rules[0].ID = ""
			},
			wantError: "empty or duplicate id",
		},
		{
			name: "duplicate rule ID",
			mutate: func(c *Config) {
				c.Rules = append(c.Rules, c.Rules[0])
			},
			wantError: "empty or duplicate id",
		},
		{
			name: "unknown rule check",
			mutate: func(c *Config) {
				c.Rules[0].Check = "unknown_check_type"
			},
			wantError: "unknown check",
		},
		{
			name: "invalid rule severity",
			mutate: func(c *Config) {
				c.Rules[0].Severity = "invalid_severity"
			},
			wantError: "invalid severity",
		},
		{
			name: "missing rule message",
			mutate: func(c *Config) {
				c.Rules[0].Message = ""
			},
			wantError: "requires message and suggestedFix",
		},
		{
			name: "missing rule suggestedFix",
			mutate: func(c *Config) {
				c.Rules[0].SuggestedFix = ""
			},
			wantError: "requires message and suggestedFix",
		},
		{
			name: "missing expected edge producer",
			mutate: func(c *Config) {
				c.Topology.ExpectedEdges[0].Producer = ""
			},
			wantError: "requires producer, system, destination, and consumer",
		},
		{
			name: "denied edge with requirePerMessage",
			mutate: func(c *Config) {
				c.Topology.DeniedEdges[0].RequirePerMessage = true
			},
			wantError: "cannot require per-message delivery",
		},
		{
			name: "ignored edge with requirePerMessage",
			mutate: func(c *Config) {
				c.Topology.IgnoredEdges[0].RequirePerMessage = true
			},
			wantError: "cannot require per-message delivery",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			tt.mutate(&cfg)
			err := cfg.Validate()
			if tt.wantError == "" {
				if err != nil {
					t.Fatalf("expected no error, got: %v", err)
				}
			} else {
				if err == nil || !strings.Contains(err.Error(), tt.wantError) {
					t.Fatalf("expected error containing %q, got: %v", tt.wantError, err)
				}
			}
		})
	}
}

func TestViolatesPolicy(t *testing.T) {
	cfg := validConfig()
	cfg.Settings.FailOnSeverity = "warning"

	testCases := []struct {
		severity string
		violates bool
	}{
		{"info", false},
		{"warning", true},
		{"error", true},
		{"critical", true},
	}

	for _, tc := range testCases {
		t.Run(tc.severity, func(t *testing.T) {
			if got := cfg.ViolatesPolicy(tc.severity); got != tc.violates {
				t.Errorf("ViolatesPolicy(%q) = %v, want %v (failOnSeverity=%s)", tc.severity, got, tc.violates, cfg.Settings.FailOnSeverity)
			}
		})
	}
}
