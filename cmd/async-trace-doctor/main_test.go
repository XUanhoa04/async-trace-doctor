package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRootCommandStructure(t *testing.T) {
	cmd := root()
	if cmd == nil {
		t.Fatal("root() returned nil")
	}
	if cmd.Use != "async-trace-doctor" {
		t.Errorf("root command Use = %q, want %q", cmd.Use, "async-trace-doctor")
	}

	subCommands := cmd.Commands()
	foundAudit := false
	foundServe := false
	for _, sc := range subCommands {
		if sc.Name() == "audit" {
			foundAudit = true
		}
		if sc.Name() == "serve" {
			foundServe = true
		}
	}
	if !foundAudit || !foundServe {
		t.Errorf("expected audit and serve subcommands, found: audit=%v, serve=%v", foundAudit, foundServe)
	}
}

func TestAuditCommandFlagDefaults(t *testing.T) {
	cmd := auditCmd()
	if cmd == nil {
		t.Fatal("auditCmd() returned nil")
	}

	rulesFlag := cmd.Flag("rules")
	if rulesFlag == nil || rulesFlag.DefValue != "config/rules.yaml" {
		t.Errorf("expected default rules flag 'config/rules.yaml', got: %v", rulesFlag)
	}

	inputFlag := cmd.Flag("input")
	if inputFlag == nil {
		t.Errorf("expected input flag to exist")
	}
}

func TestServeCommandValidation(t *testing.T) {
	rulesPath := filepath.Join("..", "..", "config", "rules.yaml")
	if _, err := os.Stat(rulesPath); err != nil {
		t.Skipf("skipping test, rules.yaml not found at %s", rulesPath)
	}

	tests := []struct {
		name      string
		args      []string
		errSubstr string
	}{
		{
			name:      "negative limits",
			args:      []string{"--rules", rulesPath, "--max-spans", "-1"},
			errSubstr: "limits, TTL, and interval must be positive",
		},
		{
			name:      "zero rate burst with positive rate limit",
			args:      []string{"--rules", rulesPath, "--rate-limit", "100", "--rate-burst", "0"},
			errSubstr: "rate limit must be non-negative and requires a positive burst",
		},
		{
			name:      "ttl smaller than correlation window",
			args:      []string{"--rules", rulesPath, "--state-ttl", "1s"}, // ttl is 1s, correlationWindow in default rules is 5m
			errSubstr: "state TTL 1s must be at least the correlation window",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := serveCmd()
			cmd.SetArgs(tt.args)
			err := cmd.Execute()
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.errSubstr)
			}
			if !strings.Contains(err.Error(), tt.errSubstr) {
				t.Errorf("error %q does not contain expected substring %q", err.Error(), tt.errSubstr)
			}
		})
	}
}
