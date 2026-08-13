package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/XUanhoa04/async-trace-doctor/internal/config"
	"github.com/prometheus/client_golang/prometheus"
)

func TestReloadConfigIsAtomic(t *testing.T) {
	source := filepath.Join("..", "..", "config", "rules.yaml")
	raw, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "rules.yaml")
	if err = os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	svc := New(cfg, Options{MaxSpans: 10, MaxRetainedBytes: 1 << 20, TTL: time.Minute, ConfigPath: path}, prometheus.NewRegistry())
	originalVersion := svc.cfg.SemanticConventionVersion
	if err = os.WriteFile(path, []byte("not: [valid"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err = svc.ReloadConfig(); err == nil {
		t.Fatal("invalid reload unexpectedly succeeded")
	}
	if svc.cfg.SemanticConventionVersion != originalVersion {
		t.Fatal("invalid reload changed active config")
	}
	updated := strings.Replace(string(raw), `semanticConventionVersion: "`+originalVersion+`"`, `semanticConventionVersion: "test-reload"`, 1)
	if updated == string(raw) {
		t.Skip("config fixture version layout changed")
	}
	if err = os.WriteFile(path, []byte(updated), 0o600); err != nil {
		t.Fatal(err)
	}
	if err = svc.ReloadConfig(); err != nil {
		t.Fatal(err)
	}
	if svc.cfg.SemanticConventionVersion != "test-reload" || svc.engine.Config.SemanticConventionVersion != "test-reload" {
		t.Fatalf("config and engine were not swapped together: %#v", svc.cfg)
	}
}
