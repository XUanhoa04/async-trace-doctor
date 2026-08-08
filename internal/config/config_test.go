package config

import (
	"os"
	"path/filepath"
	"testing"
)

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
