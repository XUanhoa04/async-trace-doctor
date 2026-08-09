package main

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestFileHashIsContentOnly(t *testing.T) {
	contents := []byte("rules: []\n")
	path := filepath.Join(t.TempDir(), "rules.yaml")
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("%x", sha256.Sum256(contents))
	if got := fileHash(path); got != want {
		t.Fatalf("fileHash() = %q, want content SHA-256 %q", got, want)
	}
}

func TestValidateLiveProvenance(t *testing.T) {
	current := map[string]any{
		"status":             "passed",
		"git_commit":         "abc",
		"rules_sha256":       "rules",
		"git_worktree_dirty": false,
	}
	got := validateLiveProvenance(current, "abc", "rules").(map[string]any)
	if got["status"] != "passed" || got["provenance_status"] != "current" {
		t.Fatalf("current artifact was not preserved: %#v", got)
	}

	dirty := map[string]any{
		"status":             "passed",
		"git_commit":         "abc",
		"rules_sha256":       "rules",
		"git_worktree_dirty": true,
	}
	got = validateLiveProvenance(dirty, "abc", "rules").(map[string]any)
	if got["status"] != "stale" || got["provenance_status"] != "stale" {
		t.Fatalf("dirty artifact was accepted: %#v", got)
	}
}
