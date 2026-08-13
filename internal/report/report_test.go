package report

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestTrimPreservesUTF8(t *testing.T) {
	got := trim("dịch-vụ-thanh-toán", 10)
	if !utf8.ValidString(got) || utf8.RuneCountInString(got) != 10 || !strings.HasSuffix(got, "…") {
		t.Fatalf("invalid UTF-8 trim %q", got)
	}
}

func TestTrimNonPositiveLimit(t *testing.T) {
	if got := trim("hello", 0); got != "" {
		t.Fatalf("trim limit zero = %q", got)
	}
}
