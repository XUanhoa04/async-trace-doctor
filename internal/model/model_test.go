package model

import (
	"fmt"
	"testing"
)

func TestSortFindingsUsesStablePrecomputedKeys(t *testing.T) {
	findings := []Finding{{RuleID: "B", SpanIDs: []string{"2"}}, {RuleID: "A", SpanIDs: []string{"9"}}, {RuleID: "A", SpanIDs: []string{"1"}}}
	SortFindings(findings)
	if findings[0].SpanIDs[0] != "1" || findings[1].SpanIDs[0] != "9" || findings[2].RuleID != "B" {
		t.Fatalf("unexpected order: %#v", findings)
	}
}

func BenchmarkSortFindings(b *testing.B) {
	template := make([]Finding, 1000)
	for i := range template {
		template[i] = Finding{RuleID: fmt.Sprintf("ATD-%03d", 999-i), SpanIDs: []string{fmt.Sprintf("%016d", i)}, Message: "finding"}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		findings := append([]Finding(nil), template...)
		SortFindings(findings)
	}
}
