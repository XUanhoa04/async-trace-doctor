package report

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/XUanhoa04/async-trace-doctor/internal/model"
)

func WriteJSON(w io.Writer, report model.Report, pretty bool) error {
	enc := json.NewEncoder(w)
	if pretty {
		enc.SetIndent("", "  ")
	}
	return enc.Encode(report)
}

func WriteTable(w io.Writer, r model.Report) error {
	coverageStatus := r.Coverage.Status
	if coverageStatus == "" {
		coverageStatus = "unknown"
	}
	if _, err := fmt.Fprintf(w, "AsyncTraceDoctor: %d spans, %d messaging spans, %d findings, context completeness %.1f%%, coverage %s\n", r.Summary.AuditedSpans, r.Summary.MessagingSpans, r.Summary.Violations, r.Summary.ContextCompletenessRatio*100, coverageStatus); err != nil {
		return err
	}
	if len(r.Findings) == 0 {
		_, err := fmt.Fprintln(w, "No policy violations detected.")
		return err
	}
	_, _ = fmt.Fprintln(w, "SEVERITY  RULE          PRODUCER -> CONSUMER                       SYSTEM/DESTINATION       METHOD                 CONF  EVIDENCE      MESSAGE")
	for _, f := range r.Findings {
		consumer := f.ConsumerService
		if f.ConsumerGroup != "" {
			consumer += "[" + f.ConsumerGroup + "]"
		}
		if f.Subscription != "" {
			consumer += "{" + f.Subscription + "}"
		}
		edge := trim(f.ProducerService+" -> "+consumer, 42)
		dest := trim(f.MessagingSystem+"/"+f.Destination, 24)
		method := trim(f.CorrelationMethod, 22)
		if _, err := fmt.Fprintf(w, "%-9s %-13s %-42s %-24s %-22s %-5s %-13s %s\n", strings.ToUpper(f.Severity), f.RuleID, edge, dest, method, f.Confidence, f.EvidenceState, f.Message); err != nil {
			return err
		}
	}
	return nil
}
func trim(s string, n int) string {
	runes := []rune(s)
	if len(runes) <= n {
		return s
	}
	if n <= 0 {
		return ""
	}
	if n < 2 {
		return string(runes[:n])
	}
	return string(runes[:n-1]) + "…"
}
