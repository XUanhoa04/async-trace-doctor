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
	if _, err := fmt.Fprintf(w, "AsyncTraceDoctor: %d spans, %d messaging spans, %d findings, context completeness %.1f%%\n", r.Summary.AuditedSpans, r.Summary.MessagingSpans, r.Summary.Violations, r.Summary.ContextCompletenessRatio*100); err != nil {
		return err
	}
	if len(r.Findings) == 0 {
		_, err := fmt.Fprintln(w, "No policy violations detected.")
		return err
	}
	_, _ = fmt.Fprintln(w, "SEVERITY  RULE          PRODUCER -> CONSUMER       SYSTEM/DESTINATION       METHOD                 CONF  MESSAGE")
	for _, f := range r.Findings {
		edge := trim(f.ProducerService+" -> "+f.ConsumerService, 26)
		dest := trim(f.MessagingSystem+"/"+f.Destination, 24)
		if _, err := fmt.Fprintf(w, "%-9s %-13s %-26s %-24s %-22s %-5s %s\n", strings.ToUpper(f.Severity), f.RuleID, edge, dest, f.CorrelationMethod, f.Confidence, f.Message); err != nil {
			return err
		}
	}
	return nil
}
func trim(s string, n int) string {
	if len(s) <= n {
		return s
	}
	if n < 2 {
		return s[:n]
	}
	return s[:n-1] + "…"
}
