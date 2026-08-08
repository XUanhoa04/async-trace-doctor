package correlation

import (
	"testing"
	"time"

	"github.com/XUanhoa04/async-trace-doctor/internal/model"
)

func TestCorrelationPriority(t *testing.T) {
	now := time.Now()
	p := model.Span{TraceID: "a", SpanID: "p", Kind: "PRODUCER", Start: now, End: now.Add(time.Millisecond), Attributes: attrs("send", "m")}
	c := model.Span{TraceID: "b", SpanID: "c", Kind: "CONSUMER", Start: now.Add(time.Second), End: now.Add(2 * time.Second), Attributes: attrs("process", "m"), Links: []model.Link{{TraceID: "a", SpanID: "p"}}}
	r := Correlate([]model.Span{p, c}, time.Minute)
	if len(r.Correlations) != 1 || r.Correlations[0].Method != "span_link" || r.Correlations[0].Confidence != model.ConfidenceHigh {
		t.Fatalf("unexpected correlation: %#v", r)
	}
}
func TestParentThenAttributeThenHeuristic(t *testing.T) {
	now := time.Now()
	p := model.Span{TraceID: "a", SpanID: "p", Kind: "PRODUCER", Start: now, End: now, Attributes: attrs("send", "m")}
	tests := []struct {
		name   string
		c      model.Span
		method string
	}{{"parent", model.Span{TraceID: "a", SpanID: "c", ParentSpanID: "p", Kind: "CONSUMER", Start: now.Add(time.Second), Attributes: attrs("process", "m")}, "parent_context"}, {"attributes", model.Span{TraceID: "b", SpanID: "c", Kind: "CONSUMER", Start: now.Add(time.Second), Attributes: withID(attrs("process", "m"), "id-1")}, "messaging_attributes"}, {"heuristic", model.Span{TraceID: "b", SpanID: "c", Kind: "CONSUMER", Start: now.Add(time.Second), Attributes: attrs("process", "m")}, "time_window_heuristic"}}
	p.Attributes = withID(p.Attributes, "id-1")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := Correlate([]model.Span{p, tt.c}, time.Minute)
			if got := r.Correlations[0].Method; got != tt.method {
				t.Fatalf("got %s", got)
			}
		})
	}
}
func attrs(op, id string) map[string]any {
	return map[string]any{"messaging.system": "kafka", "messaging.destination.name": "orders", "messaging.operation.type": op}
}
func withID(a map[string]any, id string) map[string]any {
	b := map[string]any{}
	for k, v := range a {
		b[k] = v
	}
	b["messaging.message.id"] = id
	return b
}
