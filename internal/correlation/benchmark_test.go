package correlation

import (
	"fmt"
	"testing"
	"time"

	"github.com/XUanhoa04/async-trace-doctor/internal/model"
)

func BenchmarkCorrelateIndexedMessageIdentity(b *testing.B) {
	spans := benchmarkSpans(100, 50, true)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Correlate(spans, time.Minute)
	}
}

func BenchmarkCorrelateHotRouteWithoutIdentity(b *testing.B) {
	spans := benchmarkSpans(1, 500, false)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Correlate(spans, time.Minute)
	}
}

func benchmarkSpans(routes, messagesPerRoute int, withIdentity bool) []model.Span {
	base := time.Unix(1_700_000_000, 0).UTC()
	spans := make([]model.Span, 0, routes*messagesPerRoute*2)
	for route := 0; route < routes; route++ {
		for message := 0; message < messagesPerRoute; message++ {
			id := fmt.Sprintf("message-%d-%d", route, message)
			destination := fmt.Sprintf("orders-%d", route)
			producerAttributes := map[string]any{"messaging.system": "kafka", "messaging.destination.name": destination, "messaging.operation.type": "send"}
			consumerAttributes := map[string]any{"messaging.system": "kafka", "messaging.destination.name": destination, "messaging.operation.type": "process"}
			if withIdentity {
				producerAttributes["messaging.message.id"] = id
				consumerAttributes["messaging.message.id"] = id
			}
			start := base.Add(time.Duration(message) * time.Millisecond)
			spans = append(spans,
				model.Span{TraceID: fmt.Sprintf("p-%d-%d", route, message), SpanID: id + "-p", Start: start, End: start.Add(time.Microsecond), Attributes: producerAttributes},
				model.Span{TraceID: fmt.Sprintf("c-%d-%d", route, message), SpanID: id + "-c", Start: start.Add(time.Millisecond), End: start.Add(2 * time.Millisecond), Attributes: consumerAttributes},
			)
		}
	}
	return spans
}
