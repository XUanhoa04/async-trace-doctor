package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	ctx := context.Background()
	tp, err := provider(ctx, "demo-consumer")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = tp.Shutdown(ctx) }()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	fault := env("FAULT_MODE", "normal")
	topic := env("KAFKA_TOPIC", "orders")
	count := envInt("MESSAGE_COUNT", 4)
	if fault == "orphan_consumer" {
		process(ctx, kafka.Message{Headers: []kafka.Header{{Key: "message-id", Value: []byte(uuid.NewString())}}}, topic, fault, 1)
		_ = tp.ForceFlush(ctx)
		fmt.Println("emitted orphan consumer span")
		return
	}
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{env("KAFKA_BROKER", "redpanda:9092")}, Topic: topic, GroupID: "async-trace-doctor-demo", MinBytes: 1, MaxBytes: 1e6, MaxWait: time.Second})
	defer reader.Close()
	readCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	messages := make([]kafka.Message, 0, count)
	for len(messages) < count {
		m, err := reader.ReadMessage(readCtx)
		if err != nil {
			log.Fatal(err)
		}
		messages = append(messages, m)
	}
	if fault == "batch_incomplete" {
		processBatch(ctx, messages, topic, true)
	} else {
		for _, m := range messages {
			times := 1
			if fault == "duplicate" {
				times = 3
			}
			for i := 0; i < times; i++ {
				process(ctx, m, topic, fault, 1)
			}
		}
	}
	if err := tp.ForceFlush(ctx); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("consumed %d messages with fault=%s\n", len(messages), fault)
}

func process(ctx context.Context, m kafka.Message, topic, fault string, batch int) {
	id := header(m.Headers, "message-id")
	opts := []trace.SpanStartOption{trace.WithSpanKind(trace.SpanKindConsumer), trace.WithAttributes(attribute.String("messaging.system", "kafka"), attribute.String("messaging.destination.name", topic), attribute.String("messaging.operation.type", "process"), attribute.String("messaging.operation.name", "process"), attribute.String("messaging.message.id", id))}
	remote := context.Background()
	if fault != "no_extract" && fault != "orphan_consumer" {
		remote = otel.GetTextMapPropagator().Extract(ctx, headerCarrier{headers: m.Headers})
	}
	if fault != "no_link" && fault != "no_extract" && fault != "orphan_consumer" && trace.SpanContextFromContext(remote).IsValid() {
		opts = append(opts, trace.WithLinks(trace.LinkFromContext(remote)))
	}
	_, span := otel.Tracer("async-trace-doctor-demo").Start(context.Background(), "process "+topic, opts...)
	span.End()
}
func processBatch(ctx context.Context, messages []kafka.Message, topic string, incomplete bool) {
	links := make([]trace.Link, 0, len(messages))
	for i, m := range messages {
		remote := otel.GetTextMapPropagator().Extract(ctx, headerCarrier{headers: m.Headers})
		if trace.SpanContextFromContext(remote).IsValid() && (!incomplete || i < len(messages)-1) {
			links = append(links, trace.LinkFromContext(remote))
		}
	}
	_, span := otel.Tracer("async-trace-doctor-demo").Start(context.Background(), "process batch "+topic, trace.WithSpanKind(trace.SpanKindConsumer), trace.WithLinks(links...), trace.WithAttributes(attribute.String("messaging.system", "kafka"), attribute.String("messaging.destination.name", topic), attribute.String("messaging.operation.type", "process"), attribute.String("messaging.operation.name", "process"), attribute.Int("messaging.batch.message_count", len(messages))))
	span.End()
}

type headerCarrier struct{ headers []kafka.Header }

func (c headerCarrier) Get(key string) string { return header(c.headers, key) }
func (c headerCarrier) Set(string, string)    {}
func (c headerCarrier) Keys() []string {
	out := make([]string, 0, len(c.headers))
	for _, h := range c.headers {
		out = append(out, h.Key)
	}
	return out
}
func header(headers []kafka.Header, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
func provider(ctx context.Context, service string) (*tracesdk.TracerProvider, error) {
	exp, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(env("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector:4317")), otlptracegrpc.WithTLSCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	res, err := resource.New(ctx, resource.WithAttributes(attribute.String("service.name", service)))
	if err != nil {
		return nil, err
	}
	return tracesdk.NewTracerProvider(tracesdk.WithBatcher(exp), tracesdk.WithResource(res)), nil
}
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func envInt(k string, d int) int {
	v, err := strconv.Atoi(os.Getenv(k))
	if err != nil || v < 1 {
		return d
	}
	return v
}
