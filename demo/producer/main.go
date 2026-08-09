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
	tp, err := provider(ctx, "demo-producer")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = tp.Shutdown(ctx) }()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	broker := env("KAFKA_BROKER", "redpanda:9092")
	topic := env("KAFKA_TOPIC", "orders")
	fault := env("FAULT_MODE", "normal")
	if fault == "orphan_producer" {
		topic += ".orphan"
	}
	count := envInt("MESSAGE_COUNT", 4)
	writer := &kafka.Writer{Addr: kafka.TCP(broker), Topic: topic, Balancer: &kafka.LeastBytes{}, RequiredAcks: kafka.RequireAll}
	defer writer.Close()
	for i := 0; i < count; i++ {
		id := uuid.NewString()
		spanCtx, span := otel.Tracer("async-trace-doctor-demo").Start(ctx, "send "+topic, trace.WithSpanKind(trace.SpanKindProducer), trace.WithAttributes(attribute.String("messaging.system", "kafka"), attribute.String("messaging.destination.name", topic), attribute.String("messaging.operation.type", "send"), attribute.String("messaging.operation.name", "send"), attribute.String("messaging.message.id", id)))
		msg := kafka.Message{Key: []byte(id), Value: []byte("demo-event"), Headers: []kafka.Header{{Key: "message-id", Value: []byte(id)}}}
		if fault != "no_inject" {
			carrier := headerCarrier{headers: &msg.Headers}
			otel.GetTextMapPropagator().Inject(spanCtx, carrier)
		}
		err = writeWithRetry(spanCtx, writer, msg)
		if err != nil {
			span.RecordError(err)
		}
		span.End()
		if err != nil {
			log.Fatal(err)
		}
	}
	if err := tp.ForceFlush(ctx); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("produced %d messages to %s with fault=%s\n", count, topic, fault)
}

func writeWithRetry(ctx context.Context, writer *kafka.Writer, msg kafka.Message) error {
	var last error
	for attempt := 0; attempt < 20; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		last = writer.WriteMessages(attemptCtx, msg)
		cancel()
		if last == nil {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("write message after bounded retries: %w", last)
}

type headerCarrier struct{ headers *[]kafka.Header }

func (c headerCarrier) Get(key string) string {
	for _, h := range *c.headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
func (c headerCarrier) Set(key, value string) {
	*c.headers = append(*c.headers, kafka.Header{Key: key, Value: []byte(value)})
}
func (c headerCarrier) Keys() []string {
	out := make([]string, 0, len(*c.headers))
	for _, h := range *c.headers {
		out = append(out, h.Key)
	}
	return out
}
func provider(ctx context.Context, service string) (*tracesdk.TracerProvider, error) {
	exp, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(env("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector:4317")), otlptracegrpc.WithTLSCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	res, err := resource.New(ctx, resource.WithAttributes(attribute.String("service.name", service), attribute.String("service.namespace", "async-trace-doctor-demo"), attribute.String("deployment.environment.name", env("DEPLOYMENT_ENVIRONMENT", "demo"))))
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
