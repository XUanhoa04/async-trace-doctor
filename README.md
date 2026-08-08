# AsyncTraceDoctor

AsyncTraceDoctor is a small, independent open-source telemetry-quality auditor for OpenTelemetry traces from Kafka, RabbitMQ, and other event-driven systems. It detects the missing context, invalid messaging semantics, incomplete batch links, duplicates, queue delay, orphan spans, and topology drift that make automated RCA unreliable.

It is deliberately **not** an APM backend and not SentinelLoop v2. It does not store message payloads, call an LLM, diagnose business incidents, or remediate anything. An APM shows service performance; AsyncTraceDoctor audits whether the asynchronous trace graph is trustworthy enough for APM and AIOps analysis.

Status: MVP with automated unit/golden/holdout tests and a reproducible Docker E2E. It is not claimed to be production-ready.

## Quick start (under five minutes)

Requirements: Go 1.25+ for local use; Docker with Compose for the live demo.

```bash
go build -o bin/async-trace-doctor ./cmd/async-trace-doctor
./bin/async-trace-doctor audit \
  --input testdata/core/broken-context.json \
  --rules config/rules.yaml \
  --json report.json
echo $? # 2 because an error-severity policy violation was found
```

Normal traffic exits `0`; input/runtime errors exit `1`; findings at or above `settings.failOnSeverity` exit `2`.

To receive OTLP:

```bash
./bin/async-trace-doctor serve --rules config/rules.yaml
curl http://localhost:8080/health
curl http://localhost:8080/metrics
```

The server listens on OTLP/gRPC `:4317`, OTLP/HTTP `:4318`, and admin HTTP `:8080`. Its state is bounded by `--max-spans` and `--state-ttl`.

For a live Kafka path:

```bash
docker compose up --build
# metrics: http://localhost:18080/metrics
# latest bounded report: http://localhost:18080/report
# Prometheus: http://localhost:19090
```

The default `ATD_FAULT_MODE=normal`. Supported generic demo modes are `no_inject`, `no_extract`, `no_link`, `orphan_producer`, `orphan_consumer`, `batch_incomplete`, and `duplicate`. These flags alter telemetry behavior only; expected audit answers are never sent to AsyncTraceDoctor.

## Input and output

`audit` accepts a Collector file-exporter OTLP JSON object, JSONL (one export request per line), or a directory of `.json`/`.jsonl` files. OTLP IDs are validated as hexadecimal, as required by the OTLP JSON encoding.

Example input fragment:

```json
{"traceId":"11111111111111111111111111111111","spanId":"1111111111111111","kind":4,"attributes":[{"key":"messaging.system","value":{"stringValue":"kafka"}}]}
```

Example human output:

```text
AsyncTraceDoctor: 2 spans, 2 messaging spans, 1 findings, context completeness 0.0%
SEVERITY  RULE          PRODUCER -> CONSUMER       SYSTEM/DESTINATION       METHOD                 CONF  MESSAGE
ERROR     ATD-CTX-001   -> billing                 rabbitmq/payments        semantic_validation    high  Consumer process span has no valid producer span link or parent context.
```

The JSON report contains stable schema version `1.0`, summary, findings, and observed topology. Every finding includes relevant services, messaging system/destination, trace/span IDs, method, confidence, safe evidence, and a suggested fix. Trace, span, and message IDs are never Prometheus labels.

## Rules and correlation

[`config/rules.yaml`](config/rules.yaml) is versioned and validated strictly. Each enabled rule declares an ID, implementation check, applicability, severity, explanation, and fix. Global settings define correlation window, queue latency, duplicate threshold, and policy exit threshold. Optional reviewed `topology.expectedEdges` support exact values or `*` wildcards.

Correlation follows this fixed semantic order:

1. producer span context in a consumer span link;
2. direct parent/message creation context for a single process span;
3. matching messaging system, destination, and message ID;
4. system/destination plus bounded time-window heuristic at low confidence.

`messaging.message.id` is recommended for a single message but is not mandatory. Its absence never produces an invalid-semantics finding; identity-based correlation is skipped and orphan confidence is downgraded.

The rules target OpenTelemetry Semantic Conventions 1.43.0. Messaging conventions are still marked development, so config version review is intentional. See [OpenTelemetry messaging spans](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/) and the [OTLP specification](https://opentelemetry.io/docs/specs/otlp/).

## Architecture and safety

Module boundaries and data flow are in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md). The server enforces request bytes, retained span count, TTL eviction, timeouts, and low-cardinality metrics. Message payload/body-like attributes are always redacted; additional sensitive attributes are configurable. The container runs as a non-root distroless user.

## Evaluation

Ground truth lives only under `evaluation/datasets/*` and is opened by the evaluator after audit processing. It is never part of an audit request. Core/golden, holdout, and live Docker results are separate in [`evaluation/results/latest.json`](evaluation/results/latest.json) and explained in [`docs/EVALUATION.md`](docs/EVALUATION.md).

```bash
go run ./evaluation/cmd/evaluate
pwsh ./scripts/live-e2e.ps1
go run ./evaluation/cmd/evaluate # merge the live artifact
```

## Current limits

- Correlation is in-memory and single-process; there is no durable or horizontally shared state.
- Low-confidence time matching can be ambiguous under high fan-out or high same-destination throughput.
- Duplicate detection requires `messaging.message.id` and is scoped to the retained audit window.
- Queue latency is span-clock based and inherits clock-skew error.
- Expected topology is static config; discovery approval workflows are out of scope.
- This MVP supports traces only and does not parse broker payloads.

## Roadmap

The natural next step is an OpenTelemetry Collector connector that consumes traces and emits audit metrics/findings, followed by a processor mode for annotations, stronger consumer-group-aware fan-out modeling, streaming report export, and scale/load evidence. No upstream contribution has been submitted; [`docs/UPSTREAM_CONTRIBUTION.md`](docs/UPSTREAM_CONTRIBUTION.md) is a proposal only.

## Development

```bash
gofmt -w .
go test ./...
go vet ./...
docker compose config --quiet
```

Contributions are welcome under the Apache-2.0 license; see [`CONTRIBUTING.md`](CONTRIBUTING.md).
