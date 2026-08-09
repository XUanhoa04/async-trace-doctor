<p align="center">
  <img src="docs/assets/async-trace-doctor-logo.png" width="190" alt="AsyncTraceDoctor logo">
</p>

<h1 align="center">AsyncTraceDoctor</h1>

<p align="center">
  <strong>Make asynchronous traces trustworthy before your APM or AIOps pipeline depends on them.</strong>
  <br>
  An evidence-first OpenTelemetry quality auditor for Kafka, RabbitMQ, and other event-driven systems.
</p>

<p align="center">
  <img alt="Go 1.25" src="https://img.shields.io/badge/Go-1.25-00ADD8?logo=go&logoColor=white">
  <img alt="OpenTelemetry semantic conventions 1.43" src="https://img.shields.io/badge/OpenTelemetry_SemConv-1.43-5A29E4?logo=opentelemetry&logoColor=white">
  <a href="LICENSE"><img alt="Apache 2.0 license" src="https://img.shields.io/badge/License-Apache--2.0-2F6FEB"></a>
  <img alt="Project status: MVP" src="https://img.shields.io/badge/Status-MVP-F59E0B">
</p>

<p align="center">
  <a href="#quick-start-under-five-minutes">Quick start</a> ·
  <a href="#what-it-audits">Rules</a> ·
  <a href="docs/ARCHITECTURE.md">Architecture</a> ·
  <a href="docs/EVALUATION.md">Evaluation</a> ·
  <a href="docs/QA-GAP-ANALYSIS.md">QA risk register</a>
</p>

AsyncTraceDoctor audits whether an asynchronous trace graph is trustworthy enough for RCA, service maps, and automation. It finds broken propagation and misleading telemetry before those defects become confident—but wrong—operational conclusions.

It is deliberately **not** an APM backend. It does not store message payloads, call an LLM, diagnose business incidents, or remediate anything. Your APM explains service performance; AsyncTraceDoctor checks whether the underlying async trace evidence deserves to be trusted.

## What it audits

| Risk | Evidence checked |
|---|---|
| Broken async context | Span links, direct producer parent context, message identity, and bounded fallback correlation |
| Fan-out delivery gaps | Expected consumer service, consumer group, and subscription per identifiable message |
| Retry vs duplicate processing | OTLP span status, `error.type`, message identity, consumer group, and time window |
| Batch trace quality | Declared message count, per-message links, shared creation contexts, and link attributes |
| Misleading latency | Queue delay thresholds plus explicit negative-latency/clock-skew findings |
| Runtime topology drift | Expected, denied, and reviewed ignored edges, including DLQ/replay exceptions |
| Invalid messaging semantics | Service, system, destination, operation type, and SpanKind validation |

> **Project status:** tested MVP, not production-ready. Unit, golden, holdout, and reproducible Docker E2E evidence are included; known release blockers are documented rather than hidden.

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
SEVERITY  RULE          PRODUCER -> CONSUMER                       SYSTEM/DESTINATION       METHOD                 CONF  MESSAGE
ERROR     ATD-CTX-001   -> billing                                 rabbitmq/payments        semantic_validation    high  Consumer process span has no valid producer span link or parent context.
```

The JSON report contains stable schema version `1.0`, summary, findings, and observed topology. Every finding includes relevant services, messaging system/destination, trace/span IDs, method, confidence, safe evidence, and a suggested fix. Trace, span, and message IDs are never Prometheus labels.

## Rules and correlation

[`config/rules.yaml`](config/rules.yaml) is versioned and validated strictly. Each enabled rule declares an ID, implementation check, applicability, severity, explanation, and fix. Global settings define correlation window, queue latency, clock-skew tolerance, duplicate threshold, and policy exit threshold. Reviewed topology policy supports expected, denied, and ignored edges; consumer group and subscription are optional dimensions. Set `requirePerMessage: true` only on an expected edge whose producer spans identify individual messages.

Correlation follows this fixed semantic order:

1. producer span context in a consumer span link;
2. direct parent/message creation context for a single process span;
3. matching messaging system, destination, and message ID;
4. system/destination plus bounded time-window heuristic at low confidence; a declared consumer batch may select multiple nearest producers.

Offline `audit` treats its input as a finalized dataset. The receiver's periodic audit treats state as an open window and uses the maximum span end time as an event-time watermark; it never compares historical span timestamps with the auditor host clock.

```yaml
topology:
  expectedEdges:
    - {producer: checkout, system: kafka, destination: orders, consumer: billing, consumerGroup: billing-v1, requirePerMessage: true}
    - {producer: checkout, system: kafka, destination: orders, consumer: mailer, consumerGroup: mail-v1, requirePerMessage: true}
  deniedEdges:
    - {producer: billing, system: kafka, destination: fraud, consumer: fraud-detect}
  ignoredEdges:
    - {producer: "*", system: kafka, destination: orders.dlq, consumer: replayer}
```

Wildcards match an entire field, not a glob pattern: use `*`, not `*.dlq`. List each DLQ destination explicitly when names differ.

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
- Low-confidence time matching remains ambiguous under high fan-out or high same-destination throughput and is quadratic in the worst case.
- Duplicate detection requires `messaging.message.id`, is scoped by consumer group/subscription and time window, and can distinguish retries only when failed attempts carry span status `ERROR` or `error.type`.
- Queue latency is span-clock based. Negative latency is preserved and reported by `ATD-TIM-001`; it cannot reconstruct true latency while clocks disagree.
- Expected topology is static config; discovery approval workflows are out of scope.
- Live event-time windows do not advance during an idle stream, so an unmatched final event remains pending until newer telemetry arrives.
- This MVP supports traces only and does not parse broker payloads.

The adversarial coverage and remaining release blockers are tracked in [`docs/QA-GAP-ANALYSIS.md`](docs/QA-GAP-ANALYSIS.md). Passing the small bundled evaluator is evidence against regressions in those fixtures, not evidence of production accuracy.

## Roadmap

The natural next step is an OpenTelemetry Collector connector that consumes traces and emits audit metrics/findings, followed by idle-watermark handling, indexed/durable correlation, broker-specific identity (partition/offset or delivery ID), streaming report export, and independent scale/load evidence. No upstream contribution has been submitted; [`docs/UPSTREAM_CONTRIBUTION.md`](docs/UPSTREAM_CONTRIBUTION.md) is a proposal only.

## Development

```bash
gofmt -w .
go test ./...
go vet ./...
docker compose config --quiet
```

Contributions are welcome under the Apache-2.0 license; see [`CONTRIBUTING.md`](CONTRIBUTING.md).
