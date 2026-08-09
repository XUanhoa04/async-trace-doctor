# Architecture

AsyncTraceDoctor currently provides an offline contract gate and a pre-production standalone OTLP receiver. The target production shape is an OpenTelemetry Collector connector; the standalone receiver validates semantics before that integration is proposed upstream.

```text
Collector JSON/JSONL ─┐
OTLP HTTP/gRPC ───────┼─> ingest ─> normalized span metadata ─> route/context indexes
                      │                                      │
                      └─> deduplicating admission store ─────┼─> rules ─> report/metrics
                                                             │
                      coverage/drop/eviction state ──────────┘

audit report ──────────────────────────────> evaluator <── ground truth (never audit input)
```

## Boundaries

- `internal/ingest` validates OTLP JSON IDs, decodes protobuf, redacts payload-like data, and retains isolation, flags, dropped-field counts, and messaging attributes.
- `internal/model` defines normalized spans, links, coverage, findings, topology, and schema-versioned reports.
- `internal/correlation` builds context, route, and message-identity indexes. Exact links/parents outrank attributes; time matching is explicitly low confidence.
- `internal/rules` maps versioned policy to implemented checks. YAML controls severity, thresholds, messages, and operation/system/service/destination/environment scope; checks still require Go implementation.
- `internal/server` receives OTLP, deduplicates identical exports, rejects conflicting identities/capacity overflow with OTLP partial-success, audits snapshots, and exposes bounded-label metrics.
- `evaluation/cmd/evaluate` is separate from production packages and loads ground truth only after audit output exists.

## Correlation semantics

| Priority | Evidence | Confidence | Notes |
|---|---|---:|---|
| 1 | Exact linked trace/span context | High | A causal link is not rejected because display destinations differ. |
| 2 | Same-trace direct parent producer | High | Valid for a single-message process span. |
| 3 | Message ID or Kafka partition+offset | Medium | Requires route and isolation compatibility inside the correlation window. |
| 4 | Indexed route + nearest event time | Low | Recovers a candidate topology; never proves propagated context. |

A valid link or parent reference is structurally context-complete even when its producer span is unavailable. The engine emits `ATD-COV-001` with `evidence_state=insufficient` instead of calling that situation broken propagation. Missing context means the consumer span itself carries no creation-context reference.

RabbitMQ producer and consumer destination strings can differ because a consumer destination may add its queue. Exact links remain valid; attribute fallback accepts only compatible prefix shapes. Fallback correlation refuses cross-environment/service-namespace/destination-namespace/broker-address matches when both values are known.

Batch consumers may correlate multiple exact links. When links are missing and a batch count is declared, the low-confidence route index selects up to that count of nearest producers.

## Event time and coverage

Offline audit is finalized. `settings.inputCompleteness: complete` is an explicit assertion that absence may be interpreted; `unknown` keeps absence findings insufficient.

Live snapshots use maximum observed span end time as a watermark and are always coverage-unknown. They do not compare historical timestamps with the auditor host clock for orphan decisions. Idle streams still require an explicit future idle-watermark policy.

Coverage becomes `degraded` when the receiver rejects spans, state TTL evicts evidence, or OTLP reports dropped links/attributes. Absence-based findings do not fail policy by default while evidence is insufficient.

## Bounded state and temporality

`--max-spans` is an admission limit. Existing correlation evidence is retained until TTL; excess new spans are rejected and disclosed through OTLP partial-success. `--state-ttl` must be at least the correlation window.

Identical `{trace_id, span_id}` exports are deduplicated. Conflicting content for the same identity is rejected and counted. Audit work happens on a copied snapshot so receivers do not hold the store lock during analysis.

Prometheus violation counters increment once per stable finding fingerprint. `async_trace_active_findings` represents the latest snapshot. Queue-latency histograms observe all non-negative correlations, not only threshold violations.

## Privacy and failure behavior

Message bodies and payload-like attributes are replaced with `[REDACTED]`; deployment-specific keys are configurable. Trace/span/message/service/destination values never become metric labels.

Malformed input, unsupported config, invalid IDs, exceeded offline limits, and listen failures are explicit errors. Offline input/runtime errors exit `1`; sufficient-evidence policy violations exit `2`. Built-in TLS, authentication, tenant authorization, durable state, and multi-replica ownership remain deployment blockers.
