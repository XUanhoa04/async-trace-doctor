# Architecture

AsyncTraceDoctor has one production pipeline and a deliberately separate evaluator.

```text
Collector JSON/JSONL ─┐
OTLP HTTP/gRPC ───────┼─> ingest ─> normalized span metadata ─> correlation ─> rules ─> report/metrics
                      │                    │                         │
                      └─> bounded store (max spans + TTL) ──────────┘

audit report ──────────────────────────────> evaluator <── ground truth (never audit input)
```

## Boundaries

- `internal/ingest` validates OTLP JSON deviations, decodes protobuf requests, applies redaction, and flattens trace metadata. Byte and span limits are enforced at the edge.
- `internal/model` is a broker-neutral span, link, finding, topology, and report model.
- `internal/correlation` performs deterministic, bounded-window correlation. Links and parent context are stronger than attributes; heuristic matches are explicitly low confidence.
- `internal/rules` maps versioned config checks to semantic validators. Rule IDs, severities, messages, thresholds, applicability, and topology expectations come from YAML. The engine contains no fixture/scenario knowledge.
- `internal/report` renders a human table or schema-versioned JSON.
- `internal/server` implements OTLP HTTP/gRPC, bounded state, periodic snapshots, health/readiness, the latest report, and Prometheus metrics.
- `evaluation/cmd/evaluate` is not imported by production code. It loads an input, completes the audit, and only then compares the report with a separate ground-truth manifest.

## Correlation semantics

For every receive/process span, the correlator attempts:

| Priority | Evidence | Confidence | Notes |
|---|---|---:|---|
| 1 | Exact linked trace/span context | High | Supports one link per batch message. Missing semantic attributes do not invalidate a real context link. |
| 2 | Same-trace direct parent producer | High | Valid option for a single-message process span. |
| 3 | System + destination + message ID | Medium | Used only inside the correlation window. |
| 4 | System + destination + nearest time | Low | Never presented as proof of propagated context. |

One consumer may match multiple producers through links or, when batch count is declared but links are missing, through a bounded low-confidence nearest-producer fallback. Producer/consumer match sets feed orphan rules and topology edges. Consumer group and subscription remain part of an observed edge, so independent fan-out paths are not collapsed. A process span is considered context-complete only for a real link or parent; an attribute/heuristic match can recover topology but still produces a broken-context finding.

Offline and live window closure are intentionally different. A file/directory audit is finalized: after all input is read, unmatched spans are eligible for orphan checks. A server snapshot is open: the maximum observed span end is its event-time watermark, and only spans older than the correlation window relative to that watermark are eligible. Neither path uses the auditor machine's wall clock to classify historical events.

Fan-out requirements are policy, not something a producer span can reveal. `expectedEdges[].requirePerMessage` opts an individually identifiable producer message into delivery checks for a configured consumer service/group/subscription. `deniedEdges` expresses negative topology. `ignoredEdges` suppresses reviewed exceptional routes such as DLQ and replay paths.

## Bounded state

The server keeps only normalized trace metadata. `--max-spans` is a hard capacity and `--state-ttl` removes every expired span regardless of arrival order. `/ready` exposes only retained count; `/health` exposes no config or secrets. State size and eviction reason are observable without high-cardinality labels.

Periodic audits take a copied snapshot so receivers do not hold the store lock during analysis. Findings can repeat in successive windows and the `_total` metrics therefore count audit observations, not globally deduplicated incidents.

## Privacy and cardinality

Message bodies and payload-like attributes are always replaced with `[REDACTED]`. `redactAttributes` adds deployment-specific keys such as user identifiers. The tool never connects to a broker or reads message bodies. Metrics use only rule ID, severity, and bounded eviction reason; trace/span/message/service/destination identifiers never become metric labels.

## Failure behavior

Malformed JSON, invalid ID encoding, invalid YAML fields/checks, exceeded limits, and listen/runtime failures are explicit errors. Offline audit maps these to exit `1`; policy violations map to `2`. The OTLP HTTP receiver returns bounded generic error text and never echoes request data.
