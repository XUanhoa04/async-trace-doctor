# Adversarial QA gap analysis

This document is a release-risk register, not a claim of production readiness. The bundled fixtures are intentionally small. A green evaluator proves only that those labeled cases still behave as expected.

## Review verdict

AsyncTraceDoctor addresses a real observability problem and is worth developing as an OpenTelemetry quality gate. It is no longer fair to call the implementation a toy on the specific retry, clock, batch, fan-out-policy, DLQ, and negative-topology cases covered below. It is still an MVP and should not be used as the sole evidence for message loss, duplicate business effects, or topology compliance in production.

## Claims checked against the implementation

| Review claim | Finding | Current behavior |
|---|---|---|
| One successful consumer hides a missing fan-out group | Valid when expected groups are not declared | Observed edges retain `messaging.consumer.group.name` and subscription. An expected edge can opt into `requirePerMessage`; no algorithm pretends to infer required groups from producer telemetry. |
| Batch fallback maps only one producer | Valid | A consumer with `messaging.batch.message_count=N` can correlate up to N bounded nearest producers at low confidence. Strong link coverage is still required for context completeness. |
| Negative latency is clamped to zero | Valid | Signed latency is preserved. `ATD-TIM-001` reports values below `-clockSkewTolerance`. |
| Failed retry attempts are called duplicates | Valid | OTLP span status is retained. Attempts with status `ERROR` or `error.type` are counted as failed retries, not successful duplicate processing. Consumer group/subscription and time window partition duplicate groups; repeated exports of the same span ID are ignored. |
| Historical files are judged with `time.Now` | Valid, but a max-event-time-only replacement is incomplete | Offline input is explicitly finalized. Live snapshots use a max-end event watermark. This preserves offline orphan detection without comparing old timestamps to the host clock. |
| Any span name containing publish/process is messaging | Valid | Name substring heuristics were removed. Messaging operation attributes or messaging SpanKind are required. |
| Unique link contexts must equal batch size | Valid for shared batch creation contexts | Coverage can come from distinct message link attributes or repeated links to a shared context, bounded by the linked producer's declared batch count. One unexplained link still does not prove an arbitrary batch is complete. |
| A 5 GB file will be loaded and crash | Overstated | The CLI rejects input above `--max-bytes` (64 MiB by default) before decode. Parsing is not streaming, so memory amplification below the limit remains a scale concern. |
| DLQ always creates topology noise | Valid with allow-list-only policy | Reviewed DLQ/replay paths can be placed in `ignoredEdges`; explicit prohibited paths use `deniedEdges`. |
| Negative topology cannot be configured | Valid | `topology.deniedEdges` now expresses prohibited producer-to-consumer paths. |

## Unresolved release blockers

### P0 — conclusions can exceed available evidence

- Sampling, dropped exports, collector backpressure, and TTL/capacity eviction are indistinguishable from true missing delivery. Findings need an explicit telemetry-coverage signal before they can support strong message-loss claims.
- Message identity is broker-neutral and weak. Kafka partition/offset, SQS receipt/delivery identifiers, RabbitMQ redelivery metadata, and broker-specific DLQ transitions are not normalized. Reused or absent `messaging.message.id` can still cause ambiguity.
- `requirePerMessage` cannot verify a batch `send` span that lacks per-message Create spans or link attributes. The engine skips that assertion instead of manufacturing certainty.
- Live event-time does not advance when traffic is idle. An idle-source policy or explicit flush/end-of-window signal is needed so the last unmatched event can close safely.

### P1 — scale and operational correctness

- Correlation scans producers for every consumer in fallback branches: worst-case time is O(producers × consumers). There is no index by system/destination/message ID and no high-volume benchmark.
- The server is single-process and in-memory. Restarts lose state; replicas do not share correlation state; arrival order plus capacity eviction can split a valid pair.
- `state-ttl` is not validated against `correlationWindow`. A TTL shorter than the correlation window can evict evidence before a decision is possible.
- Periodic reports recount the same finding and Prometheus violation counters on every audit. They measure audit observations, not unique incidents.
- OTLP endpoints and admin endpoints have no built-in TLS, authentication, or tenant isolation. Deployment must provide those controls externally.

### P1 — semantic and data-quality gaps

- Invalid zero IDs, duplicate/conflicting trace-span IDs, zero timestamps, end-before-start spans, and impossible durations have no dedicated findings.
- Failed retry classification depends on status `ERROR` or `error.type`. Exception events and system-specific delivery-count attributes are not normalized, so poorly instrumented retries remain ambiguous.
- `receive`, `process`, and `settle` chains are not modeled as a state machine. The same delivery represented by multiple operation spans can inflate topology counts.
- Rule applicability filters only by operation. Users cannot yet scope or suppress rules by service, system, destination, environment, tenant, or a time-bounded maintenance/replay window.
- Redacting structural correlation attributes before analysis can degrade results. A future design should retain keyed hashes for analysis while suppressing raw values from reports.

### P2 — evidence quality

- The evaluator has very few synthetic cases. Per-rule precision, negative examples for every rule, corrupted telemetry, out-of-order arrival, sampling, high-cardinality load, and broker-specific golden datasets are missing.
- Peak allocation in the evaluator is a process sample, not peak RSS, and there is no CPU/latency percentile or adversarial complexity benchmark.
- Human output truncates edge fields. JSON is the authoritative evidence format for full service/group/subscription values.

## Exit criteria before a production-ready claim

1. Add broker-specific identity adapters and an indexed correlation benchmark at realistic span volumes.
2. Make telemetry completeness, eviction, sampling, and idle watermark state visible in each report and downgrade confidence when evidence is incomplete.
3. Add durable/shared state or explicitly constrain deployment to one collector shard with tested routing guarantees.
4. Expand independent holdout data so every rule has true positives, true negatives, retry/redelivery variants, clock-skew variants, and sampled/partial traces.
5. Threat-model multi-tenant ingestion and document or implement authentication, TLS termination, quotas, and retention guarantees.
