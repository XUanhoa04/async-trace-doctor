# Adversarial QA gap analysis

This is a release-risk register, not a production-readiness claim. A green evaluator proves behavior on the named fixtures only.

## Hardened in the current contract

| Earlier risk | Current behavior |
|---|---|
| Missing target span was called broken propagation | Valid link/parent references are context-complete; unavailable targets produce `ATD-COV-001` with insufficient evidence. |
| Sampling/export loss was indistinguishable from delivery loss | Absence findings expose coverage state and do not fail policy by default when completeness is unknown. The underlying source of missing telemetry is still not identifiable. |
| RabbitMQ exact destination matching rejected valid links | Exact links ignore display-destination shape; fallback recognizes compatible producer/consumer destination prefixes. |
| Message-ID-only Kafka identity | Partition+offset can correlate when both sides record them. Cluster, commit, rebalance, and delivery attempt are still missing. |
| Producer scan for every consumer | Context, route, and identity indexes plus nearest-neighbor route lookup replace the global quadratic fallback. Dense candidate ambiguity remains. |
| Capacity silently evicted evidence while ACKing OTLP | Capacity/conflicting identities are rejected through OTLP partial-success and included in coverage. |
| Duplicate exports inflated every rule | Live state deduplicates identical trace/span identities and rejects conflicts. |
| Periodic metrics recounted every finding | Violation counters use a first-seen fingerprint; active findings are a latest-snapshot gauge. |
| TTL could be shorter than correlation window | `serve` rejects that configuration. |
| Rule scope only supported operation | Policy can scope by operation, system, service, destination, and environment. |
| Invalid messaging IDs/timestamps were silent | Dedicated semantic rules report zero/truncated identities and missing/end-before-start timestamps; offline conflicting duplicates are rejected. |

## P0 — conclusions still need stronger evidence

- Live coverage can say `unknown` or `degraded`, but cannot identify whether missing telemetry came from head/tail sampling, an SDK, an exporter, a Collector, network loss, or another shard.
- `inputCompleteness: complete` is an operator assertion, not cryptographic proof. It is appropriate for controlled CI/canary datasets, not arbitrary production exports.
- Kafka cluster identity, commit state, rebalance generation, delivery attempt, and transactional semantics are not normalized.
- RabbitMQ vhost, delivery tag scope, redelivery, ack/nack, exchange-to-queue routing, and DLQ transitions are not modeled.
- A generic `messaging.message.id` may be reused or absent. Time fallback remains a candidate inference, never delivery proof.
- No broker ground-truth stream is ingested, so AsyncTraceDoctor cannot claim business message loss or duplicate side effects.

## P1 — state and event-time correctness

- State is single-process and non-durable. Restart loses the window; replicas cannot share or consistently own correlation state.
- Maximum-event-time watermark can advance prematurely under a future-skewed source. Idle streams do not close without later telemetry.
- There is no allowed-lateness policy, retraction record, first-seen/last-seen finding lifecycle, or durable output sink.
- Admission is bounded by span count rather than retained bytes/RSS. An attribute-heavy span population can consume disproportionate memory.
- Exact correlation is indexed, but hot ambiguous routes still require candidate inspection. Allocation volume remains material in microbenchmarks.
- OTLP endpoints and admin endpoints rely on an external trusted proxy/Collector for TLS, authentication, authorization, quotas, and tenant routing.

## P1 — semantic coverage

- `receive`, `process`, and `settle` are not a full delivery state machine and may describe the same delivery more than once.
- Conflicting semantic-convention versions and legacy/new convention mixtures do not have dedicated compatibility findings.
- Retry classification depends on status/error evidence and normalized identity; exception events and broker attempt counters are not modeled.
- Batch send/create relationships across mixed destinations are only partially validated.
- Semantic-convention compatibility is declared by config; instrumentation scope/version is retained inadequately for automatic migration decisions.

## P2 — evaluation evidence

- Nine synthetic offline cases and five live scenarios do not establish production precision or recall.
- Repository-local holdout data is not blind or independent.
- There is no confidence calibration, per-span localization metric, baseline comparison, ablation, threshold sensitivity, or mixed-failure corpus.
- Microbenchmarks measure correlation only. No sustained OTLP throughput, tail latency, RSS, restart recovery, or replica/shard correctness test exists.
- RabbitMQ has semantic fixtures but no live broker E2E.

## Exit criteria for a production-ready claim

1. Implement Collector-native deployment with shard ownership and explicit output temporality.
2. Add broker lifecycle adapters and broker-backed ground truth for Kafka and RabbitMQ.
3. Add allowed lateness, retractions, durable finding identities, and restart/rebalance tests.
4. Establish retained-byte limits and sustained CPU/RSS/p99 benchmarks at declared volume envelopes.
5. Build an external compatibility corpus with per-rule/span precision, false-positive analysis, ablation, and threshold sweeps.
6. Threat-model and test tenant isolation, authentication, authorization, quotas, and retention.
