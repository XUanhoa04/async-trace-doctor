# Evaluation

AsyncTraceDoctor keeps three evidence classes separate. Scores are generated from real engine output; no threshold is tuned per scenario and no expected result enters the production audit path.

## Method

1. The evaluator reads only the case input path from a dataset manifest.
2. It parses and audits that OTLP file with `config/rules.yaml`.
3. After the report is complete, it compares detected rule IDs and topology edges with the remaining ground truth.
4. It records the artifact in `evaluation/results/latest.json`.

Core/golden cases exercise normal linking, broken context, an incomplete batch, and duplicate processing. Holdout cases exercise message-ID-free orphan producer/consumer telemetry. Live Docker uses a real one-node Redpanda, instrumented Go producer/consumer, Collector forwarding, and the running OTLP server.

Broken-link precision/recall/F1 treat a consumer `process` finding from the configured missing-context rule as the positive detection. Violation recall is case-level per rule. Topology edge accuracy is aggregate Jaccard accuracy over expected and observed edges. Normal false-positive rate is the fraction of explicitly normal scenarios with any finding.

Processing latency is average in-process audit time per small fixture. Peak allocated bytes is the maximum Go `runtime.MemStats.Alloc` sample after a case; it is evidence, not container RSS or a load-test maximum. Capacity/TTL unit tests and `async_trace_state_spans` provide bounded-state evidence.

## Latest measured result

Run `go run ./evaluation/cmd/evaluate` to refresh the authoritative JSON. The checked-in result records the exact timestamp and values. On the small deterministic core and holdout sets, current broken-link precision, recall, F1, topology accuracy, and expected-rule recall are all `1.0`; the normal false-positive rate is `0.0`. These figures demonstrate fixture behavior only and must not be generalized to production traffic.

The live result is reported separately under `live_docker`. A failed or unavailable Docker run remains visible as such; it is never replaced with a synthetic success.

## Reproduce

```bash
go test ./...
go run ./evaluation/cmd/evaluate
pwsh ./scripts/live-e2e.ps1
go run ./evaluation/cmd/evaluate
```

The live script validates Compose, builds once, recreates isolated broker state per scenario, queries the actual `/report`, writes `evaluation/results/live.json`, and fails when required findings are absent or normal traffic has findings.

## Limits of the evidence

- Six offline cases are too small to establish external validity.
- Fixtures do not measure high-throughput ambiguity, fan-out consumer groups, clock skew, long broker delays, or horizontally distributed state.
- The Docker E2E proves integration behavior on one local environment, not availability or scale.
- There is no sustained load/RSS profile yet.

These gaps are why the project is described as an MVP, not production-ready.
