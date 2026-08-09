# Evaluation

AsyncTraceDoctor separates four evidence classes: core regression cases, holdout cases, adversarial semantic cases, and live broker-backed scenarios. Expected answers never enter the production audit path.

## Method

1. Read a manifest and resolve only its input path.
2. Parse and audit the OTLP input with the normal config and engine.
3. After the report is complete, compare its rule set and topology with ground truth.
4. Record metrics and content provenance.

Current offline datasets contain:

- four core cases: normal link, broken context, incomplete batch, duplicate processing;
- two holdout cases: message-ID-free orphan producer and consumer;
- three adversarial cases: unresolved valid link, cross-environment identity collision, and RabbitMQ destination-shape compatibility.

Live Docker runs a real one-node Redpanda, instrumented Go producer/consumer, an OpenTelemetry Collector, and the OTLP server. It remains an integration test, not a scale or availability test.

## Metrics

- Broken-link precision/recall/F1 are case-level for `ATD-CTX-001`.
- Per-rule precision and recall use the exact expected rule set for each case.
- Exact finding-set accuracy fails on a missing/unexpected rule or an unexpected finding count.
- Topology accuracy is aggregate Jaccard accuracy over expected and observed edges.
- Normal false-positive rate is the fraction of declared-normal cases with any finding.
- Processing time and allocation are small-fixture process samples, not server throughput or RSS.

Every artifact includes schema version, Git revision, Go version, rules SHA-256, and a content hash over each dataset manifest plus input files. CI writes evaluation to an uploaded artifact and fails if evaluation changes tracked files.

## Reproduce

```bash
go test ./...
go run ./evaluation/cmd/evaluate
go test -run '^$' -bench BenchmarkCorrelate -benchmem -count 3 ./internal/correlation
pwsh ./scripts/live-e2e.ps1
```

The live script recreates broker state per scenario, queries the actual `/report`, and fails when required findings are absent or normal traffic has findings.

## Limits

- Nine offline cases are far too small for external validity.
- Holdout data is repository-local, not independently collected or blind.
- No per-span localization score, confidence calibration, ablation, or threshold sweep exists yet.
- No SDK/instrumentation compatibility matrix or independent real-trace labeling exists yet.
- Correlation microbenchmarks do not establish sustained receiver throughput, tail latency, RSS, restart recovery, or multi-replica correctness.
- Live scenarios use deliberate fault injection and one local broker node.

Ratios such as `1.0` must always be read with case counts and exact finding-set accuracy. They are regression evidence for these fixtures only, not production accuracy claims.
