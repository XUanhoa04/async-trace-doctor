# Upstream contribution proposals

No issue or pull request has been opened, submitted, or merged. The following are proposal drafts only and require maintainer discussion plus explicit user authorization before any upstream action.

## Messaging validation fixtures

Propose broker-neutral OTLP fixtures for linked single messages, parent-context single processing, batch links with per-message attributes, missing context, and optional message IDs. Keep assertions aligned to a named semantic-convention release and avoid prescribing vendor SDK behavior beyond the specification.

Candidate homes: OpenTelemetry semantic-conventions examples or language instrumentation conformance repositories. Open questions include fixture ownership while messaging conventions remain development and whether negative fixtures belong in a shared specification repository.

## OpenTelemetry Demo broken-context scenario

Propose an opt-in demo flag that disables producer injection or consumer extraction without embedding expected observability-tool results. Document visible trace consequences and keep normal propagation the default. This would let vendors and users reproduce a common blind spot consistently.

## Collector example

Propose a documented Collector configuration that routes traces to an external auditor through OTLP while preserving the normal backend export. It should demonstrate memory limiting, batching, TLS/auth placeholders, and a file-exporter workflow for offline audit.

## Connector or processor direction

A Collector connector could consume traces and emit bounded telemetry-quality metrics plus structured evidence logs. A later processor might annotate spans, but dropping or mutating production telemetry should not be the starting point. Correlation microbenchmarks and first-seen/active temporality now exist; before proposal, add sustained receiver CPU/RSS evidence, define multi-tenant and shard ownership, specify retractions/restart behavior, review semantic-convention stability, and settle the logs/metrics export contract.
