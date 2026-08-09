# Changelog

All notable changes are documented here. The project has not published a stable release.

## Unreleased

### Added

- Coverage-aware `sufficient`, `insufficient`, and `degraded` evidence states.
- `ATD-COV-001` for valid context references whose target telemetry is unavailable.
- Kafka partition+offset identity fallback and RabbitMQ destination-shape compatibility.
- Environment/service/destination namespace isolation for heuristic correlation.
- Indexed nearest-route correlation benchmarks and adversarial evaluation cases.
- OTLP partial-success for rejected live spans, live-state deduplication, and conflict detection.
- Semantic checks for invalid identities and timestamps.
- Exact finding-set evaluation, rules/dataset/source provenance, and stale live-artifact detection.

### Changed

- Repositioned the project as a messaging trace-contract gate rather than a message-loss detector.
- Absence-based live findings no longer fail policy by default without completeness evidence.
- Prometheus counters no longer recount the same active finding on every audit interval.
- `state-ttl` must be at least the configured correlation window.

### Known limitations

- Standalone, single-process in-memory state; no Collector connector or shared shard ownership yet.
- No live RabbitMQ E2E or broker delivery-state machines.
- No sustained receiver throughput/RSS/recovery evidence.
