# Contributing

Thanks for improving AsyncTraceDoctor. Keep changes broker-neutral, deterministic, and grounded in a named OpenTelemetry semantic-convention version.

See [CHANGELOG.md](CHANGELOG.md) for the unreleased contract and [docs/QA-GAP-ANALYSIS.md](docs/QA-GAP-ANALYSIS.md) for bounded contribution areas with production impact.

## Development workflow

1. Open an issue or local design note for semantic changes. Do not encode scenario names, fixture names, ground truth, or expected benchmark output in production packages.
2. Add rules through the versioned config contract and a generic engine check. Treat `messaging.message.id` as optional; prefer normalized broker identity when available.
3. Put golden truth only under `evaluation/datasets`; audit inputs belong under `testdata` and must not contain expected findings.
4. Add or update unit tests, a normal negative test, evidence-state behavior, and documentation. Absence-based conclusions must prove coverage or remain `insufficient`.
5. Run:

```bash
gofmt -w .
go test ./...
go vet ./...
go run ./evaluation/cmd/evaluate
docker compose config --quiet
```

For changes to propagation, batching, server ingestion, or Compose, also run `pwsh ./scripts/live-e2e.ps1`.

## Security and privacy

Never commit credentials, real message bodies, personal data, or production trace exports. Use synthetic hex IDs. New metrics must have a documented bounded label set; never label by trace ID, span ID, message ID, service name, or destination. Preserve request/state limits and non-root container execution.

## Scope

This project audits telemetry quality. LLM features, incident RCA, alert orchestration, and remediation are out of scope. Upstream issues/PRs require repository-owner authorization; proposal documents do not imply submission.
