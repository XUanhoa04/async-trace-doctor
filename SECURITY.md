# Security policy

Please report suspected vulnerabilities privately through GitHub's security advisory flow when enabled. Do not include real telemetry, credentials, message bodies, or personal data in reports.

The receiver enforces request-byte, retained-byte, span-count, rate, TTL, and HTTP timeout limits. Configure a bearer token with `ATD_AUTH_TOKEN` (recommended for secrets) or `--auth-token`; clients must send `Authorization: Bearer <token>` to OTLP and admin endpoints. `/health` intentionally remains unauthenticated for liveness probes. An empty token preserves backward compatibility and disables built-in authentication.

Built-in bearer authentication does not provide TLS or tenant authorization. Use TLS at a trusted reverse proxy or OpenTelemetry Collector, rotate tokens regularly, bind admin endpoints to private interfaces, and use `--redact-report` where topology must not be exposed. Configure `redactAttributes` for local sensitive keys; payload-like attributes are always redacted.
