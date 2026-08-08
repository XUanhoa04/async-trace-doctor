# Security policy

Please report suspected vulnerabilities privately through GitHub's security advisory flow when enabled. Do not include real telemetry, credentials, message bodies, or personal data in reports.

The MVP accepts untrusted OTLP only behind explicit byte, gRPC message, span-count, TTL, and HTTP timeout limits. Operators should place TLS and authentication at a trusted reverse proxy or Collector; the built-in receiver does not implement authentication. Configure `redactAttributes` for local sensitive keys.
