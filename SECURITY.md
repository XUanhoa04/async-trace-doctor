# Security policy

Please report suspected vulnerabilities privately through GitHub's security advisory flow when enabled. Do not include real telemetry, credentials, message bodies, or personal data in reports.

The pre-production receiver accepts untrusted OTLP only behind explicit byte, gRPC message, span-admission, TTL, and HTTP timeout limits. Operators should place TLS and authentication at a trusted reverse proxy or Collector; the built-in receiver does not implement authentication or tenant authorization. Configure `redactAttributes` for local sensitive keys.
