# Codebase improvement opportunities

This document captures high-impact improvements identified during a quick audit.

## 1) Security hardening for authentication (high priority)

- Move JWT secrets to environment variables and avoid shipping hard-coded secrets.
- Add explicit token issuer/audience claims and validate them at decode time.
- Rotate secrets periodically and support dual-key verification windows.

## 2) Credential handling (high priority)

- Researcher credentials are stored as plaintext in `device_registry.json`.
- Replace plaintext passwords with salted password hashes (e.g. `bcrypt`) and constant-time verification.
- Move all credentials to a dedicated secret manager or env-backed secure config.

## 3) Logging consistency and observability (medium priority)

- Replace most `print()` calls in producer/consumer with structured `logging`.
- Standardize log levels and include correlation fields (topic, sensor_id, request id).
- Add Prometheus-style metrics for ingest rates, auth failures, and sync lag.

## 4) Error handling quality (medium priority)

- Several broad `except Exception` handlers swallow context.
- Narrow exception types where practical and include actionable remediation in logs.
- Standardize error responses in API routers.

## 5) Project hygiene (medium priority)

- Add unit tests for auth token creation/verification and router credential paths.
- Add CI checks (`ruff`, `pytest`, and `mypy` or `pyright`) to prevent regressions.
- Introduce a pre-commit config for formatting/linting consistency.

## 6) Data quality and schema controls (medium priority)

- Add strict schema versioning for telemetry payloads.
- Introduce dead-letter routing for invalid records for later replay.
- Persist quality failure reasons in a query-friendly format.
