# Codebase Task Proposals

## 1) Typo fix task
**Task:** Standardize the legacy `sissa` config namespace to `pipeline` (or another agreed canonical name) across code comments, config reads, and response payload labels.

**Why:** The runtime configuration object is built under `pipeline` in `SyncHelpers._load_env_config`, but other modules still read from `sissa`, which looks like leftover naming and causes confusing defaults/log output.

**References:**
- `synchronization/sync_helpers.py` defines `pipeline` config keys.
- `main.py`, `routers/hydor_auto_sync_api.py`, and `synchronization/hydor_auto_sync_worker.py` still reference `sissa`.

**Suggested acceptance criteria:**
- All config lookups use one canonical namespace.
- No user-facing response contains stale namespace naming.
- README/TSDB config examples reflect the canonical namespace.

## 2) Bug fix task
**Task:** Fix `sync_all_batches` to avoid awaiting a non-async function (`trigger_sync`) and add regression coverage.

**Why:** `routers/researcher_sync_app.py` declares `trigger_sync` as a synchronous function (`def`) but `sync_all_batches` calls `await trigger_sync(...)`. This can raise a `TypeError` at runtime (`object dict can't be used in 'await' expression`) once executed.

**Suggested implementation direction:**
- Either make `trigger_sync` `async def` and keep `await`, or call it without `await`.
- Add a route-level test that hits `/sync/complete` and asserts successful iteration across batches.

## 3) Code comment / documentation discrepancy task
**Task:** Align test-script API paths with actual router paths and documented endpoints.

**Why:** `test/test_access_control.sh` calls `/api/sync/available-data`, but there is no such endpoint in `routers/researcher_sync_app.py`. The documented researcher endpoints are `/sync/trigger`, `/sync/status`, and `/sync/complete`.

**Suggested acceptance criteria:**
- Replace invalid path calls in the script with supported endpoints.
- Update inline comments in the script to match real API behavior.
- Ensure README test instructions point to scripts that target existing routes.

## 4) Test improvement task
**Task:** Improve shell test robustness by adding strict mode and explicit assertions on response body fields.

**Why:** Current shell tests mostly check HTTP status codes and can pass despite semantically broken responses (missing keys, wrong data shape, auth payload regressions).

**Suggested implementation direction:**
- Add `set -euo pipefail` to test scripts.
- Parse JSON responses and assert key fields (`status`, `researcher`, `progress.has_more`, etc.).
- Exit non-zero with actionable messages when assertions fail.
