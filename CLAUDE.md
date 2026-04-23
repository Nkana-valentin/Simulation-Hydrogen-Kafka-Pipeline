# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project is

A containerized Physical Twin → Digital Twin pipeline for hydrogen lab telemetry. Sensor data is simulated, streamed through Kafka, quality-checked, stored in QuestDB, and exposed via a FastAPI app with JWT-authenticated sync APIs targeting the ORFEO-Hydor research platform.

## Running tests

```bash
JWT_SECRET=any-dev-secret python -m pytest tests/unit/ -v
```

Unit tests have no external dependencies — no Kafka, QuestDB, or Docker needed.

## Running the stack

All services run via Docker Compose. A `.env` file is required (see README for the template):

```bash
docker compose up -d --build

# Follow service logs
docker logs -f kafka_producer
docker logs -f kafka_consumer
docker logs -f fastapi_app
```

Service UIs:
- FastAPI docs: `http://localhost:${FASTAPI_PORT}/docs`
- QuestDB web UI: `http://localhost:9000`

## Local development (without Docker)

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Run each process in a separate terminal (requires Kafka + QuestDB already running)
uvicorn main:app --reload
python3 kafka_producer/kafka_producer_service.py
python3 kafka_consumer/kafka_consumer_to_tsdb.py
```

## Diagnostics and testing

```bash
bash diagnostic.sh                  # full runtime diagnostics
bash test/test_sync_auth.sh         # auth + sync flow
bash test/test_access_control.sh    # RBAC / data-access checks
```

## Required environment variables

`SyncHelpers.validate_config()` raises on startup if any of these are absent:

| Variable | Purpose |
|---|---|
| `QUESTDB_HOST` / `QUESTDB_PORT` | QuestDB connection |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address |
| `SSH_HOST` | Remote sync target (required even when `REMOTE_SYNC_ENABLED=false`) |
| `KAFKA_TOPIC_RAW` | Table name in QuestDB (default `raw_h2_data`) |

JWT secret defaults to a hard-coded dev value; set `JWT_SECRET` in env for any real deployment.

## New project structure (refactored)

```
config/settings.py          # Single Pydantic Settings class — all env vars declared here
domain/
  telemetry.py              # TelemetryRecord, AuthBlock (Pydantic models)
  quality.py                # Pure validation + quality dimension functions, no I/O
  auth.py                   # Pure JWT functions (secret passed as parameter)
infrastructure/
  kafka/{producer,consumer}.py
  questdb/{client,schema}.py
  ssh/transfer.py
  registry/repository.py   # Abstract + JsonFile implementations
services/
  auth_service.py           # Token issuance via repository
  ingestion_service.py      # consume → validate → store orchestration
  sync_service.py           # query → paginate → save → state management
api/
  main.py                   # FastAPI app + lifespan
  dependencies.py           # FastAPI Depends wiring
  routers/{auth,researcher_sync,admin_sync}.py
workers/auto_sync_worker.py # asyncio background task, calls SyncService only
simulator/physics_model.py  # generate_physical_state() — self-contained, no other imports
tests/unit/                 # pytest, no Docker needed
```

The old `ingestion/`, `synchronization/`, `auth_service/`, and `routers/` directories are preserved and still importable but are superseded by the above structure.

## Architecture

### Data flow

```
device_data_generator.py
  → KafkaProducerService (kafka_producer/)
    → Kafka topic (KAFKA_TOPIC_RAW)
      → KafkaToTsdbConsumerService (kafka_consumer/)
          - auth check (data["auth"]["authenticated"])
          - DataQualityChecker.validate_single_record()
          - QuestDBClient.insert_row_sql()
        → QuestDB table (same name as KAFKA_TOPIC_RAW)
          → FastAPI sync APIs (routers/)
```

### Telemetry schema

`TSDB.yml` is the single source of truth for the table schema and sensor field list. `QuestDBClient` reads it at startup to derive:
- `schema["tags"]` – string index columns (currently empty list)
- `schema["fields"]` – numeric sensor columns (driven by `prev_state` keys: `H2_001PT`, `H2_002PT`, `H2_003PT`, `H2_005PT`, `H2_001FT`, `CA_001FC`, `H2_001TT`, `H2_002TT`, `H2_003TT`, `H2_005TT`, `FC_STACK_V`, `FC_STACK_i`, `FC_STATE`)

The consumer's `build_tsdb_payload()` filters the Kafka message to only the columns present in this schema before inserting.

### Authentication model

Three identity types, all JWT-based (`auth_service/auth_token.py` using PyJWT + `Config.JWT_SECRET`):

- **Device** – sensors authenticate via `POST /device/login` (device_id + device_secret). Token payload carries `permissions: ["ingest_data"]`. The consumer checks `data["auth"]["authenticated"]` in the Kafka message, not the JWT directly.
- **Researcher** – humans authenticate via `POST /researcher/login` (institution + username + password). Token payload carries `roles` and `data_access`. The `get_current_researcher` dependency enforces `identity_type == "researcher"`.
- **Admin** – researcher token with `"admin"` in `roles`. Enforced by the `get_admin_user` dependency.

Credentials are stored in plaintext in `auth_service/device_registry.json` (known issue in IMPROVEMENTS.md).

### FastAPI routers

| Router file | Prefix | Purpose |
|---|---|---|
| `routers/authentication.py` | (root) | `/device/login`, `/researcher/login`, `/verify`, `/health` |
| `routers/researcher_sync_app.py` | `/sync` | Researcher-scoped paginated data export |
| `routers/hydor_auto_sync_api.py` | (root) | Admin auto-sync control, SSH test, status |

The `hydor_auto_sync_worker.sync_worker()` coroutine runs as a background asyncio task for the app lifetime (started in `main.py` lifespan). It polls every 30 s and triggers a local filesystem save when `new_count >= BATCH_SIZE`. Remote SSH transfer (via paramiko/SFTP) only runs when `REMOTE_SYNC_ENABLED=true`.

### Sync state persistence

`SyncHelpers` persists two types of state under `LOCAL_SYNC_DIR` (default `./synced_data`):
- `synced_data/last_sync.txt` – last timestamp synced by the auto-sync worker
- `synced_data/sync_state/<researcher_id>_sync_state.json` – per-researcher offset + batch history for the manual sync API

Sync output is saved as JSON + CSV pairs under `synced_data/sync_<timestamp>/` (auto-sync) or `synced_data/<institution>_<username>/<data_type>/` (researcher sync).

### QuestDB interaction

All QuestDB communication goes through HTTP REST (`/exec` for DDL and queries, `/write` for batch line-protocol inserts). The `insert_row_sql` method builds plain `INSERT INTO` SQL rather than InfluxDB line protocol — the ingress `Sender` path is commented out. Queries via `QuestDBClient.query()` return `List[Dict]` parsed from the `columns`/`dataset` JSON structure QuestDB returns.

## Known issues / active improvements

See `IMPROVEMENTS.md` for the full list. The highest-priority items affecting active development:
- JWT secret and researcher passwords are hard-coded fallbacks / plaintext — must be replaced before any non-local deployment.
- No automated tests; `test/` contains only shell scripts for manual API checks.
- Most producer/consumer logging still uses `print()`.
