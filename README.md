# Simulation Hydrogen Kafka Pipeline

A containerized, real-time data pipeline that simulates hydrogen lab telemetry, streams it through Kafka, validates it, stores it in QuestDB, and exposes authenticated sync APIs via FastAPI.

<p align="center">
  <img src="images/github_internship_image_cropped.jpg" width="900" alt="Hydrogen pipeline architecture"/>
</p>

## What this project does

This repository models a **Physical Twin → Digital Twin** workflow:

1. A Kafka producer generates lab sensor telemetry.
2. A Kafka consumer validates/auth-checks records and writes them to QuestDB.
3. A FastAPI app exposes:
   - JWT auth endpoints for devices/researchers
   - researcher-scoped sync endpoints
   - admin auto-sync endpoints (including optional remote SSH sync)

## Tech stack

- **FastAPI** (API layer)
- **Apache Kafka + Zookeeper** (streaming backbone)
- **QuestDB** (time-series storage)
- **Docker Compose** (orchestration)
- **JWT auth** (device/researcher/admin access control)

## Repository layout

```text
.
├── docker-compose.yml               # Full stack orchestration
├── Dockerfile                       # FastAPI app image
├── docker/
│   ├── Dockerfile.producer          # Producer image
│   └── Dockerfile.consumer          # Consumer image
├── TSDB.yml                         # Table schema — single source of truth
│
├── config/
│   └── settings.py                  # Pydantic Settings — all env vars
├── domain/
│   ├── telemetry.py                 # TelemetryRecord, AuthBlock models
│   ├── quality.py                   # Pure validation / quality functions
│   └── auth.py                      # Pure JWT functions
├── infrastructure/
│   ├── kafka/{producer,consumer}.py
│   ├── questdb/{client,schema}.py
│   ├── ssh/transfer.py
│   └── registry/
│       ├── repository.py            # Abstract + JsonFile registry implementations
│       └── data/
│           ├── device_registry.json
│           └── researcher_registry.json
├── services/
│   ├── auth_service.py              # Token issuance via registry
│   ├── ingestion_service.py         # consume → validate → store
│   └── sync_service.py              # query → paginate → save → state
├── api/
│   ├── main.py                      # FastAPI app + lifespan
│   ├── dependencies.py              # FastAPI Depends wiring
│   └── routers/
│       ├── auth.py
│       ├── researcher_sync.py
│       └── admin_sync.py
├── workers/
│   └── auto_sync_worker.py          # asyncio background sync task
├── simulator/
│   └── physics_model.py             # generate_physical_state() — no external deps
├── cmd/
│   ├── producer.py                  # Producer entry point
│   └── consumer.py                  # Consumer entry point
├── scripts/
│   └── hash_credentials.py          # bcrypt credential hashing utility
├── tests/
│   └── unit/                        # pytest — no Docker needed
└── diagnostic.sh                    # Runtime diagnostics
```

## Prerequisites

- Docker + Docker Compose
- Python 3.9+ (only if running parts locally)
- `curl` and `python3` for test scripts

## Quick start (recommended: Docker Compose)

### 1) Clone and enter the repo

```bash
git clone https://github.com/Nkana-valentin/Simulation-Hydrogen-Kafka-Pipeline.git
cd Simulation-Hydrogen-Kafka-Pipeline
```

### 2) Create a `.env` file

The app relies on environment variables for Kafka, QuestDB, and sync settings.
Use the following as a practical starter template:

```env
# API
FASTAPI_PORT=8080

# Kafka
KAFKA_BROKER=broker:9092
KAFKA_BOOTSTRAP_SERVERS=broker:9092
KAFKA_TOPIC_RAW=raw_h2_data
KAFKA_TOPIC_VALIDATED=validated_h2_data

# QuestDB
QUESTDB_HOST=questdb
QUESTDB_PORT=9000
QUESTDB_USER=admin
QUESTDB_PASSWORD=quest
QUESTDB_DATABASE=qdb

# Sync worker
BATCH_SIZE=50
SYNC_INTERVAL=30s
LOCAL_SYNC_DIR=./synced_data
REMOTE_SYNC_ENABLED=false

# SSH (required by current validation logic, even if remote sync is disabled)
SSH_HOST=localhost
SSH_USER=user
SSH_REMOTE_PATH=/tmp
SSH_KEY_PATH=/app/.ssh/id_rsa

# Optional URLs
AUTH_SERVICE_URL=http://fastapi_app:8000
```

### 3) Start the platform

```bash
docker compose up -d --build
```

### 4) Verify services

- FastAPI docs: `http://localhost:${FASTAPI_PORT:-8080}/docs`
- QuestDB web UI: `http://localhost:9000`

### 5) Follow logs

```bash
docker logs -f kafka_producer   # telemetry producer
docker logs -f kafka_consumer   # ingestion + quality checks
docker logs -f fastapi_app      # API + sync worker
```

## Local run (without Compose, optional)

If you want to run components manually:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Then start your dependencies (Kafka + QuestDB), and run each in a separate terminal:

```bash
uvicorn api.main:app --reload
python3 -m cmd.producer
python3 -m cmd.consumer
```

## API overview

### Authentication

- `POST /device/login` — device JWT
- `POST /researcher/login` — researcher JWT
- `POST /verify` — token verification
- `GET /health` — auth health summary

### Researcher sync

- `POST /sync/trigger` — sync one batch for allowed data types
- `GET /sync/status` — sync state + available data
- `POST /sync/complete` — sync until exhausted

### Auto-sync to Hydor (admin)

- `GET /status` — worker + pending data status
- `POST /trigger` — trigger auto-sync batch
- `POST /remote-sync/test-ssh` — SSH connectivity/auth check
- `GET /summary` — synced output summary

## Data flow

1. **Producer** emits JSON telemetry (sensor value + metadata + auth block).
2. **Consumer** reads Kafka events, validates required fields/ranges, and tags quality.
3. **QuestDB** stores records in a topic-named table (default: `raw_h2_data`).
4. **Sync services** paginate/query QuestDB and export data to filesystem/remote target.

### Example Kafka message

The producer (`cmd/producer.py`) builds this message from `simulator/physics_model.py` and injects the device JWT before publishing:

```json
{
  "timestamp": "2026-04-23T14:22:01.123456Z",
  "FC_STATE":   100.0,
  "H2_001FT":   2.34,
  "H2_001PT":   0.12,
  "H2_002PT":   0.10,
  "H2_003PT":   0.09,
  "H2_005PT":   1.87,
  "CA_001FC":   0.03,
  "H2_001TT":   13.84,
  "H2_002TT":   12.51,
  "H2_003TT":   12.00,
  "H2_005TT":   11.48,
  "FC_STACK_V": 1.78,
  "FC_STACK_i": 8.51,
  "auth": {
    "token":     "<device JWT>",
    "device_id": "simulation_device_01"
  }
}
```

The consumer (`cmd/consumer.py`) verifies the JWT and overwrites the `auth` block with the claims extracted from the token (device_id, lab). Only the fields listed in `TSDB.yml` are written to QuestDB — the `auth` block is never stored.

## Automatic sync

The background sync worker (`workers/auto_sync_worker.py`) starts automatically with the FastAPI app (wired in `api/main.py` via the lifespan hook). It runs on a configurable interval and saves batches locally. Remote SSH transfer to the ORFEO-Hydor platform is optional.

**Relevant `.env` variables:**

| Variable | Default | Purpose |
|---|---|---|
| `SYNC_INTERVAL` | `30` | Poll interval in seconds (also accepts `30s`, `2m`) |
| `BATCH_SIZE` | `50` | Minimum new records before a batch is written |
| `LOCAL_SYNC_DIR` | `./synced_data` | Local output directory for sync batches |
| `REMOTE_SYNC_ENABLED` | `false` | Set to `true` to also push batches over SSH |
| `SSH_HOST` | — | Required when `REMOTE_SYNC_ENABLED=true` |
| `SSH_USER` | — | SSH username |
| `SSH_KEY_PATH` | — | Path to private key inside the container |
| `SSH_REMOTE_PATH` | `/tmp` | Destination path on the remote host |

To enable remote sync, set `REMOTE_SYNC_ENABLED=true` and fill in the `SSH_*` variables in your `.env` before starting the stack. The worker will then upload each completed local batch to the remote target after saving it locally.

## Testing

Unit tests require no external services (no Kafka, QuestDB, or Docker):

```bash
JWT_SECRET=any-dev-secret python -m pytest tests/unit/ -v
```

## Useful scripts

- Full diagnostics:

  ```bash
  bash diagnostic.sh
  ```

- Auth/sync check:

  ```bash
  bash test/test_sync_auth.sh
  ```

- Access-control check:

  ```bash
  bash test/test_access_control.sh
  ```

- Rehash credentials (bcrypt):

  ```bash
  python3 scripts/hash_credentials.py
  ```

## Troubleshooting

- **FastAPI exits on startup with missing env vars**
  - Ensure `KAFKA_BOOTSTRAP_SERVERS` and `SSH_HOST` are set.
- **Producer/consumer cannot reach Kafka**
  - Confirm broker health: `docker ps` and broker logs.
  - Verify broker address is `broker:9092` inside containers.
- **No data in QuestDB**
  - Check consumer logs for authentication/quality errors.
  - Query row count in QuestDB UI or via `/exec` endpoint.

## Contributing

1. Create a branch.
2. Make focused, testable changes.
3. Run diagnostics/scripts relevant to your change.
4. Open a PR with context, setup notes, and test output.

## License

This project is licensed under the terms of the [LICENSE](LICENSE) file.
