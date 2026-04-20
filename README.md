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
├── main.py                          # FastAPI app entrypoint
├── docker-compose.yml               # Full stack orchestration
├── kafka_producer/
│   └── kafka_producer_service.py    # Telemetry producer
├── kafka_consumer/
│   └── kafka_consumer_to_tsdb.py    # Consumer + quality checks + QuestDB writes
├── ingestion/                       # Generator, transforms, TSDB utilities
├── routers/                         # API routes (auth, researcher sync, auto-sync)
├── synchronization/                 # Background sync workers/helpers
├── auth_service/                    # JWT + registry models/config
├── test/                            # Shell test scripts
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
docker logs -f kafka_producer
docker logs -f kafka_consumer
docker logs -f fastapi_app
```

## Local run (without Compose, optional)

If you want to run components manually:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Then start your dependencies (Kafka + QuestDB), and run:

```bash
uvicorn main:app --reload
python3 kafka_producer/kafka_producer_service.py
python3 kafka_consumer/kafka_consumer_to_tsdb.py
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

[![Creative Commons License](https://licensebuttons.net/l/by-nc-sa/4.0/88x31.png)](http://creativecommons.org/licenses/by-nc-sa/4.0/)

This work is licensed under a [Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License](http://creativecommons.org/licenses/by-nc-sa/4.0/).
