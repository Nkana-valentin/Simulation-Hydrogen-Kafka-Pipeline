"""
Unit tests for dead-letter routing in IngestionService.
No real Kafka or QuestDB needed — everything is mocked.
"""
from unittest.mock import MagicMock

from services.ingestion_service import IngestionService

SECRET = "test-secret-do-not-use-in-prod-ok"
ALGO = "HS256"

_SCHEMA = {
    "tags": [],
    "fields": ["H2_001PT"],
    "tag_types": {},
    "field_types": {"H2_001PT": "FLOAT"},
}


def _make_service():
    kafka = MagicMock()
    db = MagicMock()
    db.create_table.return_value = True
    db.insert_row.return_value = True
    svc = IngestionService(
        kafka=kafka,
        questdb=db,
        table_name="raw_h2_data",
        schema=_SCHEMA,
        jwt_secret=SECRET,
        jwt_algorithm=ALGO,
    )
    return svc, db


def _record_with_auth(token="bad-token", device_id="dev_01"):
    return {
        "timestamp": "2026-01-01T00:00:00.000Z",
        "H2_001PT": 1.5,
        "auth": {"token": token, "device_id": device_id},
    }


def test_auth_failure_writes_to_dead_letter():
    svc, db = _make_service()
    svc._process(_record_with_auth(token="invalid"))

    # insert_row must have been called once — for the dead-letter table
    assert db.insert_row.call_count == 1
    table, row = db.insert_row.call_args[0]
    assert table == "raw_h2_data_dead_letter"
    assert row["failure_category"] == "auth"
    assert "token" not in row  # JWT must not be stored


def test_auth_failure_does_not_write_to_main_table():
    svc, db = _make_service()
    svc._process(_record_with_auth(token="invalid"))

    tables_written = [c[0][0] for c in db.insert_row.call_args_list]
    assert "raw_h2_data" not in tables_written


def test_validation_failure_writes_to_dead_letter():
    svc, db = _make_service()
    # Auth is skipped for this test — inject a pre-verified record
    # by calling _write_dead_letter directly
    svc._write_dead_letter(
        {"timestamp": "2026-01-01T00:00:00.000Z", "H2_001PT": float("nan")},
        "validation",
        "H2_001PT contains NaN",
    )

    assert db.insert_row.call_count == 1
    table, row = db.insert_row.call_args[0]
    assert table == "raw_h2_data_dead_letter"
    assert row["failure_category"] == "validation"
    assert "NaN" in row["failure_reason"]


def test_dead_letter_strips_jwt_token():
    svc, db = _make_service()
    svc._write_dead_letter(
        {"timestamp": "2026-01-01T00:00:00Z", "auth": {"token": "secret-jwt", "device_id": "x"}},
        "auth",
        "invalid_token",
    )

    _, row = db.insert_row.call_args[0]
    assert "secret-jwt" not in row.get("raw_payload", "")
    assert row["device_id"] == "x"


def test_setup_creates_dead_letter_table():
    svc, db = _make_service()
    svc.setup()

    created_tables = [c[0][0] for c in db.create_table.call_args_list]
    assert "raw_h2_data_dead_letter" in created_tables
