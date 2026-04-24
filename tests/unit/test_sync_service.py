"""
SyncService unit tests — no real QuestDB or filesystem writes.
"""
from pathlib import Path
from unittest.mock import MagicMock

from services.sync_service import SyncService


def _make_service(tmp_path: Path, records=None, count=0) -> SyncService:
    db = MagicMock()
    db.table_exists.return_value = True
    db.get_record_count.return_value = count
    db.query.return_value = records or []

    return SyncService(
        questdb=db,
        table_name="raw_h2_data",
        sync_dir=tmp_path / "synced_data",
        schema_columns=["H2_001PT", "FC_STATE"],
    )


def test_check_no_new_data(tmp_path):
    svc = _make_service(tmp_path, count=0)
    result = svc.check_for_new_data()
    assert not result["has_new_data"]


def test_check_has_new_data(tmp_path):
    svc = _make_service(tmp_path, count=75)
    result = svc.check_for_new_data()
    assert result["has_new_data"]
    assert result["new_count"] == 75


def test_sync_batch_waiting_below_threshold(tmp_path):
    svc = _make_service(tmp_path, count=10)
    result = svc.sync_batch(batch_size=50)
    assert result["status"] == "waiting"


def test_sync_batch_success(tmp_path):
    records = [
        {"timestamp": "2026-01-01T00:00:00Z", "H2_001PT": 1.0},
        {"timestamp": "2026-01-01T00:00:01Z", "H2_001PT": 1.1},
    ]
    svc = _make_service(tmp_path, records=records, count=50)
    result = svc.sync_batch(batch_size=50)
    assert result["status"] == "success"
    assert result["records_synced"] == 2


def test_researcher_state_roundtrip(tmp_path):
    svc = _make_service(tmp_path)
    state = svc.get_researcher_state("APSU_dr_smith")
    assert state["researcher_id"] == "APSU_dr_smith"
    assert state["total_records_synced"] == 0

    svc._update_researcher_state(
        "APSU_dr_smith", "dr_smith", "APSU", "pressure",
        {"end_offset": 10, "record_count": 10}
    )
    updated = svc.get_researcher_state("APSU_dr_smith")
    assert updated["total_records_synced"] == 10


def test_build_access_filter_all():
    f = SyncService._build_access_filter(["all"], None)
    assert f == "1=1"


def test_build_access_filter_specific():
    f = SyncService._build_access_filter(["pressure", "flow"], None)
    assert "pressure" in f
    assert "flow" in f


def test_build_access_filter_with_data_type():
    f = SyncService._build_access_filter(["all"], "pressure")
    assert "pressure" in f
