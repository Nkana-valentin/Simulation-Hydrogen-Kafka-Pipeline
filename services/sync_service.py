"""
Sync orchestration: query QuestDB → paginate → save to filesystem → update state.
Handles both researcher-scoped sync and system-wide auto-sync.
"""
import csv
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import structlog

from infrastructure.questdb.client import QuestDBClient

logger = structlog.get_logger(__name__)


class SyncService:
    def __init__(
        self,
        questdb: QuestDBClient,
        table_name: str,
        sync_dir: Path,
        schema_columns: List[str],
    ) -> None:
        self._db = questdb
        self._table = table_name
        self._sync_dir = sync_dir
        self._state_dir = sync_dir / "sync_state"
        self._last_sync_file = sync_dir / "last_sync.txt"
        self._columns = ["timestamp", *schema_columns]
        self._sync_dir.mkdir(parents=True, exist_ok=True)
        self._state_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Auto-sync (system-wide)
    # ------------------------------------------------------------------

    def check_for_new_data(self) -> Dict[str, Any]:
        if not self._db.table_exists(self._table):
            return {"has_new_data": False, "message": "Table does not exist"}

        last_ts = self.get_last_sync_timestamp()
        where = f"timestamp > '{last_ts}'" if last_ts else ""
        count = self._db.get_record_count(self._table, where)

        if count and count > 0:
            return {"has_new_data": True, "new_count": count, "last_timestamp": last_ts}
        return {"has_new_data": False, "new_count": 0, "last_timestamp": last_ts}

    def sync_batch(self, batch_size: int) -> Dict[str, Any]:
        check = self.check_for_new_data()
        if not check["has_new_data"]:
            return {"status": "no_new_data"}

        new_count = check["new_count"]
        if new_count < batch_size:
            return {"status": "waiting", "new_count": new_count, "batch_size": batch_size}

        last_ts = check.get("last_timestamp")
        sql = self._db.build_incremental_query(
            self._table, self._columns, last_ts, batch_size
        )
        records = self._db.query(sql) or []
        if not records:
            return {"status": "error", "message": "Count mismatch — no records returned"}

        save_result = self._save_system_batch(records)
        if save_result["status"] == "success" and records:
            self.save_last_sync_timestamp(records[-1]["timestamp"])

        return {
            "status": "success",
            "records_synced": len(records),
            "remaining": new_count - len(records),
            "time_range": {
                "start": records[0].get("timestamp"),
                "end": records[-1].get("timestamp"),
            },
            "local_save": save_result,
        }

    def get_last_sync_timestamp(self) -> Optional[str]:
        if self._last_sync_file.exists():
            ts = self._last_sync_file.read_text().strip()
            return ts or None
        return None

    def save_last_sync_timestamp(self, timestamp: str) -> None:
        self._last_sync_file.write_text(timestamp)

    # ------------------------------------------------------------------
    # Researcher-scoped sync
    # ------------------------------------------------------------------

    def fetch_researcher_batch(
        self,
        researcher_id: str,
        data_access: List[str],
        data_type: Optional[str],
        batch_size: int,) -> Dict[str, Any]:
        state = self.get_researcher_state(researcher_id, data_type)
        offset = state.get("last_offset", 0)

        access_filter = self._build_access_filter(data_access, data_type)
        total_sql = f"SELECT COUNT(*) as count FROM {self._table} WHERE {access_filter}"
        count_rows = self._db.query(total_sql) or []
        total = count_rows[0].get("count", 0) if count_rows else 0

        data_sql = (
            f"SELECT * FROM {self._table} WHERE {access_filter} "
            f"ORDER BY timestamp ASC LIMIT {batch_size} OFFSET {offset}"
        )
        records = self._db.query(data_sql) or []

        return {
            "records": records,
            "total_count": total,
            "current_offset": offset,
            "next_offset": offset + len(records) if len(records) == batch_size else None,
            "has_more": (offset + len(records)) < total,
            "record_count": len(records),
            "latest_timestamp": records[-1].get("timestamp") if records else None,
            "data_type": data_type or "all_authorized",
        }

    def process_researcher_batch(
        self,
        researcher: Dict[str, Any],
        batch_result: Dict[str, Any],
        batch_size: int,
        data_type: Optional[str],
    ) -> Dict[str, Any]:
        researcher_id = researcher["user_id"]
        records = batch_result["records"]
        total = batch_result.get("total_count", 0)

        full_state = self.get_researcher_state(researcher_id)
        data_type_key = batch_result.get("data_type") or "all_authorized"
        dt_state = full_state.get("data_types", {}).get(data_type_key, {})
        batch_number = dt_state.get("batches_completed", 0) + 1
        total_batches = math.ceil(total / batch_size) if total > 0 else 0

        batch_info = {
            "data_type": data_type_key,
            "start_offset": batch_result.get("current_offset", 0),
            "end_offset": batch_result.get("current_offset", 0) + len(records),
            "record_count": len(records),
            "latest_timestamp": batch_result.get("latest_timestamp"),
            "batch_number": batch_number,
            "total_batches": total_batches,
        }

        save_result = self._save_researcher_batch(records, researcher, batch_info)
        self._update_researcher_state(
            researcher_id,
            researcher.get("username", ""),
            researcher.get("institution", ""),
            data_type_key,
            batch_info,
        )

        next_offset = batch_result.get("next_offset")
        has_more = batch_result.get("has_more", False)
        pct = round(next_offset / total * 100, 2) if next_offset and total else 100.0

        return {
            "status": "success",
            "researcher": {
                "id": researcher_id,
                "name": researcher.get("username"),
                "institution": researcher.get("institution"),
            },
            "data_type": data_type_key,
            "batch_info": {
                "batch_number": batch_number,
                "total_batches": total_batches,
                "records_in_batch": len(records),
            },
            "progress": {
                "percent_complete": pct,
                "has_more": has_more,
                "total_available": total,
            },
            "save_location": save_result.get("file"),
        }

    # ------------------------------------------------------------------
    # Researcher state management
    # ------------------------------------------------------------------

    def get_researcher_state(
        self, researcher_id: str, data_type: Optional[str] = None
    ) -> Dict[str, Any]:
        path = self._researcher_state_file(researcher_id)
        if not path.exists():
            return {"researcher_id": researcher_id, "data_types": {}, "total_records_synced": 0}
        try:
            state = json.loads(path.read_text())
            if data_type:
                dt_state = state.get("data_types", {}).get(data_type, {})
                return {
                    "researcher_id": researcher_id,
                    "data_type": data_type,
                    "last_offset": dt_state.get("last_offset", 0),
                    "records_synced": dt_state.get("records_synced", 0),
                }
            return state
        except Exception as exc:
            logger.error("researcher_state_read_failed", researcher_id=researcher_id, error=str(exc))
            return {"researcher_id": researcher_id, "data_types": {}}

    def _update_researcher_state(
        self,
        researcher_id: str,
        name: str,
        institution: str,
        data_type: str,
        batch_info: Dict[str, Any],
    ) -> None:
        path = self._researcher_state_file(researcher_id)
        state = json.loads(path.read_text()) if path.exists() else {
            "researcher_id": researcher_id,
            "name": name,
            "institution": institution,
            "data_types": {},
            "total_records_synced": 0,
            "batches": [],
        }
        dt = state["data_types"].setdefault(data_type, {
            "last_offset": 0,
            "records_synced": 0,
            "batches_completed": 0,
        })
        dt["last_offset"] = batch_info.get("end_offset", dt["last_offset"])
        dt["records_synced"] += batch_info.get("record_count", 0)
        dt["batches_completed"] += 1
        state["total_records_synced"] += batch_info.get("record_count", 0)
        state["last_sync_time"] = datetime.now().isoformat()
        state.setdefault("batches", []).append(batch_info)
        state["batches"] = state["batches"][-10:]
        path.write_text(json.dumps(state, indent=2, default=str))

    def _researcher_state_file(self, researcher_id: str) -> Path:
        safe = researcher_id.replace("@", "_at_").replace(".", "_dot_")
        return self._state_dir / f"{safe}_sync_state.json"

    # ------------------------------------------------------------------
    # Filesystem save helpers
    # ------------------------------------------------------------------

    def _save_system_batch(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_dir = self._sync_dir / f"sync_{ts}"
        out_dir.mkdir()
        self._write_json(out_dir / "data.json", {"records": records, "record_count": len(records)})
        self._write_csv(out_dir / "data.csv", records)
        return {"status": "success", "directory": str(out_dir), "record_count": len(records)}

    def _save_researcher_batch(
        self,
        records: List[Dict[str, Any]],
        researcher: Dict[str, Any],
        batch_info: Dict[str, Any],
    ) -> Dict[str, Any]:
        if not records:
            return {"status": "no_data"}
        inst = researcher.get("institution", "unknown")
        name = researcher.get("username", "unknown")
        data_type = batch_info.get("data_type", "unknown")
        out_dir = self._sync_dir / f"{inst}_{name}" / data_type
        out_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        start = batch_info.get("start_offset", 0)
        end = batch_info.get("end_offset", 0)
        base = out_dir / f"batch_{ts}_offsets_{start}_{end}"
        self._write_json(base.with_suffix(".json"), {"batch_info": batch_info, "records": records})
        self._write_csv(base.with_suffix(".csv"), records)
        return {"status": "success", "file": str(base.with_suffix(".json"))}

    @staticmethod
    def _write_json(path: Path, data: Any) -> None:
        path.write_text(json.dumps(data, indent=2, default=str))

    @staticmethod
    def _write_csv(path: Path, records: List[Dict[str, Any]]) -> None:
        if not records:
            return
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=records[0].keys())
            writer.writeheader()
            writer.writerows(records)

    @staticmethod
    def _build_access_filter(data_access: List[str], data_type: Optional[str]) -> str:
        if data_type:
            return f"measurement_type = '{data_type}'"
        if not data_access or "all" in data_access:
            return "1=1"
        conditions = [f"measurement_type = '{a}'" for a in data_access]
        return "(" + " OR ".join(conditions) + ")"
