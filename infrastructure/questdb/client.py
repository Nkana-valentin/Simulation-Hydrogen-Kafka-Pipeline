"""
QuestDB HTTP REST client.
All DDL and DML go through /exec; batch line-protocol inserts go through /write.
"""
import math
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
import structlog

logger = structlog.get_logger(__name__)


class QuestDBClient:
    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self._exec_url = f"http://{host}:{port}/exec"
        self._write_url = f"http://{host}:{port}/write"

    # ------------------------------------------------------------------
    # Table management
    # ------------------------------------------------------------------

    def table_exists(self, table_name: str) -> bool:
        result = self._exec(f"SELECT * FROM tables() WHERE table_name = '{table_name}'")
        return bool(result and result.get("count", 0) > 0)

    def create_table(self, table_name: str, schema: Dict[str, Any]) -> bool:
        if self.table_exists(table_name):
            logger.info("questdb_table_exists", table=table_name)
            return True

        tag_types = schema.get("tag_types", {})
        field_types = schema.get("field_types", {})

        cols = ["timestamp TIMESTAMP"]
        for tag in schema.get("tags", []):
            cols.append(f"{tag} {tag_types.get(tag, 'STRING')}")
        for field in schema.get("fields", []):
            cols.append(f"{field} {field_types.get(field, 'FLOAT')}")

        ddl = (
            f"CREATE TABLE {table_name} ({', '.join(cols)}) "
            f"TIMESTAMP(timestamp) PARTITION BY DAY"
        )
        result = self._exec(ddl)
        ok = result is not None and "error" not in result
        if ok:
            logger.info("questdb_table_created", table=table_name)
        return ok

    def drop_table(self, table_name: str, *, confirm: bool = False) -> bool:
        if not confirm:
            logger.warning("drop_table requires confirm=True")
            return False
        if not self.table_exists(table_name):
            return True
        result = self._exec(f"DROP TABLE {table_name}")
        return result is not None and "error" not in result

    # ------------------------------------------------------------------
    # Data insertion
    # ------------------------------------------------------------------

    def insert_row(self, table_name: str, row: Dict[str, Any]) -> bool:
        cols, vals = [], []
        for col, val in row.items():
            if val is None:
                continue
            if isinstance(val, (int, float)):
                if not math.isfinite(float(val)):
                    continue
                cols.append(col)
                vals.append(str(val))
            elif isinstance(val, datetime):
                cols.append(col)
                vals.append(f"'{val.isoformat()}'")
            else:
                cols.append(col)
                vals.append(f"'{str(val).replace(chr(39), chr(39)*2)}'")

        if not cols:
            return False

        sql = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES ({', '.join(vals)})"
        result = self._exec(sql)
        return result is not None and "error" not in result

    # ------------------------------------------------------------------
    # Querying
    # ------------------------------------------------------------------

    def query(self, sql: str) -> Optional[List[Dict[str, Any]]]:
        result = self._exec(sql)
        if result is None:
            return None
        columns = [c["name"] for c in result.get("columns", [])]
        return [
            dict(zip(columns, row))
            for row in result.get("dataset", [])
        ]

    def get_latest_timestamp(self, table_name: str) -> Optional[str]:
        rows = self.query(f"SELECT MAX(timestamp) as latest FROM {table_name}")
        if rows:
            return rows[0].get("latest")
        return None

    def get_record_count(self, table_name: str, where: str = "") -> Optional[int]:
        clause = f"WHERE {where}" if where else ""
        rows = self.query(f"SELECT COUNT(*) as count FROM {table_name} {clause}")
        if rows:
            return rows[0].get("count")
        return None

    def build_incremental_query(
        self,
        table_name: str,
        columns: List[str],
        last_timestamp: Optional[str] = None,
        batch_size: int = 1000,
    ) -> str:
        col_str = ", ".join(columns) if columns else "*"
        where = f"WHERE timestamp > '{last_timestamp}'" if last_timestamp else ""
        return (
            f"SELECT {col_str} FROM {table_name} {where} "
            f"ORDER BY timestamp ASC LIMIT {batch_size}"
        )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _exec(self, sql: str, timeout: int = 15) -> Optional[Dict[str, Any]]:
        try:
            resp = requests.get(self._exec_url, params={"query": sql}, timeout=timeout)
            if resp.status_code == 200:
                data = resp.json()
                if "error" in data:
                    logger.error("questdb_exec_error", error=data["error"], sql=sql[:200])
                return data
            logger.error("questdb_http_error", status=resp.status_code, body=resp.text[:200])
            return None
        except Exception as exc:
            logger.error("questdb_request_failed", error=str(exc))
            return None
