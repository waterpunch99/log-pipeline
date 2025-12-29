# batch/raw_to_stg_load.py  (새로 추가)

from dataclasses import dataclass
from typing import Dict, List

import psycopg


@dataclass
class FlushResult:
    inserted: int


class StgLoader:
    def flush(self, cur: "psycopg.Cursor", rows: List[Dict]) -> FlushResult:
        if not rows:
            return FlushResult(inserted=0)

        values_sql = ",".join(["(%s,%s::jsonb,%s,%s,%s,%s,%s,%s)"] * len(rows))
        sql = f"""
        INSERT INTO github_events_stg (
          event_id, payload, created_at, ingested_at, source_key, loaded_at, is_valid, err_msg
        )
        VALUES {values_sql}
        ON CONFLICT DO NOTHING
        RETURNING 1;
        """.strip()

        params: List[object] = []
        for r in rows:
            params.extend(
                [
                    r.get("event_id"),
                    r.get("payload"),
                    r.get("created_at"),
                    r.get("ingested_at"),
                    r.get("source_key"),
                    r.get("loaded_at"),
                    r.get("is_valid"),
                    r.get("err_msg"),
                ]
            )

        cur.execute(sql, params)
        return FlushResult(inserted=len(cur.fetchall()))
