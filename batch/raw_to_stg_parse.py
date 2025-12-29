# batch/raw_to_stg_parse.py  (새로 추가)

import json
from datetime import datetime, timezone
from typing import Dict, Optional


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_iso8601_to_utc(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def parse_event(line: bytes, source_key: str, loaded_at: datetime) -> Dict:
    ingested_at = utcnow()

    raw_text = line.decode("utf-8", errors="strict")
    try:
        payload_obj = json.loads(raw_text)
        event_id = payload_obj.get("id")
        created_at = parse_iso8601_to_utc(payload_obj.get("created_at"))

        return {
            "event_id": str(event_id) if event_id is not None else None,
            "payload": raw_text,
            "created_at": created_at,
            "ingested_at": ingested_at,
            "source_key": source_key,
            "loaded_at": loaded_at,
            "is_valid": True,
            "err_msg": None,
        }
    except Exception as e:
        safe_raw = line.decode("utf-8", errors="ignore")
        return {
            "event_id": None,
            "payload": json.dumps({"raw": safe_raw}, ensure_ascii=False),
            "created_at": None,
            "ingested_at": ingested_at,
            "source_key": source_key,
            "loaded_at": loaded_at,
            "is_valid": False,
            "err_msg": str(e),
        }
