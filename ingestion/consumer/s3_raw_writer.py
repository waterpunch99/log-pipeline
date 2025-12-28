# ingestion/consumer/s3_raw_writer.py
import json
import uuid
from datetime import datetime, timezone
from typing import Iterable, List, Optional, Tuple

import boto3


class S3RawWriter:
    def __init__(self, bucket: str, prefix: str, region: str):
        if not bucket or not prefix or not region:
            raise RuntimeError("Missing bucket/prefix/region for S3RawWriter")

        self.bucket = bucket
        self.prefix = prefix.rstrip("/")
        self.s3 = boto3.client("s3", region_name=region)

    @staticmethod
    def _utcnow() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _parse_created_at(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return None

    def partition_from_event(self, event: dict) -> Tuple[str, str, datetime]:
        """
        이벤트 1건에서 파티션(dt/hour)을 계산합니다.
        created_at 파싱 실패 시 현재 UTC 시각을 사용합니다.
        """
        created_at = self._parse_created_at(event.get("created_at")) or self._utcnow()
        dt = created_at.strftime("%Y-%m-%d")
        hour = created_at.strftime("%H")
        return dt, hour, created_at

    def _build_partition_key(self, state: str, dt: str, hour: str) -> str:
        ts = self._utcnow().strftime("%Y%m%dT%H%M%S")
        uid = uuid.uuid4().hex[:8]
        return (
            f"{self.prefix}/raw/{state}/"
            f"dt={dt}/hour={hour}/"
            f"part-{ts}-{uid}.jsonl"
        )

    def write_pending(self, events: Iterable[dict], dt: str, hour: str) -> str:
        event_list = list(events)
        if not event_list:
            raise ValueError("write_pending() got empty events")

        key = self._build_partition_key("pending", dt, hour)
        body = self._to_jsonl(event_list)

        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=body,
            ContentType="application/json",
        )
        return key

    def promote_to_success(self, pending_key: str, dt: str, hour: str) -> str:
        success_key = self._build_partition_key("success", dt, hour)

        self.s3.copy_object(
            Bucket=self.bucket,
            CopySource={"Bucket": self.bucket, "Key": pending_key},
            Key=success_key,
        )
        self.s3.delete_object(Bucket=self.bucket, Key=pending_key)

        return success_key

    def move_to_failed(self, pending_key: str, dt: str, hour: str, reason: Optional[str] = None) -> str:
        failed_key = self._build_partition_key("failed", dt, hour)

        self.s3.copy_object(
            Bucket=self.bucket,
            CopySource={"Bucket": self.bucket, "Key": pending_key},
            Key=failed_key,
        )
        self.s3.delete_object(Bucket=self.bucket, Key=pending_key)

        if reason:
            self.s3.put_object_tagging(
                Bucket=self.bucket,
                Key=failed_key,
                Tagging={
                    "TagSet": [
                        {"Key": "status", "Value": "failed"},
                        {"Key": "reason", "Value": reason[:250]},
                    ]
                },
            )

        return failed_key

    @staticmethod
    def _to_jsonl(events: List[dict]) -> bytes:
        lines: List[str] = []
        for e in events:
            try:
                lines.append(json.dumps(e, ensure_ascii=False))
            except (TypeError, ValueError):
                continue
        return ("\n".join(lines) + "\n").encode("utf-8")
