import json
import uuid
from datetime import datetime, timezone
from typing import Iterable, List, Optional

import boto3


class S3RawWriter:
    

    def __init__(self, bucket: str, prefix: str, region: str):
        if not bucket or not prefix or not region:
            raise RuntimeError("Missing bucket/prefix/region for S3RawWriter")

        self.bucket = bucket
        self.prefix = prefix.rstrip("/")
        self.s3 = boto3.client("s3", region_name=region)

    def _parse_created_at(self, event: dict) -> Optional[datetime]:
        value = event.get("created_at")
        if not value:
            return None
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return dt.astimezone(timezone.utc)
        except Exception:
            return None

    def _build_partition_key(self, state: str, created_at: datetime) -> str:
        dt = created_at.strftime("%Y-%m-%d")
        hour = created_at.strftime("%H")
        ts = created_at.strftime("%Y%m%dT%H%M%S")
        uid = uuid.uuid4().hex[:8]

        return (
            f"{self.prefix}/raw/{state}/"
            f"dt={dt}/hour={hour}/"
            f"part-{ts}-{uid}.jsonl"
        )

    def write_pending(self, events: Iterable[dict]) -> str:
        event_list = list(events)
        now = datetime.now(timezone.utc)

        created_at = self._parse_created_at(event_list[0]) or now
        key = self._build_partition_key("pending", created_at)
        body = self._to_jsonl(event_list)

        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=body,
            ContentType="application/json",
        )
        return key

    def promote_to_success(self, pending_key: str, created_at: datetime) -> str:
        success_key = self._build_partition_key("success", created_at)

        self.s3.copy_object(
            Bucket=self.bucket,
            CopySource={"Bucket": self.bucket, "Key": pending_key},
            Key=success_key,
        )
        self.s3.delete_object(Bucket=self.bucket, Key=pending_key)

        return success_key

    def move_to_failed(self, pending_key: str, created_at: datetime, reason: Optional[str] = None) -> str:
        failed_key = self._build_partition_key("failed", created_at)

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
