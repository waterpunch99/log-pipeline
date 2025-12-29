# batch/compact_raw.py
import argparse
import hashlib
import json
import os
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

import boto3
from dotenv import load_dotenv

ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=ENV_PATH)

DEFAULT_REGION = os.getenv("AWS_REGION") or "ap-northeast-2"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _s3_client():
    return boto3.client("s3", region_name=DEFAULT_REGION)


def _extract_hour_from_key(key: str) -> Optional[str]:
    token = "/hour="
    if token not in key:
        return None
    try:
        tail = key.split(token, 1)[1]
        hour = tail.split("/", 1)[0]
        if len(hour) == 2 and hour.isdigit():
            return hour
        return None
    except Exception:
        return None


def _read_all_objects(bucket: str, prefix: str) -> List[dict]:
    s3 = _s3_client()
    token: Optional[str] = None
    out: List[dict] = []

    while True:
        args = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
        if token:
            args["ContinuationToken"] = token

        resp = s3.list_objects_v2(**args)

        for obj in resp.get("Contents", []):
            out.append(obj)

        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")

    return out


def list_raw_success_objects(bucket: str, prefix: str, dt: str) -> List[dict]:
    base_prefix = f"{prefix.rstrip('/')}/raw/success/dt={dt}/"
    objs = _read_all_objects(bucket, base_prefix)
    out: List[dict] = []
    for o in objs:
        key = o.get("Key", "")
        if key.endswith(".jsonl"):
            out.append(o)
    out.sort(key=lambda x: x.get("Key", ""))
    return out


def group_objects_by_hour(objs: List[dict]) -> Dict[str, List[dict]]:
    grouped: Dict[str, List[dict]] = {}
    for o in objs:
        key = o.get("Key", "")
        hour = _extract_hour_from_key(key)
        if not hour:
            continue
        grouped.setdefault(hour, []).append(o)

    for hour in grouped:
        grouped[hour].sort(key=lambda x: x.get("Key", ""))
    return grouped


def _manifest_key(prefix: str, dt: str, hour: str) -> str:
    return f"{prefix.rstrip('/')}/raw_compacted/_manifest/dt={dt}/hour={hour}/manifest.json"


def _build_output_key(prefix: str, dt: str, hour: str, digest: str) -> str:
    short = digest[:16]
    return (
        f"{prefix.rstrip('/')}/raw_compacted/success/"
        f"dt={dt}/hour={hour}/"
        f"compacted-{dt}-{hour}-{short}.jsonl"
    )


def _compute_digest(input_objs: List[dict]) -> str:
    h = hashlib.sha256()
    for o in input_objs:
        key = (o.get("Key") or "").encode("utf-8")
        etag = (o.get("ETag") or "").encode("utf-8")  # 보통 "..." 포함
        size = str(o.get("Size") or 0).encode("utf-8")
        lm = str(o.get("LastModified") or "").encode("utf-8")
        h.update(key)
        h.update(b"|")
        h.update(etag)
        h.update(b"|")
        h.update(size)
        h.update(b"|")
        h.update(lm)
        h.update(b"\n")
    return h.hexdigest()


def _s3_get_json(bucket: str, key: str) -> Optional[dict]:
    s3 = _s3_client()
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        raw = obj["Body"].read()
        return json.loads(raw.decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        return None
    except Exception:
        return None


def _s3_put_json(bucket: str, key: str, payload: dict) -> None:
    s3 = _s3_client()
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
    )


def _s3_head_exists(bucket: str, key: str) -> bool:
    s3 = _s3_client()
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


def _read_jsonl_lines(bucket: str, key: str) -> Iterator[bytes]:
    s3 = _s3_client()
    obj = s3.get_object(Bucket=bucket, Key=key)
    for line in obj["Body"].iter_lines():
        if line:
            yield line


def _upload_file(bucket: str, key: str, local_path: str) -> None:
    s3 = _s3_client()
    s3.upload_file(local_path, bucket, key)


def compact_one_hour_if_changed(bucket: str, prefix: str, dt: str, hour: str, input_objs: List[dict]) -> Tuple[str, str, int, bool]:
    """
    반환:
      (digest, output_key, merged_line_count, did_compact)
    """
    if not input_objs:
        raise ValueError("compact_one_hour_if_changed got empty input_objs")

    digest = _compute_digest(input_objs)
    manifest_k = _manifest_key(prefix, dt, hour)
    output_k = _build_output_key(prefix, dt, hour, digest)

    old = _s3_get_json(bucket, manifest_k)

    # 이전 digest와 동일하고, 출력 파일도 존재하면 스킵
    if old and old.get("digest") == digest and old.get("output_key") == output_k and _s3_head_exists(bucket, output_k):
        return digest, output_k, int(old.get("merged_lines") or 0), False

    merged = 0
    tmp_path = None
    pending_key = f"{prefix.rstrip('/')}/raw_compacted/pending/dt={dt}/hour={hour}/pending-{uuid.uuid4().hex}.jsonl"

    try:
        with tempfile.NamedTemporaryFile(prefix=f"compact-{dt}-{hour}-", suffix=".jsonl", delete=False) as fp:
            tmp_path = fp.name
            for o in input_objs:
                k = o.get("Key", "")
                for line in _read_jsonl_lines(bucket, k):
                    fp.write(line)
                    fp.write(b"\n")
                    merged += 1

        # pending 업로드 -> success로 copy(원자성 느낌) -> pending 삭제
        _upload_file(bucket, pending_key, tmp_path)

        s3 = _s3_client()
        s3.copy_object(
            Bucket=bucket,
            CopySource={"Bucket": bucket, "Key": pending_key},
            Key=output_k,
            ContentType="application/json",
        )
        s3.delete_object(Bucket=bucket, Key=pending_key)

        manifest = {
            "dt": dt,
            "hour": hour,
            "digest": digest,
            "output_key": output_k,
            "input_files": [
                {
                    "key": o.get("Key"),
                    "etag": o.get("ETag"),
                    "size": o.get("Size"),
                    "last_modified": str(o.get("LastModified")),
                }
                for o in input_objs
            ],
            "input_file_count": len(input_objs),
            "merged_lines": merged,
            "updated_at": _utcnow().isoformat().replace("+00:00", "Z"),
        }
        _s3_put_json(bucket, manifest_k, manifest)

        return digest, output_k, merged, True

    finally:
        if tmp_path:
            try:
                os.remove(tmp_path)
            except Exception:
                pass


def run(dt: str) -> None:
    bucket = os.getenv("S3_BUCKET")
    prefix = os.getenv("S3_PREFIX")
    if not bucket or not prefix:
        raise RuntimeError("Missing env: S3_BUCKET / S3_PREFIX")

    raw_objs = list_raw_success_objects(bucket, prefix, dt)
    if not raw_objs:
        print(f"[OK] compact_raw dt={dt} no raw success objects")
        return

    grouped = group_objects_by_hour(raw_objs)
    if not grouped:
        print(f"[OK] compact_raw dt={dt} no hour partitions")
        return

    total_hours = 0
    total_inputs = 0
    total_merged = 0
    total_compacted_hours = 0
    total_skipped_hours = 0

    for hour in sorted(grouped.keys()):
        input_objs = grouped[hour]
        total_hours += 1
        total_inputs += len(input_objs)

        try:
            digest, out_key, merged, did = compact_one_hour_if_changed(bucket, prefix, dt, hour, input_objs)
            total_merged += merged
            if did:
                total_compacted_hours += 1
                print(
                    f"[OK] compact_raw dt={dt} hour={hour} digest={digest[:12]} inputs={len(input_objs)} merged_lines={merged} out={out_key}"
                )
            else:
                total_skipped_hours += 1
                print(
                    f"[SKIP] compact_raw dt={dt} hour={hour} digest={digest[:12]} inputs={len(input_objs)} out={out_key}"
                )
        except Exception as e:
            print(f"[FAIL] compact_raw dt={dt} hour={hour} inputs={len(input_objs)} error={e}")

    print(
        f"[OK] compact_raw dt={dt} hours={total_hours} compacted_hours={total_compacted_hours} skipped_hours={total_skipped_hours} input_files={total_inputs} merged_lines={total_merged}"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dt", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()
    run(args.dt)


if __name__ == "__main__":
    main()
