# batch/raw_to_stg_s3.py  (새로 추가)

import json
import os
from typing import Iterable, Iterator, List, Optional

import boto3


def _s3_client() -> "boto3.client":
    region = os.getenv("AWS_REGION") or "ap-northeast-2"
    return boto3.client("s3", region_name=region)


def list_json_keys(bucket: str, prefix: str) -> List[str]:
    s3 = _s3_client()
    token: Optional[str] = None
    out: List[str] = []

    while True:
        args = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
        if token:
            args["ContinuationToken"] = token

        resp = s3.list_objects_v2(**args)
        for obj in resp.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json") or key.endswith(".jsonl"):
                out.append(key)

        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")

    out.sort()
    return out


def get_json(bucket: str, key: str) -> Optional[dict]:
    s3 = _s3_client()
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        raw = obj["Body"].read()
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return None


def list_partition_keys(bucket: str, prefix: str, dt: str) -> Iterator[str]:
    """
    우선순위:
      1) raw_compacted manifest(output_key) 존재하면 output_key만 사용
      2) 없으면 raw/success/dt=... 아래 jsonl을 사용
    """
    manifest_prefix = f"{prefix.rstrip('/')}/raw_compacted/_manifest/dt={dt}/"
    manifest_keys = [k for k in list_json_keys(bucket, manifest_prefix) if k.endswith("manifest.json")]

    output_keys: List[str] = []
    for mk in manifest_keys:
        m = get_json(bucket, mk)
        if not m:
            continue
        ok = m.get("output_key")
        if isinstance(ok, str) and ok.endswith(".jsonl"):
            output_keys.append(ok)

    output_keys = sorted(set(output_keys))
    if output_keys:
        for k in output_keys:
            yield k
        return

    raw_prefix = f"{prefix.rstrip('/')}/raw/success/dt={dt}/"
    raw_keys = [k for k in list_json_keys(bucket, raw_prefix) if k.endswith(".jsonl")]
    for k in raw_keys:
        yield k


def read_jsonl(bucket: str, key: str) -> Iterable[bytes]:
    s3 = _s3_client()
    obj = s3.get_object(Bucket=bucket, Key=key)
    for line in obj["Body"].iter_lines():
        if line:
            yield line
