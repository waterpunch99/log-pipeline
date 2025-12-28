# ingestion/consumer/kafka_to_s3.py
import json
import os
import socket
import time
from collections import defaultdict
from pathlib import Path
from typing import DefaultDict, Dict, List, Tuple

from dotenv import load_dotenv
from kafka import KafkaConsumer

from ingestion.consumer.s3_raw_writer import S3RawWriter

# 프로젝트 루트 .env 로드
ENV_PATH = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(dotenv_path=ENV_PATH)

FLUSH_INTERVAL_SEC = 10
MAX_BATCH_SIZE = 50


def resolve_bootstrap() -> str:
    bs = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if bs:
        return bs

    try:
        socket.gethostbyname("kafka")
        return "kafka:29092"
    except Exception:
        return "localhost:9092"


def _group_by_partition(writer: S3RawWriter, events: List[dict]) -> Dict[Tuple[str, str], List[dict]]:
    grouped: DefaultDict[Tuple[str, str], List[dict]] = defaultdict(list)
    for e in events:
        dt, hour, _ = writer.partition_from_event(e if isinstance(e, dict) else {})
        grouped[(dt, hour)].append(e)
    return dict(grouped)


def _flush_partitions(writer: S3RawWriter, consumer: KafkaConsumer, buffer: List[dict]) -> None:
    """
    버퍼를 dt/hour별로 분리해서 각 파티션에 따로 저장합니다.
    모든 파티션 저장이 성공했을 때만 commit 합니다.
    """
    if not buffer:
        return

    grouped = _group_by_partition(writer, buffer)

    for (dt, hour), events in grouped.items():
        pending_key = None
        try:
            pending_key = writer.write_pending(events, dt, hour)
            success_key = writer.promote_to_success(pending_key, dt, hour)
            print(f"[OK] partition=dt={dt}/hour={hour} events={len(events)} s3={success_key}")
        except Exception as e:
            if pending_key:
                try:
                    writer.move_to_failed(pending_key, dt, hour, reason=str(e))
                except Exception:
                    pass
            raise

    consumer.commit()


def main() -> None:
    topic = os.getenv("KAFKA_TOPIC")
    if not topic:
        raise RuntimeError("Missing env: KAFKA_TOPIC")

    bucket = os.getenv("S3_BUCKET")
    prefix = os.getenv("S3_PREFIX")
    region = os.getenv("AWS_REGION") or "ap-northeast-2"
    if not bucket or not prefix:
        raise RuntimeError("Missing env: S3_BUCKET / S3_PREFIX")

    writer = S3RawWriter(bucket=bucket, prefix=prefix, region=region)

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=resolve_bootstrap(),
        group_id="github-raw-consumers",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        max_poll_records=MAX_BATCH_SIZE,
    )

    buffer: List[dict] = []
    last_flush = time.time()

    print(f"[START] Kafka → S3 Raw consumer bootstrap={resolve_bootstrap()} topic={topic}")

    while True:
        now = time.time()

        records = consumer.poll(timeout_ms=1000, max_records=MAX_BATCH_SIZE)
        for _tp, msgs in records.items():
            for msg in msgs:
                if isinstance(msg.value, dict):
                    buffer.append(msg.value)

        if not buffer:
            continue

        if len(buffer) < MAX_BATCH_SIZE and (now - last_flush) < FLUSH_INTERVAL_SEC:
            continue

        try:
            _flush_partitions(writer, consumer, buffer)
            buffer.clear()
            last_flush = time.time()
        except Exception as e:
            print(f"[FAIL] flush error={e}")
            buffer.clear()
            last_flush = time.time()
            time.sleep(2)


if __name__ == "__main__":
    main()
