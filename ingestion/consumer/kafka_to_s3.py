import json
import os
import time
from datetime import datetime, timezone
from typing import List
from pathlib import Path

from kafka import KafkaConsumer
from dotenv import load_dotenv
from ingestion.consumer.s3_raw_writer import S3RawWriter

# 프로젝트 루트 .env 로드
ENV_PATH = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(dotenv_path=ENV_PATH)

FLUSH_INTERVAL_SEC = 10
MAX_BATCH_SIZE = 50

def resolve_bootstrap() -> str:
    return "localhost:9092"

def main():
    topic = os.getenv("KAFKA_TOPIC")
    if not topic:
        raise RuntimeError("Missing env: KAFKA_TOPIC")

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=resolve_bootstrap(),
        group_id="github-raw-consumers",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        max_poll_records=MAX_BATCH_SIZE,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    writer = S3RawWriter(
        bucket=os.getenv("S3_BUCKET"),
        prefix=os.getenv("S3_PREFIX"),
        region=os.getenv("AWS_REGION"),
    )

    buffer: List[dict] = []
    last_flush = time.time()

    print("[START] Kafka → S3 Raw consumer")

    for msg in consumer:
        buffer.append(msg.value)
        now = time.time()

        if len(buffer) < MAX_BATCH_SIZE and (now - last_flush) < FLUSH_INTERVAL_SEC:
            continue

        first_event_time = writer._parse_created_at(buffer[0]) or datetime.now(timezone.utc)

        pending_key = None
        try:
            pending_key = writer.write_pending(buffer)
            success_key = writer.promote_to_success(pending_key, first_event_time)
            consumer.commit()

            print(f"[OK] events={len(buffer)} s3={success_key}")
            buffer.clear()
            last_flush = now

        except Exception as e:
            if pending_key:
                writer.move_to_failed(pending_key, first_event_time, reason=str(e))

            print(f"[FAIL] events={len(buffer)} error={e}")
            buffer.clear()
            last_flush = now

if __name__ == "__main__":
    main()