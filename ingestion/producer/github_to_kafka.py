# ingestion/producer/github_to_kafka.py
import json
import os
import socket
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer

# 프로젝트 루트의 .env 로드
ENV_PATH = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(dotenv_path=ENV_PATH)

TOPIC = os.getenv("KAFKA_TOPIC")
URL = os.getenv("GITHUB_EVENTS_URL")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")  # 선택

if not TOPIC or not URL:
    raise RuntimeError("Missing env: KAFKA_TOPIC / GITHUB_EVENTS_URL")


def resolve_bootstrap() -> str:
    bs = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if bs:
        return bs

    # env가 없으면, 도커 네트워크에서 kafka 호스트가 해석되면 내부 리스너로, 아니면 로컬로
    try:
        socket.gethostbyname("kafka")
        return "kafka:29092"
    except Exception:
        return "localhost:9092"


def _make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=resolve_bootstrap(),
        acks="all",
        retries=5,
        linger_ms=50,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: (k or "").encode("utf-8"),
    )


def fetch_events() -> List[Dict[str, Any]]:
    headers = {"Accept": "application/vnd.github+json"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"

    resp = requests.get(URL, headers=headers, timeout=15)
    resp.raise_for_status()

    data = resp.json()
    if isinstance(data, list):
        return data
    return []


def main() -> None:
    producer = _make_producer()
    print(f"[BOOT] kafka={resolve_bootstrap()} topic={TOPIC}")

    while True:
        try:
            events = fetch_events()
            if not events:
                print("[OK] no events")
                time.sleep(30)
                continue

            sent = 0
            for e in events:
                event_id = str(e.get("id", "")) if isinstance(e, dict) else ""
                producer.send(TOPIC, key=event_id, value=e)
                sent += 1

            producer.flush()
            print(f"[OK] sent {sent} github events")
            time.sleep(30)

        except requests.HTTPError as he:
            print(f"[WARN] HTTP error: {he}")
            time.sleep(60)

        except Exception as e:
            print(f"[ERROR] producer error: {e}")
            time.sleep(10)


if __name__ == "__main__":
    main()
