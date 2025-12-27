import json
import os
import time
import socket
import requests
from datetime import datetime, timezone

from kafka import KafkaProducer
from dotenv import load_dotenv
from pathlib import Path

# 프로젝트 루트의 .env 로드
ENV_PATH = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(dotenv_path=ENV_PATH)

TOPIC = os.getenv("KAFKA_TOPIC")
URL = os.getenv("GITHUB_EVENTS_URL")

if not TOPIC or not URL:
    raise RuntimeError("Missing env: KAFKA_TOPIC / GITHUB_EVENTS_URL")

headers = {
    "Accept": "application/vnd.github+json",
    "User-Agent": "event-log-pipeline",
}

if os.getenv("GITHUB_TOKEN"):
    headers["Authorization"] = f"Bearer {os.getenv('GITHUB_TOKEN')}"

def resolve_bootstrap() -> str:
    # Windows에서 Docker Kafka 접근 가능하도록 localhost 고정
    return "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=resolve_bootstrap(),
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    key_serializer=lambda v: str(v).encode("utf-8"),
    acks="all",
)

print(f"[START] GitHub → Kafka producer using {resolve_bootstrap()}")

def fetch_events():
    resp = requests.get(URL, headers=headers, timeout=10)
    resp.raise_for_status()
    return resp.json()

while True:
    try:
        events = fetch_events()
        for e in events:
            producer.send(TOPIC, key=str(e.get("id", "")), value=e)

        producer.flush()
        print(f"[OK] sent {len(events)} github events")
        time.sleep(30)

    except requests.HTTPError as he:
        print(f"[WARN] HTTP error: {he}")
        time.sleep(60)

    except Exception as e:
        print(f"[ERROR] producer error: {e}")
        time.sleep(10)
