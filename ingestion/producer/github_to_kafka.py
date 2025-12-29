# ingestion/producer/github_to_kafka.py
from __future__ import annotations

import json
import os
import random
import socket
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer


ENV_PATH = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(dotenv_path=ENV_PATH)

TOPIC = os.getenv("KAFKA_TOPIC")
URL = os.getenv("GITHUB_EVENTS_URL", "https://api.github.com/events")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")  # optional

if not TOPIC or not URL:
    raise RuntimeError("Missing env: KAFKA_TOPIC / GITHUB_EVENTS_URL")

STATE_PATH = Path(__file__).resolve().parent / "github_state.json"

RECENT_IDS_MAX = int(os.getenv("GITHUB_RECENT_IDS_MAX", "800"))
BASE_POLL_SEC = int(os.getenv("GITHUB_POLL_SECONDS", "30"))
MAX_304_POLL_SEC = int(os.getenv("GITHUB_MAX_304_POLL_SECONDS", "180"))
MAX_BACKOFF_SEC = int(os.getenv("GITHUB_MAX_BACKOFF_SECONDS", "300"))
RATELIMIT_SAFETY_SEC = int(os.getenv("GITHUB_RATELIMIT_SAFETY_SECONDS", "3"))
HTTP_TIMEOUT_SEC = int(os.getenv("GITHUB_HTTP_TIMEOUT_SECONDS", "15"))

# remaining이 이 값보다 작아지면, 403/429이 오기 전에 선제적으로 reset까지 대기
RATELIMIT_SOFT_THRESHOLD = int(os.getenv("GITHUB_RATELIMIT_SOFT_THRESHOLD", "20"))

# 디버그(요청에 If-None-Match를 넣었는지 등)
DEBUG = os.getenv("GITHUB_DEBUG", "0") == "1"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _now_epoch() -> int:
    return int(time.time())


def resolve_bootstrap() -> str:
    bs = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if bs:
        return bs
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


@dataclass
class ProducerState:
    etag: Optional[str] = None
    last_modified: Optional[str] = None
    recent_event_ids: Optional[List[str]] = None
    consecutive_304: int = 0
    backoff_sec: int = 0
    last_success_utc: Optional[str] = None

    @staticmethod
    def load(path: Path) -> "ProducerState":
        if not path.exists():
            return ProducerState(recent_event_ids=[])
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            return ProducerState(
                etag=raw.get("etag"),
                last_modified=raw.get("last_modified"),
                recent_event_ids=list(raw.get("recent_event_ids") or []),
                consecutive_304=int(raw.get("consecutive_304") or 0),
                backoff_sec=int(raw.get("backoff_sec") or 0),
                last_success_utc=raw.get("last_success_utc"),
            )
        except Exception:
            return ProducerState(recent_event_ids=[])

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        data = {
            "etag": self.etag,
            "last_modified": self.last_modified,
            "recent_event_ids": (self.recent_event_ids or [])[-RECENT_IDS_MAX:],
            "consecutive_304": self.consecutive_304,
            "backoff_sec": self.backoff_sec,
            "last_success_utc": self.last_success_utc,
        }
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)


def _build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"Accept": "application/vnd.github+json", "User-Agent": "event-log-pipeline/1.0"})
    if GITHUB_TOKEN:
        s.headers.update({"Authorization": f"Bearer {GITHUB_TOKEN}"})
    return s


def _parse_int_header(resp: requests.Response, key: str) -> Optional[int]:
    v = resp.headers.get(key)
    if not v:
        return None
    try:
        return int(v)
    except Exception:
        return None


def _compute_wait_until_reset(reset_epoch: Optional[int]) -> int:
    if not reset_epoch:
        return 60
    wait = reset_epoch - _now_epoch() + RATELIMIT_SAFETY_SEC
    return max(wait, 1)


def _compute_wait_for_retry_after(resp: requests.Response) -> Optional[int]:
    ra = resp.headers.get("Retry-After")
    if not ra:
        return None
    try:
        return max(int(float(ra)), 1)
    except Exception:
        return None


def _sleep_with_jitter(seconds: int) -> None:
    if seconds <= 0:
        return
    jitter = random.uniform(0.0, min(0.8, seconds * 0.15))
    time.sleep(seconds + jitter)


def _dedup_events(events: List[Dict[str, Any]], state: ProducerState) -> Tuple[List[Dict[str, Any]], int]:
    recent = state.recent_event_ids or []
    recent_set = set(recent)

    deduped: List[Dict[str, Any]] = []
    skipped = 0

    for e in events:
        if not isinstance(e, dict):
            continue
        eid = e.get("id")
        if eid is None:
            deduped.append(e)
            continue
        eid_str = str(eid)
        if eid_str in recent_set:
            skipped += 1
            continue
        deduped.append(e)

    return deduped, skipped


def _update_recent_ids(state: ProducerState, events_sent: List[Dict[str, Any]]) -> None:
    recent = state.recent_event_ids or []
    for e in events_sent:
        if isinstance(e, dict) and e.get("id") is not None:
            recent.append(str(e["id"]))
    state.recent_event_ids = recent[-RECENT_IDS_MAX:]


def fetch_events(
    session: requests.Session,
    state: ProducerState,
) -> Tuple[
    List[Dict[str, Any]],
    Optional[str],
    Optional[str],
    str,
    int,
    int,
    Optional[int],
    Optional[int],
]:
    """
    반환:
      (events, new_etag, new_last_modified, status_tag, next_sleep, http_status, remaining, reset_epoch)

    status_tag:
      "200" / "304" / "rate_limited" / "retry_after" / "http_error" / "error" / "soft_throttle"
    """
    headers: Dict[str, str] = {}
    if state.etag:
        headers["If-None-Match"] = state.etag
    if state.last_modified:
        headers["If-Modified-Since"] = state.last_modified

    try:
        resp = session.get(URL, headers=headers, timeout=HTTP_TIMEOUT_SEC)

        http_status = resp.status_code
        remaining = _parse_int_header(resp, "X-RateLimit-Remaining")
        reset_epoch = _parse_int_header(resp, "X-RateLimit-Reset")

        # 선제 스로틀: 403/429 오기 전에 reset까지 쉬기
        if remaining is not None and remaining <= RATELIMIT_SOFT_THRESHOLD:
            wait = min(_compute_wait_until_reset(reset_epoch), MAX_BACKOFF_SEC)
            return ([], state.etag, state.last_modified, "soft_throttle", wait, http_status, remaining, reset_epoch)

        if resp.status_code == 304:
            state.consecutive_304 += 1
            poll = min(BASE_POLL_SEC + state.consecutive_304 * 5, MAX_304_POLL_SEC)
            return ([], state.etag, state.last_modified, "304", poll, http_status, remaining, reset_epoch)

        if resp.status_code == 403:
            # remaining==0이면 rate limit 가능성이 높음
            wait = _compute_wait_until_reset(reset_epoch) if (remaining == 0) else 60
            return ([], state.etag, state.last_modified, "rate_limited", min(wait, MAX_BACKOFF_SEC), http_status, remaining, reset_epoch)

        if resp.status_code == 429:
            wait = _compute_wait_for_retry_after(resp) or min(max(BASE_POLL_SEC, 10), MAX_BACKOFF_SEC)
            return ([], state.etag, state.last_modified, "retry_after", min(wait, MAX_BACKOFF_SEC), http_status, remaining, reset_epoch)

        resp.raise_for_status()

        new_etag = resp.headers.get("ETag") or state.etag
        new_last_modified = resp.headers.get("Last-Modified") or state.last_modified

        data = resp.json()
        if not isinstance(data, list):
            state.consecutive_304 = 0
            return ([], new_etag, new_last_modified, "200", BASE_POLL_SEC, http_status, remaining, reset_epoch)

        state.consecutive_304 = 0
        return (data, new_etag, new_last_modified, "200", BASE_POLL_SEC, http_status, remaining, reset_epoch)

    except requests.HTTPError:
        next_backoff = min(max(10, (state.backoff_sec or 0) * 2), MAX_BACKOFF_SEC)
        if next_backoff == 0:
            next_backoff = 10
        return ([], state.etag, state.last_modified, "http_error", next_backoff, 0, None, None)

    except Exception:
        next_backoff = min(max(10, (state.backoff_sec or 0) * 2), MAX_BACKOFF_SEC)
        if next_backoff == 0:
            next_backoff = 10
        return ([], state.etag, state.last_modified, "error", next_backoff, 0, None, None)


def main() -> None:
    producer = _make_producer()
    session = _build_session()
    state = ProducerState.load(STATE_PATH)

    print(f"[BOOT] kafka={resolve_bootstrap()} topic={TOPIC} url={URL}")

    while True:
        (
            events,
            new_etag,
            new_last_modified,
            status_tag,
            next_sleep,
            http_status,
            remaining,
            reset_epoch,
        ) = fetch_events(session, state)

        state.etag = new_etag
        state.last_modified = new_last_modified

        if DEBUG:
            inm = "Y" if bool(state.etag) else "N"
            print(f"[DBG] status={http_status} If-None-Match={inm} remaining={remaining} reset={reset_epoch}")

        if status_tag == "200" and events:
            deduped, skipped_recent = _dedup_events(events, state)

            sent = 0
            for e in deduped:
                if not isinstance(e, dict):
                    continue
                event_id = str(e.get("id", "")) if isinstance(e, dict) else ""
                producer.send(TOPIC, key=event_id, value=e)
                sent += 1

            producer.flush()

            _update_recent_ids(state, deduped)
            state.backoff_sec = 0
            state.last_success_utc = _utcnow().isoformat()
            state.save(STATE_PATH)

            print(
                f"[OK] status={http_status} fetched={len(events)} sent={sent} skipped_recent={skipped_recent} "
                f"etag={'Y' if bool(state.etag) else 'N'} remaining={remaining} reset={reset_epoch} sleep={BASE_POLL_SEC}s"
            )
            _sleep_with_jitter(BASE_POLL_SEC)
            continue

        if status_tag == "200" and not events:
            state.backoff_sec = 0
            state.save(STATE_PATH)
            print(
                f"[OK] status={http_status} fetched=0 sent=0 etag={'Y' if bool(state.etag) else 'N'} "
                f"remaining={remaining} reset={reset_epoch} sleep={BASE_POLL_SEC}s"
            )
            _sleep_with_jitter(BASE_POLL_SEC)
            continue

        if status_tag == "304":
            state.backoff_sec = 0
            state.save(STATE_PATH)
            print(
                f"[OK] status={http_status} 304 not modified etag={'Y' if bool(state.etag) else 'N'} "
                f"remaining={remaining} reset={reset_epoch} sleep={next_sleep}s"
            )
            _sleep_with_jitter(next_sleep)
            continue

        if status_tag in ("soft_throttle", "rate_limited", "retry_after"):
            state.backoff_sec = min(next_sleep, MAX_BACKOFF_SEC)
            state.save(STATE_PATH)
            print(
                f"[WARN] status={http_status} {status_tag} etag={'Y' if bool(state.etag) else 'N'} "
                f"remaining={remaining} reset={reset_epoch} sleep={next_sleep}s"
            )
            _sleep_with_jitter(next_sleep)
            continue

        state.backoff_sec = min(next_sleep, MAX_BACKOFF_SEC)
        state.save(STATE_PATH)
        print(
            f"[WARN] status={http_status} {status_tag} etag={'Y' if bool(state.etag) else 'N'} "
            f"remaining={remaining} reset={reset_epoch} sleep={state.backoff_sec}s"
        )
        _sleep_with_jitter(state.backoff_sec)


if __name__ == "__main__":
    main()
