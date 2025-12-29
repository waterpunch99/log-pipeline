# ingestion/producer/github_to_kafka.py
import json
import os
import random
import socket
import time
from collections import deque
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

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

# 폴링 주기(초) - 304(Not Modified)여도 이 주기로 계속 폴링
POLL_INTERVAL_SEC = int(os.getenv("GITHUB_POLL_INTERVAL_SEC", "30"))

# 장애/일시 오류 시 백오프 상한
MAX_BACKOFF_SEC = int(os.getenv("GITHUB_MAX_BACKOFF_SEC", "300"))

# 최근 이벤트 중복 전송 방지용(메모리/상태파일)
RECENT_ID_CACHE_SIZE = int(os.getenv("GITHUB_RECENT_ID_CACHE_SIZE", "5000"))

# ETag/Last-Modified/최근ID 저장(컨테이너 재시작에도 유지하려면 볼륨 경로에 두는 게 좋음)
STATE_FILE = os.getenv(
    "GITHUB_STATE_FILE",
    str(Path(__file__).resolve().parent / "github_state.json"),
)


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


def _load_state(path: str) -> Dict[str, Any]:
    try:
        p = Path(path)
        if not p.exists():
            return {}
        raw = p.read_text(encoding="utf-8")
        data = json.loads(raw)
        if isinstance(data, dict):
            return data
        return {}
    except Exception:
        return {}


def _save_state(path: str, state: Dict[str, Any]) -> None:
    try:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(p)
    except Exception:
        # 상태 저장 실패는 치명적이지 않으므로 무시
        pass


def _parse_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def _sleep_until(epoch_sec: int, min_sleep: int = 1) -> None:
    now = int(time.time())
    wait = max(epoch_sec - now, min_sleep)
    time.sleep(wait)


def _jitter(seconds: int) -> float:
    # 동시 다발 실행 시 깔끔하게 분산시키기 위한 작은 지터
    return seconds + random.random()


def _rate_limit_wait_seconds(resp: requests.Response) -> Optional[int]:
    remaining = _parse_int(resp.headers.get("X-RateLimit-Remaining"))
    reset = _parse_int(resp.headers.get("X-RateLimit-Reset"))
    if remaining is not None and remaining <= 0 and reset is not None:
        now = int(time.time())
        return max(reset - now + 2, 2)  # 약간 여유
    return None


def _retry_after_seconds(resp: requests.Response) -> Optional[int]:
    ra = resp.headers.get("Retry-After")
    if ra is None:
        return None
    v = _parse_int(ra)
    if v is None:
        return None
    return max(v, 1)


def _build_headers(etag: Optional[str], last_modified: Optional[str]) -> Dict[str, str]:
    headers = {"Accept": "application/vnd.github+json"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    if etag:
        headers["If-None-Match"] = etag
    if last_modified:
        headers["If-Modified-Since"] = last_modified
    return headers


def fetch_events(
    session: requests.Session,
    url: str,
    etag: Optional[str],
    last_modified: Optional[str],
) -> Tuple[List[Dict[str, Any]], Optional[str], Optional[str], str]:
    """
    반환:
      (events, new_etag, new_last_modified, status_tag)
    status_tag:
      - "not_modified" (304)
      - "ok" (200)
      - "rate_limited"
      - "retry_after"
      - "http_error"
    """
    headers = _build_headers(etag, last_modified)

    resp = session.get(url, headers=headers, timeout=15)

    # 304면 본문 없음(또는 비어있음). 상태만 갱신
    if resp.status_code == 304:
        new_etag = resp.headers.get("ETag") or etag
        new_lm = resp.headers.get("Last-Modified") or last_modified
        return [], new_etag, new_lm, "not_modified"

    # rate limit / retry after 처리(주로 403, 429)
    rl_wait = _rate_limit_wait_seconds(resp)
    if rl_wait is not None:
        return [], etag, last_modified, "rate_limited"

    ra_wait = _retry_after_seconds(resp)
    if ra_wait is not None and resp.status_code in (403, 429):
        return [], etag, last_modified, "retry_after"

    # 그 외 에러는 raise로 던져서 상위에서 백오프
    resp.raise_for_status()

    # 200 OK
    new_etag = resp.headers.get("ETag") or etag
    new_lm = resp.headers.get("Last-Modified") or last_modified

    data = resp.json()
    if isinstance(data, list):
        return data, new_etag, new_lm, "ok"

    return [], new_etag, new_lm, "ok"


def _init_recent_cache(state: Dict[str, Any]) -> Tuple[Deque[str], Set[str]]:
    dq: Deque[str] = deque(maxlen=RECENT_ID_CACHE_SIZE)
    st: Set[str] = set()

    saved = state.get("recent_event_ids")
    if isinstance(saved, list):
        for x in saved[-RECENT_ID_CACHE_SIZE:]:
            if isinstance(x, str) and x:
                dq.append(x)
                st.add(x)
    return dq, st


def _persist_recent_cache(state: Dict[str, Any], dq: Deque[str]) -> None:
    state["recent_event_ids"] = list(dq)


def main() -> None:
    producer = _make_producer()
    print(f"[BOOT] kafka={resolve_bootstrap()} topic={TOPIC}")

    session = requests.Session()

    state = _load_state(STATE_FILE)
    etag = state.get("etag") if isinstance(state.get("etag"), str) else None
    last_modified = state.get("last_modified") if isinstance(state.get("last_modified"), str) else None

    recent_dq, recent_set = _init_recent_cache(state)

    backoff = 1

    while True:
        try:
            events, new_etag, new_lm, tag = fetch_events(session, URL, etag, last_modified)

            # 상태 갱신
            etag = new_etag
            last_modified = new_lm
            state["etag"] = etag
            state["last_modified"] = last_modified

            if tag == "rate_limited":
                # reset까지 대기
                # fetch_events에서 wait 계산은 하지 않으니 여기서 헤더 기반으로 다시 계산하지 않고 안전하게 POLL_INTERVAL로 완만하게 대기
                # 더 정교하게 하려면 fetch_events에서 resp를 같이 반환하도록 바꿀 수도 있음
                print("[WARN] rate limited (remaining=0). Sleeping a bit...")
                _save_state(STATE_FILE, state)
                time.sleep(_jitter(60))
                continue

            if tag == "retry_after":
                print("[WARN] retry-after received. Sleeping a bit...")
                _save_state(STATE_FILE, state)
                time.sleep(_jitter(30))
                continue

            if tag == "not_modified" or not events:
                _save_state(STATE_FILE, state)
                backoff = 1
                time.sleep(_jitter(POLL_INTERVAL_SEC))
                continue

            sent = 0
            skipped = 0

            for e in events:
                if not isinstance(e, dict):
                    continue

                event_id = str(e.get("id", "")) if e.get("id") is not None else ""
                if not event_id:
                    continue

                if event_id in recent_set:
                    skipped += 1
                    continue

                producer.send(TOPIC, key=event_id, value=e)
                sent += 1

                recent_dq.append(event_id)
                recent_set.add(event_id)
                # deque가 maxlen으로 밀어낼 때 set도 같이 정리해야 함
                # deque가 자동 pop-left를 하므로, 길이가 꽉 찼고 새로 들어온 경우를 감지해서 정리
                while len(recent_set) > len(recent_dq):
                    # 이 경우는 거의 없지만 안전장치
                    recent_set = set(recent_dq)

                # deque가 꽉 찬 상태에서 append 되면 자동으로 하나가 빠지는데,
                # 빠진 값을 알기 어렵기 때문에 주기적으로 set을 재구성(비용은 낮음)
                if sent % 200 == 0:
                    recent_set = set(recent_dq)

            producer.flush()

            # set 재구성(정합성)
            recent_set = set(recent_dq)

            _persist_recent_cache(state, recent_dq)
            _save_state(STATE_FILE, state)

            print(f"[OK] fetched={len(events)} sent={sent} skipped_recent={skipped} etag={'Y' if etag else 'N'}")

            backoff = 1
            time.sleep(_jitter(POLL_INTERVAL_SEC))

        except requests.HTTPError as he:
            # 남은 호출이 0이면 reset까지 기다리는 게 정석인데,
            # 이 코드는 최소한 백오프로 안전하게 완화합니다.
            print(f"[WARN] HTTP error: {he} backoff={backoff}s")
            _save_state(STATE_FILE, state)
            time.sleep(_jitter(backoff))
            backoff = min(backoff * 2, MAX_BACKOFF_SEC)

        except Exception as e:
            print(f"[ERROR] producer error: {e} backoff={backoff}s")
            _save_state(STATE_FILE, state)
            time.sleep(_jitter(backoff))
            backoff = min(backoff * 2, MAX_BACKOFF_SEC)


if __name__ == "__main__":
    main()
