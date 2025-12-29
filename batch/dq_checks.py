import argparse
import json
import os
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, Optional, Tuple

import psycopg


@dataclass
class Thresholds:
    min_valid_rate: float
    max_null_created_at_rate: float
    min_dw_match_rate: float
    require_dm_hourly_coverage: bool
    require_dm_type_ratio_when_typed_exists: bool


def _get_thresholds() -> Thresholds:
    return Thresholds(
        min_valid_rate=float(os.getenv("DQ_MIN_VALID_RATE", "0.98")),
        max_null_created_at_rate=float(os.getenv("DQ_MAX_NULL_CREATED_AT_RATE", "0.01")),
        min_dw_match_rate=float(os.getenv("DQ_MIN_DW_MATCH_RATE", "0.98")),
        require_dm_hourly_coverage=os.getenv("DQ_REQUIRE_DM_HOURLY_COVERAGE", "true").lower() == "true",
        require_dm_type_ratio_when_typed_exists=os.getenv("DQ_REQUIRE_DM_TYPE_RATIO_WHEN_TYPED_EXISTS", "true").lower() == "true",
    )


def _fail_mode() -> str:
    """
    DQ_FAIL_MODE:
      - hard: 실패 시 예외 발생(기본값)
      - soft: 실패해도 예외는 던지지 않고 결과만 기록
    """
    mode = (os.getenv("DQ_FAIL_MODE", "hard") or "hard").strip().lower()
    return "soft" if mode == "soft" else "hard"


def _q(cur: "psycopg.Cursor", sql: str, params: Optional[Dict[str, Any]] = None):
    cur.execute(sql, params or {})
    return cur.fetchone()


def _to_numeric(v: Optional[float]) -> Optional[Decimal]:
    if v is None:
        return None
    try:
        return Decimal(str(v))
    except Exception:
        return None


def _upsert_dq_result(
    cur: "psycopg.Cursor",
    *,
    run_id: str,
    stage: str,
    check_name: str,
    passed: bool,
    severity: str,
    measured_value: Optional[float] = None,
    threshold_value: Optional[float] = None,
    details: Optional[Dict[str, Any]] = None,
) -> None:
    cur.execute(
        """
        INSERT INTO dq_check_results (
          run_id, stage, check_name, passed, severity,
          measured_value, threshold_value, details, checked_at
        )
        VALUES (
          %(run_id)s, %(stage)s, %(check_name)s, %(passed)s, %(severity)s,
          %(measured_value)s, %(threshold_value)s, %(details)s::jsonb, now()
        )
        ON CONFLICT (run_id, stage, check_name)
        DO UPDATE SET
          passed=EXCLUDED.passed,
          severity=EXCLUDED.severity,
          measured_value=EXCLUDED.measured_value,
          threshold_value=EXCLUDED.threshold_value,
          details=EXCLUDED.details,
          checked_at=now()
        """,
        {
            "run_id": run_id,
            "stage": stage,
            "check_name": check_name,
            "passed": passed,
            "severity": severity,
            "measured_value": _to_numeric(measured_value),
            "threshold_value": _to_numeric(threshold_value),
            "details": json.dumps(details or {}, ensure_ascii=False),
        },
    )


def _get_targets_exist(cur: "psycopg.Cursor", run_id: str) -> int:
    row = _q(
        cur,
        """
        SELECT COUNT(1)
        FROM stg_file_ingestion_log
        WHERE run_id=%(run_id)s AND status IN ('done','skipped')
        """,
        {"run_id": run_id},
    )
    return int(row[0] or 0)


def dq_after_dw(run_id: str) -> None:
    dsn = os.getenv("POSTGRES_DSN")
    if not dsn:
        raise RuntimeError("Missing env: POSTGRES_DSN")

    th = _get_thresholds()
    mode = _fail_mode()

    hard_fail = False
    hard_fail_reason = ""

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            files_done_or_skipped = _get_targets_exist(cur, run_id)
            if files_done_or_skipped == 0:
                _upsert_dq_result(
                    cur,
                    run_id=run_id,
                    stage="dw",
                    check_name="targets_exist",
                    passed=True,
                    severity="soft",
                    measured_value=0.0,
                    threshold_value=0.0,
                    details={"msg": "no done/skipped files for this run_id. skipping DW checks."},
                )
                conn.commit()
                print(f"[DQ][DW] run_id={run_id} no done/skipped files. Skipping checks (PASS).")
                return

            # STG 품질 (이번 run_id의 done 파일만 대상으로 품질 체크)
            stg = _q(
                cur,
                """
                WITH targets AS (
                  SELECT source_key
                  FROM stg_file_ingestion_log
                  WHERE run_id=%(run_id)s AND status='done'
                )
                SELECT
                  COUNT(1) AS stg_rows,
                  COUNT(1) FILTER (WHERE s.is_valid=true) AS valid_rows,
                  COUNT(1) FILTER (WHERE s.is_valid=false) AS invalid_rows,
                  COUNT(1) FILTER (WHERE s.created_at IS NULL) AS null_created_at,
                  COUNT(1) FILTER (WHERE s.event_id IS NULL) AS null_event_id,
                  COUNT(DISTINCT s.event_id) FILTER (WHERE s.is_valid=true AND s.event_id IS NOT NULL) AS distinct_valid_event_ids
                FROM github_events_stg s
                JOIN targets t ON t.source_key = s.source_key
                """,
                {"run_id": run_id},
            )

            stg_rows = int(stg[0] or 0)
            valid_rows = int(stg[1] or 0)
            invalid_rows = int(stg[2] or 0)
            null_created_at = int(stg[3] or 0)
            null_event_id = int(stg[4] or 0)
            stg_distinct_valid_ids = int(stg[5] or 0)

            valid_rate = (valid_rows / stg_rows) if stg_rows else 1.0
            null_created_at_rate = (null_created_at / stg_rows) if stg_rows else 0.0

            print(
                f"[DQ][DW] run_id={run_id} files_done_or_skipped={files_done_or_skipped} "
                f"stg_rows={stg_rows} valid_rows={valid_rows} invalid_rows={invalid_rows} "
                f"null_created_at={null_created_at} null_event_id={null_event_id} distinct_valid_event_ids={stg_distinct_valid_ids} "
                f"valid_rate={valid_rate:.4f} null_created_at_rate={null_created_at_rate:.4f}"
            )

            # 체크 1: STG rows 존재
            _upsert_dq_result(
                cur,
                run_id=run_id,
                stage="dw",
                check_name="stg_rows_nonzero",
                passed=(stg_rows > 0),
                severity="soft",
                measured_value=float(stg_rows),
                threshold_value=1.0,
                details={
                    "stg_rows": stg_rows,
                    "valid_rows": valid_rows,
                    "invalid_rows": invalid_rows,
                    "note": "stg_rows==0이면 downstream 영향이 없을 수 있어 soft로 둠",
                },
            )

            if stg_rows == 0:
                conn.commit()
                print(f"[DQ][DW] run_id={run_id} STG has 0 rows for this run. PASS.")
                return

            # 체크 2: valid_rate
            passed_valid_rate = valid_rate >= th.min_valid_rate
            _upsert_dq_result(
                cur,
                run_id=run_id,
                stage="dw",
                check_name="stg_valid_rate",
                passed=passed_valid_rate,
                severity="hard",
                measured_value=float(valid_rate),
                threshold_value=float(th.min_valid_rate),
                details={
                    "stg_rows": stg_rows,
                    "valid_rows": valid_rows,
                    "invalid_rows": invalid_rows,
                },
            )
            if not passed_valid_rate:
                hard_fail = True
                hard_fail_reason = f"valid_rate too low: {valid_rate:.4f} < {th.min_valid_rate}"

            # 체크 3: null_created_at_rate
            passed_null_created = null_created_at_rate <= th.max_null_created_at_rate
            _upsert_dq_result(
                cur,
                run_id=run_id,
                stage="dw",
                check_name="stg_null_created_at_rate",
                passed=passed_null_created,
                severity="hard",
                measured_value=float(null_created_at_rate),
                threshold_value=float(th.max_null_created_at_rate),
                details={
                    "stg_rows": stg_rows,
                    "null_created_at": null_created_at,
                },
            )
            if not passed_null_created:
                hard_fail = True
                if not hard_fail_reason:
                    hard_fail_reason = f"null_created_at_rate too high: {null_created_at_rate:.4f} > {th.max_null_created_at_rate}"

            # DW 반영 확인: 이번 run에서 본 event_id들이 DW에 존재하는지
            dw = _q(
                cur,
                """
                WITH targets AS (
                  SELECT source_key
                  FROM stg_file_ingestion_log
                  WHERE run_id=%(run_id)s AND status='done'
                ),
                stg_ids AS (
                  SELECT DISTINCT s.event_id
                  FROM github_events_stg s
                  JOIN targets t ON t.source_key=s.source_key
                  WHERE s.is_valid=true AND s.event_id IS NOT NULL AND s.created_at IS NOT NULL
                )
                SELECT
                  (SELECT COUNT(1) FROM stg_ids) AS stg_distinct,
                  (SELECT COUNT(1) FROM stg_ids i JOIN github_events_dw d ON d.event_id=i.event_id) AS dw_matched
                """,
                {"run_id": run_id},
            )

            stg_distinct = int(dw[0] or 0)
            dw_matched = int(dw[1] or 0)
            match_rate = (dw_matched / stg_distinct) if stg_distinct else 1.0

            print(
                f"[DQ][DW] run_id={run_id} stg_distinct_valid_ids={stg_distinct} dw_matched={dw_matched} match_rate={match_rate:.4f}"
            )

            passed_match_rate = match_rate >= th.min_dw_match_rate
            _upsert_dq_result(
                cur,
                run_id=run_id,
                stage="dw",
                check_name="dw_match_rate",
                passed=passed_match_rate,
                severity="hard",
                measured_value=float(match_rate),
                threshold_value=float(th.min_dw_match_rate),
                details={
                    "stg_distinct_valid_ids": stg_distinct,
                    "dw_matched": dw_matched,
                },
            )
            if not passed_match_rate:
                hard_fail = True
                if not hard_fail_reason:
                    hard_fail_reason = f"dw_match_rate too low: {match_rate:.4f} < {th.min_dw_match_rate}"

            conn.commit()

    if hard_fail and mode == "hard":
        raise RuntimeError(f"[DQ_FAIL][DW] run_id={run_id} {hard_fail_reason}")

    print(f"[DQ_PASS][DW] run_id={run_id} mode={mode}")


def dq_after_dm(run_id: str) -> None:
    dsn = os.getenv("POSTGRES_DSN")
    if not dsn:
        raise RuntimeError("Missing env: POSTGRES_DSN")

    th = _get_thresholds()
    mode = _fail_mode()

    hard_fail = False
    hard_fail_reason = ""

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            files_done_or_skipped = _get_targets_exist(cur, run_id)
            if files_done_or_skipped == 0:
                _upsert_dq_result(
                    cur,
                    run_id=run_id,
                    stage="dm",
                    check_name="targets_exist",
                    passed=True,
                    severity="soft",
                    measured_value=0.0,
                    threshold_value=0.0,
                    details={"msg": "no done/skipped files for this run_id. skipping DM checks."},
                )
                conn.commit()
                print(f"[DQ][DM] run_id={run_id} no done/skipped files. Skipping checks (PASS).")
                return

            hrs = _q(
                cur,
                """
                WITH targets AS (
                  SELECT source_key
                  FROM stg_file_ingestion_log
                  WHERE run_id=%(run_id)s AND status='done'
                ),
                affected_hours AS (
                  SELECT DISTINCT date_trunc('hour', s.created_at) AS event_hour
                  FROM github_events_stg s
                  JOIN targets t ON t.source_key=s.source_key
                  WHERE s.is_valid=true AND s.created_at IS NOT NULL
                )
                SELECT COUNT(1) FROM affected_hours
                """,
                {"run_id": run_id},
            )
            affected_hours = int(hrs[0] or 0)

            print(f"[DQ][DM] run_id={run_id} affected_hours={affected_hours}")

            _upsert_dq_result(
                cur,
                run_id=run_id,
                stage="dm",
                check_name="affected_hours_nonzero",
                passed=(affected_hours > 0),
                severity="soft",
                measured_value=float(affected_hours),
                threshold_value=1.0,
                details={"affected_hours": affected_hours},
            )

            if affected_hours == 0:
                conn.commit()
                print(f"[DQ][DM] run_id={run_id} affected_hours=0. PASS.")
                return

            miss_hourly = _q(
                cur,
                """
                WITH targets AS (
                  SELECT source_key
                  FROM stg_file_ingestion_log
                  WHERE run_id=%(run_id)s AND status='done'
                ),
                affected_hours AS (
                  SELECT DISTINCT date_trunc('hour', s.created_at) AS event_hour
                  FROM github_events_stg s
                  JOIN targets t ON t.source_key=s.source_key
                  WHERE s.is_valid=true AND s.created_at IS NOT NULL
                ),
                missing AS (
                  SELECT h.event_hour
                  FROM affected_hours h
                  LEFT JOIN dm_hourly_event_volume m ON m.event_hour=h.event_hour
                  WHERE m.event_hour IS NULL
                )
                SELECT COUNT(1) FROM missing
                """,
                {"run_id": run_id},
            )
            missing_hourly = int(miss_hourly[0] or 0)

            print(f"[DQ][DM] run_id={run_id} missing_dm_hourly_rows={missing_hourly}")

            passed_hourly_cov = (missing_hourly == 0) if th.require_dm_hourly_coverage else True
            _upsert_dq_result(
                cur,
                run_id=run_id,
                stage="dm",
                check_name="dm_hourly_coverage",
                passed=passed_hourly_cov,
                severity="hard" if th.require_dm_hourly_coverage else "soft",
                measured_value=float(missing_hourly),
                threshold_value=0.0,
                details={"missing_hours": missing_hourly, "require": th.require_dm_hourly_coverage},
            )

            if th.require_dm_hourly_coverage and missing_hourly > 0:
                hard_fail = True
                hard_fail_reason = f"dm_hourly_event_volume missing hours: {missing_hourly}"

            miss_typed = _q(
                cur,
                """
                WITH targets AS (
                  SELECT source_key
                  FROM stg_file_ingestion_log
                  WHERE run_id=%(run_id)s AND status='done'
                ),
                affected_hours AS (
                  SELECT DISTINCT date_trunc('hour', s.created_at) AS event_hour
                  FROM github_events_stg s
                  JOIN targets t ON t.source_key=s.source_key
                  WHERE s.is_valid=true AND s.created_at IS NOT NULL
                ),
                typed_hours AS (
                  SELECT DISTINCT date_trunc('hour', d.created_at) AS event_hour
                  FROM github_events_dw d
                  JOIN affected_hours h ON h.event_hour=date_trunc('hour', d.created_at)
                  WHERE d.event_type IS NOT NULL
                ),
                missing AS (
                  SELECT th.event_hour
                  FROM typed_hours th
                  LEFT JOIN dm_event_type_ratio r ON r.event_hour=th.event_hour
                  WHERE r.event_hour IS NULL
                )
                SELECT
                  (SELECT COUNT(1) FROM typed_hours) AS typed_hour_cnt,
                  (SELECT COUNT(1) FROM missing) AS missing_typed_hours
                """,
                {"run_id": run_id},
            )
            typed_hour_cnt = int(miss_typed[0] or 0)
            missing_typed_hours = int(miss_typed[1] or 0)

            print(
                f"[DQ][DM] run_id={run_id} typed_hour_cnt={typed_hour_cnt} missing_typed_hours={missing_typed_hours}"
            )

            require_type_ratio = th.require_dm_type_ratio_when_typed_exists
            passed_type_ratio = True
            if require_type_ratio and typed_hour_cnt > 0 and missing_typed_hours > 0:
                passed_type_ratio = False

            _upsert_dq_result(
                cur,
                run_id=run_id,
                stage="dm",
                check_name="dm_type_ratio_coverage_when_typed_exists",
                passed=passed_type_ratio,
                severity="hard" if require_type_ratio else "soft",
                measured_value=float(missing_typed_hours),
                threshold_value=0.0,
                details={
                    "typed_hour_cnt": typed_hour_cnt,
                    "missing_typed_hours": missing_typed_hours,
                    "require": require_type_ratio,
                },
            )

            if require_type_ratio and typed_hour_cnt > 0 and missing_typed_hours > 0:
                hard_fail = True
                if not hard_fail_reason:
                    hard_fail_reason = f"dm_event_type_ratio missing typed hours: {missing_typed_hours}"

            conn.commit()

    if hard_fail and mode == "hard":
        raise RuntimeError(f"[DQ_FAIL][DM] run_id={run_id} {hard_fail_reason}")

    print(f"[DQ_PASS][DM] run_id={run_id} mode={mode}")


def run(run_id: str, stage: str) -> None:
    stage = (stage or "").lower().strip()
    if stage == "dw":
        dq_after_dw(run_id)
        return
    if stage == "dm":
        dq_after_dm(run_id)
        return
    raise ValueError("stage must be 'dw' or 'dm'")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--stage", required=True, choices=["dw", "dm"])
    args = parser.parse_args()
    run(run_id=args.run_id, stage=args.stage)
