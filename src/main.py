import json
import logging
import os
import random
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import clickhouse_connect
import requests
from dotenv import load_dotenv


@dataclass(frozen=True)
class Config:
    api_url: str
    max_attempts: int
    base_backoff_seconds: float
    max_backoff_seconds: float
    timeout_connect: float
    timeout_read: float
    ch_host: str
    ch_port: int
    ch_user: str
    ch_password: str
    raw_table: str
    optimize_after_insert: bool
    log_level: str


def setup_json_logger(level: str) -> logging.Logger:
    logger = logging.getLogger("astro_ingest")
    logger.setLevel(level.upper())
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.handlers = [handler]
    logger.propagate = False
    return logger


def json_log(logger: logging.Logger, level: str, event: str, **fields: Any) -> None:
    payload = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "level": level.upper(),
        "event": event,
        **fields,
    }
    message = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    getattr(logger, level.lower())(message)


def parse_retry_after(header_value: Optional[str]) -> Optional[float]:
    if not header_value:
        return None

    value = header_value.strip()
    if value.isdigit():
        return float(value)

    try:
        parsed = parsedate_to_datetime(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        delay = (parsed - datetime.now(timezone.utc)).total_seconds()
        return max(0.0, delay)
    except Exception:
        return None


def compute_backoff_with_jitter(attempt: int, base: float, cap: float) -> float:
    upper_bound = min(cap, base * (2 ** (attempt - 1)))
    return random.uniform(0.0, upper_bound)


def is_retryable_status(status_code: int) -> bool:
    return status_code == 429 or 500 <= status_code <= 599


def fetch_json_with_retries(cfg: Config, logger: logging.Logger) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    session = requests.Session()
    attempt_metrics = []
    status_codes = []
    started_at = time.perf_counter()

    try:
        for attempt in range(1, cfg.max_attempts + 1):
            attempt_started_at = time.perf_counter()
            status_code: Optional[int] = None
            retry_after_header: Optional[str] = None
            error_message: Optional[str] = None

            try:
                response = session.get(
                    cfg.api_url,
                    timeout=(cfg.timeout_connect, cfg.timeout_read),
                    headers={"Accept": "application/json"},
                )
                status_code = response.status_code
                status_codes.append(status_code)
                retry_after_header = response.headers.get("Retry-After")

                if status_code == 200:
                    payload = response.json()
                    if not isinstance(payload, dict):
                        raise RuntimeError("Expected JSON object from Open Notify API.")

                    duration_ms = round((time.perf_counter() - attempt_started_at) * 1000, 2)
                    attempt_metrics.append(
                        {
                            "attempt": attempt,
                            "status_code": status_code,
                            "duration_ms": duration_ms,
                            "sleep_seconds": 0.0,
                            "retry_after": retry_after_header,
                        }
                    )

                    json_log(
                        logger,
                        "info",
                        "http_success",
                        attempt=attempt,
                        status_code=status_code,
                        duration_ms=duration_ms,
                        response_size_bytes=len(response.content or b""),
                    )

                    return payload, {
                        "attempts_made": attempt,
                        "status_codes": status_codes,
                        "total_ms": round((time.perf_counter() - started_at) * 1000, 2),
                        "attempt_details": attempt_metrics,
                    }

                if not is_retryable_status(status_code):
                    duration_ms = round((time.perf_counter() - attempt_started_at) * 1000, 2)
                    attempt_metrics.append(
                        {
                            "attempt": attempt,
                            "status_code": status_code,
                            "duration_ms": duration_ms,
                            "sleep_seconds": 0.0,
                            "retry_after": retry_after_header,
                        }
                    )
                    json_log(
                        logger,
                        "error",
                        "http_non_retryable_status",
                        attempt=attempt,
                        status_code=status_code,
                        duration_ms=duration_ms,
                    )
                    raise RuntimeError(f"Non-retryable HTTP status: {status_code}")

            except (requests.Timeout, requests.ConnectionError) as exc:
                error_message = str(exc)
            except requests.RequestException as exc:
                json_log(logger, "error", "http_request_exception", attempt=attempt, error=str(exc))
                raise RuntimeError("Unexpected HTTP request failure.") from exc
            except ValueError as exc:
                json_log(logger, "error", "http_invalid_json", attempt=attempt, error=str(exc))
                raise RuntimeError("Open Notify API returned invalid JSON.") from exc

            duration_ms = round((time.perf_counter() - attempt_started_at) * 1000, 2)
            is_last_attempt = attempt == cfg.max_attempts
            retry_after_seconds = parse_retry_after(retry_after_header) if status_code == 429 else None
            sleep_reason = "retry_after" if retry_after_seconds is not None else "exponential_backoff_with_jitter"
            sleep_seconds = retry_after_seconds
            if sleep_seconds is None:
                sleep_seconds = compute_backoff_with_jitter(
                    attempt=attempt,
                    base=cfg.base_backoff_seconds,
                    cap=cfg.max_backoff_seconds,
                )

            attempt_metrics.append(
                {
                    "attempt": attempt,
                    "status_code": status_code,
                    "duration_ms": duration_ms,
                    "sleep_seconds": round(0.0 if is_last_attempt else sleep_seconds, 3),
                    "retry_after": retry_after_header,
                    "sleep_reason": None if is_last_attempt else sleep_reason,
                    "error": error_message,
                }
            )

            if is_last_attempt:
                json_log(
                    logger,
                    "error",
                    "http_retries_exhausted",
                    attempts=cfg.max_attempts,
                    last_status=status_code,
                    last_error=error_message,
                    status_codes=status_codes,
                    total_ms=round((time.perf_counter() - started_at) * 1000, 2),
                )
                raise RuntimeError(
                    f"Failed to download payload after {cfg.max_attempts} attempts. "
                    f"last_status={status_code} last_error={error_message}"
                )

            json_log(
                logger,
                "warning",
                "http_retry_scheduled",
                attempt=attempt,
                status_code=status_code,
                duration_ms=duration_ms,
                sleep_seconds=round(sleep_seconds, 3),
                sleep_reason=sleep_reason,
                retry_after=retry_after_header,
                error=error_message,
            )
            time.sleep(sleep_seconds)
    finally:
        session.close()

    raise RuntimeError("Unexpected retry loop termination.")


def clickhouse_client(cfg: Config):
    return clickhouse_connect.get_client(
        host=cfg.ch_host,
        port=cfg.ch_port,
        username=cfg.ch_user,
        password=cfg.ch_password,
        connect_timeout=10,
    )


def format_clickhouse_datetime(value: datetime) -> str:
    utc_value = value.astimezone(timezone.utc)
    return utc_value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def insert_raw_and_optimize(
    cfg: Config,
    logger: logging.Logger,
    payload: Dict[str, Any],
    inserted_at: datetime,
) -> Dict[str, Any]:
    client = clickhouse_client(cfg)
    overall_started_at = time.perf_counter()

    try:
        client.query("SELECT 1")

        insert_started_at = time.perf_counter()
        row = {
            "_inserted_at": format_clickhouse_datetime(inserted_at),
            "payload": payload,
        }
        insert_block = (json.dumps(row, ensure_ascii=False) + "\n").encode("utf-8")

        client.raw_insert(
            table=cfg.raw_table,
            column_names=["_inserted_at", "payload"],
            insert_block=insert_block,
            fmt="JSONEachRow",
        )
        insert_ms = round((time.perf_counter() - insert_started_at) * 1000, 2)

        optimize_ms = 0.0
        if cfg.optimize_after_insert:
            optimize_started_at = time.perf_counter()
            client.command("OPTIMIZE TABLE core.people FINAL")
            optimize_ms = round((time.perf_counter() - optimize_started_at) * 1000, 2)

        metrics = {
            "insert_ms": insert_ms,
            "optimize_ms": optimize_ms,
            "total_ms": round((time.perf_counter() - overall_started_at) * 1000, 2),
            "raw_table": cfg.raw_table,
        }
        json_log(logger, "info", "clickhouse_insert_complete", **metrics)
        return metrics
    finally:
        close_method = getattr(client, "close", None)
        if callable(close_method):
            close_method()


def load_config() -> Config:
    repo_root = Path(__file__).resolve().parents[1]
    load_dotenv(repo_root / ".env")

    max_attempts = int(os.getenv("RETRY_MAX_ATTEMPTS", "5"))
    if max_attempts <= 0:
        raise RuntimeError("RETRY_MAX_ATTEMPTS must be greater than 0.")

    ch_user = os.getenv("CH_INGEST_USER")
    ch_password = os.getenv("CH_INGEST_PASSWORD")
    if not ch_user or not ch_password:
        raise RuntimeError("CH_INGEST_USER and CH_INGEST_PASSWORD must be set.")

    return Config(
        api_url=os.getenv("OPEN_NOTIFY_URL", "http://api.open-notify.org/astros.json"),
        max_attempts=max_attempts,
        base_backoff_seconds=float(os.getenv("RETRY_BASE_SECONDS", "1.0")),
        max_backoff_seconds=float(os.getenv("RETRY_MAX_SECONDS", "30.0")),
        timeout_connect=float(os.getenv("HTTP_TIMEOUT_CONNECT", "5.0")),
        timeout_read=float(os.getenv("HTTP_TIMEOUT_READ", "10.0")),
        ch_host=os.getenv("CH_HOST", "localhost"),
        ch_port=int(os.getenv("CH_HTTP_PORT", "8123")),
        ch_user=ch_user,
        ch_password=ch_password,
        raw_table=os.getenv("CH_RAW_TABLE", "raw.astros_raw"),
        optimize_after_insert=os.getenv("OPTIMIZE_FINAL", "1") == "1",
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )


def main() -> None:
    cfg = load_config()
    logger = setup_json_logger(cfg.log_level)

    json_log(
        logger,
        "info",
        "start",
        api_url=cfg.api_url,
        clickhouse_host=cfg.ch_host,
        clickhouse_port=cfg.ch_port,
        raw_table=cfg.raw_table,
        max_attempts=cfg.max_attempts,
    )

    payload, http_metrics = fetch_json_with_retries(cfg, logger)

    people = payload.get("people", [])
    json_log(
        logger,
        "info",
        "payload_received",
        message=payload.get("message"),
        people_count=len(people) if isinstance(people, list) else None,
    )

    inserted_at = datetime.now(timezone.utc)
    clickhouse_metrics = insert_raw_and_optimize(cfg, logger, payload, inserted_at)

    json_log(
        logger,
        "info",
        "done",
        inserted_at=inserted_at.isoformat(),
        http=http_metrics,
        clickhouse=clickhouse_metrics,
    )


if __name__ == "__main__":
    main()
