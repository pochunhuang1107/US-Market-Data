from __future__ import annotations

import os
import time
from math import isfinite
from typing import Any

import boto3

from us_market_data.s3_paths import curated_ticker_overview_key, raw_ticker_overview_key
from us_market_data.ticker_overview import (
    fetch_massive_ticker_overview,
    merge_ticker_overview_rows,
    normalize_massive_ticker_overview_payload,
    read_curated_parquet_s3,
    resolve_massive_api_key,
    rows_to_table,
    stale_symbols,
    utc_date,
    write_curated_parquet_s3,
    write_raw_json_s3,
)

DEFAULT_SYMBOLS_KEY = "curated/reference/public_profile_symbols/symbols.txt"
DEFAULT_RAW_PREFIX = "raw/reference/massive_ticker_overview"
DEFAULT_CURATED_KEY = "curated/reference/ticker_overview/data.parquet"
DEFAULT_REFRESH_AFTER_DAYS = 7
DEFAULT_FETCH_LIMIT = 300
DEFAULT_REQUEST_INTERVAL_SECONDS = 0.0
DEFAULT_MAX_RETRIES = 5
DEFAULT_BASE_SLEEP_SECONDS = 2.0
DEFAULT_MAX_SLEEP_SECONDS = 60.0
MESSAGE = "Massive ticker overview upsert complete"


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"{name} environment variable is required")
    return value


def event_value(event: Any, name: str, default: object) -> object:
    if isinstance(event, dict) and name in event:
        return event[name]
    return default


def parse_nonnegative_int(name: str, value: object) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer >= 0, got {value!r}") from exc

    if parsed < 0:
        raise ValueError(f"{name} must be an integer >= 0, got {value!r}")
    return parsed


def parse_nonnegative_float(name: str, value: object) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a number >= 0, got {value!r}") from exc

    if not isfinite(parsed) or parsed < 0:
        raise ValueError(f"{name} must be a number >= 0, got {value!r}")
    return parsed


def progress(message: str) -> None:
    print(message, flush=True)


def resolve_config(event: Any) -> dict[str, object]:
    return {
        "bucket": require_env("BUCKET_NAME"),
        "symbols_key": str(
            event_value(
                event,
                "symbols_key",
                os.environ.get("SYMBOLS_KEY", DEFAULT_SYMBOLS_KEY),
            )
        ),
        "raw_prefix": str(os.environ.get("RAW_PREFIX", DEFAULT_RAW_PREFIX)),
        "curated_key": curated_ticker_overview_key(
            str(os.environ.get("CURATED_KEY", DEFAULT_CURATED_KEY))
        ),
        "date": str(event_value(event, "date", utc_date())),
        "refresh_after_days": parse_nonnegative_int(
            "refresh_after_days",
            event_value(
                event,
                "refresh_after_days",
                os.environ.get("REFRESH_AFTER_DAYS", DEFAULT_REFRESH_AFTER_DAYS),
            ),
        ),
        "fetch_limit": parse_nonnegative_int(
            "fetch_limit",
            event_value(event, "fetch_limit", os.environ.get("FETCH_LIMIT", DEFAULT_FETCH_LIMIT)),
        ),
        "request_interval_seconds": parse_nonnegative_float(
            "request_interval_seconds",
            event_value(
                event,
                "request_interval_seconds",
                os.environ.get(
                    "REQUEST_INTERVAL_SECONDS",
                    DEFAULT_REQUEST_INTERVAL_SECONDS,
                ),
            ),
        ),
        "max_retries": parse_nonnegative_int(
            "max_retries",
            event_value(event, "max_retries", os.environ.get("MAX_RETRIES", DEFAULT_MAX_RETRIES)),
        ),
        "base_sleep_seconds": parse_nonnegative_float(
            "base_sleep_seconds",
            event_value(
                event,
                "base_sleep_seconds",
                os.environ.get("BASE_SLEEP_SECONDS", DEFAULT_BASE_SLEEP_SECONDS),
            ),
        ),
        "max_sleep_seconds": parse_nonnegative_float(
            "max_sleep_seconds",
            event_value(
                event,
                "max_sleep_seconds",
                os.environ.get("MAX_SLEEP_SECONDS", DEFAULT_MAX_SLEEP_SECONDS),
            ),
        ),
    }


def normalize_symbols(text: str) -> list[str]:
    symbols = sorted({line.strip().upper() for line in text.splitlines() if line.strip()})
    if not symbols:
        raise RuntimeError("symbols file did not contain any symbols")
    return symbols


def read_symbols_s3(s3_client: Any, bucket: str, key: str) -> list[str]:
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        body = response["Body"].read()
    except Exception as exc:
        error = getattr(exc, "response", {}).get("Error", {})
        if error.get("Code") in {"NoSuchKey", "404", "NotFound"}:
            raise RuntimeError(f"symbols file not found: s3://{bucket}/{key}") from exc
        raise RuntimeError(f"failed reading symbols file s3://{bucket}/{key}: {exc}") from exc

    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise RuntimeError(f"symbols file is not valid UTF-8: s3://{bucket}/{key}") from exc
    return normalize_symbols(text)


def fetch_and_write_rows(
    s3_client: Any,
    bucket: str,
    raw_prefix: str,
    date_str: str,
    symbols: list[str],
    api_key: str,
    request_interval_seconds: float,
    max_retries: int,
    base_sleep_seconds: float,
    max_sleep_seconds: float,
    fetcher=fetch_massive_ticker_overview,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index, symbol in enumerate(symbols, start=1):
        progress(f"fetching {symbol} ({index}/{len(symbols)})")
        payload = fetcher(
            symbol=symbol,
            api_key=api_key,
            max_retries=max_retries,
            base_sleep_seconds=base_sleep_seconds,
            max_sleep_seconds=max_sleep_seconds,
            progress_callback=progress,
        )
        rows.append(normalize_massive_ticker_overview_payload(payload, symbol))

        raw_key = raw_ticker_overview_key(raw_prefix, date_str, symbol)
        write_raw_json_s3(s3_client, bucket, raw_key, payload)
        progress(f"wrote raw s3: s3://{bucket}/{raw_key}")

        if request_interval_seconds > 0 and index < len(symbols):
            progress(f"sleeping {request_interval_seconds:.1f} seconds before next symbol")
            time.sleep(request_interval_seconds)
    return rows


def lambda_handler(event: Any, context: Any) -> dict[str, Any]:
    del context
    config = resolve_config(event)
    s3_client = boto3.client("s3")

    bucket = str(config["bucket"])
    symbols_key = str(config["symbols_key"])
    raw_prefix = str(config["raw_prefix"])
    curated_key = str(config["curated_key"])
    date_str = str(config["date"])
    refresh_after_days = int(config["refresh_after_days"])
    fetch_limit = int(config["fetch_limit"])
    request_interval_seconds = float(config["request_interval_seconds"])
    max_retries = int(config["max_retries"])
    base_sleep_seconds = float(config["base_sleep_seconds"])
    max_sleep_seconds = float(config["max_sleep_seconds"])

    progress(
        "Starting Massive ticker overview upsert: "
        f"date={date_str} symbols_key=s3://{bucket}/{symbols_key} curated_key={curated_key}"
    )
    desired_symbols = read_symbols_s3(s3_client, bucket, symbols_key)
    existing_rows = read_curated_parquet_s3(s3_client, bucket, curated_key)
    stale_or_missing_symbols = stale_symbols(
        existing_rows,
        desired_symbols,
        refresh_after_days,
    )
    symbols_to_fetch = stale_or_missing_symbols[:fetch_limit]

    progress(
        "ticker overview upsert summary: "
        f"desired_symbols={len(desired_symbols)} "
        f"existing_cached_rows={len(existing_rows)} "
        f"stale_missing_selected={len(stale_or_missing_symbols)} "
        f"fetch_limit={fetch_limit} "
        f"symbols_to_fetch={len(symbols_to_fetch)}"
    )

    fetched_rows: list[dict[str, object]] = []
    final_rows = existing_rows
    if symbols_to_fetch:
        api_key = resolve_massive_api_key()
        fetched_rows = fetch_and_write_rows(
            s3_client,
            bucket,
            raw_prefix,
            date_str,
            symbols_to_fetch,
            api_key,
            request_interval_seconds,
            max_retries,
            base_sleep_seconds,
            max_sleep_seconds,
            fetcher=fetch_massive_ticker_overview,
        )
        final_rows = merge_ticker_overview_rows(existing_rows, fetched_rows)
        table = rows_to_table(final_rows)
        write_curated_parquet_s3(s3_client, bucket, curated_key, table)
        progress(f"wrote curated s3: s3://{bucket}/{curated_key} rows={table.num_rows}")
    else:
        progress("no missing or stale ticker overview rows selected; curated cache unchanged")

    progress(
        "Massive ticker overview upsert complete: "
        f"date={date_str} desired_symbols={len(desired_symbols)} "
        f"existing_cached_rows={len(existing_rows)} "
        f"symbols_to_fetch={len(symbols_to_fetch)} "
        f"fetched={len(fetched_rows)} final_curated_rows={len(final_rows)}"
    )

    return {
        "statusCode": 200,
        "body": {
            "source": "massive",
            "date": date_str,
            "desired_symbols": len(desired_symbols),
            "existing_cached_rows": len(existing_rows),
            "symbols_to_fetch": len(symbols_to_fetch),
            "fetched": len(fetched_rows),
            "final_curated_rows": len(final_rows),
            "curated_key": curated_key,
            "symbols_key": symbols_key,
            "raw_prefix": raw_prefix,
            "message": MESSAGE,
        },
    }
