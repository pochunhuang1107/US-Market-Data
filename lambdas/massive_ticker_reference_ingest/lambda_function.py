from __future__ import annotations

import json
import os
from collections.abc import Iterable
from math import isfinite
from typing import Any

import boto3

from us_market_data.s3_paths import curated_ticker_reference_key, raw_ticker_reference_key
from us_market_data.ticker_reference import (
    fetch_massive_ticker_pages,
    normalize_massive_ticker_payloads,
    parse_bool,
    rows_to_table,
    utc_date,
    write_curated_parquet_s3,
    write_raw_json_s3,
)

DEFAULT_RAW_PREFIX = "raw/reference/massive_tickers"
DEFAULT_CURATED_KEY = "curated/reference/ticker_reference/data.parquet"
DEFAULT_MARKET = "stocks"
DEFAULT_ACTIVE = True
DEFAULT_LOCALE = "us"
DEFAULT_LIMIT = 1000
DEFAULT_COMMON_STOCK_TYPE_CODES = ("CS",)
DEFAULT_REQUEST_INTERVAL_SECONDS = 13.0
DEFAULT_MAX_RETRIES = 8
DEFAULT_BASE_SLEEP_SECONDS = 15.0
DEFAULT_MAX_SLEEP_SECONDS = 120.0
MESSAGE = "Massive ticker reference ingested"


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"{name} environment variable is required")
    return value


def get_api_key() -> str:
    response = boto3.client("secretsmanager").get_secret_value(
        SecretId=require_env("SECRET_NAME")
    )
    secret = json.loads(response["SecretString"])
    return secret["api_key"]


def event_value(event: Any, name: str, default: object) -> object:
    if isinstance(event, dict) and name in event:
        return event[name]
    return default


def parse_positive_int(name: str, value: object) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer >= 1, got {value!r}") from exc

    if parsed < 1:
        raise ValueError(f"{name} must be an integer >= 1, got {value!r}")
    return parsed


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


def parse_config_bool(name: str, value: object) -> bool:
    try:
        return parse_bool(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a boolean value, got {value!r}") from exc


def parse_common_stock_type_codes(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        parts: Iterable[object] = value.split(",")
    elif isinstance(value, Iterable):
        parts = value
    else:
        raise ValueError(f"common_stock_type_codes must be comma-separated text, got {value!r}")

    codes = tuple(str(part).strip().upper() for part in parts if str(part).strip())
    if not codes:
        raise ValueError("common_stock_type_codes must include at least one type code")
    return codes


def progress(message: str) -> None:
    print(message, flush=True)


def resolve_config(event: Any) -> dict[str, object]:
    return {
        "bucket": require_env("BUCKET_NAME"),
        "raw_prefix": str(os.environ.get("RAW_PREFIX", DEFAULT_RAW_PREFIX)),
        "curated_key": curated_ticker_reference_key(
            str(os.environ.get("CURATED_KEY", DEFAULT_CURATED_KEY))
        ),
        "date": str(event_value(event, "date", utc_date())),
        "market": str(event_value(event, "market", os.environ.get("MARKET", DEFAULT_MARKET))),
        "active": parse_config_bool(
            "active",
            event_value(event, "active", os.environ.get("ACTIVE", DEFAULT_ACTIVE)),
        ),
        "locale": str(event_value(event, "locale", os.environ.get("LOCALE", DEFAULT_LOCALE))),
        "limit": parse_positive_int(
            "limit",
            event_value(event, "limit", os.environ.get("LIMIT", DEFAULT_LIMIT)),
        ),
        "common_stock_type_codes": parse_common_stock_type_codes(
            event_value(
                event,
                "common_stock_type_codes",
                os.environ.get(
                    "COMMON_STOCK_TYPE_CODES",
                    ",".join(DEFAULT_COMMON_STOCK_TYPE_CODES),
                ),
            )
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


def lambda_handler(event: Any, context: Any) -> dict[str, Any]:
    del context
    config = resolve_config(event)
    api_key = get_api_key()
    s3_client = boto3.client("s3")

    bucket = str(config["bucket"])
    raw_prefix = str(config["raw_prefix"])
    curated_key = str(config["curated_key"])
    date_str = str(config["date"])
    market = str(config["market"])
    active = bool(config["active"])
    locale = str(config["locale"])
    limit = int(config["limit"])
    common_stock_type_codes = tuple(config["common_stock_type_codes"])
    request_interval_seconds = float(config["request_interval_seconds"])
    max_retries = int(config["max_retries"])
    base_sleep_seconds = float(config["base_sleep_seconds"])
    max_sleep_seconds = float(config["max_sleep_seconds"])

    progress(
        "Starting Massive ticker reference ingest: "
        f"date={date_str} market={market} locale={locale} active={active}"
    )
    payloads = fetch_massive_ticker_pages(
        api_key=api_key,
        market=market,
        active=active,
        locale=locale,
        limit=limit,
        max_retries=max_retries,
        base_sleep_seconds=base_sleep_seconds,
        max_sleep_seconds=max_sleep_seconds,
        request_interval_seconds=request_interval_seconds,
        progress_callback=progress,
    )
    rows = normalize_massive_ticker_payloads(
        payloads,
        allowed_common_stock_type_codes=common_stock_type_codes,
    )
    table = rows_to_table(rows)

    for page, payload in enumerate(payloads, start=1):
        raw_key = raw_ticker_reference_key(raw_prefix, date_str, page)
        write_raw_json_s3(s3_client, bucket, raw_key, payload)
        progress(f"wrote raw s3: s3://{bucket}/{raw_key}")

    write_curated_parquet_s3(s3_client, bucket, curated_key, table)
    progress(f"wrote curated s3: s3://{bucket}/{curated_key} rows={table.num_rows}")
    progress(
        "Massive ticker reference ingest complete: "
        f"date={date_str} pages={len(payloads)} rows={table.num_rows}"
    )

    return {
        "statusCode": 200,
        "body": {
            "source": "massive",
            "date": date_str,
            "market": market,
            "locale": locale,
            "active": active,
            "pages": len(payloads),
            "row_count": table.num_rows,
            "curated_key": curated_key,
            "raw_prefix": raw_prefix,
            "message": MESSAGE,
        },
    }
