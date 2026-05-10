from __future__ import annotations

import os
from typing import Any

import boto3

from us_market_data.apewisdom import (
    fetch_apewisdom_page,
    normalize_apewisdom_payload,
    parse_bool,
    rows_to_table,
    utc_date_hour,
    write_raw_json_s3,
    write_staging_parquet_s3,
)
from us_market_data.s3_paths import raw_apewisdom_key, staging_reddit_trends_key

DEFAULT_RAW_PREFIX = "raw/reddit/apewisdom"
DEFAULT_STAGING_PREFIX = "staging/reddit_trends"
DEFAULT_FILTER_NAME = "all-stocks"
DEFAULT_PAGE = 1
DEFAULT_WRITE_RAW = True
DEFAULT_WRITE_STAGING = True
MESSAGE = "ApeWisdom Reddit trends ingested as attention signal"


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"{name} environment variable is required")
    return value


def parse_page(value: object) -> int:
    try:
        page = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"page must be an integer >= 1, got {value!r}") from exc

    if page < 1:
        raise ValueError(f"page must be an integer >= 1, got {value!r}")
    return page


def event_value(event: Any, name: str, default: object) -> object:
    if isinstance(event, dict) and name in event:
        return event[name]
    return default


def resolve_config(event: Any) -> dict[str, object]:
    default_date, default_hour = utc_date_hour()

    return {
        "bucket": require_env("BUCKET_NAME"),
        "raw_prefix": os.environ.get("RAW_PREFIX", DEFAULT_RAW_PREFIX),
        "staging_prefix": os.environ.get("STAGING_PREFIX", DEFAULT_STAGING_PREFIX),
        "filter_name": str(
            event_value(event, "filter", os.environ.get("FILTER_NAME", DEFAULT_FILTER_NAME))
        ),
        "page": parse_page(event_value(event, "page", os.environ.get("PAGE", DEFAULT_PAGE))),
        "date": str(event_value(event, "date", default_date)),
        "hour": str(event_value(event, "hour", default_hour)).zfill(2),
        "write_raw": parse_bool(
            event_value(event, "write_raw", os.environ.get("WRITE_RAW", DEFAULT_WRITE_RAW))
        ),
        "write_staging": parse_bool(
            event_value(
                event,
                "write_staging",
                os.environ.get("WRITE_STAGING", DEFAULT_WRITE_STAGING),
            )
        ),
    }


def lambda_handler(event: Any, context: Any) -> dict[str, Any]:
    config = resolve_config(event)
    s3_client = boto3.client("s3")

    bucket = str(config["bucket"])
    raw_prefix = str(config["raw_prefix"])
    staging_prefix = str(config["staging_prefix"])
    filter_name = str(config["filter_name"])
    page = int(config["page"])
    date_str = str(config["date"])
    hour_str = str(config["hour"])
    write_raw = bool(config["write_raw"])
    write_staging = bool(config["write_staging"])

    payload = fetch_apewisdom_page(filter_name, page)
    rows = normalize_apewisdom_payload(
        payload,
        date_str=date_str,
        hour_str=hour_str,
        filter_name=filter_name,
        page=page,
    )
    table = rows_to_table(rows)

    raw_key = raw_apewisdom_key(raw_prefix, date_str, hour_str, filter_name)
    staging_key = staging_reddit_trends_key(staging_prefix, date_str)

    written_raw_key = write_raw_json_s3(s3_client, bucket, raw_key, payload) if write_raw else None
    written_staging_key = (
        write_staging_parquet_s3(s3_client, bucket, staging_key, table) if write_staging else None
    )

    return {
        "statusCode": 200,
        "body": {
            "source": "apewisdom",
            "filter": filter_name,
            "page": page,
            "date": date_str,
            "hour": hour_str,
            "row_count": table.num_rows,
            "raw_key": written_raw_key,
            "staging_key": written_staging_key,
            "message": MESSAGE,
        },
    }
