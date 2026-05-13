from __future__ import annotations

import io
import json
import time
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

import pyarrow as pa
import pyarrow.parquet as pq

from us_market_data.ticker_reference import (
    RETRYABLE_MASSIVE_HTTP_CODES,
    SOURCE,
    clean_lower_string,
    clean_string,
    clean_upper_string,
    exponential_backoff_seconds,
    parse_optional_bool,
    resolve_massive_api_key,
    retry_after_seconds,
    sanitize_api_key_from_url,
)

MASSIVE_TICKER_OVERVIEW_URL = "https://api.massive.com/v3/reference/tickers"

NORMALIZED_COLUMNS = [
    "symbol",
    "name",
    "market",
    "locale",
    "primary_exchange",
    "type",
    "active",
    "currency_name",
    "cik",
    "composite_figi",
    "share_class_figi",
    "market_cap",
    "weighted_shares_outstanding",
    "share_class_shares_outstanding",
    "sic_code",
    "sic_description",
    "description",
    "homepage_url",
    "total_employees",
    "list_date",
    "branding_logo_url",
    "branding_icon_url",
    "source",
    "ingested_at",
]

NORMALIZED_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string()),
        pa.field("name", pa.string()),
        pa.field("market", pa.string()),
        pa.field("locale", pa.string()),
        pa.field("primary_exchange", pa.string()),
        pa.field("type", pa.string()),
        pa.field("active", pa.bool_()),
        pa.field("currency_name", pa.string()),
        pa.field("cik", pa.string()),
        pa.field("composite_figi", pa.string()),
        pa.field("share_class_figi", pa.string()),
        pa.field("market_cap", pa.float64()),
        pa.field("weighted_shares_outstanding", pa.float64()),
        pa.field("share_class_shares_outstanding", pa.float64()),
        pa.field("sic_code", pa.string()),
        pa.field("sic_description", pa.string()),
        pa.field("description", pa.string()),
        pa.field("homepage_url", pa.string()),
        pa.field("total_employees", pa.int64()),
        pa.field("list_date", pa.string()),
        pa.field("branding_logo_url", pa.string()),
        pa.field("branding_icon_url", pa.string()),
        pa.field("source", pa.string()),
        pa.field("ingested_at", pa.string()),
    ]
)


def utc_date(now: datetime | None = None) -> str:
    current = now or datetime.now(timezone.utc)
    return current.date().isoformat()


def utc_timestamp(now: datetime | None = None) -> str:
    current = now or datetime.now(timezone.utc)
    return current.isoformat()


def build_ticker_overview_url(api_key: str, symbol: str) -> str:
    normalized_symbol = clean_upper_string(symbol)
    if not normalized_symbol:
        raise ValueError("symbol is required")
    ticker = quote(normalized_symbol, safe="")
    return f"{MASSIVE_TICKER_OVERVIEW_URL}/{ticker}?{urlencode({'apiKey': api_key})}"


def fetch_massive_ticker_overview(
    symbol: str,
    api_key: str,
    opener: Callable[..., Any] = urlopen,
    timeout: int = 30,
    max_retries: int = 5,
    base_sleep_seconds: float = 2.0,
    max_sleep_seconds: float = 60.0,
    sleeper: Callable[[float], Any] = time.sleep,
    progress_callback: Callable[[str], Any] | None = None,
) -> dict[str, Any]:
    if max_retries < 0:
        raise ValueError("max_retries must be >= 0")
    if base_sleep_seconds < 0:
        raise ValueError("base_sleep_seconds must be >= 0")
    if max_sleep_seconds < 0:
        raise ValueError("max_sleep_seconds must be >= 0")

    normalized_symbol = clean_upper_string(symbol)
    if not normalized_symbol:
        raise ValueError("symbol is required")

    url = build_ticker_overview_url(api_key, normalized_symbol)
    request = Request(
        url,
        method="GET",
        headers={
            "Accept": "application/json",
            "User-Agent": "us-market-data-ticker-overview-ingest/0.1",
        },
    )
    safe_url = sanitize_api_key_from_url(url)

    for retry_index in range(max_retries + 1):
        try:
            with opener(request, timeout=timeout) as response:
                body = response.read().decode("utf-8")
            break
        except HTTPError as exc:
            message = f"HTTP {exc.code} fetching Massive ticker overview URL {safe_url}"
            if exc.code not in RETRYABLE_MASSIVE_HTTP_CODES:
                raise RuntimeError(message) from exc
            if retry_index >= max_retries:
                raise RuntimeError(f"{message} after {retry_index + 1} attempts") from exc
            sleep_seconds = retry_after_seconds(exc)
            if sleep_seconds is None:
                sleep_seconds = exponential_backoff_seconds(
                    retry_index,
                    base_sleep_seconds,
                    max_sleep_seconds,
                )
            if progress_callback:
                progress_callback(
                    f"HTTP {exc.code} fetching {normalized_symbol}. "
                    f"Sleeping {sleep_seconds:.1f} seconds before retry "
                    f"{retry_index + 1}/{max_retries}..."
                )
            sleeper(sleep_seconds)
        except URLError as exc:
            message = f"failed fetching Massive ticker overview URL {safe_url}: {exc.reason}"
            if retry_index >= max_retries:
                raise RuntimeError(f"{message} after {retry_index + 1} attempts") from exc
            sleep_seconds = exponential_backoff_seconds(
                retry_index,
                base_sleep_seconds,
                max_sleep_seconds,
            )
            if progress_callback:
                progress_callback(
                    f"Error fetching {normalized_symbol}: {exc.reason}. "
                    f"Sleeping {sleep_seconds:.1f} seconds before retry "
                    f"{retry_index + 1}/{max_retries}..."
                )
            sleeper(sleep_seconds)

    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Massive ticker overview endpoint returned non-JSON response") from exc

    if not isinstance(payload, dict):
        raise RuntimeError("Massive ticker overview response was not a JSON object")
    return payload


def clean_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def clean_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def payload_result(payload: dict[str, Any]) -> dict[str, Any]:
    result = payload.get("results", {})
    return result if isinstance(result, dict) else {}


def normalize_massive_ticker_overview_payload(
    payload: dict[str, Any],
    symbol: str,
    ingested_at: str | None = None,
) -> dict[str, Any]:
    timestamp = ingested_at or utc_timestamp()
    raw = payload_result(payload)
    normalized_symbol = clean_upper_string(raw.get("ticker") or raw.get("symbol") or symbol)
    if not normalized_symbol:
        raise ValueError("symbol is required")

    branding = raw.get("branding")
    if not isinstance(branding, dict):
        branding = {}

    return {
        "symbol": normalized_symbol,
        "name": clean_string(raw.get("name")),
        "market": clean_lower_string(raw.get("market")),
        "locale": clean_lower_string(raw.get("locale")),
        "primary_exchange": clean_string(raw.get("primary_exchange")),
        "type": clean_upper_string(raw.get("type")),
        "active": parse_optional_bool(raw.get("active")),
        "currency_name": clean_string(raw.get("currency_name")),
        "cik": clean_string(raw.get("cik")),
        "composite_figi": clean_string(raw.get("composite_figi")),
        "share_class_figi": clean_string(raw.get("share_class_figi")),
        "market_cap": clean_float(raw.get("market_cap")),
        "weighted_shares_outstanding": clean_float(raw.get("weighted_shares_outstanding")),
        "share_class_shares_outstanding": clean_float(raw.get("share_class_shares_outstanding")),
        "sic_code": clean_string(raw.get("sic_code")),
        "sic_description": clean_string(raw.get("sic_description")),
        "description": clean_string(raw.get("description")),
        "homepage_url": clean_string(raw.get("homepage_url")),
        "total_employees": clean_int(raw.get("total_employees")),
        "list_date": clean_string(raw.get("list_date")),
        "branding_logo_url": clean_string(branding.get("logo_url")),
        "branding_icon_url": clean_string(branding.get("icon_url")),
        "source": SOURCE,
        "ingested_at": timestamp,
    }


def rows_to_table(rows: list[dict[str, Any]]) -> pa.Table:
    arrays = []
    for field in NORMALIZED_SCHEMA:
        values = [row.get(field.name) for row in rows]
        arrays.append(pa.array(values, type=field.type))
    return pa.Table.from_arrays(arrays, schema=NORMALIZED_SCHEMA)


def table_to_rows(table: pa.Table) -> list[dict[str, Any]]:
    selected_columns = [
        table.column(column).to_pylist()
        if column in table.column_names
        else [None] * table.num_rows
        for column in NORMALIZED_COLUMNS
    ]
    rows: list[dict[str, Any]] = []
    for index in range(table.num_rows):
        rows.append(
            {
                column: selected_columns[column_index][index]
                for column_index, column in enumerate(NORMALIZED_COLUMNS)
            }
        )
    return rows


def rows_from_parquet_bytes(body: bytes) -> list[dict[str, Any]]:
    return table_to_rows(pq.read_table(io.BytesIO(body)))


def normalize_ticker_overview_row(row: dict[str, Any]) -> dict[str, Any] | None:
    symbol = clean_upper_string(row.get("symbol"))
    if not symbol:
        return None
    normalized = {column: row.get(column) for column in NORMALIZED_COLUMNS}
    normalized["symbol"] = symbol
    return normalized


def merge_ticker_overview_rows(
    existing_rows: list[dict[str, Any]],
    fetched_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_symbol: dict[str, dict[str, Any]] = {}
    for row in [*existing_rows, *fetched_rows]:
        normalized = normalize_ticker_overview_row(row)
        if normalized is not None:
            by_symbol[normalized["symbol"]] = normalized
    return [by_symbol[symbol] for symbol in sorted(by_symbol)]


def parse_ingested_at(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def stale_symbols(
    existing_rows: list[dict[str, Any]],
    requested_symbols: list[str],
    refresh_after_days: int,
    now: datetime | None = None,
) -> list[str]:
    normalized_requested = []
    seen_requested: set[str] = set()
    for requested_symbol in requested_symbols:
        symbol = clean_upper_string(requested_symbol)
        if symbol and symbol not in seen_requested:
            normalized_requested.append(symbol)
            seen_requested.add(symbol)

    if refresh_after_days <= 0:
        return normalized_requested

    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    current = current.astimezone(timezone.utc)
    cutoff = current - timedelta(days=refresh_after_days)

    existing_by_symbol: dict[str, dict[str, Any]] = {}
    for row in existing_rows:
        normalized = normalize_ticker_overview_row(row)
        if normalized is not None:
            existing_by_symbol[normalized["symbol"]] = normalized

    stale: list[str] = []
    for symbol in normalized_requested:
        row = existing_by_symbol.get(symbol)
        if row is None:
            stale.append(symbol)
            continue
        ingested_at = parse_ingested_at(row.get("ingested_at"))
        if ingested_at is None or ingested_at < cutoff:
            stale.append(symbol)
    return stale


def table_to_parquet_bytes(table: pa.Table) -> bytes:
    out = io.BytesIO()
    pq.write_table(table, out, compression="snappy")
    return out.getvalue()


def local_path_for_key(local_output_dir: str | Path, key: str) -> Path:
    return Path(local_output_dir) / key


def write_bytes_local(local_output_dir: str | Path, key: str, body: bytes) -> Path:
    path = local_path_for_key(local_output_dir, key)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)
    return path


def write_raw_json_local(local_output_dir: str | Path, key: str, payload: dict[str, Any]) -> Path:
    body = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
    return write_bytes_local(local_output_dir, key, body)


def write_curated_parquet_local(local_output_dir: str | Path, key: str, table: pa.Table) -> Path:
    return write_bytes_local(local_output_dir, key, table_to_parquet_bytes(table))


def read_curated_parquet_local(local_output_dir: str | Path, key: str) -> list[dict[str, Any]]:
    path = local_path_for_key(local_output_dir, key)
    if not path.exists():
        return []
    try:
        return table_to_rows(pq.read_table(path))
    except Exception as exc:
        raise RuntimeError(f"failed reading existing curated parquet {path}: {exc}") from exc


def write_raw_json_s3(s3_client: Any, bucket: str, key: str, payload: dict[str, Any]) -> str:
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload).encode("utf-8"),
        ContentType="application/json",
    )
    return key


def write_curated_parquet_s3(s3_client: Any, bucket: str, key: str, table: pa.Table) -> str:
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=table_to_parquet_bytes(table),
        ContentType="application/octet-stream",
    )
    return key


def read_curated_parquet_s3(s3_client: Any, bucket: str, key: str) -> list[dict[str, Any]]:
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        body = response["Body"].read()
    except Exception as exc:
        error = getattr(exc, "response", {}).get("Error", {})
        if error.get("Code") in {"NoSuchKey", "404", "NotFound"}:
            return []
        message = f"failed reading existing curated parquet s3://{bucket}/{key}: {exc}"
        raise RuntimeError(message) from exc
    try:
        return rows_from_parquet_bytes(body)
    except Exception as exc:
        message = f"failed parsing existing curated parquet s3://{bucket}/{key}: {exc}"
        raise RuntimeError(message) from exc


__all__ = [
    "NORMALIZED_COLUMNS",
    "NORMALIZED_SCHEMA",
    "build_ticker_overview_url",
    "fetch_massive_ticker_overview",
    "merge_ticker_overview_rows",
    "normalize_massive_ticker_overview_payload",
    "read_curated_parquet_local",
    "read_curated_parquet_s3",
    "resolve_massive_api_key",
    "rows_from_parquet_bytes",
    "rows_to_table",
    "stale_symbols",
    "table_to_rows",
    "utc_date",
    "write_curated_parquet_local",
    "write_curated_parquet_s3",
    "write_raw_json_local",
    "write_raw_json_s3",
]
