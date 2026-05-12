from __future__ import annotations

import io
import json
import os
import time
from collections.abc import Callable, Container, Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from urllib.request import Request, urlopen

import pyarrow as pa
import pyarrow.parquet as pq

MASSIVE_TICKERS_URL = "https://api.massive.com/v3/reference/tickers"
SOURCE = "massive"
DEFAULT_COMMON_STOCK_TYPE_CODES = frozenset({"CS"})
RETRYABLE_MASSIVE_HTTP_CODES = frozenset({429, 500, 502, 503, 504})

NORMALIZED_COLUMNS = [
    "symbol",
    "name",
    "market",
    "locale",
    "type",
    "active",
    "currency_name",
    "primary_exchange",
    "cik",
    "composite_figi",
    "share_class_figi",
    "last_updated_utc",
    "ingested_at",
    "source",
    "is_common_stock_candidate",
]

NORMALIZED_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string()),
        pa.field("name", pa.string()),
        pa.field("market", pa.string()),
        pa.field("locale", pa.string()),
        pa.field("type", pa.string()),
        pa.field("active", pa.bool_()),
        pa.field("currency_name", pa.string()),
        pa.field("primary_exchange", pa.string()),
        pa.field("cik", pa.string()),
        pa.field("composite_figi", pa.string()),
        pa.field("share_class_figi", pa.string()),
        pa.field("last_updated_utc", pa.string()),
        pa.field("ingested_at", pa.string()),
        pa.field("source", pa.string()),
        pa.field("is_common_stock_candidate", pa.bool_()),
    ]
)


def parse_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "t", "yes", "y"}:
        return True
    if text in {"0", "false", "f", "no", "n"}:
        return False
    raise ValueError(f"invalid boolean value: {value!r}")


def utc_date(now: datetime | None = None) -> str:
    current = now or datetime.now(timezone.utc)
    return current.date().isoformat()


def utc_timestamp(now: datetime | None = None) -> str:
    current = now or datetime.now(timezone.utc)
    return current.isoformat()


def resolve_massive_api_key(env: Mapping[str, str] | None = None) -> str:
    source_env = os.environ if env is None else env
    api_key = source_env.get("MASSIVE_API_KEY") or source_env.get("POLYGON_API_KEY")
    if api_key and api_key.strip():
        return api_key.strip()
    raise RuntimeError("missing API key: set MASSIVE_API_KEY or POLYGON_API_KEY")


def build_tickers_url(
    api_key: str,
    market: str = "stocks",
    active: bool = True,
    locale: str = "us",
    limit: int = 1000,
) -> str:
    params = {
        "market": market,
        "active": str(active).lower(),
        "locale": locale,
        "limit": str(limit),
        "apiKey": api_key,
    }
    return f"{MASSIVE_TICKERS_URL}?{urlencode(params)}"


def ensure_api_key_param(url: str, api_key: str) -> str:
    parsed = urlparse(url)
    query = parse_qsl(parsed.query, keep_blank_values=True)
    if not any(key == "apiKey" for key, _ in query):
        query.append(("apiKey", api_key))
    return urlunparse(parsed._replace(query=urlencode(query)))


def sanitize_api_key_from_url(url: str) -> str:
    parsed = urlparse(url)
    query = [
        (key, "<redacted>" if key.lower() == "apikey" else value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
    ]
    sanitized_url = urlunparse(parsed._replace(query=urlencode(query)))
    return sanitized_url.replace("%3Credacted%3E", "<redacted>")


def retry_after_seconds(exc: HTTPError) -> float | None:
    retry_after = exc.headers.get("Retry-After") if exc.headers else None
    if retry_after is None:
        return None
    try:
        seconds = float(str(retry_after).strip())
    except ValueError:
        return None
    return seconds if seconds >= 0 else None


def exponential_backoff_seconds(
    retry_index: int,
    base_sleep_seconds: float,
    max_sleep_seconds: float,
) -> float:
    return min(max_sleep_seconds, base_sleep_seconds * (2**retry_index))


def fetch_massive_ticker_page(
    url: str,
    opener: Callable[..., Any] = urlopen,
    timeout: int = 30,
    max_retries: int = 5,
    base_sleep_seconds: float = 2.0,
    max_sleep_seconds: float = 60.0,
    sleeper: Callable[[float], Any] = time.sleep,
    progress_callback: Callable[[str], Any] | None = None,
    page_number: int | None = None,
) -> dict[str, Any]:
    if max_retries < 0:
        raise ValueError("max_retries must be >= 0")
    if base_sleep_seconds < 0:
        raise ValueError("base_sleep_seconds must be >= 0")
    if max_sleep_seconds < 0:
        raise ValueError("max_sleep_seconds must be >= 0")

    request = Request(
        url,
        method="GET",
        headers={
            "Accept": "application/json",
            "User-Agent": "us-market-data-ticker-reference-ingest/0.1",
        },
    )
    safe_url = sanitize_api_key_from_url(url)
    page_label = f"page {page_number}" if page_number is not None else "page"

    for retry_index in range(max_retries + 1):
        try:
            with opener(request, timeout=timeout) as response:
                body = response.read().decode("utf-8")
            break
        except HTTPError as exc:
            message = f"HTTP {exc.code} fetching Massive ticker reference URL {safe_url}"
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
                    f"HTTP {exc.code} fetching {page_label}. "
                    f"Sleeping {sleep_seconds:.1f} seconds before retry "
                    f"{retry_index + 1}/{max_retries}..."
                )
            sleeper(sleep_seconds)
        except URLError as exc:
            message = f"failed fetching Massive ticker reference URL {safe_url}: {exc.reason}"
            if retry_index >= max_retries:
                raise RuntimeError(f"{message} after {retry_index + 1} attempts") from exc
            sleep_seconds = exponential_backoff_seconds(
                retry_index,
                base_sleep_seconds,
                max_sleep_seconds,
            )
            if progress_callback:
                progress_callback(
                    f"Error fetching {page_label}: {exc.reason}. "
                    f"Sleeping {sleep_seconds:.1f} seconds before retry "
                    f"{retry_index + 1}/{max_retries}..."
                )
            sleeper(sleep_seconds)

    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Massive ticker reference endpoint returned non-JSON response") from exc

    if not isinstance(payload, dict):
        raise RuntimeError("Massive ticker reference response was not a JSON object")
    return payload


def fetch_massive_ticker_pages(
    api_key: str,
    market: str = "stocks",
    active: bool = True,
    locale: str = "us",
    limit: int = 1000,
    page_fetcher: Callable[[str], dict[str, Any]] | None = None,
    max_retries: int = 5,
    base_sleep_seconds: float = 2.0,
    max_sleep_seconds: float = 60.0,
    sleeper: Callable[[float], Any] = time.sleep,
    progress_callback: Callable[[str], Any] | None = None,
) -> list[dict[str, Any]]:
    if limit < 1:
        raise ValueError("limit must be >= 1")

    def fetcher(fetch_url: str, page_number: int) -> dict[str, Any]:
        if page_fetcher:
            return page_fetcher(fetch_url)
        return fetch_massive_ticker_page(
            fetch_url,
            max_retries=max_retries,
            base_sleep_seconds=base_sleep_seconds,
            max_sleep_seconds=max_sleep_seconds,
            sleeper=sleeper,
            progress_callback=progress_callback,
            page_number=page_number,
        )

    url = build_tickers_url(
        api_key=api_key,
        market=market,
        active=active,
        locale=locale,
        limit=limit,
    )
    pages: list[dict[str, Any]] = []
    seen_urls: set[str] = set()

    page_number = 1
    while url:
        if url in seen_urls:
            safe_url = sanitize_api_key_from_url(url)
            raise RuntimeError(f"Massive ticker pagination loop detected at {safe_url}")
        seen_urls.add(url)

        if progress_callback:
            progress_callback(f"Fetching Massive ticker reference page {page_number}...")
        payload = fetcher(url, page_number)
        pages.append(payload)

        next_url = payload.get("next_url")
        if progress_callback:
            row_count = len(payload_results(payload))
            progress_callback(
                f"Fetched page {page_number}: "
                f"rows={row_count} next_page={str(isinstance(next_url, str) and bool(next_url.strip())).lower()}"
            )
        if not isinstance(next_url, str) or not next_url.strip():
            break
        url = ensure_api_key_param(next_url.strip(), api_key)
        page_number += 1

    return pages


def clean_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def clean_upper_string(value: Any) -> str | None:
    text = clean_string(value)
    return text.upper() if text else None


def clean_lower_string(value: Any) -> str | None:
    text = clean_string(value)
    return text.lower() if text else None


def payload_results(payload: dict[str, Any]) -> list[dict[str, Any]]:
    results = payload.get("results", [])
    if not isinstance(results, list):
        return []
    return [row for row in results if isinstance(row, dict)]


def parse_optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    return parse_bool(value)


def is_common_stock_type(
    type_code: str | None,
    allowed_type_codes: Container[str] | None = None,
) -> bool:
    if type_code is None:
        return False
    allowed = allowed_type_codes or DEFAULT_COMMON_STOCK_TYPE_CODES
    return type_code.strip().upper() in {code.upper() for code in allowed}


def is_common_stock_candidate(
    row: dict[str, Any],
    allowed_type_codes: Container[str] | None = None,
) -> bool:
    return (
        row.get("market") == "stocks"
        and row.get("locale") == "us"
        and row.get("active") is True
        and is_common_stock_type(row.get("type"), allowed_type_codes)
    )


def normalize_massive_ticker_payloads(
    payloads: list[dict[str, Any]],
    ingested_at: str | None = None,
    allowed_common_stock_type_codes: Container[str] | None = None,
) -> list[dict[str, Any]]:
    timestamp = ingested_at or utc_timestamp()
    normalized: list[dict[str, Any]] = []

    for payload in payloads:
        for raw in payload_results(payload):
            symbol = clean_upper_string(raw.get("ticker") or raw.get("symbol"))
            if not symbol:
                continue

            row = {
                "symbol": symbol,
                "name": clean_string(raw.get("name")),
                "market": clean_lower_string(raw.get("market")),
                "locale": clean_lower_string(raw.get("locale")),
                "type": clean_upper_string(raw.get("type")),
                "active": parse_optional_bool(raw.get("active")),
                "currency_name": clean_string(raw.get("currency_name")),
                "primary_exchange": clean_string(raw.get("primary_exchange")),
                "cik": clean_string(raw.get("cik")),
                "composite_figi": clean_string(raw.get("composite_figi")),
                "share_class_figi": clean_string(raw.get("share_class_figi")),
                "last_updated_utc": clean_string(raw.get("last_updated_utc")),
                "ingested_at": timestamp,
                "source": SOURCE,
            }
            row["is_common_stock_candidate"] = is_common_stock_candidate(
                row,
                allowed_type_codes=allowed_common_stock_type_codes,
            )
            normalized.append(row)

    return normalized


def rows_to_table(rows: list[dict[str, Any]]) -> pa.Table:
    arrays = []
    for field in NORMALIZED_SCHEMA:
        values = [row.get(field.name) for row in rows]
        arrays.append(pa.array(values, type=field.type))
    return pa.Table.from_arrays(arrays, schema=NORMALIZED_SCHEMA)


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
