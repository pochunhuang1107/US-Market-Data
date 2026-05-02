from __future__ import annotations

import csv
import io
import json
import os
import shutil
import subprocess
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from http.client import RemoteDisconnected
from typing import Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

NORMALIZED_COLUMNS = [
    "date",
    "symbol",
    "source",
    "series_type",
    "open",
    "high",
    "low",
    "close",
    "value",
    "unit",
]

NORMALIZED_SCHEMA = pa.schema(
    [
        pa.field("date", pa.string()),
        pa.field("symbol", pa.string()),
        pa.field("source", pa.string()),
        pa.field("series_type", pa.string()),
        pa.field("open", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("close", pa.float64()),
        pa.field("value", pa.float64()),
        pa.field("unit", pa.string()),
    ]
)

METADATA_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string()),
        pa.field("source", pa.string()),
        pa.field("series_type", pa.string()),
        pa.field("first_available_date", pa.string()),
        pa.field("last_available_date", pa.string()),
        pa.field("row_count", pa.int64()),
        pa.field("updated_at", pa.string()),
    ]
)

CBOE_SYMBOLS = {"VIX", "VIX9D", "VXEEM"}
FRED_SYMBOLS = {"DGS10", "DGS2", "T10Y2Y", "DFF"}
DEFAULT_REFERENCE_SYMBOLS = ["VIX", "VIX9D", "VXEEM", "DGS10", "DGS2", "T10Y2Y", "DFF"]

PROVIDER_BY_SYMBOL = {
    **{symbol: "cboe" for symbol in CBOE_SYMBOLS},
    **{symbol: "fred" for symbol in FRED_SYMBOLS},
}

CBOE_CSV_URLS = {
    symbol: f"https://cdn.cboe.com/api/global/us_indices/daily_prices/{symbol}_History.csv"
    for symbol in CBOE_SYMBOLS
}

FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"

FRED_SERIES_TYPES = {
    "DGS10": "treasury_yield",
    "DGS2": "treasury_yield",
    "T10Y2Y": "yield_spread",
    "DFF": "policy_rate",
}

DEFAULT_CURATED_PREFIX = "curated/reference_series"
DEFAULT_METADATA_KEY = "curated/reference/reference_series_metadata/data.parquet"
DEFAULT_HTTP_TIMEOUT_SECONDS = 30.0


@dataclass(frozen=True)
class SeriesFetchResult:
    symbol: str
    source: str
    url: str
    normalized_rows: list[dict[str, object]]


@dataclass(frozen=True)
class LambdaConfig:
    bucket_name: str
    curated_prefix: str
    metadata_key: str
    sns_topic_arn: str | None


@dataclass(frozen=True)
class UpdateRequest:
    symbols: list[str]
    start_date: date
    end_date: date
    dry_run: bool


def get_s3_client():
    return boto3.client("s3")


def get_sns_client():
    return boto3.client("sns")


def current_new_york_date() -> date:
    return datetime.now(ZoneInfo("America/New_York")).date()


def parse_iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"invalid date {value!r}; expected YYYY-MM-DD") from exc


def parse_bool(value: object, *, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "y"}:
            return True
        if normalized in {"0", "false", "no", "n"}:
            return False
    raise ValueError(f"invalid boolean value {value!r}")


def parse_http_timeout_seconds(value: object | None) -> float:
    if value is None or str(value).strip() == "":
        return DEFAULT_HTTP_TIMEOUT_SECONDS
    try:
        timeout_seconds = float(str(value).strip())
    except ValueError as exc:
        raise ValueError(
            f"HTTP_TIMEOUT_SECONDS must be a positive number, got {value!r}"
        ) from exc
    if timeout_seconds <= 0:
        raise ValueError(f"HTTP_TIMEOUT_SECONDS must be positive, got {value!r}")
    return timeout_seconds


def resolve_http_timeout_seconds(environ: dict[str, str] | None = None) -> float:
    environ = os.environ if environ is None else environ
    return parse_http_timeout_seconds(environ.get("HTTP_TIMEOUT_SECONDS"))


def parse_symbols(value: object) -> list[str]:
    if isinstance(value, str):
        parts = value.split(",")
    elif isinstance(value, Iterable):
        parts = [str(part) for part in value]
    else:
        raise ValueError("symbols must be a comma-separated string or list")

    symbols = [part.strip().upper() for part in parts if part and part.strip()]
    if not symbols:
        raise ValueError("at least one reference-series symbol is required")

    unsupported = [symbol for symbol in symbols if symbol not in PROVIDER_BY_SYMBOL]
    if unsupported:
        supported = ",".join(DEFAULT_REFERENCE_SYMBOLS)
        raise ValueError(f"unsupported symbols: {','.join(unsupported)}; supported: {supported}")
    return symbols


def resolve_update_request(event: Any, environ: dict[str, str] | None = None) -> UpdateRequest:
    environ = os.environ if environ is None else environ
    event = event if isinstance(event, dict) else {}

    symbol_value = event.get("symbols")
    if symbol_value is None:
        symbol_value = environ.get("REFERENCE_SYMBOLS", ",".join(DEFAULT_REFERENCE_SYMBOLS))
    symbols = parse_symbols(symbol_value)

    end_date = (
        parse_iso_date(event["end_date"])
        if event.get("end_date")
        else current_new_york_date()
    )
    start_date = (
        parse_iso_date(event["start_date"])
        if event.get("start_date")
        else date(end_date.year, 1, 1)
    )
    if start_date > end_date:
        raise ValueError("start_date must be on or before end_date")

    return UpdateRequest(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        dry_run=parse_bool(event.get("dry_run"), default=False),
    )


def resolve_config(environ: dict[str, str] | None = None) -> LambdaConfig:
    environ = os.environ if environ is None else environ
    bucket_name = environ.get("BUCKET_NAME")
    if not bucket_name:
        raise ValueError("BUCKET_NAME environment variable is required")
    return LambdaConfig(
        bucket_name=bucket_name,
        curated_prefix=environ.get("CURATED_PREFIX", DEFAULT_CURATED_PREFIX).strip("/"),
        metadata_key=environ.get("METADATA_KEY", DEFAULT_METADATA_KEY),
        sns_topic_arn=environ.get("SNS_TOPIC_ARN"),
    )


def curated_key(symbol: str, year: int, curated_prefix: str = DEFAULT_CURATED_PREFIX) -> str:
    return f"{curated_prefix.strip('/')}/symbol={symbol.upper()}/year={year}/data.parquet"


def metadata_key(environ: dict[str, str] | None = None) -> str:
    environ = os.environ if environ is None else environ
    return environ.get("METADATA_KEY", DEFAULT_METADATA_KEY)


def parse_observation_date(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def parse_float(value: object) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if not text or text == ".":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def csv_rows(raw_bytes: bytes) -> list[dict[str, str]]:
    text = raw_bytes.decode("utf-8-sig")
    return list(csv.DictReader(io.StringIO(text)))


def find_column(row: dict[str, object], names: Iterable[str]) -> str | None:
    wanted = {name.lower() for name in names}
    for column in row:
        if column.lower() in wanted:
            return column
    return None


def filter_sort_dedupe_rows(
    rows: Iterable[dict[str, object]], start_date: date, end_date: date
) -> list[dict[str, object]]:
    by_date: dict[str, dict[str, object]] = {}
    for row in rows:
        date_str = row.get("date")
        if not isinstance(date_str, str):
            continue
        row_date = parse_iso_date(date_str)
        if row_date < start_date or row_date > end_date:
            continue
        if row.get("value") is None:
            continue
        by_date[date_str] = {column: row.get(column) for column in NORMALIZED_COLUMNS}
    return [by_date[date_str] for date_str in sorted(by_date)]


def normalize_cboe_rows(
    raw_rows: Iterable[dict[str, object]], symbol: str, start_date: date, end_date: date
) -> list[dict[str, object]]:
    symbol = symbol.upper()
    normalized = []
    for row in raw_rows:
        date_col = find_column(row, ["date"])
        close_col = find_column(row, ["close"])
        if date_col is None or close_col is None:
            continue
        date_str = parse_observation_date(row.get(date_col))
        close = parse_float(row.get(close_col))
        if date_str is None or close is None:
            continue
        open_col = find_column(row, ["open"])
        high_col = find_column(row, ["high"])
        low_col = find_column(row, ["low"])
        normalized.append(
            {
                "date": date_str,
                "symbol": symbol,
                "source": "cboe",
                "series_type": "volatility_index",
                "open": parse_float(row.get(open_col)) if open_col else None,
                "high": parse_float(row.get(high_col)) if high_col else None,
                "low": parse_float(row.get(low_col)) if low_col else None,
                "close": close,
                "value": close,
                "unit": "index_level",
            }
        )
    return filter_sort_dedupe_rows(normalized, start_date, end_date)


def normalize_fred_rows(
    raw_rows: Iterable[dict[str, object]], symbol: str, start_date: date, end_date: date
) -> list[dict[str, object]]:
    symbol = symbol.upper()
    normalized = []
    for row in raw_rows:
        date_col = find_column(row, ["observation_date", "date"])
        value_col = find_column(row, [symbol])
        if date_col is None or value_col is None:
            continue
        date_str = parse_observation_date(row.get(date_col))
        value = parse_float(row.get(value_col))
        if date_str is None or value is None:
            continue
        normalized.append(
            {
                "date": date_str,
                "symbol": symbol,
                "source": "fred",
                "series_type": FRED_SERIES_TYPES[symbol],
                "open": None,
                "high": None,
                "low": None,
                "close": value,
                "value": value,
                "unit": "percent",
            }
        )
    return filter_sort_dedupe_rows(normalized, start_date, end_date)


def fetch_url(
    url: str,
    *,
    source: str | None = None,
    symbol: str | None = None,
    timeout_seconds: float | None = None,
) -> bytes:
    timeout_seconds = (
        resolve_http_timeout_seconds() if timeout_seconds is None else timeout_seconds
    )
    timeout_seconds = parse_http_timeout_seconds(timeout_seconds)
    request = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 us-market-data-reference-series-lambda/0.1",
            "Accept": "text/csv,application/csv,text/plain,*/*",
            "Connection": "close",
        },
    )
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            return response.read()
    except HTTPError as exc:
        raise RuntimeError(
            provider_fetch_error_message(source, symbol, url, timeout_seconds, exc)
        ) from exc
    except (RemoteDisconnected, TimeoutError, URLError) as exc:
        try:
            return fetch_url_with_curl(
                url,
                source=source,
                symbol=symbol,
                timeout_seconds=timeout_seconds,
            )
        except RuntimeError as curl_exc:
            detail = f"{exc}; curl fallback failed: {curl_exc}"
            raise RuntimeError(
                provider_fetch_error_message(source, symbol, url, timeout_seconds, detail)
            ) from exc


def fetch_url_with_curl(
    url: str,
    *,
    source: str | None = None,
    symbol: str | None = None,
    timeout_seconds: float,
) -> bytes:
    curl_path = shutil.which("curl")
    if curl_path is None:
        raise RuntimeError("curl is not available for fallback provider fetch")

    result = subprocess.run(
        [
            curl_path,
            "-fsSL",
            "--http1.1",
            "--max-time",
            format_timeout_seconds(timeout_seconds),
            url,
        ],
        check=False,
        capture_output=True,
    )
    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        detail = stderr or str(result.returncode)
        raise RuntimeError(f"curl failed: {detail}")
    return result.stdout


def provider_fetch_error_message(
    source: str | None,
    symbol: str | None,
    url: str,
    timeout_seconds: float,
    exc: object,
) -> str:
    context = []
    if source:
        context.append(str(source).upper())
    if symbol:
        context.append(f"symbol={symbol.upper()}")
    context.append(f"url={url}")
    context.append(f"timeout_seconds={format_timeout_seconds(timeout_seconds)}")
    return f"failed fetching {' '.join(context)}: {exc}"


def format_timeout_seconds(timeout_seconds: float) -> str:
    normalized = float(timeout_seconds)
    if normalized.is_integer():
        return str(int(normalized))
    return str(normalized)


def fetch_cboe_series(symbol: str, start_date: date, end_date: date) -> SeriesFetchResult:
    symbol = symbol.upper()
    url = CBOE_CSV_URLS[symbol]
    rows = normalize_cboe_rows(
        csv_rows(fetch_url(url, source="cboe", symbol=symbol)),
        symbol,
        start_date,
        end_date,
    )
    return SeriesFetchResult(symbol=symbol, source="cboe", url=url, normalized_rows=rows)


def fetch_fred_series(symbol: str, start_date: date, end_date: date) -> SeriesFetchResult:
    symbol = symbol.upper()
    url = (
        f"{FRED_CSV_URL}?"
        f"{urlencode({'id': symbol, 'cosd': start_date.isoformat(), 'coed': end_date.isoformat()})}"
    )
    rows = normalize_fred_rows(
        csv_rows(fetch_url(url, source="fred", symbol=symbol)),
        symbol,
        start_date,
        end_date,
    )
    return SeriesFetchResult(symbol=symbol, source="fred", url=url, normalized_rows=rows)


def fetch_reference_series(symbol: str, start_date: date, end_date: date) -> SeriesFetchResult:
    provider = PROVIDER_BY_SYMBOL[symbol.upper()]
    if provider == "cboe":
        return fetch_cboe_series(symbol, start_date, end_date)
    if provider == "fred":
        return fetch_fred_series(symbol, start_date, end_date)
    raise ValueError(f"unsupported provider {provider!r} for {symbol}")


def empty_normalized_table() -> pa.Table:
    arrays = [pa.array([], type=field.type) for field in NORMALIZED_SCHEMA]
    return pa.Table.from_arrays(arrays, schema=NORMALIZED_SCHEMA)


def empty_metadata_table() -> pa.Table:
    arrays = [pa.array([], type=field.type) for field in METADATA_SCHEMA]
    return pa.Table.from_arrays(arrays, schema=METADATA_SCHEMA)


def rows_to_table(rows: list[dict[str, object]]) -> pa.Table:
    arrays = []
    for field in NORMALIZED_SCHEMA:
        arrays.append(pa.array([row.get(field.name) for row in rows], type=field.type))
    return pa.Table.from_arrays(arrays, schema=NORMALIZED_SCHEMA)


def metadata_rows_to_table(rows: list[dict[str, object]]) -> pa.Table:
    arrays = []
    for field in METADATA_SCHEMA:
        arrays.append(pa.array([row.get(field.name) for row in rows], type=field.type))
    return pa.Table.from_arrays(arrays, schema=METADATA_SCHEMA)


def align_table_to_schema(table: pa.Table, schema: pa.Schema) -> pa.Table:
    arrays = []
    for field in schema:
        if field.name in table.column_names:
            arrays.append(table[field.name].cast(field.type))
        else:
            arrays.append(pa.array([None] * table.num_rows, type=field.type))
    return pa.Table.from_arrays(arrays, schema=schema)


def table_to_rows(table: pa.Table, schema: pa.Schema) -> list[dict[str, object]]:
    table = align_table_to_schema(table, schema)
    columns = {name: table[name].to_pylist() for name in schema.names}
    return [{name: columns[name][idx] for name in schema.names} for idx in range(table.num_rows)]


def object_exists(s3_client, bucket: str, key: str) -> bool:
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def load_curated_rows(s3_client, bucket: str, key: str) -> list[dict[str, object]]:
    if not object_exists(s3_client, bucket, key):
        return []
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return table_to_rows(pq.read_table(io.BytesIO(response["Body"].read())), NORMALIZED_SCHEMA)


def write_parquet_table(s3_client, bucket: str, key: str, table: pa.Table) -> None:
    out = io.BytesIO()
    pq.write_table(table, out, compression="snappy")
    out.seek(0)
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=out.getvalue(),
        ContentType="application/octet-stream",
    )


def sort_dedupe_rows(rows: Iterable[dict[str, object]]) -> list[dict[str, object]]:
    by_date: dict[str, dict[str, object]] = {}
    for row in rows:
        date_str = row.get("date")
        if not isinstance(date_str, str):
            continue
        by_date[date_str] = {column: row.get(column) for column in NORMALIZED_COLUMNS}
    return [by_date[date_str] for date_str in sorted(by_date)]


def merge_curated_rows(
    existing_rows: Iterable[dict[str, object]],
    new_rows: Iterable[dict[str, object]],
) -> list[dict[str, object]]:
    return sort_dedupe_rows([*existing_rows, *new_rows])


def group_rows_by_year(rows: Iterable[dict[str, object]]) -> dict[int, list[dict[str, object]]]:
    grouped: dict[int, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        row_date = parse_iso_date(str(row["date"]))
        grouped[row_date.year].append(row)
    return dict(grouped)


def list_symbol_curated_keys(
    s3_client,
    bucket: str,
    curated_prefix: str,
    symbol: str,
) -> list[str]:
    prefix = f"{curated_prefix.strip('/')}/symbol={symbol.upper()}/"
    keys: list[str] = []
    continuation_token = None
    while True:
        kwargs: dict[str, object] = {"Bucket": bucket, "Prefix": prefix}
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token
        response = s3_client.list_objects_v2(**kwargs)
        keys.extend(
            item["Key"]
            for item in response.get("Contents", [])
            if str(item.get("Key", "")).endswith("/data.parquet")
        )
        if not response.get("IsTruncated"):
            return sorted(keys)
        continuation_token = response.get("NextContinuationToken")


def load_all_symbol_curated_rows(
    s3_client,
    bucket: str,
    curated_prefix: str,
    symbol: str,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for key in list_symbol_curated_keys(s3_client, bucket, curated_prefix, symbol):
        rows.extend(load_curated_rows(s3_client, bucket, key))
    return sort_dedupe_rows(rows)


def load_metadata_rows(s3_client, bucket: str, key: str) -> list[dict[str, object]]:
    if not object_exists(s3_client, bucket, key):
        return []
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return table_to_rows(pq.read_table(io.BytesIO(response["Body"].read())), METADATA_SCHEMA)


def merge_metadata_rows(
    existing_rows: list[dict[str, object]],
    updated_rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    updated_by_symbol = {str(row["symbol"]).upper(): row for row in updated_rows}
    merged = [
        row
        for row in existing_rows
        if str(row.get("symbol", "")).upper() not in updated_by_symbol
    ]
    merged.extend(updated_by_symbol[symbol] for symbol in sorted(updated_by_symbol))
    return sorted(merged, key=lambda row: str(row.get("symbol", "")))


def metadata_row_for_symbol(
    symbol: str,
    rows: list[dict[str, object]],
    updated_at: str,
) -> dict[str, object] | None:
    curated_rows = sort_dedupe_rows(rows)
    if not curated_rows:
        return None
    dates = [str(row["date"]) for row in curated_rows]
    latest_row = curated_rows[-1]
    return {
        "symbol": symbol.upper(),
        "source": latest_row.get("source"),
        "series_type": latest_row.get("series_type"),
        "first_available_date": min(dates),
        "last_available_date": max(dates),
        "row_count": len(curated_rows),
        "updated_at": updated_at,
    }


def publish_failure(config: LambdaConfig | None, event: Any, exc: Exception) -> None:
    sns_topic_arn = config.sns_topic_arn if config else os.environ.get("SNS_TOPIC_ARN")
    if not sns_topic_arn:
        print(f"reference-series curated update failed: {exc}")
        return
    message = (
        f"Reference-series curated update failed for event={json.dumps(event)}"
        f"\n\nError: {exc}"
    )
    try:
        get_sns_client().publish(
            TopicArn=sns_topic_arn,
            Subject="us-market-data reference-series failure",
            Message=message,
        )
    except Exception as publish_exc:
        print(f"failed to publish SNS failure notification: {publish_exc}")


def update_reference_series(
    event: Any,
    config: LambdaConfig,
    request: UpdateRequest,
) -> dict[str, Any]:
    s3_client = None if request.dry_run else get_s3_client()
    updated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    rows_fetched = 0
    rows_written = 0
    updated_keys: list[str] = []
    symbols_updated: list[str] = []
    skipped_symbols: list[str] = []
    symbol_statuses: dict[str, str] = {}

    for symbol in request.symbols:
        result = fetch_reference_series(symbol, request.start_date, request.end_date)
        rows = sort_dedupe_rows(result.normalized_rows)
        rows_fetched += len(rows)

        if not rows:
            skipped_symbols.append(symbol)
            symbol_statuses[symbol] = "no_new_rows"
            print(f"skip symbol={symbol}: no provider rows in requested date range")
            continue

        symbols_updated.append(symbol)
        symbol_statuses[symbol] = "planned" if request.dry_run else "written"
        for year, new_rows in sorted(group_rows_by_year(rows).items()):
            key = curated_key(symbol, year, config.curated_prefix)
            updated_keys.append(key)
            if request.dry_run:
                print(f"dry-run curated: s3://{config.bucket_name}/{key} rows={len(new_rows)}")
                continue

            existing_rows = load_curated_rows(s3_client, config.bucket_name, key)
            merged_rows = merge_curated_rows(existing_rows, new_rows)
            write_parquet_table(s3_client, config.bucket_name, key, rows_to_table(merged_rows))
            rows_written += len(merged_rows)
            print(f"wrote curated: s3://{config.bucket_name}/{key} rows={len(merged_rows)}")

    if not request.dry_run and symbols_updated:
        metadata_updates = []
        for symbol in symbols_updated:
            symbol_rows = load_all_symbol_curated_rows(
                s3_client,
                config.bucket_name,
                config.curated_prefix,
                symbol,
            )
            metadata_row = metadata_row_for_symbol(symbol, symbol_rows, updated_at)
            if metadata_row is not None:
                metadata_updates.append(metadata_row)

        if metadata_updates:
            existing_metadata = load_metadata_rows(
                s3_client,
                config.bucket_name,
                config.metadata_key,
            )
            merged_metadata = merge_metadata_rows(existing_metadata, metadata_updates)
            write_parquet_table(
                s3_client,
                config.bucket_name,
                config.metadata_key,
                metadata_rows_to_table(merged_metadata),
            )
            print(f"wrote metadata: s3://{config.bucket_name}/{config.metadata_key}")

    status = "DRY_RUN" if request.dry_run else "SUCCEEDED"
    if not symbols_updated:
        status = "NO_DATA"

    return {
        "statusCode": 200,
        "status": status,
        "symbols_requested": request.symbols,
        "symbols_updated": symbols_updated,
        "skipped_symbols": skipped_symbols,
        "symbol_statuses": symbol_statuses,
        "rows_fetched": rows_fetched,
        "rows_written": rows_written,
        "updated_keys": updated_keys,
        "metadata_key": config.metadata_key,
        "dry_run": request.dry_run,
        "start_date": request.start_date.isoformat(),
        "end_date": request.end_date.isoformat(),
    }


def lambda_handler(event, context):
    config: LambdaConfig | None = None
    try:
        config = resolve_config()
        request = resolve_update_request(event)
        return update_reference_series(event, config, request)
    except Exception as exc:
        publish_failure(config, event, exc)
        raise
