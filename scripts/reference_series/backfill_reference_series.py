#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import io
import shutil
import subprocess
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from http.client import RemoteDisconnected
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

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

CBOE_SYMBOLS = {"VIX", "VIX9D", "VXEEM"}
FRED_SYMBOLS = {"DGS10", "DGS2", "T10Y2Y", "DFF"}

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


@dataclass(frozen=True)
class SeriesFetchResult:
    symbol: str
    source: str
    url: str
    normalized_rows: list[dict[str, object]]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill reference-series data to curated S3")
    parser.add_argument("--symbols", required=True, help="Comma-separated symbols")
    parser.add_argument("--start-date", required=True, help="Inclusive YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="Inclusive YYYY-MM-DD")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--region", default="us-west-2")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def parse_symbols(value: str) -> list[str]:
    symbols = [part.strip().upper() for part in value.split(",") if part.strip()]
    unsupported = [symbol for symbol in symbols if symbol not in PROVIDER_BY_SYMBOL]
    if unsupported:
        supported = ",".join(sorted(PROVIDER_BY_SYMBOL))
        raise ValueError(f"unsupported symbols: {','.join(unsupported)}; supported: {supported}")
    return symbols


def parse_iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"invalid date {value!r}; expected YYYY-MM-DD") from exc


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


def fetch_url_with_curl(url: str) -> bytes:
    curl_path = shutil.which("curl")
    if curl_path is None:
        raise RuntimeError("curl is not available for fallback provider fetch")
    result = subprocess.run(
        [
            curl_path,
            "-fsSL",
            "--http1.1",
            "--max-time",
            "60",
            url,
        ],
        check=False,
        capture_output=True,
    )
    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"curl failed fetching {url}: {stderr or result.returncode}")
    return result.stdout


def fetch_url(url: str) -> bytes:
    request = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 us-market-data-reference-series/0.1",
            "Accept": "text/csv,application/csv,text/plain,*/*",
            "Connection": "close",
        },
    )
    try:
        with urlopen(request, timeout=20) as response:
            return response.read()
    except HTTPError as exc:
        raise RuntimeError(f"HTTP {exc.code} fetching {url}") from exc
    except (RemoteDisconnected, TimeoutError, URLError):
        return fetch_url_with_curl(url)


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


def fetch_cboe_series(symbol: str, start_date: date, end_date: date) -> SeriesFetchResult:
    symbol = symbol.upper()
    url = CBOE_CSV_URLS[symbol]
    raw_bytes = fetch_url(url)
    rows = normalize_cboe_rows(csv_rows(raw_bytes), symbol, start_date, end_date)
    return SeriesFetchResult(
        symbol=symbol,
        source="cboe",
        url=url,
        normalized_rows=rows,
    )


def fetch_fred_series(symbol: str, start_date: date, end_date: date) -> SeriesFetchResult:
    symbol = symbol.upper()
    url = (
        f"{FRED_CSV_URL}?"
        f"{urlencode({'id': symbol, 'cosd': start_date.isoformat(), 'coed': end_date.isoformat()})}"
    )
    raw_bytes = fetch_url(url)
    rows = normalize_fred_rows(csv_rows(raw_bytes), symbol, start_date, end_date)
    return SeriesFetchResult(
        symbol=symbol,
        source="fred",
        url=url,
        normalized_rows=rows,
    )


def fetch_reference_series(symbol: str, start_date: date, end_date: date) -> SeriesFetchResult:
    provider = PROVIDER_BY_SYMBOL[symbol.upper()]
    if provider == "cboe":
        return fetch_cboe_series(symbol, start_date, end_date)
    if provider == "fred":
        return fetch_fred_series(symbol, start_date, end_date)
    raise ValueError(f"unsupported provider {provider!r} for {symbol}")


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


def curated_key(symbol: str, year: int) -> str:
    return f"curated/reference_series/symbol={symbol.upper()}/year={year}/data.parquet"


def metadata_key() -> str:
    return "curated/reference/reference_series_metadata/data.parquet"


def object_exists(s3_client, bucket: str, key: str) -> bool:
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def rows_to_table(rows: list[dict[str, object]]) -> pa.Table:
    arrays = []
    for field in NORMALIZED_SCHEMA:
        values = [row.get(field.name) for row in rows]
        arrays.append(pa.array(values, type=field.type))
    return pa.Table.from_arrays(arrays, schema=NORMALIZED_SCHEMA)


def metadata_rows_to_table(rows: list[dict[str, object]]) -> pa.Table:
    arrays = []
    for field in METADATA_SCHEMA:
        values = [row.get(field.name) for row in rows]
        arrays.append(pa.array(values, type=field.type))
    return pa.Table.from_arrays(arrays, schema=METADATA_SCHEMA)


def align_metadata_table(table: pa.Table) -> pa.Table:
    arrays = []
    for field in METADATA_SCHEMA:
        if field.name in table.column_names:
            arrays.append(table[field.name].cast(field.type))
        else:
            arrays.append(pa.array([None] * table.num_rows, type=field.type))
    return pa.Table.from_arrays(arrays, schema=METADATA_SCHEMA)


def table_to_metadata_rows(table: pa.Table) -> list[dict[str, object]]:
    table = align_metadata_table(table)
    columns = {name: table[name].to_pylist() for name in METADATA_SCHEMA.names}
    return [
        {name: columns[name][idx] for name in METADATA_SCHEMA.names}
        for idx in range(table.num_rows)
    ]


def sort_dedupe_rows(rows: Iterable[dict[str, object]]) -> list[dict[str, object]]:
    by_date: dict[str, dict[str, object]] = {}
    for row in rows:
        date_str = row.get("date")
        if not isinstance(date_str, str):
            continue
        by_date[date_str] = {column: row.get(column) for column in NORMALIZED_COLUMNS}
    return [by_date[date_str] for date_str in sorted(by_date)]


def group_rows_by_year(rows: Iterable[dict[str, object]]) -> dict[int, list[dict[str, object]]]:
    grouped: dict[int, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        row_date = parse_iso_date(str(row["date"]))
        grouped[row_date.year].append(row)
    return dict(grouped)


def write_table(s3_client, bucket: str, key: str, table: pa.Table) -> None:
    out = io.BytesIO()
    pq.write_table(table, out, compression="snappy")
    out.seek(0)
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=out.getvalue(),
        ContentType="application/octet-stream",
    )


def write_curated(
    s3_client,
    bucket: str,
    result: SeriesFetchResult,
    dry_run: bool,
    overwrite: bool,
) -> list[str]:
    keys = []
    rows_by_year = group_rows_by_year(sort_dedupe_rows(result.normalized_rows))
    for year, rows in sorted(rows_by_year.items()):
        key = curated_key(result.symbol, year)
        keys.append(key)
        if dry_run:
            print(f"dry-run curated: s3://{bucket}/{key} rows={len(rows)}")
            continue
        if not overwrite and object_exists(s3_client, bucket, key):
            print(f"skip existing curated: s3://{bucket}/{key} rows={len(rows)}")
            continue
        write_table(s3_client, bucket, key, rows_to_table(rows))
        print(f"wrote curated: s3://{bucket}/{key} rows={len(rows)}")
    return keys


def metadata_row_for_result(result: SeriesFetchResult, updated_at: str) -> dict[str, object] | None:
    rows = sort_dedupe_rows(result.normalized_rows)
    if not rows:
        return None
    dates = [str(row["date"]) for row in rows]
    series_types = [row.get("series_type") for row in rows if row.get("series_type")]
    return {
        "symbol": result.symbol,
        "source": result.source,
        "series_type": str(series_types[-1]) if series_types else None,
        "first_available_date": min(dates),
        "last_available_date": max(dates),
        "row_count": len(rows),
        "updated_at": updated_at,
    }


def load_existing_metadata_rows(s3_client, bucket: str) -> list[dict[str, object]]:
    key = metadata_key()
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return []
        raise
    return table_to_metadata_rows(pq.read_table(io.BytesIO(response["Body"].read())))


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


def update_metadata(
    s3_client,
    bucket: str,
    metadata_rows: list[dict[str, object]],
    dry_run: bool,
) -> None:
    key = metadata_key()
    if dry_run:
        print(f"dry-run metadata: s3://{bucket}/{key} rows={len(metadata_rows)}")
        return
    existing_rows = load_existing_metadata_rows(s3_client, bucket)
    merged_rows = merge_metadata_rows(existing_rows, metadata_rows)
    write_table(s3_client, bucket, key, metadata_rows_to_table(merged_rows))
    print(f"wrote metadata: s3://{bucket}/{key} rows={len(merged_rows)}")


def print_fetch_summary(result: SeriesFetchResult) -> None:
    dates = [str(row["date"]) for row in result.normalized_rows]
    first_date = dates[0] if dates else "n/a"
    last_date = dates[-1] if dates else "n/a"
    print(
        "fetched "
        f"symbol={result.symbol} source={result.source} rows={len(result.normalized_rows)} "
        f"first_date={first_date} last_date={last_date} url={result.url}"
    )


def main() -> int:
    args = parse_args()
    try:
        symbols = parse_symbols(args.symbols)
        start_date = parse_iso_date(args.start_date)
        end_date = parse_iso_date(args.end_date)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    if start_date > end_date:
        print("--start-date must be on or before --end-date", file=sys.stderr)
        return 2

    s3_client = boto3.client("s3", region_name=args.region)

    total_rows = 0
    total_curated_files = 0
    metadata_rows = []
    updated_at = datetime.now(timezone.utc).isoformat()

    for symbol in symbols:
        try:
            result = fetch_reference_series(symbol, start_date, end_date)
        except RuntimeError as exc:
            print(f"failed symbol={symbol}: {exc}", file=sys.stderr)
            return 1
        print_fetch_summary(result)
        total_rows += len(result.normalized_rows)
        keys = write_curated(
            s3_client,
            args.bucket,
            result,
            dry_run=args.dry_run,
            overwrite=args.overwrite,
        )
        total_curated_files += len(keys)
        metadata_row = metadata_row_for_result(result, updated_at)
        if metadata_row is not None:
            metadata_rows.append(metadata_row)

    if metadata_rows:
        update_metadata(s3_client, args.bucket, metadata_rows, dry_run=args.dry_run)

    mode = "dry-run" if args.dry_run else "write"
    print(
        f"reference-series backfill complete: mode={mode}, "
        f"symbols={len(symbols)}, rows={total_rows}, curated_files={total_curated_files}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
