#!/usr/bin/env python3
from __future__ import annotations

import argparse
import io
from collections import defaultdict
from datetime import datetime
from typing import Iterable

import boto3
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from us_market_data.s3_paths import curated_key

SCHEMA = pa.schema(
    [
        pa.field("date", pa.string()),
        pa.field("year", pa.int32()),
        pa.field("month", pa.int32()),
        pa.field("symbol", pa.string()),
        pa.field("open", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("close", pa.float64()),
        pa.field("volume", pa.float64()),
        pa.field("vwap", pa.float64()),
        pa.field("transactions", pa.float64()),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill curated ETF parquet from staging monthly parquet")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--staging-prefix", default="staging/market_bars")
    parser.add_argument("--curated-prefix", default="curated/etf_daily_prices")
    parser.add_argument("--symbols-file", required=True)
    parser.add_argument("--start-month", required=True, help="YYYY-MM")
    parser.add_argument("--end-month", required=True, help="YYYY-MM")
    return parser.parse_args()


def load_symbols(path: str) -> set[str]:
    with open(path, "r", encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip() and not line.startswith("#")}


def month_iter(start_month: str, end_month: str) -> Iterable[tuple[int, int]]:
    current = datetime.strptime(start_month, "%Y-%m")
    end = datetime.strptime(end_month, "%Y-%m")
    while current <= end:
        yield current.year, current.month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)


def staging_key(staging_prefix: str, year: int, month: int) -> str:
    return f"{staging_prefix}/year={year}/month={month:02d}/data.parquet"


def object_exists(s3_client, bucket: str, key: str) -> bool:
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def empty_table() -> pa.Table:
    arrays = [pa.array([], type=field.type) for field in SCHEMA]
    return pa.Table.from_arrays(arrays, schema=SCHEMA)


def align_table_to_schema(table: pa.Table) -> pa.Table:
    arrays = []
    for field in SCHEMA:
        if field.name in table.column_names:
            arrays.append(pc.cast(table[field.name], field.type))
        else:
            arrays.append(pa.array([None] * table.num_rows, type=field.type))
    return pa.Table.from_arrays(arrays, schema=SCHEMA)


def filter_table_for_symbols(table: pa.Table, symbols: set[str]) -> pa.Table:
    if table.num_rows == 0:
        return table
    keep = [symbol in symbols for symbol in table["symbol"].to_pylist()]
    return table.filter(pa.array(keep, type=pa.bool_()))


def split_table_by_symbol_year(table: pa.Table) -> dict[tuple[str, int], pa.Table]:
    if table.num_rows == 0:
        return {}
    groups: dict[tuple[str, int], list[int]] = defaultdict(list)
    for idx, (symbol, year) in enumerate(zip(table["symbol"].to_pylist(), table["year"].to_pylist())):
        groups[(str(symbol), int(year))].append(idx)
    out: dict[tuple[str, int], pa.Table] = {}
    for key, indices in groups.items():
        out[key] = table.take(pa.array(indices, type=pa.int64()))
    return out


def dedupe_and_sort(table: pa.Table) -> pa.Table:
    if table.num_rows == 0:
        return table
    sort_indices = pc.sort_indices(table, sort_keys=[("date", "ascending"), ("symbol", "ascending")])
    sorted_table = table.take(sort_indices)
    seen = set()
    keep = []
    for key in zip(sorted_table["date"].to_pylist(), sorted_table["symbol"].to_pylist()):
        if key in seen:
            keep.append(False)
        else:
            seen.add(key)
            keep.append(True)
    return sorted_table.filter(pa.array(keep, type=pa.bool_()))


def load_existing_curated_table(s3_client, bucket: str, curated_prefix: str, symbol: str, year: int) -> pa.Table:
    key = curated_key(curated_prefix, symbol, year)
    if not object_exists(s3_client, bucket, key):
        return empty_table()
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read()
    table = pq.read_table(io.BytesIO(body))
    return align_table_to_schema(table)


def write_curated_table(s3_client, bucket: str, curated_prefix: str, symbol: str, year: int, table: pa.Table) -> str:
    key = curated_key(curated_prefix, symbol, year)
    out = io.BytesIO()
    pq.write_table(table, out, compression="snappy")
    out.seek(0)
    s3_client.put_object(Bucket=bucket, Key=key, Body=out.getvalue(), ContentType="application/octet-stream")
    return key


def main() -> None:
    args = parse_args()
    s3_client = boto3.client("s3")
    etf_symbols = load_symbols(args.symbols_file)

    total_months = 0
    total_symbols = 0
    total_rows = 0

    for year, month in month_iter(args.start_month, args.end_month):
        key = staging_key(args.staging_prefix, year, month)
        if not object_exists(s3_client, args.bucket, key):
            print(f"skip missing staging file: s3://{args.bucket}/{key}")
            continue

        response = s3_client.get_object(Bucket=args.bucket, Key=key)
        table = pq.read_table(io.BytesIO(response["Body"].read()))
        table = align_table_to_schema(table)
        etf_table = filter_table_for_symbols(table, etf_symbols)

        if etf_table.num_rows == 0:
            print(f"skip no ETF rows: s3://{args.bucket}/{key}")
            continue

        grouped = split_table_by_symbol_year(etf_table)
        for (symbol, out_year), symbol_table in sorted(grouped.items()):
            existing = load_existing_curated_table(s3_client, args.bucket, args.curated_prefix, symbol, out_year)
            merged = dedupe_and_sort(pa.concat_tables([existing, symbol_table], promote_options="default"))
            write_curated_table(s3_client, args.bucket, args.curated_prefix, symbol, out_year, merged)
            total_symbols += 1
            total_rows += symbol_table.num_rows

        total_months += 1
        print(f"processed month {year}-{month:02d}: {len(grouped)} symbol-year groups")

    print(
        f"backfill complete: months={total_months}, symbol_year_writes={total_symbols}, rows={total_rows}"
    )


if __name__ == "__main__":
    main()
