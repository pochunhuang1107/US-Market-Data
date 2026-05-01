#!/usr/bin/env python3
from __future__ import annotations

import argparse
import io
import sys

import boto3
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

SCRIPT_DIR = __import__("pathlib").Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from backfill_reference_series import PROVIDER_BY_SYMBOL  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check reference-series staging parquet in S3")
    parser.add_argument("--symbols", required=True, help="Comma-separated symbols")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--region", default="us-west-2")
    return parser.parse_args()


def parse_symbols(value: str) -> list[str]:
    symbols = [part.strip().upper() for part in value.split(",") if part.strip()]
    unsupported = [symbol for symbol in symbols if symbol not in PROVIDER_BY_SYMBOL]
    if unsupported:
        supported = ",".join(sorted(PROVIDER_BY_SYMBOL))
        raise ValueError(f"unsupported symbols: {','.join(unsupported)}; supported: {supported}")
    return symbols


def staging_prefix(source: str, symbol: str) -> str:
    return f"staging/reference_series/source={source}/symbol={symbol.upper()}/"


def list_parquet_keys(s3_client, bucket: str, prefix: str) -> list[str]:
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/data.parquet"):
                keys.append(key)
    return sorted(keys)


def read_s3_parquet(s3_client, bucket: str, key: str) -> pa.Table:
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return pq.read_table(io.BytesIO(response["Body"].read()))


def concat_tables(tables: list[pa.Table]) -> pa.Table:
    if not tables:
        return pa.table({"date": [], "value": []})
    return pa.concat_tables(tables, promote_options="default")


def scalar_to_python(value):
    return value.as_py() if hasattr(value, "as_py") else value


def summarize_table(symbol: str, source: str, table: pa.Table) -> dict[str, object]:
    if table.num_rows == 0:
        return {
            "symbol": symbol,
            "source": source,
            "row_count": 0,
            "first_date": "n/a",
            "last_date": "n/a",
            "missing_value_count": 0,
            "min_value": "n/a",
            "max_value": "n/a",
        }

    sort_indices = pc.sort_indices(table, sort_keys=[("date", "ascending")])
    table = table.take(sort_indices)
    values = table["value"]
    missing_value_count = pc.sum(pc.is_null(values)).as_py()
    numeric_values = pc.drop_null(values)
    min_value = scalar_to_python(pc.min(numeric_values))
    max_value = scalar_to_python(pc.max(numeric_values))

    return {
        "symbol": symbol,
        "source": source,
        "row_count": table.num_rows,
        "first_date": table["date"][0].as_py(),
        "last_date": table["date"][table.num_rows - 1].as_py(),
        "missing_value_count": missing_value_count,
        "min_value": min_value,
        "max_value": max_value,
    }


def print_summary(summary: dict[str, object]) -> None:
    print(
        "{symbol}\t{source}\t{row_count}\t{first_date}\t{last_date}\t"
        "{missing_value_count}\t{min_value}\t{max_value}".format(**summary)
    )


def main() -> int:
    args = parse_args()
    try:
        symbols = parse_symbols(args.symbols)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    s3_client = boto3.client("s3", region_name=args.region)
    print(
        "symbol\tsource\trow_count\tfirst_date\tlast_date\t"
        "missing_value_count\tmin_value\tmax_value"
    )
    for symbol in symbols:
        source = PROVIDER_BY_SYMBOL[symbol]
        prefix = staging_prefix(source, symbol)
        keys = list_parquet_keys(s3_client, args.bucket, prefix)
        tables = [read_s3_parquet(s3_client, args.bucket, key) for key in keys]
        print_summary(summarize_table(symbol, source, concat_tables(tables)))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
