#!/usr/bin/env python3
from __future__ import annotations

import argparse
import io
import re
from collections import defaultdict

import boto3
import pyarrow.compute as pc
import pyarrow.parquet as pq

from us_market_data.etf_metadata import build_metadata_table_from_ranges, write_metadata_table

KEY_RE = re.compile(r"symbol=([^/]+)/year=(\d{4})/data\.parquet$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bootstrap ETF symbol metadata from curated ETF parquet")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--curated-prefix", default="curated/etf_daily_prices")
    parser.add_argument("--metadata-key", default="curated/reference/etf_symbol_metadata/data.parquet")
    return parser.parse_args()


def list_curated_keys(s3_client, bucket: str, prefix: str) -> list[str]:
    paginator = s3_client.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet"):
                keys.append(key)
    return keys


def main() -> None:
    args = parse_args()
    s3_client = boto3.client("s3")
    keys = list_curated_keys(s3_client, args.bucket, args.curated_prefix)

    symbol_ranges: dict[str, list[str]] = defaultdict(lambda: ["9999-12-31", "0000-01-01"])

    for key in keys:
        match = KEY_RE.search(key)
        if not match:
            continue
        symbol = match.group(1)

        response = s3_client.get_object(Bucket=args.bucket, Key=key)
        table = pq.read_table(io.BytesIO(response["Body"].read()), columns=["date"])
        if table.num_rows == 0:
            continue

        first_date = pc.min(table["date"]).as_py()
        last_date = pc.max(table["date"]).as_py()
        current_first, current_last = symbol_ranges[symbol]
        symbol_ranges[symbol][0] = min(current_first, first_date)
        symbol_ranges[symbol][1] = max(current_last, last_date)

    compact_ranges = {symbol: (first, last) for symbol, (first, last) in symbol_ranges.items()}
    metadata_table = build_metadata_table_from_ranges(compact_ranges)
    write_metadata_table(s3_client, args.bucket, args.metadata_key, metadata_table)
    print(f"wrote metadata parquet to s3://{args.bucket}/{args.metadata_key} for {len(compact_ranges)} symbols")


if __name__ == "__main__":
    main()
