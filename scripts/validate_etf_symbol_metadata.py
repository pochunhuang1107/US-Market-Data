#!/usr/bin/env python3
from __future__ import annotations

import argparse
import io
import re
from collections import defaultdict

import boto3
import pyarrow.compute as pc
import pyarrow.parquet as pq

from us_market_data.etf_metadata import load_metadata_table

KEY_RE = re.compile(r"symbol=([^/]+)/year=(\d{4})/data\.parquet$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate ETF symbol metadata against curated parquet")
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

    metadata = load_metadata_table(s3_client, args.bucket, args.metadata_key)
    metadata_map = {row["symbol"]: row for row in metadata.to_pylist()}

    symbol_ranges: dict[str, list[str]] = defaultdict(lambda: ["9999-12-31", "0000-01-01"])
    for key in list_curated_keys(s3_client, args.bucket, args.curated_prefix):
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
        symbol_ranges[symbol][0] = min(symbol_ranges[symbol][0], first_date)
        symbol_ranges[symbol][1] = max(symbol_ranges[symbol][1], last_date)

    mismatches = []
    for symbol, (first_date, last_date) in sorted(symbol_ranges.items()):
        row = metadata_map.get(symbol)
        if row is None:
            mismatches.append(f"missing metadata row for {symbol}")
            continue
        if row["first_available_date"] != first_date or row["last_available_date"] != last_date:
            mismatches.append(
                f"{symbol}: metadata=({row['first_available_date']}, {row['last_available_date']}) "
                f"curated=({first_date}, {last_date})"
            )

    extra_symbols = sorted(set(metadata_map) - set(symbol_ranges))
    for symbol in extra_symbols:
        mismatches.append(f"metadata has extra symbol not found in curated parquet: {symbol}")

    if mismatches:
        print("metadata validation FAILED")
        for item in mismatches:
            print(f" - {item}")
        raise SystemExit(1)

    print(f"metadata validation passed for {len(symbol_ranges)} symbols")


if __name__ == "__main__":
    main()
