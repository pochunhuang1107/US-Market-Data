#!/usr/bin/env python3
# ruff: noqa: E402
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import boto3

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from us_market_data.s3_paths import curated_ticker_reference_key, raw_ticker_reference_key
from us_market_data.ticker_reference import (
    NORMALIZED_COLUMNS,
    NORMALIZED_SCHEMA,
    fetch_massive_ticker_pages,
    normalize_massive_ticker_payloads,
    parse_bool,
    resolve_massive_api_key,
    rows_to_table,
    utc_date,
    write_curated_parquet_local,
    write_curated_parquet_s3,
    write_raw_json_local,
    write_raw_json_s3,
)

__all__ = [
    "NORMALIZED_COLUMNS",
    "NORMALIZED_SCHEMA",
    "fetch_massive_ticker_pages",
    "normalize_massive_ticker_payloads",
    "parse_bool",
    "resolve_massive_api_key",
    "rows_to_table",
    "write_curated_parquet_local",
    "write_curated_parquet_s3",
    "write_raw_json_local",
    "write_raw_json_s3",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch Massive ticker reference metadata and write raw JSON pages plus curated parquet."
        )
    )
    parser.add_argument("--bucket", default="us-market-data-casper")
    parser.add_argument("--raw-prefix", default="raw/reference/massive_tickers")
    parser.add_argument("--curated-key", default="curated/reference/ticker_reference/data.parquet")
    parser.add_argument("--date", default=None, help="UTC date YYYY-MM-DD; defaults to today")
    parser.add_argument("--market", default="stocks")
    parser.add_argument("--active", default=True, type=parse_arg_bool)
    parser.add_argument("--locale", default="us")
    parser.add_argument("--limit", default=1000, type=int)
    parser.add_argument("--max-retries", default=5, type=int)
    parser.add_argument("--base-sleep-seconds", default=2.0, type=float)
    parser.add_argument("--max-sleep-seconds", default=60.0, type=float)
    parser.add_argument(
        "--common-stock-type",
        action="append",
        default=["CS"],
        help="Allowed Massive ticker type for common-stock candidates; repeat to allow more",
    )
    parser.add_argument(
        "--local-output-dir",
        default=None,
        help="Write under this local directory instead of S3, mirroring S3 key layout",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress page fetch and retry progress logs",
    )
    return parser.parse_args()


def parse_arg_bool(value: object) -> bool:
    try:
        return parse_bool(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def main() -> int:
    args = parse_args()
    date_str = args.date or utc_date()
    api_key = resolve_massive_api_key()
    progress_callback = None if args.quiet else lambda message: print(message, flush=True)

    payloads = fetch_massive_ticker_pages(
        api_key=api_key,
        market=args.market,
        active=args.active,
        locale=args.locale,
        limit=args.limit,
        max_retries=args.max_retries,
        base_sleep_seconds=args.base_sleep_seconds,
        max_sleep_seconds=args.max_sleep_seconds,
        progress_callback=progress_callback,
    )
    rows = normalize_massive_ticker_payloads(
        payloads,
        allowed_common_stock_type_codes=args.common_stock_type,
    )
    table = rows_to_table(rows)
    curated_key = curated_ticker_reference_key(args.curated_key)

    if args.local_output_dir:
        for page, payload in enumerate(payloads, start=1):
            raw_key = raw_ticker_reference_key(args.raw_prefix, date_str, page)
            path = write_raw_json_local(args.local_output_dir, raw_key, payload)
            print(f"wrote raw local: {path}", flush=True)

        path = write_curated_parquet_local(args.local_output_dir, curated_key, table)
        print(f"wrote curated local: {path} rows={table.num_rows}", flush=True)
    else:
        s3_client = boto3.client("s3")
        for page, payload in enumerate(payloads, start=1):
            raw_key = raw_ticker_reference_key(args.raw_prefix, date_str, page)
            write_raw_json_s3(s3_client, args.bucket, raw_key, payload)
            print(f"wrote raw s3: s3://{args.bucket}/{raw_key}", flush=True)

        write_curated_parquet_s3(s3_client, args.bucket, curated_key, table)
        print(f"wrote curated s3: s3://{args.bucket}/{curated_key} rows={table.num_rows}", flush=True)

    print(
        "massive ticker reference ingest complete: "
        f"date={date_str} market={args.market} locale={args.locale} "
        f"active={args.active} pages={len(payloads)} rows={table.num_rows}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
