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

from us_market_data.apewisdom import (
    NORMALIZED_COLUMNS,
    NORMALIZED_SCHEMA,
    fetch_apewisdom_page,
    normalize_apewisdom_payload,
    parse_bool,
    rows_to_table,
    utc_date_hour,
    write_raw_json_local,
    write_raw_json_s3,
    write_staging_parquet_local,
    write_staging_parquet_s3,
)
from us_market_data.s3_paths import raw_apewisdom_key, staging_reddit_trends_key

__all__ = [
    "NORMALIZED_COLUMNS",
    "NORMALIZED_SCHEMA",
    "fetch_apewisdom_page",
    "normalize_apewisdom_payload",
    "parse_bool",
    "rows_to_table",
    "utc_date_hour",
    "write_raw_json_local",
    "write_raw_json_s3",
    "write_staging_parquet_local",
    "write_staging_parquet_s3",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch ApeWisdom Reddit trend data and write raw JSON plus staging parquet. "
            "ApeWisdom is treated as an attention signal, not a buy/sell signal."
        )
    )
    parser.add_argument("--bucket", default="us-market-data-casper")
    parser.add_argument("--raw-prefix", default="raw/reddit/apewisdom")
    parser.add_argument("--staging-prefix", default="staging/reddit_trends")
    parser.add_argument("--filter", default="all-stocks", dest="filter_name")
    parser.add_argument("--page", default=1, type=int)
    parser.add_argument("--date", default=None, help="UTC date YYYY-MM-DD; defaults to today")
    parser.add_argument("--hour", default=None, help="UTC hour HH; defaults to current hour")
    parser.add_argument("--write-raw", default=True, type=parse_arg_bool)
    parser.add_argument("--write-staging", default=True, type=parse_arg_bool)
    parser.add_argument(
        "--local-output-dir",
        default=None,
        help="Write under this local directory instead of S3, mirroring S3 key layout",
    )
    return parser.parse_args()


def parse_arg_bool(value: object) -> bool:
    try:
        return parse_bool(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def main() -> int:
    args = parse_args()
    default_date, default_hour = utc_date_hour()
    date_str = args.date or default_date
    hour_str = args.hour or default_hour

    payload = fetch_apewisdom_page(args.filter_name, args.page)
    rows = normalize_apewisdom_payload(
        payload,
        date_str=date_str,
        hour_str=hour_str,
        filter_name=args.filter_name,
        page=args.page,
    )
    table = rows_to_table(rows)

    raw_key = raw_apewisdom_key(args.raw_prefix, date_str, hour_str, args.filter_name)
    staging_key = staging_reddit_trends_key(args.staging_prefix, date_str)

    if args.local_output_dir:
        if args.write_raw:
            path = write_raw_json_local(args.local_output_dir, raw_key, payload)
            print(f"wrote raw local: {path}")
        if args.write_staging:
            path = write_staging_parquet_local(args.local_output_dir, staging_key, table)
            print(f"wrote staging local: {path} rows={table.num_rows}")
    else:
        s3_client = boto3.client("s3")
        if args.write_raw:
            write_raw_json_s3(s3_client, args.bucket, raw_key, payload)
            print(f"wrote raw s3: s3://{args.bucket}/{raw_key}")
        if args.write_staging:
            write_staging_parquet_s3(s3_client, args.bucket, staging_key, table)
            print(f"wrote staging s3: s3://{args.bucket}/{staging_key} rows={table.num_rows}")

    print(
        "apewisdom ingest complete: "
        f"filter={args.filter_name} page={args.page} "
        f"date={date_str} hour={hour_str} rows={table.num_rows}"
    )
    print("note: ApeWisdom Reddit trends are an attention signal, not a buy/sell signal")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
