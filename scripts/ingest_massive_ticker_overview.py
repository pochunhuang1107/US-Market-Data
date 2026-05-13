#!/usr/bin/env python3
# ruff: noqa: E402
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import boto3
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from us_market_data.s3_paths import curated_ticker_overview_key, raw_ticker_overview_key
from us_market_data.ticker_overview import (
    NORMALIZED_COLUMNS,
    NORMALIZED_SCHEMA,
    fetch_massive_ticker_overview,
    merge_ticker_overview_rows,
    normalize_massive_ticker_overview_payload,
    read_curated_parquet_local,
    read_curated_parquet_s3,
    resolve_massive_api_key,
    rows_to_table,
    stale_symbols,
    utc_date,
    write_curated_parquet_local,
    write_curated_parquet_s3,
    write_raw_json_local,
    write_raw_json_s3,
)

__all__ = [
    "NORMALIZED_COLUMNS",
    "NORMALIZED_SCHEMA",
    "collect_symbols",
    "fetch_massive_ticker_overview",
    "merge_ticker_overview_rows",
    "normalize_massive_ticker_overview_payload",
    "read_existing_curated_rows",
    "resolve_massive_api_key",
    "rows_to_table",
    "stale_symbols",
    "write_curated_parquet_local",
    "write_curated_parquet_s3",
    "write_raw_json_local",
    "write_raw_json_s3",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch Massive ticker overview company profiles for selected symbols and write "
            "raw JSON plus curated parquet."
        )
    )
    parser.add_argument("--symbols", default=None, help="Comma-separated symbols, e.g. IONQ,NVDA")
    parser.add_argument("--symbols-file", default=None, help="Text file with one symbol per line")
    parser.add_argument(
        "--from-ticker-reference",
        default=None,
        help="Ticker reference parquet; uses rows where is_common_stock_candidate is true",
    )
    parser.add_argument("--limit", default=None, type=int)
    parser.add_argument(
        "--local-output-dir",
        default=None,
        help="Write under this local directory instead of S3, mirroring S3 key layout",
    )
    parser.add_argument("--s3-bucket", default=None)
    parser.add_argument("--raw-prefix", default="raw/reference/massive_ticker_overview")
    parser.add_argument("--curated-prefix", default="curated/reference/ticker_overview")
    parser.add_argument("--request-interval-seconds", default=0.0, type=float)
    parser.add_argument("--max-retries", default=5, type=int)
    parser.add_argument("--base-sleep-seconds", default=2.0, type=float)
    parser.add_argument("--max-sleep-seconds", default=60.0, type=float)
    parser.add_argument(
        "--merge-existing",
        action="store_true",
        help="Read existing curated ticker overview parquet and upsert fetched rows into it.",
    )
    parser.add_argument(
        "--refresh-after-days",
        default=7,
        type=int,
        help="When --merge-existing is set, refetch cached symbols older than this many days.",
    )
    parser.add_argument(
        "--fetch-limit",
        default=None,
        type=int,
        help="When --merge-existing is set, fetch at most this many missing/stale symbols.",
    )
    return parser.parse_args()


def normalize_symbol(value: object) -> str | None:
    text = str(value).strip().upper()
    return text or None


def parse_symbols_arg(symbols: str) -> list[str]:
    return [symbol for item in symbols.split(",") if (symbol := normalize_symbol(item))]


def read_symbols_file(path: str | Path) -> list[str]:
    symbols: list[str] = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        symbol = normalize_symbol(line)
        if symbol:
            symbols.append(symbol)
    return symbols


def read_symbols_from_ticker_reference(path: str | Path) -> list[str]:
    table = pq.read_table(path, columns=["symbol", "is_common_stock_candidate"])
    symbols = table["symbol"].to_pylist()
    flags = table["is_common_stock_candidate"].to_pylist()
    selected: list[str] = []
    for raw_symbol, is_candidate in zip(symbols, flags, strict=True):
        symbol = normalize_symbol(raw_symbol)
        if is_candidate is True and symbol:
            selected.append(symbol)
    return selected


def dedupe_sort_limit(symbols: list[str], limit: int | None = None) -> list[str]:
    if limit is not None and limit < 0:
        raise ValueError("limit must be >= 0")
    normalized = sorted({symbol for item in symbols if (symbol := normalize_symbol(item))})
    return normalized[:limit] if limit is not None else normalized


def collect_symbols(args: argparse.Namespace) -> list[str]:
    if args.symbols:
        symbols = parse_symbols_arg(args.symbols)
    elif args.symbols_file:
        symbols = read_symbols_file(args.symbols_file)
    elif args.from_ticker_reference:
        symbols = read_symbols_from_ticker_reference(args.from_ticker_reference)
    else:
        raise RuntimeError(
            "provide one symbol source: --symbols, --symbols-file, or --from-ticker-reference"
        )
    return dedupe_sort_limit(symbols, args.limit)


def read_existing_curated_rows(
    args: argparse.Namespace,
    s3_client: object | None,
    curated_key: str,
) -> list[dict[str, object]]:
    if args.local_output_dir:
        return read_curated_parquet_local(args.local_output_dir, curated_key)
    if not s3_client:
        raise RuntimeError("s3_client is required when reading existing curated parquet from S3")
    return read_curated_parquet_s3(s3_client, args.s3_bucket, curated_key)


def fetch_and_write_rows(
    args: argparse.Namespace,
    symbols: list[str],
    date_str: str,
    api_key: str,
    s3_client: object | None,
    fetcher=fetch_massive_ticker_overview,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index, symbol in enumerate(symbols, start=1):
        print(f"fetching {symbol} ({index}/{len(symbols)})", flush=True)
        payload = fetcher(
            symbol=symbol,
            api_key=api_key,
            max_retries=args.max_retries,
            base_sleep_seconds=args.base_sleep_seconds,
            max_sleep_seconds=args.max_sleep_seconds,
            progress_callback=lambda message: print(message, flush=True),
        )
        row = normalize_massive_ticker_overview_payload(payload, symbol)
        rows.append(row)

        raw_key = raw_ticker_overview_key(args.raw_prefix, date_str, symbol)
        if args.local_output_dir:
            path = write_raw_json_local(args.local_output_dir, raw_key, payload)
            print(f"wrote raw local: {path}", flush=True)
        else:
            write_raw_json_s3(s3_client, args.s3_bucket, raw_key, payload)
            print(f"wrote raw s3: s3://{args.s3_bucket}/{raw_key}", flush=True)

        if args.request_interval_seconds > 0 and index < len(symbols):
            print(
                f"sleeping {args.request_interval_seconds:.1f} seconds before next symbol",
                flush=True,
            )
            time.sleep(args.request_interval_seconds)
    return rows


def write_curated_rows(
    args: argparse.Namespace,
    s3_client: object | None,
    curated_key: str,
    rows: list[dict[str, object]],
):
    table = rows_to_table(rows)
    if args.local_output_dir:
        path = write_curated_parquet_local(args.local_output_dir, curated_key, table)
        print(f"wrote curated local: {path} rows={table.num_rows}", flush=True)
    else:
        write_curated_parquet_s3(s3_client, args.s3_bucket, curated_key, table)
        print(
            f"wrote curated s3: s3://{args.s3_bucket}/{curated_key} rows={table.num_rows}",
            flush=True,
        )
    return table


def run_ingest(args: argparse.Namespace, fetcher=fetch_massive_ticker_overview) -> int:
    if args.request_interval_seconds < 0:
        raise ValueError("request_interval_seconds must be >= 0")
    if args.fetch_limit is not None and args.fetch_limit < 0:
        raise ValueError("fetch_limit must be >= 0")
    if not args.local_output_dir and not args.s3_bucket:
        raise RuntimeError("provide --local-output-dir or --s3-bucket")

    date_str = utc_date()
    symbols = collect_symbols(args)
    if not symbols:
        raise RuntimeError("no symbols selected")

    s3_client = None if args.local_output_dir else boto3.client("s3")
    curated_key = curated_ticker_overview_key(args.curated_prefix)

    if args.merge_existing:
        existing_rows = read_existing_curated_rows(args, s3_client, curated_key)
        stale_or_missing_symbols = stale_symbols(
            existing_rows,
            symbols,
            args.refresh_after_days,
        )
        symbols_to_fetch = (
            stale_or_missing_symbols[: args.fetch_limit]
            if args.fetch_limit is not None
            else stale_or_missing_symbols
        )
        print(
            "ticker overview upsert summary: "
            f"desired_symbols={len(symbols)} "
            f"existing_cached_rows={len(existing_rows)} "
            f"stale_missing_selected={len(stale_or_missing_symbols)} "
            f"fetch_limit={args.fetch_limit if args.fetch_limit is not None else 'none'} "
            f"symbols_to_fetch={len(symbols_to_fetch)}",
            flush=True,
        )
        if not symbols_to_fetch:
            print(
                "massive ticker overview ingest complete: "
                f"date={date_str} desired_symbols={len(symbols)} "
                f"existing_cached_rows={len(existing_rows)} "
                f"stale_missing_selected={len(stale_or_missing_symbols)} "
                f"fetched=0 final_curated_rows={len(existing_rows)}",
                flush=True,
            )
            return 0

        api_key = resolve_massive_api_key()
        fetched_rows = fetch_and_write_rows(
            args,
            symbols_to_fetch,
            date_str,
            api_key,
            s3_client,
            fetcher=fetcher,
        )
        merged_rows = merge_ticker_overview_rows(existing_rows, fetched_rows)
        table = write_curated_rows(args, s3_client, curated_key, merged_rows)
        print(
            "massive ticker overview ingest complete: "
            f"date={date_str} desired_symbols={len(symbols)} "
            f"existing_cached_rows={len(existing_rows)} "
            f"stale_missing_selected={len(stale_or_missing_symbols)} "
            f"fetched={len(fetched_rows)} final_curated_rows={table.num_rows}",
            flush=True,
        )
        return 0

    api_key = resolve_massive_api_key()
    rows = fetch_and_write_rows(args, symbols, date_str, api_key, s3_client, fetcher=fetcher)
    table = write_curated_rows(args, s3_client, curated_key, rows)
    print(
        "massive ticker overview ingest complete: "
        f"date={date_str} symbols={len(symbols)} rows={table.num_rows}",
        flush=True,
    )
    return 0


def main() -> int:
    args = parse_args()
    return run_ingest(args)


if __name__ == "__main__":
    raise SystemExit(main())
