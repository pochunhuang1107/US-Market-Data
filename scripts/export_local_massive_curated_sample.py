#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from us_market_data.s3_paths import curated_key  # noqa: E402

DEFAULT_BUCKET = "us-market-data-casper"
DEFAULT_CURATED_PREFIX = "curated/etf_daily_prices"
DEFAULT_OUTPUT_DIR = "data/manual_backfill/massive_curated_sample"
DEFAULT_SYMBOLS_FILE = "config/etf_symbols.example.txt"
DEFAULT_START_YEAR = 2018
DEFAULT_END_YEAR = 2026

MANIFEST_COLUMNS = [
    "symbol",
    "year",
    "s3_uri",
    "local_path",
    "status",
    "size_bytes",
    "warning",
]


class MissingS3ObjectsError(RuntimeError):
    pass


class ExportFailuresError(RuntimeError):
    pass


@dataclass(frozen=True)
class ExportConfig:
    bucket: str
    curated_prefix: str
    output_dir: Path
    symbols: list[str]
    start_year: int
    end_year: int
    dry_run: bool = False
    allow_missing: bool = True


@dataclass
class ExportResult:
    manifest_rows: list[dict[str, Any]]
    manifest_path: Path
    summary_path: Path
    downloaded_count: int
    missing_count: int
    failed_count: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download existing Massive-backed curated ETF parquet partitions into a local "
            "manual_backfill mirror for overlap validation."
        )
    )
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--curated-prefix", default=DEFAULT_CURATED_PREFIX)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--symbols-file", default=DEFAULT_SYMBOLS_FILE)
    parser.add_argument("--symbols", help="Comma-separated symbol override")
    parser.add_argument("--start-year", type=int, default=DEFAULT_START_YEAR)
    parser.add_argument("--end-year", type=int, default=DEFAULT_END_YEAR)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--profile", help="Optional AWS profile name")
    parser.add_argument(
        "--allow-missing",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Continue when requested S3 objects are missing; use --no-allow-missing to fail.",
    )
    return parser.parse_args()


def parse_symbols_csv(value: str) -> list[str]:
    symbols = [part.strip().upper() for part in value.split(",") if part.strip()]
    return sorted(dict.fromkeys(symbols))


def load_symbols_from_file(path: Path) -> list[str]:
    with open(path, "r", encoding="utf-8") as f:
        symbols = [
            line.strip().upper()
            for line in f
            if line.strip() and not line.lstrip().startswith("#")
        ]
    return sorted(dict.fromkeys(symbols))


def resolve_symbols(symbols: str | None, symbols_file: str) -> list[str]:
    if symbols:
        return parse_symbols_csv(symbols)
    return load_symbols_from_file(Path(symbols_file))


def local_curated_path(output_dir: Path, symbol: str, year: int) -> Path:
    return output_dir / f"symbol={symbol}" / f"year={year}" / "data.parquet"


def manifest_path_for_output_dir(output_dir: Path) -> Path:
    return output_dir.parent / f"{output_dir.name}_manifest.csv"


def summary_path_for_output_dir(output_dir: Path) -> Path:
    return output_dir.parent / f"{output_dir.name}_summary.md"


def s3_uri(bucket: str, key: str) -> str:
    return f"s3://{bucket}/{key}"


def client_error_code(exc: ClientError) -> str:
    return str(exc.response.get("Error", {}).get("Code", ""))


def is_missing_error(exc: ClientError) -> bool:
    return client_error_code(exc) in {"404", "NoSuchKey", "NotFound"}


def create_s3_client(profile: str | None):
    if profile:
        return boto3.Session(profile_name=profile).client("s3")
    return boto3.client("s3")


def config_from_args(args: argparse.Namespace) -> ExportConfig:
    symbols = resolve_symbols(args.symbols, args.symbols_file)
    if not symbols:
        raise SystemExit("no symbols found from --symbols or --symbols-file")
    if args.start_year > args.end_year:
        raise SystemExit("--start-year must be <= --end-year")
    return ExportConfig(
        bucket=args.bucket,
        curated_prefix=args.curated_prefix.strip("/"),
        output_dir=Path(args.output_dir),
        symbols=symbols,
        start_year=args.start_year,
        end_year=args.end_year,
        dry_run=bool(args.dry_run),
        allow_missing=bool(args.allow_missing),
    )


def build_manifest_row(
    *,
    symbol: str,
    year: int,
    bucket: str,
    key: str,
    local_path: Path,
    status: str,
    size_bytes: int | None,
    warning: str = "",
) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "year": year,
        "s3_uri": s3_uri(bucket, key),
        "local_path": str(local_path),
        "status": status,
        "size_bytes": "" if size_bytes is None else size_bytes,
        "warning": warning,
    }


def write_manifest(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def write_summary(
    path: Path,
    *,
    config: ExportConfig,
    manifest_path: Path,
    downloaded_count: int,
    missing_count: int,
    failed_count: int,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Massive Curated ETF Local Sample Summary",
        "",
        "## Purpose",
        (
            "Download existing Massive-backed curated ETF parquet partitions into a local "
            "manual_backfill mirror so the historical ETF backfill validator can compare "
            "Stooq/Tiingo data against Massive data over the overlap period."
        ),
        "",
        "## Run",
        f"- Bucket: `{config.bucket}`",
        f"- Curated prefix: `{config.curated_prefix}`",
        f"- Output dir: `{config.output_dir}`",
        f"- Year range: {config.start_year}-{config.end_year}",
        f"- Symbol count: {len(config.symbols)}",
        f"- Downloaded count: {downloaded_count}",
        f"- Missing count: {missing_count}",
        f"- Failed count: {failed_count}",
        f"- Manifest: `{manifest_path}`",
        "- Local-only note: this helper only reads S3 objects and writes local files.",
        "- No S3 writes, Massive automation changes, or historical backfill promotion happened.",
        "",
        "## Next Command",
        "```bash",
        "python scripts/validate_manual_historical_etf_backfill.py \\",
        "  --raw-dir data/manual_backfill/raw/stooq \\",
        "  --source-provider stooq \\",
        "  --massive-curated-dir data/manual_backfill/massive_curated_sample \\",
        "  --output-dir data/manual_backfill \\",
        "  --symbols-file config/etf_symbols.example.txt \\",
        "  --backfill-end-date 2017-12-31 \\",
        "  --overlap-start-date 2018-01-01 \\",
        "  --overlap-end-date 2026-05-08",
        "```",
    ]
    if config.dry_run:
        lines.insert(13, "- Dry run: parquet files were not downloaded.")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
        f.write("\n")


def export_partition(
    s3_client,
    *,
    config: ExportConfig,
    symbol: str,
    year: int,
) -> dict[str, Any]:
    key = curated_key(config.curated_prefix, symbol, year)
    local_path = local_curated_path(config.output_dir, symbol, year)
    uri = s3_uri(config.bucket, key)

    try:
        metadata = s3_client.head_object(Bucket=config.bucket, Key=key)
        size_bytes = int(metadata.get("ContentLength", 0))
    except ClientError as exc:
        if is_missing_error(exc):
            warning = "missing S3 object"
            print(f"missing {symbol} {year}: {uri}")
            return build_manifest_row(
                symbol=symbol,
                year=year,
                bucket=config.bucket,
                key=key,
                local_path=local_path,
                status="missing",
                size_bytes=None,
                warning=warning,
            )
        raise

    if config.dry_run:
        print(f"plan {symbol} {year}: {uri} -> {local_path}")
        return build_manifest_row(
            symbol=symbol,
            year=year,
            bucket=config.bucket,
            key=key,
            local_path=local_path,
            status="planned",
            size_bytes=size_bytes,
        )

    try:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        s3_client.download_file(config.bucket, key, str(local_path))
        print(f"downloaded {symbol} {year}: {uri} -> {local_path}")
        return build_manifest_row(
            symbol=symbol,
            year=year,
            bucket=config.bucket,
            key=key,
            local_path=local_path,
            status="downloaded",
            size_bytes=size_bytes,
        )
    except Exception as exc:  # noqa: BLE001 - preserve manifest details for manual runs.
        warning = f"download failed: {exc}"
        print(f"failed {symbol} {year}: {warning}")
        return build_manifest_row(
            symbol=symbol,
            year=year,
            bucket=config.bucket,
            key=key,
            local_path=local_path,
            status="failed",
            size_bytes=size_bytes,
            warning=warning,
        )


def run_export(config: ExportConfig, s3_client) -> ExportResult:
    manifest_rows: list[dict[str, Any]] = []
    for symbol in config.symbols:
        for year in range(config.start_year, config.end_year + 1):
            try:
                manifest_rows.append(
                    export_partition(
                        s3_client,
                        config=config,
                        symbol=symbol,
                        year=year,
                    )
                )
            except ClientError as exc:
                key = curated_key(config.curated_prefix, symbol, year)
                local_path = local_curated_path(config.output_dir, symbol, year)
                warning = f"S3 error {client_error_code(exc) or 'unknown'}: {exc}"
                print(f"failed {symbol} {year}: {warning}")
                manifest_rows.append(
                    build_manifest_row(
                        symbol=symbol,
                        year=year,
                        bucket=config.bucket,
                        key=key,
                        local_path=local_path,
                        status="failed",
                        size_bytes=None,
                        warning=warning,
                    )
                )

    manifest_path = manifest_path_for_output_dir(config.output_dir)
    summary_path = summary_path_for_output_dir(config.output_dir)
    downloaded_count = sum(1 for row in manifest_rows if row["status"] == "downloaded")
    missing_count = sum(1 for row in manifest_rows if row["status"] == "missing")
    failed_count = sum(1 for row in manifest_rows if row["status"] == "failed")

    write_manifest(manifest_path, manifest_rows)
    write_summary(
        summary_path,
        config=config,
        manifest_path=manifest_path,
        downloaded_count=downloaded_count,
        missing_count=missing_count,
        failed_count=failed_count,
    )

    result = ExportResult(
        manifest_rows=manifest_rows,
        manifest_path=manifest_path,
        summary_path=summary_path,
        downloaded_count=downloaded_count,
        missing_count=missing_count,
        failed_count=failed_count,
    )
    if missing_count and not config.allow_missing:
        missing = [row["s3_uri"] for row in manifest_rows if row["status"] == "missing"]
        raise MissingS3ObjectsError(
            "missing requested S3 object(s) and --no-allow-missing was set: "
            + ", ".join(missing[:10])
            + (" ..." if len(missing) > 10 else "")
        )
    if failed_count:
        raise ExportFailuresError(
            f"{failed_count} S3 object(s) failed to download; see {manifest_path}"
        )
    return result


def main() -> None:
    args = parse_args()
    config = config_from_args(args)
    s3_client = create_s3_client(args.profile)
    result = run_export(config, s3_client)
    print(
        "local Massive curated sample export complete: "
        f"symbols={len(config.symbols)} downloaded={result.downloaded_count} "
        f"missing={result.missing_count} failed={result.failed_count}"
    )
    print(f"manifest: {result.manifest_path}")
    print(f"summary: {result.summary_path}")
    if config.dry_run:
        print("dry run: no parquet files were downloaded")


if __name__ == "__main__":
    main()
