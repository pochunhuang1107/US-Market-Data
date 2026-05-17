#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.validate_manual_historical_etf_backfill import CURATED_ETF_SCHEMA  # noqa: E402

DEFAULT_INPUT_DIR = (
    "data/manual_backfill_tiingo_split/stitched_validation/etf_daily_prices"
)
DEFAULT_OUTPUT_DIR = "data/research/etf_daily_prices_extended_tiingo_split"
DEFAULT_VALIDATION_DIR = "data/manual_backfill_tiingo_split/validation"
DEFAULT_SOURCE_PROVIDER = "tiingo_split"
DEFAULT_DATASET_NAME = "etf_daily_prices_extended_tiingo_split"
DEFAULT_SYMBOLS_FILE = "config/etf_symbols.example.txt"

MANIFEST_FILENAME = "local_extended_dataset_manifest.json"
SYMBOL_SUMMARY_FILENAME = "local_extended_dataset_symbol_summary.csv"
SUMMARY_FILENAME = "local_extended_dataset_summary.md"

SUMMARY_COLUMNS = [
    "symbol",
    "status",
    "source_path_count",
    "output_path_count",
    "row_count",
    "first_date",
    "last_date",
    "years",
    "duplicate_rows",
    "ohlc_bad_rows",
    "warning",
]

SOURCE_VALIDATION_REPORT_CANDIDATES = [
    "stitched_dataset_report.csv",
    "stitched_dataset_manifest.json",
    "stitched_dataset_summary.md",
    "manual_backfill_source_comparison_report.csv",
    "manual_backfill_source_comparison_manifest.json",
    "manual_backfill_source_comparison_summary.md",
    "rebased_export_report.csv",
    "massive_boundary_report.csv",
    "tiingo_split_adjusted_materialization_manifest.csv",
    "tiingo_split_adjusted_materialization_summary.md",
]


class LocalExtendedDatasetError(RuntimeError):
    pass


@dataclass(frozen=True)
class LocalExtendedDatasetConfig:
    input_dir: Path = Path(DEFAULT_INPUT_DIR)
    output_dir: Path = Path(DEFAULT_OUTPUT_DIR)
    validation_dir: Path = Path(DEFAULT_VALIDATION_DIR)
    source_provider: str = DEFAULT_SOURCE_PROVIDER
    dataset_name: str = DEFAULT_DATASET_NAME
    symbols_file: Path = Path(DEFAULT_SYMBOLS_FILE)
    symbols: list[str] | None = None
    overwrite: bool = False
    dry_run: bool = False
    progress_callback: Any = print


@dataclass
class LocalExtendedDatasetResult:
    symbol_rows: list[dict[str, Any]]
    manifest: dict[str, Any]
    manifest_path: Path
    symbol_summary_path: Path
    summary_path: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Copy and validate the local stitched Tiingo split-adjusted ETF dataset "
            "into a stable local research directory."
        )
    )
    parser.add_argument("--input-dir", default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--validation-dir", default=DEFAULT_VALIDATION_DIR)
    parser.add_argument("--source-provider", default=DEFAULT_SOURCE_PROVIDER)
    parser.add_argument("--dataset-name", default=DEFAULT_DATASET_NAME)
    parser.add_argument("--symbols-file", default=DEFAULT_SYMBOLS_FILE)
    parser.add_argument("--symbols", help="Optional comma-separated symbol override")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def parse_symbols_text(value: str) -> list[str]:
    symbols: list[str] = []
    for raw_line in value.splitlines():
        line = raw_line.split("#", 1)[0]
        for part in line.replace(",", " ").split():
            symbol = part.strip().upper()
            if symbol:
                symbols.append(symbol)
    return sorted(dict.fromkeys(symbols))


def load_symbols(symbols_file: str | Path) -> list[str]:
    with open(symbols_file, "r", encoding="utf-8") as f:
        return parse_symbols_text(f.read())


def config_from_args(args: argparse.Namespace) -> LocalExtendedDatasetConfig:
    symbols = parse_symbols_text(args.symbols) if args.symbols else load_symbols(args.symbols_file)
    return LocalExtendedDatasetConfig(
        input_dir=Path(args.input_dir),
        output_dir=Path(args.output_dir),
        validation_dir=Path(args.validation_dir),
        source_provider=args.source_provider,
        dataset_name=args.dataset_name,
        symbols_file=Path(args.symbols_file),
        symbols=symbols,
        overwrite=bool(args.overwrite),
        dry_run=bool(args.dry_run),
    )


def write_csv_report(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
        f.write("\n")


def report_paths(config: LocalExtendedDatasetConfig) -> dict[str, str]:
    reports_dir = config.output_dir / "_reports"
    return {
        "manifest": str(reports_dir / MANIFEST_FILENAME),
        "symbol_summary": str(reports_dir / SYMBOL_SUMMARY_FILENAME),
        "summary": str(reports_dir / SUMMARY_FILENAME),
    }


def source_validation_reports_used(validation_dir: Path) -> list[str]:
    reports = [
        str(validation_dir / name)
        for name in SOURCE_VALIDATION_REPORT_CANDIDATES
        if (validation_dir / name).exists()
    ]
    return sorted(reports)


def source_stitched_warnings(validation_dir: Path) -> dict[str, str]:
    path = validation_dir / "stitched_dataset_report.csv"
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if "symbol" not in (reader.fieldnames or []):
            return {}
        warnings: dict[str, str] = {}
        for row in reader:
            symbol = str(row.get("symbol", "")).strip().upper()
            warning = str(row.get("warning", "")).strip()
            if symbol and warning:
                warnings[symbol] = warning
        return warnings


def has_files(path: Path) -> bool:
    return path.exists() and any(candidate.is_file() for candidate in path.rglob("*"))


def is_relative_to(path: Path, other: Path) -> bool:
    try:
        path.relative_to(other)
        return True
    except ValueError:
        return False


def validate_path_relationships(input_dir: Path, output_dir: Path) -> None:
    resolved_input = input_dir.resolve()
    resolved_output = output_dir.resolve()
    if resolved_input == resolved_output:
        raise LocalExtendedDatasetError("input-dir and output-dir must be different")
    if is_relative_to(resolved_input, resolved_output):
        raise LocalExtendedDatasetError("output-dir must not be a parent of input-dir")
    if is_relative_to(resolved_output, resolved_input):
        raise LocalExtendedDatasetError("output-dir must not be inside input-dir")


def validate_config(config: LocalExtendedDatasetConfig) -> None:
    if not config.symbols:
        raise LocalExtendedDatasetError("no symbols requested")
    if not config.input_dir.exists():
        raise LocalExtendedDatasetError(f"input-dir does not exist: {config.input_dir}")
    validate_path_relationships(config.input_dir, config.output_dir)
    if not config.dry_run and not config.overwrite and has_files(config.output_dir):
        raise LocalExtendedDatasetError(
            f"output-dir already exists with files; rerun with --overwrite: {config.output_dir}"
        )


def source_paths_for_symbol(input_dir: Path, symbol: str) -> list[Path]:
    return sorted((input_dir / f"symbol={symbol}").glob("year=*/data.parquet"))


def output_path_for_source(config: LocalExtendedDatasetConfig, source_path: Path) -> Path:
    return config.output_dir / source_path.relative_to(config.input_dir)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def copy_partition_exact(source_path: Path, target_path: Path) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_path, target_path)
    if sha256_file(source_path) != sha256_file(target_path):
        raise LocalExtendedDatasetError(f"copied parquet checksum mismatch: {target_path}")


def partition_values(path: Path) -> tuple[str, int]:
    symbol_part = path.parent.parent.name
    year_part = path.parent.name
    if not symbol_part.startswith("symbol=") or not year_part.startswith("year="):
        raise LocalExtendedDatasetError(f"unexpected partition path: {path}")
    symbol = symbol_part.split("=", 1)[1].upper()
    try:
        year = int(year_part.split("=", 1)[1])
    except ValueError as exc:
        raise LocalExtendedDatasetError(f"unexpected year partition path: {path}") from exc
    return symbol, year


def duplicate_symbol_date_count(rows: list[dict[str, Any]]) -> int:
    seen: set[tuple[str, str]] = set()
    duplicates = 0
    for row in rows:
        key = (str(row.get("symbol", "")), str(row.get("date", "")))
        if key in seen:
            duplicates += 1
        seen.add(key)
    return duplicates


def count_ohlc_bad_rows(rows: list[dict[str, Any]]) -> int:
    bad_rows = 0
    for row in rows:
        open_value = row.get("open")
        high_value = row.get("high")
        low_value = row.get("low")
        close_value = row.get("close")
        values = [open_value, high_value, low_value, close_value]
        has_bad_price = any(value is None or value <= 0 for value in values)
        has_bad_high_low = (
            not has_bad_price
            and (
                high_value < max(open_value, close_value, low_value)
                or low_value > min(open_value, close_value, high_value)
            )
        )
        if has_bad_price or has_bad_high_low:
            bad_rows += 1
    return bad_rows


def validate_partition_table(path: Path) -> tuple[pa.Table, list[str]]:
    table = pq.ParquetFile(path).read()
    warnings: list[str] = []
    if table.schema != CURATED_ETF_SCHEMA:
        warnings.append("schema does not match curated ETF schema")

    path_symbol, path_year = partition_values(path)
    for row in table.to_pylist():
        try:
            row_year = int(row.get("year"))
        except (TypeError, ValueError):
            row_year = None
        if str(row.get("symbol", "")).upper() != path_symbol or row_year != path_year:
            warnings.append("symbol/year partition path does not match row values")
            break
    return table, warnings


def validate_symbol_paths(
    *,
    symbol: str,
    paths: list[Path],
) -> tuple[list[dict[str, Any]], list[str]]:
    all_rows: list[dict[str, Any]] = []
    warnings: list[str] = []
    for path in paths:
        table, partition_warnings = validate_partition_table(path)
        warnings.extend(partition_warnings)
        all_rows.extend(table.to_pylist())

    duplicate_rows = duplicate_symbol_date_count(all_rows)
    ohlc_bad_rows = count_ohlc_bad_rows(all_rows)
    if duplicate_rows:
        warnings.append("duplicate symbol-date rows detected")
    if ohlc_bad_rows:
        warnings.append("OHLC sanity failures detected")
    if not all_rows:
        warnings.append("no rows available for written symbol")

    return all_rows, warnings


def symbol_row_from_rows(
    *,
    symbol: str,
    status: str,
    source_path_count: int,
    output_path_count: int,
    rows: list[dict[str, Any]],
    duplicate_rows: int,
    ohlc_bad_rows: int,
    warning: str,
) -> dict[str, Any]:
    dates = [str(row["date"]) for row in rows if row.get("date") is not None]
    years = sorted({str(row["year"]) for row in rows if row.get("year") is not None})
    return {
        "symbol": symbol,
        "status": status,
        "source_path_count": source_path_count,
        "output_path_count": output_path_count,
        "row_count": len(rows),
        "first_date": min(dates) if dates else "",
        "last_date": max(dates) if dates else "",
        "years": ";".join(years),
        "duplicate_rows": duplicate_rows,
        "ohlc_bad_rows": ohlc_bad_rows,
        "warning": warning,
    }


def build_symbol_summary_row(
    *,
    config: LocalExtendedDatasetConfig,
    symbol: str,
    source_paths: list[Path],
    source_warning: str = "",
) -> dict[str, Any]:
    if not source_paths:
        warnings = ["no source parquet files found"]
        if source_warning:
            warnings.append(f"source stitched validation warning: {source_warning}")
        return symbol_row_from_rows(
            symbol=symbol,
            status="skipped",
            source_path_count=0,
            output_path_count=0,
            rows=[],
            duplicate_rows=0,
            ohlc_bad_rows=0,
            warning="; ".join(warnings),
        )

    for source_path in source_paths:
        target_path = output_path_for_source(config, source_path)
        if config.dry_run:
            if config.progress_callback:
                config.progress_callback(f"plan: {source_path} -> {target_path}")
        else:
            copy_partition_exact(source_path, target_path)

    validation_paths = source_paths if config.dry_run else [
        output_path_for_source(config, source_path) for source_path in source_paths
    ]
    rows, warnings = validate_symbol_paths(symbol=symbol, paths=validation_paths)
    if source_warning:
        warnings.append(f"source stitched validation warning: {source_warning}")
    duplicate_rows = duplicate_symbol_date_count(rows)
    ohlc_bad_rows = count_ohlc_bad_rows(rows)
    has_schema_warning = any("schema does not match" in warning for warning in warnings)
    has_partition_warning = any("partition path" in warning for warning in warnings)
    status = "planned" if config.dry_run else "written"
    if duplicate_rows or ohlc_bad_rows or has_schema_warning or has_partition_warning or not rows:
        status = "fail"
    return symbol_row_from_rows(
        symbol=symbol,
        status=status,
        source_path_count=len(source_paths),
        output_path_count=0 if config.dry_run else len(validation_paths),
        rows=rows,
        duplicate_rows=duplicate_rows,
        ohlc_bad_rows=ohlc_bad_rows,
        warning="; ".join(dict.fromkeys(warnings)),
    )


def write_summary(
    path: Path,
    *,
    config: LocalExtendedDatasetConfig,
    rows: list[dict[str, Any]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    written = [row for row in rows if row["status"] in {"written", "planned"}]
    skipped = [row for row in rows if row["status"] == "skipped"]
    warnings = [row for row in rows if row.get("warning")]
    dates = [
        str(row[field])
        for row in written
        for field in ("first_date", "last_date")
        if row.get(field)
    ]

    lines = [
        "# Local Extended ETF Research Dataset",
        "",
        "## Purpose",
        (
            "Publish the validated stitched Tiingo split-adjusted ETF parquet dataset "
            "to a stable local path for local ML research."
        ),
        "",
        "## Local-Only Note",
        "- This is a local-only research export.",
        "- No S3 write happened during this run.",
        "- No historical data was promoted to production.",
        "- Massive automation was not modified.",
    ]
    if config.dry_run:
        lines.append("- Dry run: parquet files were not copied.")
    lines.extend(
        [
            "",
            "## Dataset",
            f"- Source input path: {config.input_dir}",
            f"- Output path: {config.output_dir}",
            f"- Dataset name: {config.dataset_name}",
            f"- Source provider: {config.source_provider}",
            "",
            "## Coverage",
            f"- Total symbols written: {len(written)}",
            "- Skipped symbols: "
            + (", ".join(row["symbol"] for row in skipped) if skipped else "None"),
            f"- Total rows: {sum(int(row['row_count']) for row in written)}",
            f"- First date: {min(dates) if dates else ''}",
            f"- Last date: {max(dates) if dates else ''}",
            "",
            "## Warnings",
        ]
    )
    if warnings:
        lines.extend(
            f"- {row['symbol']}: {row['warning']}" for row in warnings if row.get("warning")
        )
    else:
        lines.append("- None")
    lines.extend(
        [
            "",
            "## Research Use",
            "This dataset is intended for local ML research.",
            "",
            "## DuckDB / Parquet Glob",
            "```text",
            "data/research/etf_daily_prices_extended_tiingo_split/**/*.parquet",
            "```",
            "",
            "## Next Step",
            "Point US-Market-ML-Research to this local path and run a long study.",
        ]
    )
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
        f.write("\n")


def build_manifest(
    *,
    config: LocalExtendedDatasetConfig,
    rows: list[dict[str, Any]],
    paths: dict[str, str],
) -> dict[str, Any]:
    written = [row for row in rows if row["status"] in {"written", "planned"}]
    skipped = [row for row in rows if row["status"] == "skipped"]
    dates = [
        str(row[field])
        for row in written
        for field in ("first_date", "last_date")
        if row.get(field)
    ]
    notes = [
        (
            "Local-only research export; no S3 writes, no production promotion, "
            "no Massive automation changes."
        ),
        "Parquet files are copied byte-for-byte from the validated stitched source dataset.",
    ]
    warning_count = sum(1 for row in rows if row.get("warning"))
    if warning_count:
        notes.append(f"{warning_count} symbol-level warning(s) recorded in the symbol summary.")
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset_name": config.dataset_name,
        "source_provider": config.source_provider,
        "input_dir": str(config.input_dir),
        "output_dir": str(config.output_dir),
        "validation_dir": str(config.validation_dir),
        "dry_run": config.dry_run,
        "symbols_requested": list(config.symbols or []),
        "symbols_written": [row["symbol"] for row in written],
        "symbols_skipped": [row["symbol"] for row in skipped],
        "total_rows": sum(int(row["row_count"]) for row in written),
        "first_date": min(dates) if dates else "",
        "last_date": max(dates) if dates else "",
        "report_paths": paths,
        "source_validation_reports_used": source_validation_reports_used(config.validation_dir),
        "notes": notes,
    }


def run_export(config: LocalExtendedDatasetConfig) -> LocalExtendedDatasetResult:
    validate_config(config)
    if config.overwrite and not config.dry_run and config.output_dir.exists():
        shutil.rmtree(config.output_dir)

    stitched_warnings = source_stitched_warnings(config.validation_dir)
    symbol_rows = [
        build_symbol_summary_row(
            config=config,
            symbol=symbol,
            source_paths=source_paths_for_symbol(config.input_dir, symbol),
            source_warning=stitched_warnings.get(symbol, ""),
        )
        for symbol in (config.symbols or [])
    ]

    paths = report_paths(config)
    manifest = build_manifest(config=config, rows=symbol_rows, paths=paths)
    manifest_path = Path(paths["manifest"])
    symbol_summary_path = Path(paths["symbol_summary"])
    summary_path = Path(paths["summary"])
    write_csv_report(symbol_summary_path, symbol_rows, SUMMARY_COLUMNS)
    write_json(manifest_path, manifest)
    write_summary(summary_path, config=config, rows=symbol_rows)

    failures = [row["symbol"] for row in symbol_rows if row["status"] == "fail"]
    if failures:
        raise LocalExtendedDatasetError(
            "local extended ETF research dataset validation failed for: "
            + ", ".join(sorted(failures))
        )

    return LocalExtendedDatasetResult(
        symbol_rows=symbol_rows,
        manifest=manifest,
        manifest_path=manifest_path,
        symbol_summary_path=symbol_summary_path,
        summary_path=summary_path,
    )


def main() -> None:
    config = config_from_args(parse_args())
    result = run_export(config)
    print(
        "local extended ETF research dataset export complete: "
        f"written={len(result.manifest['symbols_written'])} "
        f"skipped={len(result.manifest['symbols_skipped'])} "
        f"rows={result.manifest['total_rows']}"
    )
    print(f"manifest: {result.manifest_path}")
    print(f"symbol summary: {result.symbol_summary_path}")
    print(f"summary: {result.summary_path}")
    if config.dry_run:
        print("dry run: no parquet files were written")


if __name__ == "__main__":
    main()
