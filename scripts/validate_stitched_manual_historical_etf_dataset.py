#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
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

from scripts.validate_manual_historical_etf_backfill import (  # noqa: E402
    CURATED_ETF_SCHEMA,
    align_table_to_curated_schema,
    count_ohlc_bad_rows,
    rows_to_curated_table,
    sort_table,
)
from us_market_data.s3_paths import curated_key  # noqa: E402

DEFAULT_REBASED_EXPORT_DIR = (
    "data/manual_backfill/curated_export_rebased/source=stooq/etf_daily_prices"
)
DEFAULT_MASSIVE_CURATED_DIR = "data/manual_backfill/massive_curated_sample"
DEFAULT_VALIDATION_DIR = "data/manual_backfill/validation"
DEFAULT_OUTPUT_DIR = "data/manual_backfill/stitched_validation/etf_daily_prices"
DEFAULT_SOURCE_PROVIDER = "stooq"

MASSIVE_BOUNDARY_REPORT = "massive_boundary_report.csv"
REBASED_EXPORT_REPORT = "rebased_export_report.csv"
STITCHED_DATASET_REPORT = "stitched_dataset_report.csv"
STITCHED_DATASET_MANIFEST = "stitched_dataset_manifest.json"
STITCHED_DATASET_SUMMARY = "stitched_dataset_summary.md"

BOUNDARY_CLOSE_JUMP_WARNING_THRESHOLD = 0.02
BOUNDARY_CLOSE_JUMP_FAIL_THRESHOLD = 0.10

REPORT_COLUMNS = [
    "symbol",
    "source_provider",
    "massive_first_available_date",
    "rebased_rows_used",
    "massive_rows_used",
    "stitched_rows",
    "first_stitched_date",
    "last_stitched_date",
    "stitched_years",
    "duplicate_rows",
    "ohlc_bad_rows",
    "boundary_year",
    "has_rebased_boundary_year_rows",
    "has_massive_boundary_year_rows",
    "same_year_merge_required",
    "same_year_merge_validated",
    "last_rebased_date",
    "first_massive_date",
    "last_rebased_close",
    "first_massive_close",
    "boundary_close_jump_pct",
    "boundary_status",
    "status",
    "warning",
]


class StitchedValidationError(RuntimeError):
    pass


@dataclass(frozen=True)
class StitchedValidationConfig:
    rebased_export_dir: Path
    massive_curated_dir: Path
    validation_dir: Path
    output_dir: Path
    source_provider: str = DEFAULT_SOURCE_PROVIDER
    dry_run: bool = False
    allowed_symbols: set[str] | None = None
    fail_on_duplicates: bool = True
    fail_on_boundary_gap: bool = False


@dataclass
class StitchedValidationResult:
    report_rows: list[dict[str, Any]]
    manifest: dict[str, Any]
    report_path: Path
    manifest_path: Path
    summary_path: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create and validate a local-only stitched ETF dataset from rebased "
            "pre-Massive rows plus local Massive rows from each symbol boundary onward."
        )
    )
    parser.add_argument("--rebased-export-dir", default=DEFAULT_REBASED_EXPORT_DIR)
    parser.add_argument("--massive-curated-dir", default=DEFAULT_MASSIVE_CURATED_DIR)
    parser.add_argument("--validation-dir", default=DEFAULT_VALIDATION_DIR)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--source-provider", default=DEFAULT_SOURCE_PROVIDER)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--allowed-symbols", help="Optional comma-separated symbol allow-list")
    parser.add_argument(
        "--fail-on-duplicates",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Fail when stitched output contains duplicate symbol-date rows.",
    )
    parser.add_argument(
        "--fail-on-boundary-gap",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "Fail when the stitched boundary has no rebased row before the Massive "
            "boundary or no Massive row at/after it."
        ),
    )
    return parser.parse_args()


def parse_symbols_csv(value: str | None) -> set[str] | None:
    if not value:
        return None
    symbols = {part.strip().upper() for part in value.split(",") if part.strip()}
    return symbols or None


def config_from_args(args: argparse.Namespace) -> StitchedValidationConfig:
    return StitchedValidationConfig(
        rebased_export_dir=Path(args.rebased_export_dir),
        massive_curated_dir=Path(args.massive_curated_dir),
        validation_dir=Path(args.validation_dir),
        output_dir=Path(args.output_dir),
        source_provider=args.source_provider,
        dry_run=bool(args.dry_run),
        allowed_symbols=parse_symbols_csv(args.allowed_symbols),
        fail_on_duplicates=bool(args.fail_on_duplicates),
        fail_on_boundary_gap=bool(args.fail_on_boundary_gap),
    )


def read_csv_report(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"required report does not exist: {path}")
    with open(path, "r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


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


def parse_int(value: object) -> int:
    text = str(value).strip() if value is not None else ""
    return int(text) if text else 0


def parse_float(value: object) -> float | None:
    text = str(value).strip() if value is not None else ""
    if not text:
        return None
    try:
        number = float(text)
    except ValueError:
        return None
    return number if math.isfinite(number) else None


def symbol_from_row(row: dict[str, Any]) -> str:
    return str(row.get("symbol", "")).strip().upper()


def report_key_by_symbol(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {symbol_from_row(row): row for row in rows if symbol_from_row(row)}


def load_symbol_table(root: Path, symbol: str) -> pa.Table | None:
    symbol_dir = root / f"symbol={symbol}"
    if not symbol_dir.exists():
        return None
    tables: list[pa.Table] = []
    for path in sorted(symbol_dir.glob("year=*/data.parquet")):
        tables.append(align_table_to_curated_schema(pq.ParquetFile(path).read()))
    if not tables:
        return None
    return sort_table(pa.concat_tables(tables, promote_options="default"))


def table_rows(table: pa.Table | None) -> list[dict[str, Any]]:
    return [] if table is None else table.to_pylist()


def duplicate_symbol_date_count(rows: list[dict[str, Any]]) -> int:
    seen: set[tuple[str, str]] = set()
    duplicates = 0
    for row in rows:
        key = (str(row.get("symbol", "")), str(row.get("date", "")))
        if key in seen:
            duplicates += 1
        seen.add(key)
    return duplicates


def rows_before(rows: list[dict[str, Any]], boundary: str) -> list[dict[str, Any]]:
    return [row for row in rows if row.get("date") is not None and str(row["date"]) < boundary]


def rows_from(rows: list[dict[str, Any]], boundary: str) -> list[dict[str, Any]]:
    return [row for row in rows if row.get("date") is not None and str(row["date"]) >= boundary]


def stitch_output_path(output_dir: Path, symbol: str, year: int) -> Path:
    relative_key = curated_key("", symbol, year).lstrip("/")
    return output_dir / relative_key


def write_stitched_symbol_rows(
    output_dir: Path,
    symbol: str,
    rows: list[dict[str, Any]],
) -> list[Path]:
    grouped: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(int(row["year"]), []).append(row)

    written_paths: list[Path] = []
    for year, group_rows in sorted(grouped.items()):
        path = stitch_output_path(output_dir, symbol, year)
        path.parent.mkdir(parents=True, exist_ok=True)
        table = rows_to_curated_table(sorted(group_rows, key=lambda r: (r["symbol"], r["date"])))
        pq.write_table(table, path, compression="snappy")
        written_paths.append(path)
    return written_paths


def first_last_dates(rows: list[dict[str, Any]]) -> tuple[str, str]:
    dates = [str(row["date"]) for row in rows if row.get("date") is not None]
    if not dates:
        return "", ""
    return min(dates), max(dates)


def last_row_before(rows: list[dict[str, Any]], boundary: str) -> dict[str, Any] | None:
    candidates = rows_before(rows, boundary)
    return max(candidates, key=lambda row: str(row["date"])) if candidates else None


def first_row_from(rows: list[dict[str, Any]], boundary: str) -> dict[str, Any] | None:
    candidates = rows_from(rows, boundary)
    return min(candidates, key=lambda row: str(row["date"])) if candidates else None


def boundary_jump_pct(
    last_rebased_close: float | None,
    first_massive_close: float | None,
) -> float | None:
    if last_rebased_close is None or first_massive_close is None or last_rebased_close == 0:
        return None
    return abs(first_massive_close / last_rebased_close - 1.0)


def classify_boundary(
    *,
    last_rebased_row: dict[str, Any] | None,
    first_massive_row: dict[str, Any] | None,
    jump_pct: float | None,
    fail_on_boundary_gap: bool,
) -> tuple[str, str]:
    warnings: list[str] = []
    status = "pass"
    if last_rebased_row is None or first_massive_row is None:
        warnings.append("missing one side of stitched boundary")
        status = "fail" if fail_on_boundary_gap else "warning"
    if jump_pct is not None:
        if jump_pct > BOUNDARY_CLOSE_JUMP_FAIL_THRESHOLD:
            warnings.append(
                "boundary close jump exceeds "
                f"{BOUNDARY_CLOSE_JUMP_FAIL_THRESHOLD:.0%} fail threshold"
            )
            status = "fail"
        elif jump_pct > BOUNDARY_CLOSE_JUMP_WARNING_THRESHOLD and status == "pass":
            warnings.append(
                "boundary close jump exceeds "
                f"{BOUNDARY_CLOSE_JUMP_WARNING_THRESHOLD:.0%} warning threshold"
            )
            status = "warning"
    return status, "; ".join(warnings)


def build_symbol_report_row(
    *,
    config: StitchedValidationConfig,
    rebased_report_row: dict[str, str],
    boundary_row: dict[str, str] | None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    symbol = symbol_from_row(rebased_report_row)
    boundary = (
        str(boundary_row.get("massive_first_available_date", "")).strip()
        if boundary_row
        else ""
    )

    if not boundary:
        return (
            empty_report_row(
                symbol=symbol,
                source_provider=config.source_provider,
                status="skipped",
                warning="missing massive_first_available_date in massive boundary report",
            ),
            [],
        )

    rebased_rows_all = table_rows(load_symbol_table(config.rebased_export_dir, symbol))
    massive_rows_all = table_rows(load_symbol_table(config.massive_curated_dir, symbol))
    rebased_rows = rows_before(rebased_rows_all, boundary)
    massive_rows = rows_from(massive_rows_all, boundary)
    stitched_rows = sorted(
        [*rebased_rows, *massive_rows],
        key=lambda row: (str(row["symbol"]), str(row["date"])),
    )

    duplicate_rows = duplicate_symbol_date_count(stitched_rows)
    ohlc_bad_rows = count_ohlc_bad_rows(stitched_rows)
    schema_valid = rows_to_curated_table(stitched_rows).schema == CURATED_ETF_SCHEMA
    first_date, last_date = first_last_dates(stitched_rows)
    stitched_years = sorted({str(row["year"]) for row in stitched_rows})
    boundary_year = int(boundary[:4])
    has_rebased_boundary_year_rows = any(int(row["year"]) == boundary_year for row in rebased_rows)
    has_massive_boundary_year_rows = any(int(row["year"]) == boundary_year for row in massive_rows)
    same_year_merge_required = has_rebased_boundary_year_rows and has_massive_boundary_year_rows
    same_year_merge_validated = (
        same_year_merge_required and duplicate_rows == 0
    ) or not same_year_merge_required

    last_rebased = last_row_before(rebased_rows, boundary)
    first_massive = first_row_from(massive_rows, boundary)
    last_rebased_close = parse_float(last_rebased.get("close")) if last_rebased else None
    first_massive_close = parse_float(first_massive.get("close")) if first_massive else None
    jump_pct = boundary_jump_pct(last_rebased_close, first_massive_close)
    boundary_status, boundary_warning = classify_boundary(
        last_rebased_row=last_rebased,
        first_massive_row=first_massive,
        jump_pct=jump_pct,
        fail_on_boundary_gap=config.fail_on_boundary_gap,
    )

    warnings = [boundary_warning] if boundary_warning else []
    status = "written"
    if duplicate_rows:
        if config.fail_on_duplicates:
            status = "fail"
        elif status == "written":
            status = "warning"
        warnings.append("duplicate symbol-date rows detected")
    if ohlc_bad_rows:
        status = "fail"
        warnings.append("OHLC sanity failures detected")
    if not schema_valid:
        status = "fail"
        warnings.append("stitched schema does not match curated ETF schema")
    if boundary_status == "fail":
        status = "fail"
    elif boundary_status == "warning" and status == "written":
        status = "warning"
    if not stitched_rows:
        status = "skipped"
        warnings.append("no stitched rows available")

    row = {
        "symbol": symbol,
        "source_provider": config.source_provider,
        "massive_first_available_date": boundary,
        "rebased_rows_used": len(rebased_rows),
        "massive_rows_used": len(massive_rows),
        "stitched_rows": len(stitched_rows),
        "first_stitched_date": first_date,
        "last_stitched_date": last_date,
        "stitched_years": ";".join(stitched_years),
        "duplicate_rows": duplicate_rows,
        "ohlc_bad_rows": ohlc_bad_rows,
        "boundary_year": boundary_year,
        "has_rebased_boundary_year_rows": has_rebased_boundary_year_rows,
        "has_massive_boundary_year_rows": has_massive_boundary_year_rows,
        "same_year_merge_required": same_year_merge_required,
        "same_year_merge_validated": same_year_merge_validated,
        "last_rebased_date": str(last_rebased["date"]) if last_rebased else "",
        "first_massive_date": str(first_massive["date"]) if first_massive else "",
        "last_rebased_close": "" if last_rebased_close is None else last_rebased_close,
        "first_massive_close": "" if first_massive_close is None else first_massive_close,
        "boundary_close_jump_pct": "" if jump_pct is None else jump_pct,
        "boundary_status": boundary_status,
        "status": status,
        "warning": "; ".join(warning for warning in warnings if warning),
    }
    return row, stitched_rows


def empty_report_row(
    *,
    symbol: str,
    source_provider: str,
    status: str,
    warning: str,
) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "source_provider": source_provider,
        "massive_first_available_date": "",
        "rebased_rows_used": 0,
        "massive_rows_used": 0,
        "stitched_rows": 0,
        "first_stitched_date": "",
        "last_stitched_date": "",
        "stitched_years": "",
        "duplicate_rows": 0,
        "ohlc_bad_rows": 0,
        "boundary_year": "",
        "has_rebased_boundary_year_rows": False,
        "has_massive_boundary_year_rows": False,
        "same_year_merge_required": False,
        "same_year_merge_validated": False,
        "last_rebased_date": "",
        "first_massive_date": "",
        "last_rebased_close": "",
        "first_massive_close": "",
        "boundary_close_jump_pct": "",
        "boundary_status": "warning",
        "status": status,
        "warning": warning,
    }


def should_skip_rebased_report_row(row: dict[str, str]) -> bool:
    return str(row.get("status", "")).strip().lower() == "skipped" or parse_int(
        row.get("output_rows")
    ) == 0


def report_paths(config: StitchedValidationConfig) -> dict[str, str]:
    return {
        "massive_boundary_report": str(config.validation_dir / MASSIVE_BOUNDARY_REPORT),
        "rebased_export_report": str(config.validation_dir / REBASED_EXPORT_REPORT),
        "stitched_dataset_report": str(config.validation_dir / STITCHED_DATASET_REPORT),
        "stitched_dataset_manifest": str(config.validation_dir / STITCHED_DATASET_MANIFEST),
        "stitched_dataset_summary": str(config.validation_dir / STITCHED_DATASET_SUMMARY),
    }


def write_summary(
    path: Path,
    *,
    config: StitchedValidationConfig,
    rows: list[dict[str, Any]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    symbols_written = [
        row for row in rows if row["status"] in {"written", "planned", "warning"}
    ]
    skipped = [row for row in rows if row["status"] == "skipped"]
    duplicate_failures = [
        row for row in rows if int(row["duplicate_rows"]) > 0 and config.fail_on_duplicates
    ]
    boundary_gap_warnings = [
        row
        for row in rows
        if "missing one side of stitched boundary" in str(row.get("warning", ""))
    ]
    high_jump_warnings = [
        row
        for row in rows
        if parse_float(row.get("boundary_close_jump_pct"))
        and parse_float(row.get("boundary_close_jump_pct"))
        > BOUNDARY_CLOSE_JUMP_WARNING_THRESHOLD
    ]
    largest_jumps = sorted(
        (
            row
            for row in rows
            if parse_float(row.get("boundary_close_jump_pct")) is not None
        ),
        key=lambda row: parse_float(row.get("boundary_close_jump_pct")) or 0.0,
        reverse=True,
    )[:10]
    same_year_merge_rows = [row for row in rows if row["same_year_merge_required"]]

    lines = [
        "# Stitched Manual Historical ETF Dataset Summary",
        "",
        "## Purpose",
        (
            "Create and validate a local stitched ETF dataset made from rebased "
            "pre-Massive manual rows plus Massive rows from each symbol's first "
            "Massive date onward."
        ),
        "",
        "## Local-Only Notes",
        "- This script reads local parquet and validation CSV files only.",
        "- No S3 write happened during this run.",
        "- No Massive automation changes happened during this run.",
        "- No historical data was promoted.",
        "- This stitched dataset is validation output only.",
    ]
    if config.dry_run:
        lines.append("- Dry run: stitched parquet files were not written.")
    lines.extend(
        [
            "",
            "## Counts",
            f"- Symbols processed: {len(rows)}",
            f"- Symbols written: {len(symbols_written)}",
            f"- Skipped: {len(skipped)}",
            f"- Total stitched rows: {sum(int(row['stitched_rows']) for row in rows)}",
            f"- Duplicate failures: {len(duplicate_failures)}",
            f"- Boundary gap warnings: {len(boundary_gap_warnings)}",
            f"- High boundary jump warnings: {len(high_jump_warnings)}",
            "",
            "## Largest Boundary Close Jumps",
        ]
    )
    if largest_jumps:
        lines.extend(
            (
                f"- {row['symbol']}: {float(row['boundary_close_jump_pct']):.6f} "
                f"({row['last_rebased_date']} -> {row['first_massive_date']})"
            )
            for row in largest_jumps
        )
    else:
        lines.append("- None")

    lines.extend(["", "## Same-Year Merge Required"])
    if same_year_merge_rows:
        lines.extend(
            (
                f"- {row['symbol']}: boundary_year={row['boundary_year']}, "
                f"validated={row['same_year_merge_validated']}"
            )
            for row in sorted(same_year_merge_rows, key=lambda row: row["symbol"])
        )
    else:
        lines.append("- None")

    lines.extend(
        [
            "",
            "## Validation Output Only",
            "This stitched dataset is validation output only and must not be promoted as-is.",
            "",
            "## Suggested Next Step",
            "Inspect boundary jumps before creating any S3 promotion tool.",
        ]
    )
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
        f.write("\n")


def run_stitched_validation(config: StitchedValidationConfig) -> StitchedValidationResult:
    rebased_report_rows = read_csv_report(config.validation_dir / REBASED_EXPORT_REPORT)
    boundary_rows = read_csv_report(config.validation_dir / MASSIVE_BOUNDARY_REPORT)
    boundary_by_symbol = report_key_by_symbol(boundary_rows)

    if not config.dry_run and config.output_dir.exists():
        shutil.rmtree(config.output_dir)

    report_rows: list[dict[str, Any]] = []
    for rebased_row in rebased_report_rows:
        symbol = symbol_from_row(rebased_row)
        if not symbol:
            continue
        if config.allowed_symbols is not None and symbol not in config.allowed_symbols:
            continue
        if should_skip_rebased_report_row(rebased_row):
            report_rows.append(
                empty_report_row(
                    symbol=symbol,
                    source_provider=config.source_provider,
                    status="skipped",
                    warning="skipped by rebased export report",
                )
            )
            continue

        report_row, stitched_rows = build_symbol_report_row(
            config=config,
            rebased_report_row=rebased_row,
            boundary_row=boundary_by_symbol.get(symbol),
        )
        if stitched_rows and not config.dry_run and report_row["status"] in {"written", "warning"}:
            write_stitched_symbol_rows(config.output_dir, symbol, stitched_rows)
        if config.dry_run and report_row["status"] == "written":
            report_row["status"] = "planned"
        report_rows.append(report_row)

    report_path = config.validation_dir / STITCHED_DATASET_REPORT
    manifest_path = config.validation_dir / STITCHED_DATASET_MANIFEST
    summary_path = config.validation_dir / STITCHED_DATASET_SUMMARY
    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_provider": config.source_provider,
        "rebased_export_dir": str(config.rebased_export_dir),
        "massive_curated_dir": str(config.massive_curated_dir),
        "output_dir": str(config.output_dir),
        "validation_dir": str(config.validation_dir),
        "symbols_processed": len(report_rows),
        "symbols_written": sum(
            1 for row in report_rows if row["status"] in {"written", "planned", "warning"}
        ),
        "symbols_skipped": sum(1 for row in report_rows if row["status"] == "skipped"),
        "total_stitched_rows": sum(int(row["stitched_rows"]) for row in report_rows),
        "dry_run": config.dry_run,
        "report_paths": report_paths(config),
    }

    write_csv_report(report_path, report_rows, REPORT_COLUMNS)
    write_json(manifest_path, manifest)
    write_summary(summary_path, config=config, rows=report_rows)

    failures = [row for row in report_rows if row["status"] == "fail"]
    if failures:
        symbols = ", ".join(row["symbol"] for row in failures)
        raise StitchedValidationError(f"stitched validation failed for: {symbols}")

    return StitchedValidationResult(
        report_rows=report_rows,
        manifest=manifest,
        report_path=report_path,
        manifest_path=manifest_path,
        summary_path=summary_path,
    )


def main() -> None:
    config = config_from_args(parse_args())
    result = run_stitched_validation(config)
    print(
        "stitched manual historical ETF dataset validation complete: "
        f"processed={result.manifest['symbols_processed']} "
        f"written={result.manifest['symbols_written']} "
        f"skipped={result.manifest['symbols_skipped']} "
        f"rows={result.manifest['total_stitched_rows']}"
    )
    print(f"report: {result.report_path}")
    print(f"manifest: {result.manifest_path}")
    print(f"summary: {result.summary_path}")
    if config.dry_run:
        print("dry run: no parquet files were written")


if __name__ == "__main__":
    main()
