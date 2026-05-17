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
    align_table_to_curated_schema,
    rows_to_curated_table,
)
from us_market_data.s3_paths import curated_key  # noqa: E402

DEFAULT_INPUT_EXPORT_DIR = "data/manual_backfill/curated_export/source=stooq/etf_daily_prices"
DEFAULT_VALIDATION_DIR = "data/manual_backfill/validation"
DEFAULT_OUTPUT_DIR = "data/manual_backfill/curated_export_rebased/source=stooq/etf_daily_prices"
DEFAULT_SOURCE_PROVIDER = "stooq"

MASSIVE_BOUNDARY_REPORT = "massive_boundary_report.csv"
SYMBOL_QUALITY_REPORT = "symbol_quality_report.csv"
BOUNDARY_REBASE_DIAGNOSTICS_REPORT = "boundary_rebase_diagnostics_report.csv"
ADJUSTMENT_DIAGNOSTICS_REPORT = "adjustment_diagnostics_report.csv"
REBASED_EXPORT_REPORT = "rebased_export_report.csv"
REBASED_EXPORT_MANIFEST = "rebased_export_manifest.json"
REBASED_EXPORT_SUMMARY = "rebased_export_summary.md"

PRICE_COLUMNS = ["open", "high", "low", "close", "vwap"]
REPORT_COLUMNS = [
    "symbol",
    "source_provider",
    "action",
    "rebase_factor",
    "massive_first_available_date",
    "source_backfill_end_date",
    "input_rows",
    "output_rows",
    "first_output_date",
    "last_output_date",
    "output_years",
    "output_path_count",
    "requires_merge_with_existing_partition",
    "status",
    "warning",
]


@dataclass(frozen=True)
class RebasedExportConfig:
    input_export_dir: Path
    validation_dir: Path
    output_dir: Path
    source_provider: str
    dry_run: bool = False
    allowed_symbols: set[str] | None = None
    include_warning_symbols: bool = True
    fail_on_missing_factor: bool = True


@dataclass
class RebasedExportResult:
    report_rows: list[dict[str, Any]]
    manifest: dict[str, Any]
    report_path: Path
    manifest_path: Path
    summary_path: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create a local-only rebased pre-Massive ETF parquet export from the "
            "manual historical backfill validation outputs."
        )
    )
    parser.add_argument("--input-export-dir", default=DEFAULT_INPUT_EXPORT_DIR)
    parser.add_argument("--validation-dir", default=DEFAULT_VALIDATION_DIR)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--source-provider", default=DEFAULT_SOURCE_PROVIDER)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--allowed-symbols", help="Optional comma-separated symbol allow-list")
    parser.add_argument(
        "--include-warning-symbols",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Allow warning-action symbols with a valid rebase factor.",
    )
    parser.add_argument(
        "--fail-on-missing-factor",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Fail when a symbol selected for rebasing is missing a valid factor.",
    )
    return parser.parse_args()


def parse_symbols_csv(value: str | None) -> set[str] | None:
    if not value:
        return None
    symbols = {part.strip().upper() for part in value.split(",") if part.strip()}
    return symbols or None


def config_from_args(args: argparse.Namespace) -> RebasedExportConfig:
    return RebasedExportConfig(
        input_export_dir=Path(args.input_export_dir),
        validation_dir=Path(args.validation_dir),
        output_dir=Path(args.output_dir),
        source_provider=args.source_provider,
        dry_run=bool(args.dry_run),
        allowed_symbols=parse_symbols_csv(args.allowed_symbols),
        include_warning_symbols=bool(args.include_warning_symbols),
        fail_on_missing_factor=bool(args.fail_on_missing_factor),
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


def parse_bool(value: object) -> bool:
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


def parse_int(value: object) -> int:
    text = str(value).strip() if value is not None else ""
    return int(text) if text else 0


def parse_factor(value: object) -> float | None:
    text = str(value).strip() if value is not None else ""
    if not text:
        return None
    try:
        factor = float(text)
    except ValueError:
        return None
    if not math.isfinite(factor) or factor <= 0:
        return None
    return factor


def symbol_from_boundary_row(row: dict[str, str]) -> str:
    return str(row.get("symbol", "")).strip().upper()


def load_symbol_export_table(input_export_dir: Path, symbol: str) -> pa.Table | None:
    symbol_dir = input_export_dir / f"symbol={symbol}"
    if not symbol_dir.exists():
        return None
    tables: list[pa.Table] = []
    for path in sorted(symbol_dir.glob("year=*/data.parquet")):
        tables.append(align_table_to_curated_schema(pq.ParquetFile(path).read()))
    if not tables:
        return None
    return pa.concat_tables(tables, promote_options="default")


def filtered_pre_massive_rows(
    table: pa.Table | None,
    *,
    source_backfill_end_date: str,
    massive_first_available_date: str,
) -> tuple[int, list[dict[str, Any]]]:
    if table is None:
        return 0, []
    rows = table.to_pylist()
    filtered = []
    for row in rows:
        row_date = row.get("date")
        if row_date is None:
            continue
        row_date_text = str(row_date)
        if source_backfill_end_date and row_date_text > source_backfill_end_date:
            continue
        if massive_first_available_date and row_date_text >= massive_first_available_date:
            continue
        filtered.append(row)
    return len(rows), sorted(filtered, key=lambda r: (str(r["symbol"]), str(r["date"])))


def determine_action(
    *,
    symbol: str,
    quality_row: dict[str, str] | None,
    include_warning_symbols: bool,
    fail_on_missing_factor: bool,
) -> tuple[str, float | None, str]:
    if quality_row is None:
        return "skip", None, "missing symbol quality report row"

    boundary_action = str(quality_row.get("boundary_suggested_action", "")).strip()
    factor = parse_factor(quality_row.get("boundary_rebase_factor"))
    if boundary_action == "use_as_is":
        return "use_as_is", 1.0, ""
    if boundary_action == "rebase_pre_massive_history_before_promotion":
        if factor is None:
            message = f"{symbol} requires rebasing but has no valid boundary_rebase_factor"
            if fail_on_missing_factor:
                raise ValueError(message)
            return "skip", None, message
        return "rebase", factor, ""

    if include_warning_symbols and factor is not None:
        return "rebase", factor, (
            "boundary_suggested_action was not accepted directly; "
            "included because --include-warning-symbols is enabled and a valid factor exists"
        )
    return "skip", None, f"unsupported boundary_suggested_action: {boundary_action or '(blank)'}"


def apply_rebase_factor(rows: list[dict[str, Any]], factor: float) -> list[dict[str, Any]]:
    rebased_rows: list[dict[str, Any]] = []
    for row in rows:
        rebased = dict(row)
        for column in PRICE_COLUMNS:
            value = rebased.get(column)
            if value is not None:
                rebased[column] = float(value) * factor
        rebased_rows.append(rebased)
    return rebased_rows


def output_path(output_dir: Path, symbol: str, year: int) -> Path:
    relative_key = curated_key("", symbol, year).lstrip("/")
    return output_dir / relative_key


def write_rebased_symbol_rows(
    output_dir: Path,
    symbol: str,
    rows: list[dict[str, Any]],
) -> list[Path]:
    written_paths: list[Path] = []
    grouped: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(int(row["year"]), []).append(row)
    for year, group_rows in sorted(grouped.items()):
        path = output_path(output_dir, symbol, year)
        path.parent.mkdir(parents=True, exist_ok=True)
        table = rows_to_curated_table(sorted(group_rows, key=lambda r: (r["symbol"], r["date"])))
        pq.write_table(table, path, compression="snappy")
        written_paths.append(path)
    return written_paths


def build_report_row(
    *,
    boundary_row: dict[str, str],
    action: str,
    rebase_factor: float | None,
    input_rows: int,
    output_rows: list[dict[str, Any]],
    output_path_count: int,
    status: str,
    warning: str = "",
) -> dict[str, Any]:
    dates = [str(row["date"]) for row in output_rows if row.get("date") is not None]
    years = sorted({str(row["year"]) for row in output_rows})
    return {
        "symbol": symbol_from_boundary_row(boundary_row),
        "source_provider": boundary_row.get("source_provider", ""),
        "action": action,
        "rebase_factor": "" if rebase_factor is None else rebase_factor,
        "massive_first_available_date": boundary_row.get("massive_first_available_date", ""),
        "source_backfill_end_date": boundary_row.get("source_backfill_end_date", ""),
        "input_rows": input_rows,
        "output_rows": len(output_rows),
        "first_output_date": min(dates) if dates else "",
        "last_output_date": max(dates) if dates else "",
        "output_years": ";".join(years),
        "output_path_count": output_path_count,
        "requires_merge_with_existing_partition": boundary_row.get(
            "requires_merge_with_existing_partition", ""
        ),
        "status": status,
        "warning": warning,
    }


def report_paths(config: RebasedExportConfig) -> dict[str, str]:
    paths = {
        "massive_boundary_report": str(config.validation_dir / MASSIVE_BOUNDARY_REPORT),
        "symbol_quality_report": str(config.validation_dir / SYMBOL_QUALITY_REPORT),
        "rebased_export_report": str(config.validation_dir / REBASED_EXPORT_REPORT),
        "rebased_export_manifest": str(config.validation_dir / REBASED_EXPORT_MANIFEST),
        "rebased_export_summary": str(config.validation_dir / REBASED_EXPORT_SUMMARY),
    }
    boundary_diagnostics = config.validation_dir / BOUNDARY_REBASE_DIAGNOSTICS_REPORT
    adjustment_diagnostics = config.validation_dir / ADJUSTMENT_DIAGNOSTICS_REPORT
    if boundary_diagnostics.exists():
        paths["boundary_rebase_diagnostics_report"] = str(boundary_diagnostics)
    if adjustment_diagnostics.exists():
        paths["adjustment_diagnostics_report"] = str(adjustment_diagnostics)
    return paths


def write_summary(path: Path, *, config: RebasedExportConfig, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rebased = [row for row in rows if row["action"] == "rebase" and row["status"] == "written"]
    used_as_is = [
        row for row in rows if row["action"] == "use_as_is" and row["status"] == "written"
    ]
    skipped = [row for row in rows if row["action"] == "skip" or row["status"] == "skipped"]
    total_output_rows = sum(int(row["output_rows"]) for row in rows)
    merge_required = [
        row["symbol"] for row in rows if parse_bool(row["requires_merge_with_existing_partition"])
    ]

    lines = [
        "# Rebased Manual Historical ETF Export Summary",
        "",
        "## Purpose",
        (
            "Create a separate local pre-Massive curated-style ETF export by scaling "
            "accepted manual source rows to Massive's boundary price level."
        ),
        "",
        "## Local-Only Notes",
        "- This script reads local parquet and validation CSV files only.",
        "- No S3 write happened during this run.",
        "- No Massive automation changes happened during this run.",
        "- This script does not perform S3 promotion.",
    ]
    if config.dry_run:
        lines.append("- Dry run: parquet output files were not written.")
    lines.extend(
        [
            "",
            "## Counts",
            f"- Rebased symbols: {len(rebased)}",
            f"- Use-as-is symbols: {len(used_as_is)}",
            f"- Skipped symbols: {len(skipped)}",
            f"- Total output rows: {total_output_rows}",
            "",
            "## Rebased Symbols",
        ]
    )
    if rebased:
        lines.extend(
            f"- {row['symbol']}: factor={row['rebase_factor']}"
            for row in sorted(rebased, key=lambda r: r["symbol"])
        )
    else:
        lines.append("- None")
    lines.extend(["", "## Use-As-Is Symbols"])
    if used_as_is:
        lines.extend(f"- {row['symbol']}" for row in sorted(used_as_is, key=lambda r: r["symbol"]))
    else:
        lines.append("- None")
    lines.extend(["", "## Skipped Symbols"])
    if skipped:
        lines.extend(
            f"- {row['symbol']}: {row['warning'] or row['status']}"
            for row in sorted(skipped, key=lambda r: r["symbol"])
        )
    else:
        lines.append("- None")
    lines.extend(
        [
            "",
            "## Future Promotion Note",
            (
                "Future S3 promotion must merge same-year partitions instead of overwriting "
                "whenever `requires_merge_with_existing_partition` is true."
            ),
            "- Merge-required symbols: "
            + (", ".join(sorted(merge_required)) if merge_required else "None"),
        ]
    )
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
        f.write("\n")


def run_rebased_export(config: RebasedExportConfig) -> RebasedExportResult:
    boundary_rows = read_csv_report(config.validation_dir / MASSIVE_BOUNDARY_REPORT)
    quality_rows = read_csv_report(config.validation_dir / SYMBOL_QUALITY_REPORT)
    quality_by_symbol = {
        str(row.get("symbol", "")).strip().upper(): row for row in quality_rows
    }

    if not config.dry_run and config.output_dir.exists():
        shutil.rmtree(config.output_dir)

    report_rows: list[dict[str, Any]] = []
    for boundary_row in boundary_rows:
        symbol = symbol_from_boundary_row(boundary_row)
        if config.allowed_symbols is not None and symbol not in config.allowed_symbols:
            continue

        if not parse_bool(boundary_row.get("has_pre_massive_history")) or parse_int(
            boundary_row.get("export_row_count")
        ) == 0:
            report_rows.append(
                build_report_row(
                    boundary_row=boundary_row,
                    action="skip",
                    rebase_factor=None,
                    input_rows=0,
                    output_rows=[],
                    output_path_count=0,
                    status="skipped",
                    warning="no pre-Massive export-eligible rows",
                )
            )
            continue

        action, factor, warning = determine_action(
            symbol=symbol,
            quality_row=quality_by_symbol.get(symbol),
            include_warning_symbols=config.include_warning_symbols,
            fail_on_missing_factor=config.fail_on_missing_factor,
        )
        table = load_symbol_export_table(config.input_export_dir, symbol)
        input_rows, pre_massive_rows = filtered_pre_massive_rows(
            table,
            source_backfill_end_date=boundary_row.get("source_backfill_end_date", ""),
            massive_first_available_date=boundary_row.get("massive_first_available_date", ""),
        )

        if action == "skip":
            report_rows.append(
                build_report_row(
                    boundary_row=boundary_row,
                    action=action,
                    rebase_factor=factor,
                    input_rows=input_rows,
                    output_rows=[],
                    output_path_count=0,
                    status="skipped",
                    warning=warning,
                )
            )
            continue
        if not pre_massive_rows:
            report_rows.append(
                build_report_row(
                    boundary_row=boundary_row,
                    action="skip",
                    rebase_factor=factor,
                    input_rows=input_rows,
                    output_rows=[],
                    output_path_count=0,
                    status="skipped",
                    warning="input export has no rows within the pre-Massive boundary",
                )
            )
            continue

        output_rows = pre_massive_rows
        if action != "use_as_is":
            output_rows = apply_rebase_factor(pre_massive_rows, factor or 1.0)
        written_paths = (
            []
            if config.dry_run
            else write_rebased_symbol_rows(config.output_dir, symbol, output_rows)
        )
        report_rows.append(
            build_report_row(
                boundary_row=boundary_row,
                action=action,
                rebase_factor=factor,
                input_rows=input_rows,
                output_rows=output_rows,
                output_path_count=len({int(row["year"]) for row in output_rows})
                if config.dry_run
                else len(written_paths),
                status="planned" if config.dry_run else "written",
                warning=warning,
            )
        )

    report_path = config.validation_dir / REBASED_EXPORT_REPORT
    manifest_path = config.validation_dir / REBASED_EXPORT_MANIFEST
    summary_path = config.validation_dir / REBASED_EXPORT_SUMMARY
    paths = report_paths(config)
    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_provider": config.source_provider,
        "input_export_dir": str(config.input_export_dir),
        "output_dir": str(config.output_dir),
        "validation_dir": str(config.validation_dir),
        "report_paths": paths,
        "symbols_processed": len(report_rows),
        "symbols_rebased": sum(
            1 for row in report_rows if row["action"] == "rebase" and row["status"] != "skipped"
        ),
        "symbols_used_as_is": sum(
            1
            for row in report_rows
            if row["action"] == "use_as_is" and row["status"] != "skipped"
        ),
        "symbols_skipped": sum(1 for row in report_rows if row["status"] == "skipped"),
        "total_output_rows": sum(int(row["output_rows"]) for row in report_rows),
        "dry_run": config.dry_run,
    }

    write_csv_report(report_path, report_rows, REPORT_COLUMNS)
    write_json(manifest_path, manifest)
    write_summary(summary_path, config=config, rows=report_rows)
    return RebasedExportResult(
        report_rows=report_rows,
        manifest=manifest,
        report_path=report_path,
        manifest_path=manifest_path,
        summary_path=summary_path,
    )


def main() -> None:
    config = config_from_args(parse_args())
    result = run_rebased_export(config)
    print(
        "rebased manual historical ETF export complete: "
        f"processed={result.manifest['symbols_processed']} "
        f"rebased={result.manifest['symbols_rebased']} "
        f"use_as_is={result.manifest['symbols_used_as_is']} "
        f"skipped={result.manifest['symbols_skipped']} "
        f"rows={result.manifest['total_output_rows']}"
    )
    print(f"report: {result.report_path}")
    print(f"manifest: {result.manifest_path}")
    print(f"summary: {result.summary_path}")
    if config.dry_run:
        print("dry run: no parquet files were written")


if __name__ == "__main__":
    main()
