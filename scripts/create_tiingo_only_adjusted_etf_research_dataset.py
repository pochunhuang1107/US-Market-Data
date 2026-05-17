#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
import shutil
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable

import pyarrow as pa
import pyarrow.parquet as pq

DEFAULT_INPUT_DIR = "data/manual_backfill/raw/tiingo"
DEFAULT_OUTPUT_DIR = "data/research/etf_daily_prices_tiingo_only"
DEFAULT_SYMBOLS_FILE = "config/etf_symbols.example.txt"
DEFAULT_START_DATE = "1993-01-01"
SOURCE_PROVIDER = "tiingo"
PRICE_MODE = "tiingo_adjusted"
DATASET_NAME = "etf_daily_prices_tiingo_only"

DOWNLOAD_HINT = (
    "python scripts/download_tiingo_historical_etf_data.py "
    "--symbols-file config/etf_symbols.example.txt "
    "--start-date 1993-01-01 "
    "--output-dir data/manual_backfill/raw/tiingo "
    "--price-mode adjusted"
)

OUTPUT_COLUMNS = [
    "date",
    "symbol",
    "year",
    "month",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "vwap",
    "transactions",
    "source_provider",
    "source_symbol",
    "price_mode",
    "raw_open",
    "raw_high",
    "raw_low",
    "raw_close",
    "raw_volume",
    "adj_open",
    "adj_high",
    "adj_low",
    "adj_close",
    "adj_volume",
    "div_cash",
    "split_factor",
]

RESEARCH_SCHEMA = pa.schema(
    [
        pa.field("date", pa.string()),
        pa.field("symbol", pa.string()),
        pa.field("year", pa.int32()),
        pa.field("month", pa.int32()),
        pa.field("open", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("close", pa.float64()),
        pa.field("volume", pa.float64()),
        pa.field("vwap", pa.float64()),
        pa.field("transactions", pa.float64()),
        pa.field("source_provider", pa.string()),
        pa.field("source_symbol", pa.string()),
        pa.field("price_mode", pa.string()),
        pa.field("raw_open", pa.float64()),
        pa.field("raw_high", pa.float64()),
        pa.field("raw_low", pa.float64()),
        pa.field("raw_close", pa.float64()),
        pa.field("raw_volume", pa.float64()),
        pa.field("adj_open", pa.float64()),
        pa.field("adj_high", pa.float64()),
        pa.field("adj_low", pa.float64()),
        pa.field("adj_close", pa.float64()),
        pa.field("adj_volume", pa.float64()),
        pa.field("div_cash", pa.float64()),
        pa.field("split_factor", pa.float64()),
    ]
)

MANIFEST_COLUMNS = [
    "symbol",
    "status",
    "input_path",
    "output_paths",
    "row_count",
    "first_date",
    "last_date",
    "price_mode",
    "warning",
]

MANIFEST_FILENAME = "tiingo_only_adjusted_manifest.csv"
SUMMARY_FILENAME = "tiingo_only_adjusted_summary.md"


class TiingoOnlyDatasetError(RuntimeError):
    pass


@dataclass(frozen=True)
class TiingoOnlyAdjustedConfig:
    input_dir: Path = Path(DEFAULT_INPUT_DIR)
    output_dir: Path = Path(DEFAULT_OUTPUT_DIR)
    symbols_file: Path = Path(DEFAULT_SYMBOLS_FILE)
    symbols: list[str] | None = None
    start_date: str = DEFAULT_START_DATE
    end_date: str | None = None
    overwrite: bool = False
    dry_run: bool = False
    progress_callback: Any = print


@dataclass
class TiingoOnlyAdjustedResult:
    manifest_rows: list[dict[str, Any]]
    manifest_path: Path
    summary_path: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Materialize a local-only Tiingo-only adjusted ETF research parquet dataset "
            "from already-downloaded Tiingo normalized CSV files."
        )
    )
    parser.add_argument("--input-dir", default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--symbols-file", default=DEFAULT_SYMBOLS_FILE)
    parser.add_argument("--symbols", help="Optional comma-separated symbol override")
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--end-date")
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


def validate_iso_date(value: str, name: str) -> str:
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise TiingoOnlyDatasetError(f"{name} must be YYYY-MM-DD: {value}") from exc


def today_iso() -> str:
    return date.today().isoformat()


def effective_end_date(config: TiingoOnlyAdjustedConfig) -> str:
    return config.end_date or today_iso()


def config_from_args(args: argparse.Namespace) -> TiingoOnlyAdjustedConfig:
    symbols = parse_symbols_text(args.symbols) if args.symbols else load_symbols(args.symbols_file)
    return TiingoOnlyAdjustedConfig(
        input_dir=Path(args.input_dir),
        output_dir=Path(args.output_dir),
        symbols_file=Path(args.symbols_file),
        symbols=symbols,
        start_date=validate_iso_date(args.start_date, "--start-date"),
        end_date=validate_iso_date(args.end_date or today_iso(), "--end-date"),
        overwrite=bool(args.overwrite),
        dry_run=bool(args.dry_run),
    )


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
        raise TiingoOnlyDatasetError("input-dir and output-dir must be different")
    if is_relative_to(resolved_input, resolved_output):
        raise TiingoOnlyDatasetError("output-dir must not be a parent of input-dir")
    if is_relative_to(resolved_output, resolved_input):
        raise TiingoOnlyDatasetError("output-dir must not be inside input-dir")


def validate_config(config: TiingoOnlyAdjustedConfig) -> None:
    if not config.symbols:
        raise TiingoOnlyDatasetError("no symbols requested")
    if not config.input_dir.exists():
        raise TiingoOnlyDatasetError(
            f"input-dir does not exist: {config.input_dir}. "
            f"Run the Tiingo downloader first: {DOWNLOAD_HINT}"
        )
    if config.start_date > effective_end_date(config):
        raise TiingoOnlyDatasetError("--start-date must be on or before --end-date")
    validate_path_relationships(config.input_dir, config.output_dir)
    if not config.dry_run and not config.overwrite and has_files(config.output_dir):
        raise TiingoOnlyDatasetError(
            f"output-dir already exists with files; rerun with --overwrite: {config.output_dir}"
        )


def csv_column_map(fieldnames: Iterable[str] | None) -> dict[str, str]:
    return {name.strip().lower(): name for name in (fieldnames or [])}


def value_from_row(
    row: dict[str, str],
    columns: dict[str, str],
    *names: str,
) -> object:
    for name in names:
        source_column = columns.get(name.strip().lower())
        if source_column:
            return row.get(source_column)
    return None


def parse_date_value(value: object) -> str | None:
    text = str(value).strip() if value is not None else ""
    if not text:
        return None
    candidate = text[:10]
    try:
        return date.fromisoformat(candidate).isoformat()
    except ValueError:
        return None


def parse_number(value: object) -> float | None:
    text = str(value).strip() if value is not None else ""
    if not text:
        return None
    try:
        number = float(text.replace(",", ""))
    except ValueError:
        return None
    return number if math.isfinite(number) else None


def parsed_value_from_row(
    row: dict[str, str],
    columns: dict[str, str],
    *names: str,
) -> float | None:
    return parse_number(value_from_row(row, columns, *names))


def choose_adjusted_value(
    row: dict[str, str],
    columns: dict[str, str],
    adjusted_name: str,
    fallback_name: str,
    fallback_counts: dict[str, int],
) -> float | None:
    adjusted_value = parsed_value_from_row(row, columns, adjusted_name)
    if adjusted_value is not None:
        return adjusted_value

    fallback_value = parsed_value_from_row(row, columns, fallback_name)
    if fallback_value is not None:
        fallback_counts[adjusted_name] = fallback_counts.get(adjusted_name, 0) + 1
    return fallback_value


def output_row_quality_issue(row: dict[str, Any], *, row_number: int) -> str:
    missing = [name for name in ["open", "high", "low", "close", "volume"] if row[name] is None]
    if missing:
        return f"row {row_number}: missing required adjusted output values: {', '.join(missing)}"

    open_value = float(row["open"])
    high_value = float(row["high"])
    low_value = float(row["low"])
    close_value = float(row["close"])
    volume_value = float(row["volume"])
    if open_value <= 0 or high_value <= 0 or low_value <= 0 or close_value <= 0:
        return f"row {row_number}: adjusted OHLC values must be positive"
    if high_value < max(open_value, close_value, low_value):
        return f"row {row_number}: adjusted high is below open/low/close"
    if low_value > min(open_value, close_value, high_value):
        return f"row {row_number}: adjusted low is above open/high/close"
    if volume_value < 0:
        return f"row {row_number}: adjusted volume must be non-negative"
    return ""


def normalize_tiingo_csv(
    input_path: Path,
    *,
    symbol: str,
    start_date: str,
    end_date: str,
) -> tuple[list[dict[str, Any]], str]:
    rows_by_date: dict[str, dict[str, Any]] = {}
    invalid_dates = 0
    out_of_range_rows = 0
    invalid_rows: list[str] = []
    fallback_counts: dict[str, int] = {}
    raw_row_count = 0
    valid_candidate_count = 0

    with open(input_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        columns = csv_column_map(reader.fieldnames)
        if "date" not in columns:
            raise TiingoOnlyDatasetError("missing required input CSV column: date")

        for row_number, raw in enumerate(reader, start=2):
            raw_row_count += 1
            normalized_date = parse_date_value(value_from_row(raw, columns, "date"))
            if normalized_date is None:
                invalid_dates += 1
                continue
            if normalized_date < start_date or normalized_date > end_date:
                out_of_range_rows += 1
                continue

            year_text, month_text, _ = normalized_date.split("-")
            output_row = {
                "date": normalized_date,
                "symbol": symbol.upper(),
                "year": int(year_text),
                "month": int(month_text),
                "open": choose_adjusted_value(
                    raw, columns, "adj_open", "open", fallback_counts
                ),
                "high": choose_adjusted_value(
                    raw, columns, "adj_high", "high", fallback_counts
                ),
                "low": choose_adjusted_value(raw, columns, "adj_low", "low", fallback_counts),
                "close": choose_adjusted_value(
                    raw, columns, "adj_close", "close", fallback_counts
                ),
                "volume": choose_adjusted_value(
                    raw, columns, "adj_volume", "volume", fallback_counts
                ),
                "vwap": parsed_value_from_row(raw, columns, "vwap"),
                "transactions": parsed_value_from_row(raw, columns, "transactions"),
                "source_provider": SOURCE_PROVIDER,
                "source_symbol": str(
                    value_from_row(raw, columns, "source_symbol") or symbol.upper()
                ).upper(),
                "price_mode": PRICE_MODE,
                "raw_open": parsed_value_from_row(raw, columns, "raw_open"),
                "raw_high": parsed_value_from_row(raw, columns, "raw_high"),
                "raw_low": parsed_value_from_row(raw, columns, "raw_low"),
                "raw_close": parsed_value_from_row(raw, columns, "raw_close"),
                "raw_volume": parsed_value_from_row(raw, columns, "raw_volume"),
                "adj_open": parsed_value_from_row(raw, columns, "adj_open"),
                "adj_high": parsed_value_from_row(raw, columns, "adj_high"),
                "adj_low": parsed_value_from_row(raw, columns, "adj_low"),
                "adj_close": parsed_value_from_row(raw, columns, "adj_close"),
                "adj_volume": parsed_value_from_row(raw, columns, "adj_volume"),
                "div_cash": parsed_value_from_row(raw, columns, "div_cash"),
                "split_factor": parsed_value_from_row(raw, columns, "split_factor"),
            }
            row_issue = output_row_quality_issue(output_row, row_number=row_number)
            if row_issue:
                invalid_rows.append(row_issue)
                continue
            valid_candidate_count += 1
            rows_by_date[normalized_date] = output_row

    rows = [rows_by_date[key] for key in sorted(rows_by_date)]
    if invalid_rows:
        raise TiingoOnlyDatasetError("; ".join(invalid_rows[:5]))
    if not rows:
        raise TiingoOnlyDatasetError("no valid Tiingo adjusted price rows in requested date range")

    warnings: list[str] = []
    duplicate_count = valid_candidate_count - len(rows)
    if duplicate_count:
        warnings.append(f"duplicate date rows deduplicated: {duplicate_count}")
    if invalid_dates:
        warnings.append(f"rows skipped for invalid date: {invalid_dates}")
    if out_of_range_rows:
        warnings.append(f"rows skipped outside requested date range: {out_of_range_rows}")
    for field_name, count in sorted(fallback_counts.items()):
        warnings.append(f"{field_name} missing; used effective CSV fallback rows: {count}")
    if raw_row_count == 0:
        warnings.append("input CSV contained no data rows")
    return rows, "; ".join(warnings)


def rows_to_research_table(rows: list[dict[str, Any]]) -> pa.Table:
    if not rows:
        arrays = [pa.array([], type=field.type) for field in RESEARCH_SCHEMA]
        return pa.Table.from_arrays(arrays, schema=RESEARCH_SCHEMA)
    return pa.Table.from_pylist(
        [{column: row.get(column) for column in OUTPUT_COLUMNS} for row in rows],
        schema=RESEARCH_SCHEMA,
    )


def output_path_for_partition(output_dir: Path, symbol: str, year: int) -> Path:
    return output_dir / f"symbol={symbol.upper()}" / f"year={year}" / "data.parquet"


def write_partitioned_parquet(
    output_dir: Path,
    *,
    symbol: str,
    rows: list[dict[str, Any]],
) -> list[Path]:
    output_paths: list[Path] = []
    rows_by_year: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        rows_by_year.setdefault(int(row["year"]), []).append(row)

    for year, year_rows in sorted(rows_by_year.items()):
        output_path = output_path_for_partition(output_dir, symbol, year)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        table = rows_to_research_table(sorted(year_rows, key=lambda row: str(row["date"])))
        pq.write_table(table, output_path)
        output_paths.append(output_path)
    return output_paths


def planned_output_paths(
    output_dir: Path,
    *,
    symbol: str,
    rows: list[dict[str, Any]],
) -> list[Path]:
    years = sorted({int(row["year"]) for row in rows})
    return [output_path_for_partition(output_dir, symbol, year) for year in years]


def write_csv_report(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def first_last_dates(rows: list[dict[str, Any]]) -> tuple[str, str]:
    dates = [str(row["date"]) for row in rows if row.get("date")]
    if not dates:
        return "", ""
    return min(dates), max(dates)


def manifest_row(
    *,
    symbol: str,
    status: str,
    input_path: Path,
    output_paths: list[Path] | None = None,
    rows: list[dict[str, Any]] | None = None,
    warning: str = "",
) -> dict[str, Any]:
    first_date, last_date = first_last_dates(rows or [])
    return {
        "symbol": symbol,
        "status": status,
        "input_path": str(input_path),
        "output_paths": ";".join(str(path) for path in (output_paths or [])),
        "row_count": len(rows or []),
        "first_date": first_date,
        "last_date": last_date,
        "price_mode": PRICE_MODE,
        "warning": warning,
    }


def report_paths(output_dir: Path) -> tuple[Path, Path]:
    reports_dir = output_dir / "_reports"
    return reports_dir / MANIFEST_FILENAME, reports_dir / SUMMARY_FILENAME


def row_statuses(rows: list[dict[str, Any]], statuses: set[str]) -> list[dict[str, Any]]:
    return [row for row in rows if str(row.get("status")) in statuses]


def write_summary(
    path: Path,
    *,
    config: TiingoOnlyAdjustedConfig,
    rows: list[dict[str, Any]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    materialized = row_statuses(rows, {"written", "planned"})
    skipped = row_statuses(rows, {"skipped_missing_input"})
    failed = row_statuses(rows, {"failed"})
    warnings = [row for row in rows if row.get("warning")]
    dates = [
        str(row[field])
        for row in materialized
        for field in ("first_date", "last_date")
        if row.get(field)
    ]

    lines = [
        "# Tiingo-Only Adjusted ETF Research Dataset",
        "",
        "## Dataset",
        f"- Dataset path: {config.output_dir}",
        f"- Source input path: {config.input_dir}",
        f"- Source provider: {SOURCE_PROVIDER}",
        f"- Price mode: {PRICE_MODE}",
        f"- Start date: {config.start_date}",
        f"- End date: {effective_end_date(config)}",
        "",
        "## Coverage",
        f"- Symbol count: {len(rows)}",
        f"- Materialized symbols: {len(materialized)}",
        f"- Total rows: {sum(int(row['row_count']) for row in materialized)}",
        f"- First date: {min(dates) if dates else ''}",
        f"- Last date: {max(dates) if dates else ''}",
        "- Skipped symbols: "
        + (", ".join(row["symbol"] for row in skipped) if skipped else "None"),
        f"- Failed symbols: {', '.join(row['symbol'] for row in failed) if failed else 'None'}",
        "",
        "## Local-Only Notes",
        "- This dataset is local-only and was not promoted to S3.",
        "- No S3 write happened during this run.",
        "- No Tiingo API call happened during this run.",
        "- Massive remains the app/platform data source.",
        "- The Tiingo/Massive bridged extended research dataset was not used as input.",
    ]
    if config.dry_run:
        lines.append("- Dry run: parquet files were not written.")
    lines.extend(
        [
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
            "## Parquet Glob",
            "```text",
            f"{config.output_dir}/**/*.parquet",
            "```",
        ]
    )
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
        f.write("\n")


def run_materialization(config: TiingoOnlyAdjustedConfig) -> TiingoOnlyAdjustedResult:
    validate_config(config)
    if config.overwrite and not config.dry_run and config.output_dir.exists():
        shutil.rmtree(config.output_dir)

    assert config.symbols is not None
    end_date = effective_end_date(config)
    manifest_rows: list[dict[str, Any]] = []

    for symbol in config.symbols:
        normalized_symbol = symbol.upper()
        input_path = config.input_dir / f"{normalized_symbol}.csv"
        if not input_path.exists():
            warning = (
                "input CSV does not exist; run the Tiingo downloader first: "
                f"{DOWNLOAD_HINT}"
            )
            manifest_rows.append(
                manifest_row(
                    symbol=normalized_symbol,
                    status="skipped_missing_input",
                    input_path=input_path,
                    warning=warning,
                )
            )
            if config.progress_callback:
                config.progress_callback(f"skipped {normalized_symbol}: {warning}")
            continue

        try:
            rows, warning = normalize_tiingo_csv(
                input_path,
                symbol=normalized_symbol,
                start_date=config.start_date,
                end_date=end_date,
            )
            status = "planned" if config.dry_run else "written"
            output_paths = (
                planned_output_paths(config.output_dir, symbol=normalized_symbol, rows=rows)
                if config.dry_run
                else write_partitioned_parquet(
                    config.output_dir,
                    symbol=normalized_symbol,
                    rows=rows,
                )
            )
            manifest_rows.append(
                manifest_row(
                    symbol=normalized_symbol,
                    status=status,
                    input_path=input_path,
                    output_paths=output_paths,
                    rows=rows,
                    warning="dry run: parquet files not written" if config.dry_run else warning,
                )
            )
            if config.progress_callback:
                config.progress_callback(
                    f"{status} {normalized_symbol}: "
                    f"{len(rows)} rows across {len(output_paths)} parquet file(s)"
                )
        except Exception as exc:  # noqa: BLE001 - per-symbol failure should continue.
            manifest_rows.append(
                manifest_row(
                    symbol=normalized_symbol,
                    status="failed",
                    input_path=input_path,
                    warning=str(exc),
                )
            )
            if config.progress_callback:
                config.progress_callback(f"failed {normalized_symbol}: {exc}")

    manifest_path, summary_path = report_paths(config.output_dir)
    write_csv_report(manifest_path, manifest_rows, MANIFEST_COLUMNS)
    write_summary(summary_path, config=config, rows=manifest_rows)
    return TiingoOnlyAdjustedResult(
        manifest_rows=manifest_rows,
        manifest_path=manifest_path,
        summary_path=summary_path,
    )


def all_requested_symbols_unmaterialized(result: TiingoOnlyAdjustedResult) -> bool:
    materialized_statuses = {"written", "planned"}
    return bool(result.manifest_rows) and all(
        str(row.get("status")) not in materialized_statuses for row in result.manifest_rows
    )


def main() -> None:
    try:
        config = config_from_args(parse_args())
        result = run_materialization(config)
    except Exception as exc:  # noqa: BLE001 - CLI should fail clearly.
        print(
            f"Tiingo-only adjusted ETF research dataset materialization failed: {exc}",
            file=sys.stderr,
        )
        raise SystemExit(2) from exc

    written = sum(1 for row in result.manifest_rows if row["status"] == "written")
    planned = sum(1 for row in result.manifest_rows if row["status"] == "planned")
    skipped = sum(1 for row in result.manifest_rows if row["status"] == "skipped_missing_input")
    failed = sum(1 for row in result.manifest_rows if row["status"] == "failed")
    total_rows = sum(
        int(row["row_count"])
        for row in result.manifest_rows
        if row["status"] in {"written", "planned"}
    )
    print(
        "Tiingo-only adjusted ETF research dataset materialization complete: "
        f"symbols={len(result.manifest_rows)} written={written} planned={planned} "
        f"skipped_missing_input={skipped} failed={failed} rows={total_rows}"
    )
    print(f"manifest: {result.manifest_path}")
    print(f"summary: {result.summary_path}")
    print(f"parquet glob: {config.output_dir}/**/*.parquet")
    if all_requested_symbols_unmaterialized(result):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
