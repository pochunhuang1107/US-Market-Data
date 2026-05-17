#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable

DEFAULT_INPUT_DIR = "data/manual_backfill/raw/tiingo"
DEFAULT_OUTPUT_DIR = "data/manual_backfill/raw/tiingo_split_adjusted"
DEFAULT_SYMBOLS_FILE = "config/etf_symbols.example.txt"
DEFAULT_VALIDATION_DIR = "data/manual_backfill/validation"
SOURCE_PROVIDER = "tiingo"
PRICE_MODE = "split_adjusted"

OUTPUT_COLUMNS = [
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
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

MANIFEST_COLUMNS = [
    "symbol",
    "input_path",
    "output_path",
    "status",
    "row_count",
    "first_date",
    "last_date",
    "split_event_count",
    "cumulative_split_factor_start",
    "cumulative_split_factor_end",
    "warning",
]

MANIFEST_FILENAME = "tiingo_split_adjusted_materialization_manifest.csv"
SUMMARY_FILENAME = "tiingo_split_adjusted_materialization_summary.md"


@dataclass(frozen=True)
class TiingoSplitAdjustedConfig:
    input_dir: Path = Path(DEFAULT_INPUT_DIR)
    output_dir: Path = Path(DEFAULT_OUTPUT_DIR)
    symbols_file: Path = Path(DEFAULT_SYMBOLS_FILE)
    symbols: list[str] | None = None
    validation_dir: Path = Path(DEFAULT_VALIDATION_DIR)
    overwrite: bool = False
    dry_run: bool = False
    progress_callback: Any = print


@dataclass
class TiingoSplitAdjustedResult:
    manifest_rows: list[dict[str, Any]]
    manifest_path: Path
    summary_path: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Materialize a local-only Tiingo split-adjusted manual backfill source "
            "from already-downloaded Tiingo normalized CSV files."
        )
    )
    parser.add_argument("--input-dir", default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--symbols-file", default=DEFAULT_SYMBOLS_FILE)
    parser.add_argument("--symbols", help="Optional comma-separated symbol override")
    parser.add_argument("--validation-dir", default=DEFAULT_VALIDATION_DIR)
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


def config_from_args(args: argparse.Namespace) -> TiingoSplitAdjustedConfig:
    symbols = parse_symbols_text(args.symbols) if args.symbols else load_symbols(args.symbols_file)
    return TiingoSplitAdjustedConfig(
        input_dir=Path(args.input_dir),
        output_dir=Path(args.output_dir),
        symbols_file=Path(args.symbols_file),
        symbols=symbols,
        validation_dir=Path(args.validation_dir),
        overwrite=bool(args.overwrite),
        dry_run=bool(args.dry_run),
    )


def validate_config(config: TiingoSplitAdjustedConfig) -> None:
    if not config.symbols:
        raise ValueError("no symbols requested")


def csv_column_map(fieldnames: Iterable[str] | None) -> dict[str, str]:
    return {name.strip().lower(): name for name in (fieldnames or [])}


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


def value_from_row(
    row: dict[str, str],
    columns: dict[str, str],
    name: str,
) -> object:
    source_column = columns.get(name)
    return row.get(source_column) if source_column else None


def parsed_value_from_row(
    row: dict[str, str],
    columns: dict[str, str],
    name: str,
) -> float | None:
    return parse_number(value_from_row(row, columns, name))


def choose_raw_value(
    row: dict[str, str],
    columns: dict[str, str],
    raw_name: str,
    fallback_name: str,
) -> float | None:
    raw_value = parsed_value_from_row(row, columns, raw_name)
    if raw_value is not None:
        return raw_value
    return parsed_value_from_row(row, columns, fallback_name)


def split_factor_from_row(row: dict[str, str], columns: dict[str, str]) -> float:
    split_factor = parsed_value_from_row(row, columns, "split_factor")
    if split_factor is None or split_factor <= 0:
        return 1.0
    return split_factor


def output_row_quality_issue(row: dict[str, Any], *, row_number: int) -> str:
    missing = [name for name in ["open", "high", "low", "close", "volume"] if row[name] is None]
    if missing:
        return f"row {row_number}: missing required output values: {', '.join(missing)}"

    open_value = float(row["open"])
    high_value = float(row["high"])
    low_value = float(row["low"])
    close_value = float(row["close"])
    volume_value = float(row["volume"])
    if open_value <= 0 or high_value <= 0 or low_value <= 0 or close_value <= 0:
        return f"row {row_number}: OHLC values must be positive"
    if volume_value < 0:
        return f"row {row_number}: volume must be non-negative"
    return ""


def apply_split_adjustments(rows: list[dict[str, Any]]) -> tuple[int, float, float]:
    running_factor = 1.0
    split_event_count = 0

    for row in reversed(rows):
        row["_cumulative_future_split_factor"] = running_factor
        split_factor = float(row["split_factor"])
        if not math.isclose(split_factor, 1.0):
            split_event_count += 1
        running_factor *= split_factor

    for row in rows:
        cumulative_factor = float(row["_cumulative_future_split_factor"])
        for name in ["open", "high", "low", "close"]:
            if row[name] is not None:
                row[name] = float(row[name]) / cumulative_factor
        if row["volume"] is not None:
            row["volume"] = float(row["volume"]) * cumulative_factor

    if not rows:
        return 0, 1.0, 1.0
    return (
        split_event_count,
        float(rows[0]["_cumulative_future_split_factor"]),
        float(rows[-1]["_cumulative_future_split_factor"]),
    )


def remove_internal_columns(rows: list[dict[str, Any]]) -> None:
    for row in rows:
        row.pop("_row_number", None)
        row.pop("_cumulative_future_split_factor", None)


def normalize_tiingo_csv(
    input_path: Path,
    *,
    symbol: str,
) -> tuple[list[dict[str, Any]], str, int, float, float]:
    rows_by_date: dict[str, dict[str, Any]] = {}
    invalid_dates = 0
    invalid_rows: list[str] = []
    raw_row_count = 0

    with open(input_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        columns = csv_column_map(reader.fieldnames)
        if "date" not in columns:
            raise ValueError("missing required input CSV column: date")

        for row_number, raw in enumerate(reader, start=2):
            raw_row_count += 1
            normalized_date = parse_date_value(value_from_row(raw, columns, "date"))
            if normalized_date is None:
                invalid_dates += 1
                continue

            output_row = {
                "date": normalized_date,
                "open": choose_raw_value(raw, columns, "raw_open", "open"),
                "high": choose_raw_value(raw, columns, "raw_high", "high"),
                "low": choose_raw_value(raw, columns, "raw_low", "low"),
                "close": choose_raw_value(raw, columns, "raw_close", "close"),
                "volume": choose_raw_value(raw, columns, "raw_volume", "volume"),
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
                "split_factor": split_factor_from_row(raw, columns),
                "_row_number": row_number,
            }
            rows_by_date[normalized_date] = output_row

    rows = [rows_by_date[key] for key in sorted(rows_by_date)]
    if not rows:
        raise ValueError("no valid split-adjusted price rows")

    split_event_count, factor_start, factor_end = apply_split_adjustments(rows)
    for row in rows:
        row_issue = output_row_quality_issue(row, row_number=int(row["_row_number"]))
        if row_issue:
            invalid_rows.append(row_issue)
    if invalid_rows:
        raise ValueError("; ".join(invalid_rows[:5]))
    remove_internal_columns(rows)

    warnings: list[str] = []
    duplicate_count = raw_row_count - invalid_dates - len(rows)
    if duplicate_count:
        warnings.append(f"duplicate date rows deduplicated: {duplicate_count}")
    if invalid_dates:
        warnings.append(f"rows skipped for invalid date: {invalid_dates}")
    return rows, "; ".join(warnings), split_event_count, factor_start, factor_end


def write_normalized_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


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
    input_path: Path,
    output_path: Path,
    status: str,
    rows: list[dict[str, Any]] | None = None,
    split_event_count: int = 0,
    cumulative_split_factor_start: float | str = "",
    cumulative_split_factor_end: float | str = "",
    warning: str = "",
) -> dict[str, Any]:
    first_date, last_date = first_last_dates(rows or [])
    return {
        "symbol": symbol,
        "input_path": str(input_path),
        "output_path": str(output_path),
        "status": status,
        "row_count": len(rows or []),
        "first_date": first_date,
        "last_date": last_date,
        "split_event_count": split_event_count,
        "cumulative_split_factor_start": cumulative_split_factor_start,
        "cumulative_split_factor_end": cumulative_split_factor_end,
        "warning": warning,
    }


def write_summary(
    path: Path,
    *,
    config: TiingoSplitAdjustedConfig,
    rows: list[dict[str, Any]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    written_count = sum(1 for row in rows if row["status"] == "written")
    skipped_count = sum(1 for row in rows if row["status"] == "skipped_existing")
    failed_count = sum(1 for row in rows if row["status"] == "failed")
    planned_count = sum(1 for row in rows if row["status"] == "planned")
    symbols_with_splits = [
        str(row["symbol"])
        for row in rows
        if int(row.get("split_event_count") or 0) > 0
    ]

    lines = [
        "# Tiingo Split-Adjusted Materialization Summary",
        "",
        "## Purpose",
        (
            "Materialize a validator-compatible local Tiingo candidate whose effective "
            "OHLCV columns use Tiingo raw fields adjusted for stock splits only."
        ),
        "",
        "## Local-Only Notes",
        "- This script reads already-downloaded local Tiingo CSV files only.",
        "- No API call happened during this run.",
        "- No S3 write happened during this run.",
        "- No data promotion happened during this run.",
        "- No Massive automation changes happened during this run.",
        "- Tiingo adjusted OHLC fields were preserved only as diagnostics, not used as effective prices.",
        "- Dividend cash fields were preserved only as diagnostics; no dividend adjustment was applied.",
    ]
    if config.dry_run:
        lines.append("- Dry run: symbol CSV files were not written.")
    lines.extend(
        [
            "",
            "## Counts",
            f"- Symbol count: {len(rows)}",
            f"- Written count: {written_count}",
            f"- Skipped count: {skipped_count}",
            f"- Failed count: {failed_count}",
        ]
    )
    if planned_count:
        lines.append(f"- Planned count: {planned_count}")
    lines.extend(
        [
            "",
            "## Symbols With Split Events",
            f"- Count: {len(symbols_with_splits)}",
            f"- Symbols: {', '.join(symbols_with_splits) if symbols_with_splits else 'none'}",
            "",
            "## Output",
            f"- Output dir: {config.output_dir}",
            "",
            "## Next Validation Command",
            "```bash",
            "python scripts/validate_manual_historical_etf_backfill.py \\",
            "  --raw-dir data/manual_backfill/raw/tiingo_split_adjusted \\",
            "  --source-provider tiingo_split \\",
            "  --massive-curated-dir data/manual_backfill/massive_curated_sample \\",
            "  --output-dir data/manual_backfill_tiingo_split \\",
            "  --symbols-file config/etf_symbols.example.txt \\",
            "  --backfill-boundary-mode infer_from_massive \\",
            "  --backfill-end-date 2017-12-31 \\",
            "  --overlap-start-date 2016-01-01 \\",
            "  --overlap-end-date 2026-05-08 \\",
            "  --boundary-window-days 20,60,120",
            "```",
        ]
    )
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
        f.write("\n")


def run_materialization(config: TiingoSplitAdjustedConfig) -> TiingoSplitAdjustedResult:
    validate_config(config)
    assert config.symbols is not None
    manifest_rows: list[dict[str, Any]] = []

    for symbol in config.symbols:
        normalized_symbol = symbol.upper()
        input_path = config.input_dir / f"{normalized_symbol}.csv"
        output_path = config.output_dir / f"{normalized_symbol}.csv"

        if output_path.exists() and not config.overwrite:
            manifest_rows.append(
                manifest_row(
                    symbol=normalized_symbol,
                    input_path=input_path,
                    output_path=output_path,
                    status="skipped_existing",
                    warning="output file exists; rerun with --overwrite to replace it",
                )
            )
            continue

        try:
            (
                rows,
                warning,
                split_event_count,
                factor_start,
                factor_end,
            ) = normalize_tiingo_csv(input_path, symbol=normalized_symbol)
            status = "planned" if config.dry_run else "written"
            if not config.dry_run:
                write_normalized_csv(output_path, rows)
            if config.progress_callback:
                config.progress_callback(f"{status} {normalized_symbol}: {output_path}")
            manifest_rows.append(
                manifest_row(
                    symbol=normalized_symbol,
                    input_path=input_path,
                    output_path=output_path,
                    status=status,
                    rows=rows,
                    split_event_count=split_event_count,
                    cumulative_split_factor_start=factor_start,
                    cumulative_split_factor_end=factor_end,
                    warning="dry run: symbol CSV not written" if config.dry_run else warning,
                )
            )
        except Exception as exc:  # noqa: BLE001 - per-symbol failure should continue.
            manifest_rows.append(
                manifest_row(
                    symbol=normalized_symbol,
                    input_path=input_path,
                    output_path=output_path,
                    status="failed",
                    warning=str(exc),
                )
            )
            if config.progress_callback:
                config.progress_callback(f"failed {normalized_symbol}: {exc}")

    manifest_path = config.validation_dir / MANIFEST_FILENAME
    summary_path = config.validation_dir / SUMMARY_FILENAME
    write_csv_report(manifest_path, manifest_rows, MANIFEST_COLUMNS)
    write_summary(summary_path, config=config, rows=manifest_rows)

    return TiingoSplitAdjustedResult(
        manifest_rows=manifest_rows,
        manifest_path=manifest_path,
        summary_path=summary_path,
    )


def all_requested_symbols_failed(result: TiingoSplitAdjustedResult) -> bool:
    return bool(result.manifest_rows) and all(
        row["status"] == "failed" for row in result.manifest_rows
    )


def main() -> None:
    try:
        config = config_from_args(parse_args())
        result = run_materialization(config)
    except Exception as exc:  # noqa: BLE001 - CLI should fail clearly.
        print(f"Tiingo split-adjusted materialization failed: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc

    written = sum(1 for row in result.manifest_rows if row["status"] == "written")
    skipped = sum(1 for row in result.manifest_rows if row["status"] == "skipped_existing")
    failed = sum(1 for row in result.manifest_rows if row["status"] == "failed")
    planned = sum(1 for row in result.manifest_rows if row["status"] == "planned")
    print(
        "Tiingo split-adjusted materialization complete: "
        f"symbols={len(result.manifest_rows)} written={written} "
        f"skipped_existing={skipped} failed={failed} planned={planned}"
    )
    print(f"manifest: {result.manifest_path}")
    print(f"summary: {result.summary_path}")
    if all_requested_symbols_failed(result):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
