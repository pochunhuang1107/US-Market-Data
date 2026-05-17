#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from datetime import date, datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any, Iterable

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from us_market_data.s3_paths import curated_key  # noqa: E402

CURATED_ETF_SCHEMA = pa.schema(
    [
        pa.field("date", pa.string()),
        pa.field("year", pa.int32()),
        pa.field("month", pa.int32()),
        pa.field("symbol", pa.string()),
        pa.field("open", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("close", pa.float64()),
        pa.field("volume", pa.float64()),
        pa.field("vwap", pa.float64()),
        pa.field("transactions", pa.float64()),
    ]
)

REPORT_FILENAMES = {
    "coverage": "coverage_report.csv",
    "overlap": "overlap_report.csv",
    "event_rows": "event_rows_report.csv",
    "symbol_quality": "symbol_quality_report.csv",
    "manifest": "backfill_manifest.json",
    "summary": "manual_backfill_summary.md",
}

OVERLAP_CLOSE_PCT_FAIL_THRESHOLD = 0.05
OVERLAP_RETURN_DIFF_FAIL_THRESHOLD = 0.05
PRICE_COLUMN_NAMES = {
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "vwap",
    "transactions",
    "symbol",
    "ticker",
}
EVENT_METADATA_FIELDS = [
    "_symbol",
    "_source_provider",
    "_raw_row_number",
    "_event_types_detected",
    "_event_detection_reason",
]


@dataclass
class SymbolResult:
    symbol: str
    table: pa.Table
    issues: list[str] = dataclass_field(default_factory=list)
    duplicate_rows_removed: int = 0
    ohlc_bad_rows: int = 0
    raw_row_count: int = 0
    price_row_count: int = 0
    event_rows: list[dict[str, Any]] = dataclass_field(default_factory=list)
    event_fieldnames: list[str] = dataclass_field(default_factory=list)
    event_types_detected: list[str] = dataclass_field(default_factory=list)
    quality_status: str = "pass"


@dataclass
class ValidationRunResult:
    coverage_rows: list[dict[str, Any]]
    overlap_rows: list[dict[str, Any]]
    event_rows: list[dict[str, Any]]
    symbol_quality_rows: list[dict[str, Any]]
    manifest: dict[str, Any]
    warnings: list[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate local manual historical ETF CSV backfills and stage curated-style parquet"
        )
    )
    parser.add_argument("--raw-dir", default="data/manual_backfill/raw/stooq")
    parser.add_argument("--source-provider", default="stooq")
    parser.add_argument(
        "--massive-curated-dir",
        default="data/manual_backfill/massive_curated_sample",
    )
    parser.add_argument("--output-dir", default="data/manual_backfill")
    parser.add_argument("--symbols-file")
    parser.add_argument("--backfill-end-date", default="2017-12-31")
    parser.add_argument("--overlap-start-date", default="2018-01-01")
    parser.add_argument("--overlap-end-date", default="2026-05-08")
    parser.add_argument("--start-date")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def empty_curated_table() -> pa.Table:
    arrays = [pa.array([], type=field.type) for field in CURATED_ETF_SCHEMA]
    return pa.Table.from_arrays(arrays, schema=CURATED_ETF_SCHEMA)


def rows_to_curated_table(rows: list[dict[str, Any]]) -> pa.Table:
    if not rows:
        return empty_curated_table()
    return pa.Table.from_pylist(rows, schema=CURATED_ETF_SCHEMA)


def align_table_to_curated_schema(table: pa.Table) -> pa.Table:
    arrays = []
    for field in CURATED_ETF_SCHEMA:
        if field.name in table.column_names:
            arrays.append(pc.cast(table[field.name], field.type))
        else:
            arrays.append(pa.array([None] * table.num_rows, type=field.type))
    return pa.Table.from_arrays(arrays, schema=CURATED_ETF_SCHEMA)


def validate_date_arg(value: str | None, name: str) -> str | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise SystemExit(f"{name} must be YYYY-MM-DD: {value}") from exc


def parse_date_value(value: object) -> str | None:
    text = str(value).strip() if value is not None else ""
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def parse_float_value(value: object) -> float | None:
    text = str(value).strip() if value is not None else ""
    if not text:
        return None
    try:
        return float(text.replace(",", ""))
    except ValueError:
        return None


def event_types_from_text(value: object) -> set[str]:
    text = str(value).strip().lower() if value is not None else ""
    if not text:
        return set()

    detected: set[str] = set()
    padded = "".join(char if char.isalnum() else " " for char in text)
    tokens = set(padded.split())
    if "dividend" in text or "dividends" in text or "div" in tokens:
        detected.add("dividend")
    if "split" in text:
        detected.add("split")
    if "distribution" in text:
        detected.add("distribution")
    if "event" in text:
        detected.add("event")
    if "action" in text:
        detected.add("action")
    return detected


def has_substantive_value(value: object) -> bool:
    text = str(value).strip() if value is not None else ""
    if not text:
        return False
    lowered = text.lower()
    if lowered in {"none", "nan", "null", "false", "n/a", "na"}:
        return False
    numeric_value = parse_float_value(text)
    if numeric_value is not None:
        return numeric_value != 0
    return True


def has_complete_ohlc(raw: dict[str, Any], column_map: dict[str, str]) -> bool:
    values = [
        parse_float_value(raw.get(column_map[name]))
        for name in ["open", "high", "low", "close"]
        if name in column_map
    ]
    return len(values) == 4 and all(value is not None and value > 0 for value in values)


def detect_event_row(
    raw: dict[str, Any],
    column_map: dict[str, str],
) -> tuple[set[str], list[str]]:
    if has_complete_ohlc(raw, column_map):
        return set(), []

    event_types: set[str] = set()
    reasons: list[str] = []
    for column, value in raw.items():
        if column is None:
            continue
        normalized_column = str(column).strip().lower()
        if normalized_column in PRICE_COLUMN_NAMES:
            continue

        column_event_types = event_types_from_text(normalized_column)
        value_event_types = event_types_from_text(value)
        if column_event_types and has_substantive_value(value):
            event_types.update(column_event_types)
            reasons.append(f"event-like column populated: {column}")
        if value_event_types:
            event_types.update(value_event_types)
            reasons.append(f"event-like value in column: {column}")

    return event_types, reasons


def load_symbols(symbols_file: str | None, raw_dir: Path) -> list[str]:
    if symbols_file:
        with open(symbols_file, "r", encoding="utf-8") as f:
            symbols = [
                line.strip().upper()
                for line in f
                if line.strip() and not line.lstrip().startswith("#")
            ]
        return sorted(set(symbols))

    if not raw_dir.exists():
        return []
    return sorted(path.stem.upper() for path in raw_dir.glob("*.csv"))


def csv_column_map(fieldnames: Iterable[str] | None) -> dict[str, str]:
    return {name.strip().lower(): name for name in (fieldnames or [])}


def normalize_csv_file(
    csv_path: Path,
    *,
    symbol: str,
    source_provider: str = "",
    start_date: str | None = None,
) -> SymbolResult:
    issues: list[str] = []
    rows: list[dict[str, Any]] = []
    event_rows: list[dict[str, Any]] = []
    event_fieldnames: list[str] = []
    raw_row_count = 0
    required = ["date", "open", "high", "low", "close", "volume"]
    optional = ["vwap", "transactions"]

    try:
        with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            column_map = csv_column_map(reader.fieldnames)
            event_fieldnames = [
                *(name for name in (reader.fieldnames or []) if name is not None),
                *EVENT_METADATA_FIELDS,
            ]
            missing_columns = [name for name in required if name not in column_map]
            if missing_columns:
                issues.append(f"missing required CSV columns: {', '.join(missing_columns)}")

            for row_number, raw in enumerate(reader, start=2):
                raw_row_count += 1
                raw_clean = {str(key): value for key, value in raw.items() if key is not None}
                event_types, event_reasons = detect_event_row(raw_clean, column_map)
                if event_types:
                    event_rows.append(
                        {
                            **raw_clean,
                            "_symbol": symbol.upper(),
                            "_source_provider": source_provider,
                            "_raw_row_number": row_number,
                            "_event_types_detected": ";".join(sorted(event_types)),
                            "_event_detection_reason": "; ".join(dict.fromkeys(event_reasons)),
                        }
                    )
                    continue

                if missing_columns:
                    continue

                normalized_date = parse_date_value(raw.get(column_map["date"]))
                if normalized_date is None:
                    issues.append(f"row {row_number}: invalid date")
                    continue
                if start_date and normalized_date < start_date:
                    continue

                parsed_date = date.fromisoformat(normalized_date)
                normalized_row: dict[str, Any] = {
                    "date": normalized_date,
                    "year": parsed_date.year,
                    "month": parsed_date.month,
                    "symbol": symbol.upper(),
                    "_row_number": row_number,
                }
                for name in ["open", "high", "low", "close", "volume", *optional]:
                    source_column = column_map.get(name)
                    normalized_row[name] = (
                        parse_float_value(raw.get(source_column)) if source_column else None
                    )
                rows.append(normalized_row)
    except FileNotFoundError:
        return SymbolResult(
            symbol=symbol,
            table=empty_curated_table(),
            issues=[f"missing raw CSV: {csv_path}"],
            raw_row_count=0,
            price_row_count=0,
            quality_status="warning",
        )

    rows.sort(key=lambda r: (r["symbol"], r["date"], r["_row_number"]))
    seen_keys: set[tuple[str, str]] = set()
    deduped_rows: list[dict[str, Any]] = []
    duplicate_rows_removed = 0
    for row in rows:
        key = (str(row["symbol"]), str(row["date"]))
        if key in seen_keys:
            duplicate_rows_removed += 1
            continue
        seen_keys.add(key)
        row.pop("_row_number", None)
        deduped_rows.append(row)

    if duplicate_rows_removed:
        issues.append(f"duplicate symbol-date rows removed: {duplicate_rows_removed}")

    ohlc_bad_rows = count_ohlc_bad_rows(deduped_rows)
    if ohlc_bad_rows:
        issues.append(f"OHLC sanity failures: {ohlc_bad_rows}")

    if not deduped_rows:
        issues.append("no normalized rows")

    quality_status = "pass"
    if ohlc_bad_rows or not deduped_rows or any(issue.startswith("row ") for issue in issues):
        quality_status = "fail"
    elif duplicate_rows_removed:
        quality_status = "warning"

    return SymbolResult(
        symbol=symbol,
        table=rows_to_curated_table(deduped_rows),
        issues=issues,
        duplicate_rows_removed=duplicate_rows_removed,
        ohlc_bad_rows=ohlc_bad_rows,
        raw_row_count=raw_row_count,
        price_row_count=len(deduped_rows),
        event_rows=event_rows,
        event_fieldnames=event_fieldnames,
        event_types_detected=sorted(
            {
                event_type
                for event_row in event_rows
                for event_type in str(event_row.get("_event_types_detected", "")).split(";")
                if event_type
            }
        ),
        quality_status=quality_status,
    )


def count_ohlc_bad_rows(rows: list[dict[str, Any]]) -> int:
    bad_rows = 0
    for row in rows:
        open_value = row.get("open")
        high_value = row.get("high")
        low_value = row.get("low")
        close_value = row.get("close")
        volume_value = row.get("volume")

        numeric_values = [open_value, high_value, low_value, close_value]
        has_bad_price = any(value is None or value <= 0 for value in numeric_values)
        has_bad_high_low = (
            not has_bad_price
            and (
                high_value < max(open_value, close_value, low_value)
                or low_value > min(open_value, close_value, high_value)
            )
        )
        has_bad_volume = volume_value is not None and volume_value < 0
        if has_bad_price or has_bad_high_low or has_bad_volume:
            bad_rows += 1
    return bad_rows


def table_rows_between(table: pa.Table, start_date: str, end_date: str) -> list[dict[str, Any]]:
    return [
        row
        for row in table.to_pylist()
        if row["date"] is not None and start_date <= row["date"] <= end_date
    ]


def table_rows_through(table: pa.Table, end_date: str) -> list[dict[str, Any]]:
    return [row for row in table.to_pylist() if row["date"] is not None and row["date"] <= end_date]


def sort_table(table: pa.Table) -> pa.Table:
    if table.num_rows == 0:
        return table
    indices = pc.sort_indices(table, sort_keys=[("symbol", "ascending"), ("date", "ascending")])
    return table.take(indices)


def split_rows_by_symbol_year(
    rows: list[dict[str, Any]],
) -> dict[tuple[str, int], list[dict[str, Any]]]:
    grouped: dict[tuple[str, int], list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault((str(row["symbol"]), int(row["year"])), []).append(row)
    return grouped


def write_normalized_table(
    table: pa.Table,
    output_dir: Path,
    source_provider: str,
    symbol: str,
) -> Path:
    path = output_dir / "normalized" / source_provider / f"{symbol}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(sort_table(table), path, compression="snappy")
    return path


def event_output_path(output_dir: Path, source_provider: str, symbol: str) -> Path:
    return output_dir / "events" / f"source={source_provider}" / f"{symbol.upper()}_events.csv"


def write_event_rows(result: SymbolResult, output_dir: Path, source_provider: str) -> Path | None:
    if not result.event_rows:
        return None
    path = event_output_path(output_dir, source_provider, result.symbol)
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = result.event_fieldnames or list(result.event_rows[0])
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(result.event_rows)
    return path


def write_curated_export(
    rows: list[dict[str, Any]],
    output_dir: Path,
    source_provider: str,
) -> list[Path]:
    written_paths: list[Path] = []
    export_root = output_dir / "curated_export" / f"source={source_provider}" / "etf_daily_prices"
    for (symbol, year), group_rows in sorted(split_rows_by_symbol_year(rows).items()):
        relative_key = curated_key("", symbol, year).lstrip("/")
        path = export_root / relative_key
        path.parent.mkdir(parents=True, exist_ok=True)
        table = rows_to_curated_table(sorted(group_rows, key=lambda r: (r["symbol"], r["date"])))
        pq.write_table(table, path, compression="snappy")
        written_paths.append(path)
    return written_paths


def load_massive_curated_symbol_table(massive_curated_dir: Path, symbol: str) -> pa.Table | None:
    symbol_dir = massive_curated_dir / f"symbol={symbol}"
    if not symbol_dir.exists():
        return None
    tables = []
    for path in sorted(symbol_dir.glob("year=*/data.parquet")):
        tables.append(align_table_to_curated_schema(pq.ParquetFile(path).read()))
    if not tables:
        return None
    return sort_table(pa.concat_tables(tables, promote_options="default"))


def close_returns_by_date(rows: list[dict[str, Any]]) -> dict[str, float | None]:
    returns: dict[str, float | None] = {}
    previous_close: float | None = None
    for row in sorted(rows, key=lambda r: r["date"]):
        current_close = row.get("close")
        if previous_close is None or current_close is None or previous_close == 0:
            returns[str(row["date"])] = None
        else:
            returns[str(row["date"])] = (float(current_close) / previous_close) - 1.0
        previous_close = float(current_close) if current_close is not None else None
    return returns


def metric_mean(values: list[float]) -> float | None:
    return mean(values) if values else None


def metric_max(values: list[float]) -> float | None:
    return max(values) if values else None


def build_overlap_row(
    *,
    symbol: str,
    source_table: pa.Table,
    massive_table: pa.Table | None,
    overlap_start_date: str,
    overlap_end_date: str,
) -> dict[str, Any]:
    source_rows = table_rows_between(source_table, overlap_start_date, overlap_end_date)
    warning = ""

    if massive_table is None:
        warning = f"missing local Massive curated sample for {symbol}"
        return {
            "symbol": symbol,
            "overlap_start": overlap_start_date,
            "overlap_end": overlap_end_date,
            "source_rows": len(source_rows),
            "massive_rows": 0,
            "joined_rows": 0,
            "missing_in_source_count": 0,
            "missing_in_massive_count": len(source_rows),
            "close_abs_diff_mean": None,
            "close_abs_diff_max": None,
            "close_pct_diff_mean": None,
            "close_pct_diff_max": None,
            "return_1d_abs_diff_mean": None,
            "return_1d_abs_diff_max": None,
            "overlap_quality_status": "warning",
            "warning": warning,
        }

    massive_rows = table_rows_between(massive_table, overlap_start_date, overlap_end_date)
    source_by_date = {str(row["date"]): row for row in source_rows}
    massive_by_date = {str(row["date"]): row for row in massive_rows}
    source_dates = set(source_by_date)
    massive_dates = set(massive_by_date)
    joined_dates = sorted(source_dates & massive_dates)

    close_abs_diffs: list[float] = []
    close_pct_diffs: list[float] = []
    for joined_date in joined_dates:
        source_close = source_by_date[joined_date].get("close")
        massive_close = massive_by_date[joined_date].get("close")
        if source_close is None or massive_close is None:
            continue
        abs_diff = abs(float(source_close) - float(massive_close))
        close_abs_diffs.append(abs_diff)
        if float(massive_close) != 0:
            close_pct_diffs.append(abs_diff / abs(float(massive_close)))

    source_returns = close_returns_by_date(source_rows)
    massive_returns = close_returns_by_date(massive_rows)
    return_abs_diffs: list[float] = []
    for joined_date in joined_dates:
        source_return = source_returns.get(joined_date)
        massive_return = massive_returns.get(joined_date)
        if source_return is None or massive_return is None:
            continue
        return_abs_diffs.append(abs(source_return - massive_return))

    missing_in_source_count = len(massive_dates - source_dates)
    missing_in_massive_count = len(source_dates - massive_dates)
    close_pct_diff_max = metric_max(close_pct_diffs)
    return_1d_abs_diff_max = metric_max(return_abs_diffs)

    overlap_quality_status = "pass"
    if not joined_dates:
        overlap_quality_status = "warning"
        warning = f"no joined overlap rows for {symbol}"
    elif (
        close_pct_diff_max is not None
        and close_pct_diff_max > OVERLAP_CLOSE_PCT_FAIL_THRESHOLD
    ) or (
        return_1d_abs_diff_max is not None
        and return_1d_abs_diff_max > OVERLAP_RETURN_DIFF_FAIL_THRESHOLD
    ):
        overlap_quality_status = "fail"
    elif missing_in_source_count or missing_in_massive_count:
        overlap_quality_status = "warning"
        warning = "overlap date coverage differs between source and Massive sample"

    return {
        "symbol": symbol,
        "overlap_start": overlap_start_date,
        "overlap_end": overlap_end_date,
        "source_rows": len(source_rows),
        "massive_rows": len(massive_rows),
        "joined_rows": len(joined_dates),
        "missing_in_source_count": missing_in_source_count,
        "missing_in_massive_count": missing_in_massive_count,
        "close_abs_diff_mean": metric_mean(close_abs_diffs),
        "close_abs_diff_max": metric_max(close_abs_diffs),
        "close_pct_diff_mean": metric_mean(close_pct_diffs),
        "close_pct_diff_max": close_pct_diff_max,
        "return_1d_abs_diff_mean": metric_mean(return_abs_diffs),
        "return_1d_abs_diff_max": return_1d_abs_diff_max,
        "overlap_quality_status": overlap_quality_status,
        "warning": warning,
    }


def build_coverage_row(
    result: SymbolResult,
    *,
    source_provider: str,
    backfill_end_date: str,
    overlap_start_date: str,
    overlap_end_date: str,
) -> dict[str, Any]:
    rows = result.table.to_pylist()
    dates = [row["date"] for row in rows if row["date"] is not None]
    backfill_rows = [
        row for row in rows if row["date"] is not None and row["date"] <= backfill_end_date
    ]
    overlap_rows = [
        row
        for row in rows
        if row["date"] is not None and overlap_start_date <= row["date"] <= overlap_end_date
    ]
    exported_years = sorted({str(row["year"]) for row in backfill_rows})
    return {
        "symbol": result.symbol,
        "source_provider": source_provider,
        "first_date": min(dates) if dates else "",
        "last_date": max(dates) if dates else "",
        "row_count": len(rows),
        "backfill_row_count": len(backfill_rows),
        "overlap_row_count": len(overlap_rows),
        "exported_years": ";".join(exported_years),
        "has_pre_massive_history": bool(backfill_rows),
        "quality_status": result.quality_status,
    }


def build_symbol_quality_row(result: SymbolResult, overlap_status: str) -> dict[str, Any]:
    quality_status = result.quality_status
    if overlap_status == "fail":
        quality_status = "fail"
    elif quality_status == "pass" and overlap_status == "warning":
        quality_status = "warning"

    issues = list(result.issues)
    if overlap_status != "pass":
        issues.append(f"overlap validation status: {overlap_status}")

    return {
        "symbol": result.symbol,
        "quality_status": quality_status,
        "issue_count": len(issues),
        "issues": "; ".join(issues),
        "ohlc_bad_rows": result.ohlc_bad_rows,
        "duplicate_rows_removed": result.duplicate_rows_removed,
        "event_row_count": len(result.event_rows),
        "event_warning": (
            "event rows quarantined from normalized and curated ETF price parquet"
            if result.event_rows
            else ""
        ),
        "overlap_quality_status": overlap_status,
    }


def build_event_report_row(
    result: SymbolResult,
    *,
    source_provider: str,
    output_dir: Path,
) -> dict[str, Any]:
    output_path = (
        str(event_output_path(output_dir, source_provider, result.symbol))
        if result.event_rows
        else ""
    )
    return {
        "symbol": result.symbol,
        "source_provider": source_provider,
        "raw_row_count": result.raw_row_count,
        "price_row_count": result.price_row_count,
        "event_row_count": len(result.event_rows),
        "event_output_path": output_path,
        "event_types_detected": ";".join(result.event_types_detected),
        "warning": (
            "event rows quarantined and excluded from ETF price parquet"
            if result.event_rows
            else ""
        ),
    }


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


def write_markdown_summary(
    path: Path,
    *,
    source_provider: str,
    output_dir: Path,
    backfill_end_date: str,
    coverage_rows: list[dict[str, Any]],
    overlap_rows: list[dict[str, Any]],
    event_rows: list[dict[str, Any]],
    warnings: list[str],
    dry_run: bool,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    symbols = len(coverage_rows)
    total_rows = sum(int(row["row_count"]) for row in coverage_rows)
    total_backfill_rows = sum(int(row["backfill_row_count"]) for row in coverage_rows)
    pass_count = sum(1 for row in coverage_rows if row["quality_status"] == "pass")
    warning_count = sum(1 for row in coverage_rows if row["quality_status"] == "warning")
    fail_count = sum(1 for row in coverage_rows if row["quality_status"] == "fail")
    overlap_pass = sum(1 for row in overlap_rows if row["overlap_quality_status"] == "pass")
    overlap_warning = sum(1 for row in overlap_rows if row["overlap_quality_status"] == "warning")
    overlap_fail = sum(1 for row in overlap_rows if row["overlap_quality_status"] == "fail")
    total_event_rows = sum(int(row["event_row_count"]) for row in event_rows)
    event_symbol_count = sum(1 for row in event_rows if int(row["event_row_count"]) > 0)

    lines = [
        "# Manual Historical ETF Backfill Validation Summary",
        "",
        "## Purpose",
        "Validate manually downloaded historical ETF CSV data locally before any future promotion.",
        "",
        f"- Source provider: `{source_provider}`",
        "- Local-only note: this script does not call external APIs and does not write to S3.",
        (
            "- Final promotion intent: if validation is accepted later, a separate manual "
            "promotion "
            "task can upload only the pre-Massive partitions to "
            "`s3://us-market-data-casper/curated/etf_daily_prices/`."
        ),
        "- No S3 write happened during this run.",
        (
            f"- Exported curated-style files contain only rows with `date <= {backfill_end_date}`."
        ),
    ]
    if dry_run:
        lines.append("- Dry run: normalized and curated-style parquet writes were skipped.")
    lines.extend(
        [
            "",
            "## Coverage Summary",
            f"- Symbols evaluated: {symbols}",
            f"- Normalized rows: {total_rows}",
            f"- Pre-Massive export-eligible rows: {total_backfill_rows}",
            "- Coverage quality statuses: "
            f"pass={pass_count}, warning={warning_count}, fail={fail_count}",
            "",
            "## Overlap Validation Summary",
            "- Overlap statuses: "
            f"pass={overlap_pass}, warning={overlap_warning}, fail={overlap_fail}",
            "",
            "## Event Row Summary",
            f"- Event rows quarantined: {total_event_rows} across {event_symbol_count} symbols",
            (
                "- Event rows are written under "
                f"`events/source={source_provider}/` and are not exported to ETF price parquet."
            ),
            (
                "- Dividends, splits, distributions, and other events should be handled by a "
                "separate corporate-action/event dataset if needed later."
            ),
            "",
            "## Local Outputs",
            f"- Output directory: `{output_dir}`",
            "- Validation reports are staged under `validation/`.",
            "- Curated-style export files are staged locally only.",
            "",
            "## Warnings",
        ]
    )
    if warnings:
        lines.extend(f"- {warning}" for warning in warnings)
    else:
        lines.append("- None.")
    lines.append("")

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def normalize_quality_statuses(
    results: list[SymbolResult],
    overlap_rows: list[dict[str, Any]],
) -> None:
    overlap_by_symbol = {row["symbol"]: row["overlap_quality_status"] for row in overlap_rows}
    for result in results:
        overlap_status = overlap_by_symbol.get(result.symbol, "warning")
        if overlap_status == "fail":
            result.quality_status = "fail"
        elif result.quality_status == "pass" and overlap_status == "warning":
            result.quality_status = "warning"


def run_validation(args: argparse.Namespace) -> ValidationRunResult:
    raw_dir = Path(args.raw_dir)
    massive_curated_dir = Path(args.massive_curated_dir)
    output_dir = Path(args.output_dir)
    source_provider = str(args.source_provider).strip()
    backfill_end_date = validate_date_arg(args.backfill_end_date, "--backfill-end-date")
    overlap_start_date = validate_date_arg(args.overlap_start_date, "--overlap-start-date")
    overlap_end_date = validate_date_arg(args.overlap_end_date, "--overlap-end-date")
    start_date = validate_date_arg(args.start_date, "--start-date")
    assert backfill_end_date is not None
    assert overlap_start_date is not None
    assert overlap_end_date is not None

    symbols = load_symbols(args.symbols_file, raw_dir)
    warnings: list[str] = []
    if not symbols:
        warnings.append(f"no symbols found from symbols file or raw directory: {raw_dir}")

    results: list[SymbolResult] = []
    for symbol in symbols:
        result = normalize_csv_file(
            raw_dir / f"{symbol}.csv",
            symbol=symbol,
            source_provider=source_provider,
            start_date=start_date,
        )
        results.append(result)
        warnings.extend(f"{symbol}: {issue}" for issue in result.issues if "missing" in issue)

    overlap_rows: list[dict[str, Any]] = []
    for result in results:
        massive_table = load_massive_curated_symbol_table(massive_curated_dir, result.symbol)
        overlap_row = build_overlap_row(
            symbol=result.symbol,
            source_table=result.table,
            massive_table=massive_table,
            overlap_start_date=overlap_start_date,
            overlap_end_date=overlap_end_date,
        )
        overlap_rows.append(overlap_row)
        if overlap_row.get("warning"):
            warnings.append(f"{result.symbol}: {overlap_row['warning']}")

    normalize_quality_statuses(results, overlap_rows)

    coverage_rows = [
        build_coverage_row(
            result,
            source_provider=source_provider,
            backfill_end_date=backfill_end_date,
            overlap_start_date=overlap_start_date,
            overlap_end_date=overlap_end_date,
        )
        for result in results
    ]
    overlap_status_by_symbol = {
        row["symbol"]: row["overlap_quality_status"] for row in overlap_rows
    }
    symbol_quality_rows = [
        build_symbol_quality_row(result, overlap_status_by_symbol.get(result.symbol, "warning"))
        for result in results
    ]
    event_report_rows = [
        build_event_report_row(
            result,
            source_provider=source_provider,
            output_dir=output_dir,
        )
        for result in results
    ]

    (output_dir / "events" / f"source={source_provider}").mkdir(parents=True, exist_ok=True)
    for result in results:
        write_event_rows(result, output_dir, source_provider)

    if not args.dry_run:
        all_backfill_rows: list[dict[str, Any]] = []
        for result in results:
            if result.table.num_rows:
                write_normalized_table(result.table, output_dir, source_provider, result.symbol)
                all_backfill_rows.extend(table_rows_through(result.table, backfill_end_date))
        write_curated_export(all_backfill_rows, output_dir, source_provider)

    validation_dir = output_dir / "validation"
    report_paths = {
        "coverage": str(validation_dir / REPORT_FILENAMES["coverage"]),
        "overlap": str(validation_dir / REPORT_FILENAMES["overlap"]),
        "event_rows": str(validation_dir / REPORT_FILENAMES["event_rows"]),
        "symbol_quality": str(validation_dir / REPORT_FILENAMES["symbol_quality"]),
        "manifest": str(validation_dir / REPORT_FILENAMES["manifest"]),
        "summary": str(validation_dir / REPORT_FILENAMES["summary"]),
    }
    export_path = output_dir / "curated_export" / f"source={source_provider}" / "etf_daily_prices"
    manifest = {
        "source_provider": source_provider,
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "raw_dir": str(raw_dir),
        "massive_curated_dir": str(massive_curated_dir),
        "output_dir": str(output_dir),
        "backfill_end_date": backfill_end_date,
        "overlap_start_date": overlap_start_date,
        "overlap_end_date": overlap_end_date,
        "symbols": symbols,
        "export_path": str(export_path),
        "report_paths": report_paths,
        "warning_count": len(warnings),
        "dry_run": bool(args.dry_run),
    }

    write_csv_report(
        validation_dir / REPORT_FILENAMES["coverage"],
        coverage_rows,
        [
            "symbol",
            "source_provider",
            "first_date",
            "last_date",
            "row_count",
            "backfill_row_count",
            "overlap_row_count",
            "exported_years",
            "has_pre_massive_history",
            "quality_status",
        ],
    )
    write_csv_report(
        validation_dir / REPORT_FILENAMES["overlap"],
        overlap_rows,
        [
            "symbol",
            "overlap_start",
            "overlap_end",
            "source_rows",
            "massive_rows",
            "joined_rows",
            "missing_in_source_count",
            "missing_in_massive_count",
            "close_abs_diff_mean",
            "close_abs_diff_max",
            "close_pct_diff_mean",
            "close_pct_diff_max",
            "return_1d_abs_diff_mean",
            "return_1d_abs_diff_max",
            "overlap_quality_status",
            "warning",
        ],
    )
    write_csv_report(
        validation_dir / REPORT_FILENAMES["event_rows"],
        event_report_rows,
        [
            "symbol",
            "source_provider",
            "raw_row_count",
            "price_row_count",
            "event_row_count",
            "event_output_path",
            "event_types_detected",
            "warning",
        ],
    )
    write_csv_report(
        validation_dir / REPORT_FILENAMES["symbol_quality"],
        symbol_quality_rows,
        [
            "symbol",
            "quality_status",
            "issue_count",
            "issues",
            "ohlc_bad_rows",
            "duplicate_rows_removed",
            "event_row_count",
            "event_warning",
            "overlap_quality_status",
        ],
    )
    write_json(validation_dir / REPORT_FILENAMES["manifest"], manifest)
    write_markdown_summary(
        validation_dir / REPORT_FILENAMES["summary"],
        source_provider=source_provider,
        output_dir=output_dir,
        backfill_end_date=backfill_end_date,
        coverage_rows=coverage_rows,
        overlap_rows=overlap_rows,
        event_rows=event_report_rows,
        warnings=warnings,
        dry_run=bool(args.dry_run),
    )

    return ValidationRunResult(
        coverage_rows=coverage_rows,
        overlap_rows=overlap_rows,
        event_rows=event_report_rows,
        symbol_quality_rows=symbol_quality_rows,
        manifest=manifest,
        warnings=warnings,
    )


def main() -> None:
    result = run_validation(parse_args())
    print(
        "manual historical ETF backfill validation complete: "
        f"symbols={len(result.coverage_rows)} warnings={len(result.warnings)}"
    )
    print(f"reports: {result.manifest['report_paths']['summary']}")
    if result.manifest.get("dry_run"):
        print("dry run: skipped normalized and curated-style parquet writes")


if __name__ == "__main__":
    main()
