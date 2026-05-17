#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from statistics import mean, median, pstdev
from typing import Any, Iterable

import pyarrow as pa
import pyarrow.parquet as pq

DEFAULT_TIINGO_RAW_DIR = "data/manual_backfill/raw/tiingo"
DEFAULT_STOOQ_RAW_DIR = "data/manual_backfill/raw/stooq"
DEFAULT_MASSIVE_CURATED_DIR = "data/manual_backfill/massive_curated_sample"
DEFAULT_SYMBOLS_FILE = "config/etf_symbols.example.txt"
DEFAULT_OUTPUT_DIR = "data/manual_backfill_price_convention"
DEFAULT_START_DATE = "2016-01-01"
DEFAULT_END_DATE = "2026-05-08"

PAIRWISE_REPORT_FILENAME = "price_convention_pairwise_report.csv"
SYMBOL_SUMMARY_FILENAME = "price_convention_symbol_summary.csv"
MARKDOWN_REPORT_FILENAME = "price_convention_report.md"

RETURN_DIFF_SMALL_THRESHOLD = 0.005
CLOSE_PCT_CLOSE_THRESHOLD = 0.005

PAIRWISE_COLUMNS = [
    "symbol",
    "pair",
    "start_date",
    "end_date",
    "left_source",
    "right_source",
    "joined_rows",
    "left_rows",
    "right_rows",
    "missing_left_count",
    "missing_right_count",
    "close_abs_diff_mean",
    "close_abs_diff_max",
    "close_pct_diff_mean",
    "close_pct_diff_max",
    "close_ratio_mean",
    "close_ratio_median",
    "close_ratio_std",
    "close_ratio_min",
    "close_ratio_max",
    "return_1d_abs_diff_mean",
    "return_1d_abs_diff_max",
    "status",
    "warning",
]

SYMBOL_SUMMARY_COLUMNS = [
    "symbol",
    "joined_rows_massive_tiingo_adjusted",
    "joined_rows_massive_tiingo_raw",
    "joined_rows_massive_stooq",
    "closest_to_massive_by_close_pct_mean",
    "closest_to_massive_by_return_diff_mean",
    "tiingo_adjusted_vs_stooq_close_pct_mean",
    "tiingo_adjusted_vs_stooq_return_diff_mean",
    "tiingo_raw_vs_stooq_close_pct_mean",
    "tiingo_raw_vs_stooq_return_diff_mean",
    "tiingo_adjusted_raw_close_pct_mean",
    "tiingo_adjusted_raw_return_diff_mean",
    "likely_convention_explanation",
    "recommendation",
]

PAIR_DEFINITIONS = [
    ("massive_vs_tiingo_adjusted", "massive", "tiingo_adjusted"),
    ("massive_vs_tiingo_raw", "massive", "tiingo_raw"),
    ("massive_vs_stooq", "massive", "stooq"),
    ("tiingo_adjusted_vs_stooq", "tiingo_adjusted", "stooq"),
    ("tiingo_raw_vs_stooq", "tiingo_raw", "stooq"),
    ("tiingo_adjusted_vs_tiingo_raw", "tiingo_adjusted", "tiingo_raw"),
]


@dataclass(frozen=True)
class PriceConventionConfig:
    tiingo_raw_dir: Path
    stooq_raw_dir: Path
    massive_curated_dir: Path
    symbols_file: Path
    symbols: list[str]
    output_dir: Path
    start_date: str
    end_date: str


@dataclass(frozen=True)
class PriceSeries:
    source: str
    rows: list[dict[str, Any]]
    warning: str = ""


@dataclass
class PriceConventionResult:
    pairwise_rows: list[dict[str, Any]]
    symbol_summary_rows: list[dict[str, Any]]
    pairwise_report_path: Path
    symbol_summary_path: Path
    markdown_report_path: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create a local-only price-convention diagnostic report comparing Massive, "
            "Tiingo adjusted, Tiingo raw, and Stooq same-date ETF close prices."
        )
    )
    parser.add_argument("--tiingo-raw-dir", default=DEFAULT_TIINGO_RAW_DIR)
    parser.add_argument("--stooq-raw-dir", default=DEFAULT_STOOQ_RAW_DIR)
    parser.add_argument("--massive-curated-dir", default=DEFAULT_MASSIVE_CURATED_DIR)
    parser.add_argument("--symbols-file", default=DEFAULT_SYMBOLS_FILE)
    parser.add_argument("--symbols", help="Optional comma-separated symbol override")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", default=DEFAULT_END_DATE)
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


def load_symbols(symbols_file: Path) -> list[str]:
    with open(symbols_file, "r", encoding="utf-8") as f:
        return parse_symbols_text(f.read())


def validate_iso_date(value: str, name: str) -> str:
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise ValueError(f"{name} must be YYYY-MM-DD: {value}") from exc


def config_from_args(args: argparse.Namespace) -> PriceConventionConfig:
    symbols = (
        parse_symbols_text(args.symbols)
        if args.symbols
        else load_symbols(Path(args.symbols_file))
    )
    start_date = validate_iso_date(str(args.start_date), "--start-date")
    end_date = validate_iso_date(str(args.end_date), "--end-date")
    if start_date > end_date:
        raise ValueError("--start-date must be on or before --end-date")
    if not symbols:
        raise ValueError("no symbols requested")
    return PriceConventionConfig(
        tiingo_raw_dir=Path(args.tiingo_raw_dir),
        stooq_raw_dir=Path(args.stooq_raw_dir),
        massive_curated_dir=Path(args.massive_curated_dir),
        symbols_file=Path(args.symbols_file),
        symbols=symbols,
        output_dir=Path(args.output_dir),
        start_date=start_date,
        end_date=end_date,
    )


def parse_float(value: object) -> float | None:
    text = str(value).strip() if value is not None else ""
    if not text:
        return None
    try:
        number = float(text.replace(",", ""))
    except ValueError:
        return None
    return number if math.isfinite(number) else None


def parse_date_value(value: object) -> str | None:
    text = str(value).strip() if value is not None else ""
    if not text:
        return None
    candidate = text[:10]
    try:
        return date.fromisoformat(candidate).isoformat()
    except ValueError:
        pass
    for fmt in ("%Y/%m/%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def csv_column_map(fieldnames: Iterable[str] | None) -> dict[str, str]:
    return {name.strip().lower(): name for name in (fieldnames or [])}


def row_in_range(row: dict[str, Any], start_date: str, end_date: str) -> bool:
    row_date = str(row.get("date", ""))
    return start_date <= row_date <= end_date


def dedupe_and_sort_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows_by_date = {str(row["date"]): row for row in rows if row.get("date")}
    return [rows_by_date[key] for key in sorted(rows_by_date)]


def load_tiingo_prices(
    raw_dir: Path,
    symbol: str,
    *,
    start_date: str,
    end_date: str,
) -> tuple[PriceSeries, PriceSeries]:
    path = raw_dir / f"{symbol.upper()}.csv"
    if not path.exists():
        warning = f"missing Tiingo raw CSV: {path}"
        return (
            PriceSeries("tiingo_adjusted", [], warning),
            PriceSeries("tiingo_raw", [], warning),
        )

    adjusted_rows: list[dict[str, Any]] = []
    raw_rows: list[dict[str, Any]] = []
    invalid_rows = 0
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        columns = csv_column_map(reader.fieldnames)
        date_column = columns.get("date")
        close_column = columns.get("close")
        adj_close_column = columns.get("adj_close")
        raw_close_column = columns.get("raw_close")

        for row in reader:
            row_date = parse_date_value(row.get(date_column) if date_column else None)
            if row_date is None or not (start_date <= row_date <= end_date):
                invalid_rows += 1 if row_date is None else 0
                continue
            fallback_close = parse_float(row.get(close_column)) if close_column else None
            adjusted_close = (
                parse_float(row.get(adj_close_column)) if adj_close_column else None
            )
            raw_close = parse_float(row.get(raw_close_column)) if raw_close_column else None
            if adjusted_close is None:
                adjusted_close = fallback_close
            if raw_close is None:
                raw_close = fallback_close
            if adjusted_close is not None:
                adjusted_rows.append({"date": row_date, "close": adjusted_close})
            if raw_close is not None:
                raw_rows.append({"date": row_date, "close": raw_close})

    warning = f"rows skipped for invalid date: {invalid_rows}" if invalid_rows else ""
    return (
        PriceSeries("tiingo_adjusted", dedupe_and_sort_rows(adjusted_rows), warning),
        PriceSeries("tiingo_raw", dedupe_and_sort_rows(raw_rows), warning),
    )


def load_stooq_prices(
    raw_dir: Path,
    symbol: str,
    *,
    start_date: str,
    end_date: str,
) -> PriceSeries:
    path = raw_dir / f"{symbol.upper()}.csv"
    if not path.exists():
        return PriceSeries("stooq", [], f"missing Stooq raw CSV: {path}")

    rows: list[dict[str, Any]] = []
    invalid_rows = 0
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        columns = csv_column_map(reader.fieldnames)
        date_column = columns.get("date")
        close_column = columns.get("close")
        for row in reader:
            row_date = parse_date_value(row.get(date_column) if date_column else None)
            if row_date is None or not (start_date <= row_date <= end_date):
                invalid_rows += 1 if row_date is None else 0
                continue
            close = parse_float(row.get(close_column)) if close_column else None
            if close is not None:
                rows.append({"date": row_date, "close": close})

    warning = f"rows skipped for invalid date: {invalid_rows}" if invalid_rows else ""
    return PriceSeries("stooq", dedupe_and_sort_rows(rows), warning)


def load_massive_prices(
    curated_dir: Path,
    symbol: str,
    *,
    start_date: str,
    end_date: str,
) -> PriceSeries:
    symbol_dir = curated_dir / f"symbol={symbol.upper()}"
    if not symbol_dir.exists():
        return PriceSeries("massive", [], f"missing local Massive curated sample: {symbol_dir}")

    rows: list[dict[str, Any]] = []
    warnings: list[str] = []
    for path in sorted(symbol_dir.glob("year=*/data.parquet")):
        try:
            table = pq.ParquetFile(path).read()
        except (pa.ArrowInvalid, OSError) as exc:
            warnings.append(f"failed reading {path}: {exc}")
            continue
        for row in table.to_pylist():
            row_date = parse_date_value(row.get("date"))
            if row_date is None or not (start_date <= row_date <= end_date):
                continue
            close = parse_float(row.get("close"))
            if close is not None:
                rows.append({"date": row_date, "close": close})

    if not rows and not warnings:
        warnings.append(f"no Massive parquet rows in date range for {symbol.upper()}")
    return PriceSeries("massive", dedupe_and_sort_rows(rows), "; ".join(warnings))


def metric_mean(values: list[float]) -> float | None:
    return mean(values) if values else None


def metric_median(values: list[float]) -> float | None:
    return median(values) if values else None


def metric_std(values: list[float]) -> float | None:
    return pstdev(values) if values else None


def metric_min(values: list[float]) -> float | None:
    return min(values) if values else None


def metric_max(values: list[float]) -> float | None:
    return max(values) if values else None


def returns_by_date(rows: list[dict[str, Any]]) -> dict[str, float | None]:
    returns: dict[str, float | None] = {}
    previous_close: float | None = None
    for row in sorted(rows, key=lambda item: str(item["date"])):
        current_close = parse_float(row.get("close"))
        if current_close is None or previous_close is None or previous_close == 0:
            returns[str(row["date"])] = None
        else:
            returns[str(row["date"])] = current_close / previous_close - 1.0
        previous_close = current_close
    return returns


def build_pairwise_row(
    *,
    symbol: str,
    pair: str,
    left: PriceSeries,
    right: PriceSeries,
    start_date: str,
    end_date: str,
) -> dict[str, Any]:
    left_rows = [row for row in left.rows if row_in_range(row, start_date, end_date)]
    right_rows = [row for row in right.rows if row_in_range(row, start_date, end_date)]
    left_by_date = {str(row["date"]): row for row in left_rows}
    right_by_date = {str(row["date"]): row for row in right_rows}
    joined_dates = sorted(set(left_by_date) & set(right_by_date))

    close_abs_diffs: list[float] = []
    close_pct_diffs: list[float] = []
    close_ratios: list[float] = []
    for joined_date in joined_dates:
        left_close = parse_float(left_by_date[joined_date].get("close"))
        right_close = parse_float(right_by_date[joined_date].get("close"))
        if left_close is None or right_close is None:
            continue
        abs_diff = abs(left_close - right_close)
        close_abs_diffs.append(abs_diff)
        if left_close != 0:
            close_pct_diffs.append(abs_diff / abs(left_close))
        if right_close != 0:
            close_ratios.append(left_close / right_close)

    left_returns = returns_by_date(left_rows)
    right_returns = returns_by_date(right_rows)
    return_abs_diffs: list[float] = []
    for joined_date in joined_dates:
        left_return = left_returns.get(joined_date)
        right_return = right_returns.get(joined_date)
        if left_return is None or right_return is None:
            continue
        return_abs_diffs.append(abs(left_return - right_return))

    warnings = [warning for warning in [left.warning, right.warning] if warning]
    if not joined_dates:
        warnings.append(f"no joined rows for {symbol} {pair}")
        status = "warning"
    elif (
        metric_max(return_abs_diffs) is not None
        and metric_max(return_abs_diffs) > RETURN_DIFF_SMALL_THRESHOLD
    ):
        status = "warning"
    else:
        status = "pass"

    return {
        "symbol": symbol,
        "pair": pair,
        "start_date": start_date,
        "end_date": end_date,
        "left_source": left.source,
        "right_source": right.source,
        "joined_rows": len(joined_dates),
        "left_rows": len(left_rows),
        "right_rows": len(right_rows),
        "missing_left_count": len(set(right_by_date) - set(left_by_date)),
        "missing_right_count": len(set(left_by_date) - set(right_by_date)),
        "close_abs_diff_mean": metric_mean(close_abs_diffs),
        "close_abs_diff_max": metric_max(close_abs_diffs),
        "close_pct_diff_mean": metric_mean(close_pct_diffs),
        "close_pct_diff_max": metric_max(close_pct_diffs),
        "close_ratio_mean": metric_mean(close_ratios),
        "close_ratio_median": metric_median(close_ratios),
        "close_ratio_std": metric_std(close_ratios),
        "close_ratio_min": metric_min(close_ratios),
        "close_ratio_max": metric_max(close_ratios),
        "return_1d_abs_diff_mean": metric_mean(return_abs_diffs),
        "return_1d_abs_diff_max": metric_max(return_abs_diffs),
        "status": status,
        "warning": "; ".join(dict.fromkeys(warnings)),
    }


def pair_row_by_name(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(row["pair"]): row for row in rows}


def finite_metric(row: dict[str, Any] | None, column: str) -> float | None:
    if row is None:
        return None
    value = row.get(column)
    return value if isinstance(value, float | int) and math.isfinite(float(value)) else None


def closest_pair(
    pairs: dict[str, dict[str, Any]],
    *,
    column: str,
) -> str:
    candidates = {
        "tiingo_adjusted": finite_metric(pairs.get("massive_vs_tiingo_adjusted"), column),
        "tiingo_raw": finite_metric(pairs.get("massive_vs_tiingo_raw"), column),
        "stooq": finite_metric(pairs.get("massive_vs_stooq"), column),
    }
    finite_candidates = {
        name: value for name, value in candidates.items() if value is not None
    }
    if not finite_candidates:
        return ""
    return min(finite_candidates, key=lambda name: float(finite_candidates[name]))


def all_available_return_diffs_small(pairs: dict[str, dict[str, Any]]) -> bool:
    values = [
        finite_metric(row, "return_1d_abs_diff_mean")
        for row in pairs.values()
        if finite_metric(row, "return_1d_abs_diff_mean") is not None
    ]
    return bool(values) and all(value <= RETURN_DIFF_SMALL_THRESHOLD for value in values)


def return_alignment_bad(pairs: dict[str, dict[str, Any]]) -> bool:
    massive_values = [
        finite_metric(pairs.get(pair), "return_1d_abs_diff_mean")
        for pair in [
            "massive_vs_tiingo_adjusted",
            "massive_vs_tiingo_raw",
            "massive_vs_stooq",
        ]
    ]
    finite_values = [value for value in massive_values if value is not None]
    return not finite_values or any(value > RETURN_DIFF_SMALL_THRESHOLD for value in finite_values)


def tiingo_stooq_are_close(pairs: dict[str, dict[str, Any]]) -> bool:
    adjusted_stooq = finite_metric(
        pairs.get("tiingo_adjusted_vs_stooq"),
        "close_pct_diff_mean",
    )
    adjusted_stooq_return = finite_metric(
        pairs.get("tiingo_adjusted_vs_stooq"),
        "return_1d_abs_diff_mean",
    )
    if adjusted_stooq is None or adjusted_stooq_return is None:
        return False
    return (
        adjusted_stooq <= CLOSE_PCT_CLOSE_THRESHOLD
        and adjusted_stooq_return <= RETURN_DIFF_SMALL_THRESHOLD
    )


def metric_less(left: float | None, right: float | None) -> bool:
    if left is None:
        return False
    if right is None:
        return True
    return left < right


def choose_explanation_and_recommendation(
    pairs: dict[str, dict[str, Any]],
    closest_by_close: str,
) -> tuple[str, str]:
    adjusted_massive_close = finite_metric(
        pairs.get("massive_vs_tiingo_adjusted"),
        "close_pct_diff_mean",
    )
    raw_massive_close = finite_metric(pairs.get("massive_vs_tiingo_raw"), "close_pct_diff_mean")
    massive_adjusted_return = finite_metric(
        pairs.get("massive_vs_tiingo_adjusted"),
        "return_1d_abs_diff_mean",
    )
    massive_stooq_return = finite_metric(
        pairs.get("massive_vs_stooq"),
        "return_1d_abs_diff_mean",
    )

    if return_alignment_bad(pairs):
        explanation = "inspect_source_quality"
    elif (
        tiingo_stooq_are_close(pairs)
        and metric_less(CLOSE_PCT_CLOSE_THRESHOLD, adjusted_massive_close)
        and massive_adjusted_return is not None
        and massive_adjusted_return <= RETURN_DIFF_SMALL_THRESHOLD
        and massive_stooq_return is not None
        and massive_stooq_return <= RETURN_DIFF_SMALL_THRESHOLD
    ):
        explanation = "source_adjustment_method_differs_from_massive"
    elif metric_less(raw_massive_close, adjusted_massive_close):
        explanation = "massive_closer_to_raw_or_split_adjusted_convention"
    elif metric_less(adjusted_massive_close, raw_massive_close):
        explanation = "massive_closer_to_adjusted_convention"
    elif (
        adjusted_massive_close is not None
        and raw_massive_close is not None
        and adjusted_massive_close <= CLOSE_PCT_CLOSE_THRESHOLD
        and raw_massive_close <= CLOSE_PCT_CLOSE_THRESHOLD
    ):
        explanation = "massive_closer_to_adjusted_convention"
    else:
        explanation = "inspect_source_quality"

    if all_available_return_diffs_small(pairs):
        recommendation = (
            "usable for return-based backtesting after boundary rebasing; preserve source lineage"
        )
    elif explanation == "inspect_source_quality":
        recommendation = "inspect source quality before relying on price or return comparisons"
    elif closest_by_close:
        recommendation = (
            f"{closest_by_close} is closest to Massive by price level in this local overlap; "
            "preserve source lineage and do not promote without separate review"
        )
    else:
        recommendation = "insufficient joined rows for a price-convention recommendation"

    return explanation, recommendation


def build_symbol_summary_row(
    symbol: str,
    pair_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    pairs = pair_row_by_name(pair_rows)
    closest_by_close = closest_pair(pairs, column="close_pct_diff_mean")
    closest_by_return = closest_pair(pairs, column="return_1d_abs_diff_mean")
    explanation, recommendation = choose_explanation_and_recommendation(
        pairs,
        closest_by_close,
    )
    return {
        "symbol": symbol,
        "joined_rows_massive_tiingo_adjusted": pairs.get(
            "massive_vs_tiingo_adjusted",
            {},
        ).get("joined_rows", 0),
        "joined_rows_massive_tiingo_raw": pairs.get("massive_vs_tiingo_raw", {}).get(
            "joined_rows",
            0,
        ),
        "joined_rows_massive_stooq": pairs.get("massive_vs_stooq", {}).get("joined_rows", 0),
        "closest_to_massive_by_close_pct_mean": closest_by_close,
        "closest_to_massive_by_return_diff_mean": closest_by_return,
        "tiingo_adjusted_vs_stooq_close_pct_mean": finite_metric(
            pairs.get("tiingo_adjusted_vs_stooq"),
            "close_pct_diff_mean",
        ),
        "tiingo_adjusted_vs_stooq_return_diff_mean": finite_metric(
            pairs.get("tiingo_adjusted_vs_stooq"),
            "return_1d_abs_diff_mean",
        ),
        "tiingo_raw_vs_stooq_close_pct_mean": finite_metric(
            pairs.get("tiingo_raw_vs_stooq"),
            "close_pct_diff_mean",
        ),
        "tiingo_raw_vs_stooq_return_diff_mean": finite_metric(
            pairs.get("tiingo_raw_vs_stooq"),
            "return_1d_abs_diff_mean",
        ),
        "tiingo_adjusted_raw_close_pct_mean": finite_metric(
            pairs.get("tiingo_adjusted_vs_tiingo_raw"),
            "close_pct_diff_mean",
        ),
        "tiingo_adjusted_raw_return_diff_mean": finite_metric(
            pairs.get("tiingo_adjusted_vs_tiingo_raw"),
            "return_1d_abs_diff_mean",
        ),
        "likely_convention_explanation": explanation,
        "recommendation": recommendation,
    }


def write_csv_report(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def markdown_table(rows: list[dict[str, Any]], columns: list[str]) -> list[str]:
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for row in rows:
        values = [str(row.get(column, "")).replace("|", "\\|") for column in columns]
        lines.append("| " + " | ".join(values) + " |")
    return lines


def pct_text(value: object) -> str:
    number = parse_float(value)
    if number is None:
        return ""
    return f"{number:.2%}"


def write_markdown_report(
    path: Path,
    *,
    config: PriceConventionConfig,
    symbol_rows: list[dict[str, Any]],
    pairwise_rows: list[dict[str, Any]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    by_price_counts: dict[str, int] = {}
    explanation_counts: dict[str, int] = {}
    for row in symbol_rows:
        by_price = str(row["closest_to_massive_by_close_pct_mean"])
        explanation = str(row["likely_convention_explanation"])
        by_price_counts[by_price] = by_price_counts.get(by_price, 0) + 1
        explanation_counts[explanation] = explanation_counts.get(explanation, 0) + 1

    tiingo_stooq_close_rows = [
        row
        for row in pairwise_rows
        if row["pair"] in {"tiingo_adjusted_vs_stooq", "tiingo_raw_vs_stooq"}
    ]
    tiingo_stooq_close_means = [
        float(row["close_pct_diff_mean"])
        for row in tiingo_stooq_close_rows
        if row.get("close_pct_diff_mean") not in {"", None}
    ]
    median_tiingo_stooq_diff = metric_median(tiingo_stooq_close_means)

    symbol_table_rows = [
        {
            "symbol": row["symbol"],
            "closest_price": row["closest_to_massive_by_close_pct_mean"],
            "closest_return": row["closest_to_massive_by_return_diff_mean"],
            "adj_stooq_price_diff": pct_text(
                row["tiingo_adjusted_vs_stooq_close_pct_mean"]
            ),
            "explanation": row["likely_convention_explanation"],
            "recommendation": row["recommendation"],
        }
        for row in symbol_rows
    ]

    lines = [
        "# Manual Backfill Price Convention Diagnostics",
        "",
        f"Generated at: {generated_at}",
        "",
        "## Executive summary",
        (
            "This local-only diagnostic compares same-date close prices and one-day returns "
            "across Massive, Tiingo adjusted, Tiingo raw, and Stooq. It is intended to "
            "separate price-level convention mismatches from source-quality problems before "
            "any S3 promotion."
        ),
        "",
        "- Closest to Massive by close percentage mean: "
        + ", ".join(f"{key}={value}" for key, value in sorted(by_price_counts.items())),
        "- Likely convention explanations: "
        + ", ".join(f"{key}={value}" for key, value in sorted(explanation_counts.items())),
        "",
        "## Inputs and date range",
        f"- Tiingo raw CSV dir: `{config.tiingo_raw_dir}`",
        f"- Stooq raw CSV dir: `{config.stooq_raw_dir}`",
        f"- Massive curated sample dir: `{config.massive_curated_dir}`",
        f"- Symbols file: `{config.symbols_file}`",
        f"- Date range: `{config.start_date}` through `{config.end_date}`",
        f"- Symbols evaluated: {len(symbol_rows)}",
        "",
        "## Massive adjusted=true note",
        (
            "The Massive grouped daily ingestion currently requests `adjusted=true`. This "
            "report treats local Massive close as the platform-canonical overlap baseline, "
            "but `adjusted=true` may be split-adjusted while Tiingo adjusted fields can "
            "reflect a different vendor adjustment convention."
        ),
        "",
        "## Tiingo raw or adjusted closer to Massive",
        (
            "Use `closest_to_massive_by_close_pct_mean` for price-level closeness and "
            "`closest_to_massive_by_return_diff_mean` for return-series closeness. A stable "
            "price-level gap with aligned returns points toward convention differences, not "
            "automatically bad Massive data."
        ),
        "",
        "## Tiingo and Stooq closeness",
        (
            "Tiingo/Stooq pair rows show whether the manual backfill sources agree more "
            "closely with each other than either agrees with Massive."
        ),
        (
            "- Median Tiingo/Stooq close pct mean across available pairs: "
            f"{pct_text(median_tiingo_stooq_diff)}"
        ),
        "",
        "## Symbol-level table",
        *markdown_table(
            symbol_table_rows,
            [
                "symbol",
                "closest_price",
                "closest_return",
                "adj_stooq_price_diff",
                "explanation",
                "recommendation",
            ],
        ),
        "",
        "## Interpretation",
        "- Price-level mismatch does not automatically mean Massive is wrong.",
        (
            "- The likely explanation for stable level gaps with aligned returns is "
            "vendor adjustment convention differences."
        ),
        "- Massive remains canonical for the current period because the platform already uses it.",
        (
            "- Tiingo remains preferred pre-Massive backfill because of coverage and "
            "clean stitched validation."
        ),
        "- Stooq remains useful as a backup and validation source.",
        "",
        "## Recommendation before S3 promotion",
        "- Do not promote data from this diagnostic report.",
        "- Keep Massive canonical from each symbol's Massive first available date onward.",
        (
            "- Use Tiingo as the preferred pre-Massive source only through a separate "
            "reviewed promotion workflow."
        ),
        (
            "- Rebase pre-Massive history at the Massive boundary when using price levels, "
            "and preserve source lineage."
        ),
    ]
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
        f.write("\n")


def load_symbol_series(
    config: PriceConventionConfig,
    symbol: str,
) -> dict[str, PriceSeries]:
    tiingo_adjusted, tiingo_raw = load_tiingo_prices(
        config.tiingo_raw_dir,
        symbol,
        start_date=config.start_date,
        end_date=config.end_date,
    )
    stooq = load_stooq_prices(
        config.stooq_raw_dir,
        symbol,
        start_date=config.start_date,
        end_date=config.end_date,
    )
    massive = load_massive_prices(
        config.massive_curated_dir,
        symbol,
        start_date=config.start_date,
        end_date=config.end_date,
    )
    return {
        "massive": massive,
        "tiingo_adjusted": tiingo_adjusted,
        "tiingo_raw": tiingo_raw,
        "stooq": stooq,
    }


def run_price_convention_diagnostics(
    config: PriceConventionConfig,
) -> PriceConventionResult:
    pairwise_rows: list[dict[str, Any]] = []
    symbol_summary_rows: list[dict[str, Any]] = []
    for symbol in config.symbols:
        series_by_source = load_symbol_series(config, symbol)
        symbol_pair_rows = [
            build_pairwise_row(
                symbol=symbol,
                pair=pair,
                left=series_by_source[left_source],
                right=series_by_source[right_source],
                start_date=config.start_date,
                end_date=config.end_date,
            )
            for pair, left_source, right_source in PAIR_DEFINITIONS
        ]
        pairwise_rows.extend(symbol_pair_rows)
        symbol_summary_rows.append(build_symbol_summary_row(symbol, symbol_pair_rows))

    pairwise_report_path = config.output_dir / PAIRWISE_REPORT_FILENAME
    symbol_summary_path = config.output_dir / SYMBOL_SUMMARY_FILENAME
    markdown_report_path = config.output_dir / MARKDOWN_REPORT_FILENAME
    write_csv_report(pairwise_report_path, pairwise_rows, PAIRWISE_COLUMNS)
    write_csv_report(symbol_summary_path, symbol_summary_rows, SYMBOL_SUMMARY_COLUMNS)
    write_markdown_report(
        markdown_report_path,
        config=config,
        symbol_rows=symbol_summary_rows,
        pairwise_rows=pairwise_rows,
    )
    return PriceConventionResult(
        pairwise_rows=pairwise_rows,
        symbol_summary_rows=symbol_summary_rows,
        pairwise_report_path=pairwise_report_path,
        symbol_summary_path=symbol_summary_path,
        markdown_report_path=markdown_report_path,
    )


def main() -> None:
    try:
        config = config_from_args(parse_args())
        result = run_price_convention_diagnostics(config)
    except Exception as exc:  # noqa: BLE001 - CLI should fail clearly.
        raise SystemExit(f"price convention diagnostics failed: {exc}") from exc

    print(
        "manual backfill price convention diagnostics complete: "
        f"symbols={len(result.symbol_summary_rows)} pairs={len(result.pairwise_rows)}"
    )
    print(f"pairwise report: {result.pairwise_report_path}")
    print(f"symbol summary: {result.symbol_summary_path}")
    print(f"markdown report: {result.markdown_report_path}")


if __name__ == "__main__":
    main()
