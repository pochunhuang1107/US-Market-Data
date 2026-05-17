#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

DEFAULT_TIINGO_RAW_DIR = "data/manual_backfill/raw/tiingo"
DEFAULT_SYMBOLS_FILE = "config/etf_symbols.example.txt"
DEFAULT_OUTPUT_DIR = "data/manual_backfill_tiingo_raw_diagnostics"
DEFAULT_MASSIVE_BOUNDARY_REPORT = (
    "data/manual_backfill_tiingo_raw/validation/massive_boundary_report.csv"
)

EVENT_REPORT_FILENAME = "split_discontinuity_event_report.csv"
SYMBOL_SUMMARY_FILENAME = "split_discontinuity_symbol_summary.csv"
MARKDOWN_REPORT_FILENAME = "split_discontinuity_report.md"

LARGE_RAW_RETURN_THRESHOLD = 0.20
SEVERE_RAW_RETURN_THRESHOLD = 0.40
RAW_ADJ_RATIO_JUMP_THRESHOLD = 0.20

EVENT_COLUMNS = [
    "symbol",
    "date",
    "event_type",
    "raw_close_prev",
    "raw_close",
    "raw_return_1d",
    "adj_close_prev",
    "adj_close",
    "adj_return_1d",
    "split_factor",
    "div_cash",
    "raw_adj_ratio_prev",
    "raw_adj_ratio",
    "raw_adj_ratio_change",
    "severity",
    "warning",
]

SUMMARY_COLUMNS = [
    "symbol",
    "first_date",
    "last_date",
    "row_count",
    "split_factor_event_count",
    "large_raw_return_count",
    "severe_raw_return_count",
    "raw_adj_ratio_jump_count",
    "max_abs_raw_return",
    "max_abs_raw_return_date",
    "max_abs_raw_adj_ratio_change",
    "max_abs_raw_adj_ratio_change_date",
    "raw_adj_ratio_start",
    "raw_adj_ratio_end",
    "likely_split_adjustment_needed",
    "raw_compatible_usage_recommendation",
    "reason",
]


@dataclass(frozen=True)
class TiingoRawSplitDiscontinuityConfig:
    tiingo_raw_dir: Path
    symbols_file: Path
    symbols: list[str]
    output_dir: Path
    start_date: str | None = None
    end_date: str | None = None
    pre_massive_only: bool = False
    massive_boundary_report: Path = Path(DEFAULT_MASSIVE_BOUNDARY_REPORT)


@dataclass
class TiingoRawSplitDiscontinuityResult:
    event_rows: list[dict[str, Any]]
    symbol_summary_rows: list[dict[str, Any]]
    event_report_path: Path
    symbol_summary_path: Path
    markdown_report_path: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create a local-only diagnostic report for split discontinuities and large "
            "return spikes in already-downloaded Tiingo raw-compatible ETF history."
        )
    )
    parser.add_argument("--tiingo-raw-dir", default=DEFAULT_TIINGO_RAW_DIR)
    parser.add_argument("--symbols-file", default=DEFAULT_SYMBOLS_FILE)
    parser.add_argument("--symbols", help="Optional comma-separated symbol override")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--pre-massive-only", action="store_true", default=False)
    parser.add_argument(
        "--massive-boundary-report",
        default=DEFAULT_MASSIVE_BOUNDARY_REPORT,
    )
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


def validate_iso_date(value: str | None, name: str) -> str | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise ValueError(f"{name} must be YYYY-MM-DD: {value}") from exc


def config_from_args(args: argparse.Namespace) -> TiingoRawSplitDiscontinuityConfig:
    symbols = (
        parse_symbols_text(args.symbols)
        if args.symbols
        else load_symbols(Path(args.symbols_file))
    )
    start_date = validate_iso_date(args.start_date, "--start-date")
    end_date = validate_iso_date(args.end_date, "--end-date")
    if start_date and end_date and start_date > end_date:
        raise ValueError("--start-date must be on or before --end-date")
    if not symbols:
        raise ValueError("no symbols requested")
    return TiingoRawSplitDiscontinuityConfig(
        tiingo_raw_dir=Path(args.tiingo_raw_dir),
        symbols_file=Path(args.symbols_file),
        symbols=symbols,
        output_dir=Path(args.output_dir),
        start_date=start_date,
        end_date=end_date,
        pre_massive_only=bool(args.pre_massive_only),
        massive_boundary_report=Path(args.massive_boundary_report),
    )


def csv_column_map(fieldnames: Iterable[str] | None) -> dict[str, str]:
    return {name.strip().lower(): name for name in (fieldnames or [])}


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


def value_from_row(
    row: dict[str, str],
    columns: dict[str, str],
    name: str,
) -> object:
    source_column = columns.get(name)
    return row.get(source_column) if source_column else None


def row_in_range(
    row_date: str,
    *,
    start_date: str | None,
    end_date: str | None,
) -> bool:
    if start_date and row_date < start_date:
        return False
    if end_date and row_date > end_date:
        return False
    return True


def previous_calendar_day(value: str) -> str:
    return (date.fromisoformat(value) - timedelta(days=1)).isoformat()


def load_pre_massive_end_dates(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    end_dates: dict[str, str] = {}
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        columns = csv_column_map(reader.fieldnames)
        symbol_column = columns.get("symbol")
        source_backfill_column = columns.get("source_backfill_end_date")
        massive_first_column = columns.get("massive_first_available_date")
        for row in reader:
            symbol = str(row.get(symbol_column, "") if symbol_column else "").strip().upper()
            if not symbol:
                continue
            end_date = parse_date_value(
                row.get(source_backfill_column) if source_backfill_column else None
            )
            if end_date is None and massive_first_column:
                massive_first = parse_date_value(row.get(massive_first_column))
                if massive_first is not None:
                    end_date = previous_calendar_day(massive_first)
            if end_date is not None:
                end_dates[symbol] = end_date
    return end_dates


def effective_end_date(
    *,
    config_end_date: str | None,
    pre_massive_end_date: str | None,
) -> str | None:
    candidates = [value for value in [config_end_date, pre_massive_end_date] if value]
    return min(candidates) if candidates else None


def load_tiingo_rows(
    path: Path,
    *,
    start_date: str | None,
    end_date: str | None,
) -> list[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"missing Tiingo raw CSV: {path}")

    rows_by_date: dict[str, dict[str, Any]] = {}
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        columns = csv_column_map(reader.fieldnames)
        if "date" not in columns:
            raise ValueError("missing required input CSV column: date")
        if "raw_close" not in columns and "close" not in columns:
            raise ValueError("missing required input CSV column: raw_close or close")

        for raw in reader:
            normalized_date = parse_date_value(value_from_row(raw, columns, "date"))
            if normalized_date is None:
                continue
            if not row_in_range(normalized_date, start_date=start_date, end_date=end_date):
                continue

            raw_close = parse_float(value_from_row(raw, columns, "raw_close"))
            if raw_close is None:
                raw_close = parse_float(value_from_row(raw, columns, "close"))
            rows_by_date[normalized_date] = {
                "date": normalized_date,
                "raw_close": raw_close,
                "adj_close": parse_float(value_from_row(raw, columns, "adj_close")),
                "split_factor": parse_float(value_from_row(raw, columns, "split_factor")),
                "div_cash": parse_float(value_from_row(raw, columns, "div_cash")),
            }

    return [rows_by_date[key] for key in sorted(rows_by_date)]


def compute_metrics(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    enriched: list[dict[str, Any]] = []
    previous_raw_close: float | None = None
    previous_adj_close: float | None = None
    previous_raw_adj_ratio: float | None = None

    for row in rows:
        current = dict(row)
        raw_close = parse_float(current.get("raw_close"))
        adj_close = parse_float(current.get("adj_close"))
        raw_return = (
            raw_close / previous_raw_close - 1.0
            if raw_close is not None and previous_raw_close not in {None, 0}
            else None
        )
        adj_return = (
            adj_close / previous_adj_close - 1.0
            if adj_close is not None and previous_adj_close not in {None, 0}
            else None
        )
        raw_adj_ratio = (
            adj_close / raw_close
            if adj_close is not None and raw_close not in {None, 0}
            else None
        )
        raw_adj_ratio_change = (
            raw_adj_ratio / previous_raw_adj_ratio - 1.0
            if raw_adj_ratio is not None and previous_raw_adj_ratio not in {None, 0}
            else None
        )

        current["raw_close_prev"] = previous_raw_close
        current["adj_close_prev"] = previous_adj_close
        current["raw_adj_ratio_prev"] = previous_raw_adj_ratio
        current["raw_return_1d"] = raw_return
        current["adj_return_1d"] = adj_return
        current["raw_adj_ratio"] = raw_adj_ratio
        current["raw_adj_ratio_change"] = raw_adj_ratio_change
        enriched.append(current)

        if raw_close is not None:
            previous_raw_close = raw_close
        if adj_close is not None:
            previous_adj_close = adj_close
        if raw_adj_ratio is not None:
            previous_raw_adj_ratio = raw_adj_ratio

    return enriched


def is_split_factor_event(value: object) -> bool:
    split_factor = parse_float(value)
    return split_factor is not None and not math.isclose(split_factor, 1.0, abs_tol=1e-12)


def event_row(
    *,
    symbol: str,
    row: dict[str, Any],
    event_type: str,
    severity: str,
    warning: str,
) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "date": row.get("date", ""),
        "event_type": event_type,
        "raw_close_prev": row.get("raw_close_prev"),
        "raw_close": row.get("raw_close"),
        "raw_return_1d": row.get("raw_return_1d"),
        "adj_close_prev": row.get("adj_close_prev"),
        "adj_close": row.get("adj_close"),
        "adj_return_1d": row.get("adj_return_1d"),
        "split_factor": row.get("split_factor"),
        "div_cash": row.get("div_cash"),
        "raw_adj_ratio_prev": row.get("raw_adj_ratio_prev"),
        "raw_adj_ratio": row.get("raw_adj_ratio"),
        "raw_adj_ratio_change": row.get("raw_adj_ratio_change"),
        "severity": severity,
        "warning": warning,
    }


def build_event_rows(symbol: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for row in rows:
        raw_return = parse_float(row.get("raw_return_1d"))
        if raw_return is not None and abs(raw_return) >= SEVERE_RAW_RETURN_THRESHOLD:
            events.append(
                event_row(
                    symbol=symbol,
                    row=row,
                    event_type="severe_raw_return",
                    severity="severe",
                    warning="one-day raw return is >= 40%; inspect for split or bad tick",
                )
            )
        elif raw_return is not None and abs(raw_return) >= LARGE_RAW_RETURN_THRESHOLD:
            events.append(
                event_row(
                    symbol=symbol,
                    row=row,
                    event_type="large_raw_return",
                    severity="warning",
                    warning="one-day raw return is >= 20%; inspect before long-run use",
                )
            )

        if is_split_factor_event(row.get("split_factor")):
            events.append(
                event_row(
                    symbol=symbol,
                    row=row,
                    event_type="split_factor",
                    severity="severe",
                    warning="Tiingo split_factor is not 1; raw series may need split adjustment",
                )
            )

        ratio_change = parse_float(row.get("raw_adj_ratio_change"))
        if ratio_change is not None and abs(ratio_change) >= RAW_ADJ_RATIO_JUMP_THRESHOLD:
            events.append(
                event_row(
                    symbol=symbol,
                    row=row,
                    event_type="raw_adj_ratio_jump",
                    severity="warning",
                    warning="raw/adjusted close ratio changed by >= 20%",
                )
            )
    return events


def max_abs_metric(rows: list[dict[str, Any]], column: str) -> tuple[float | None, str]:
    candidates = [
        (abs(float(value)), str(row["date"]))
        for row in rows
        if (value := parse_float(row.get(column))) is not None
    ]
    if not candidates:
        return None, ""
    return max(candidates, key=lambda item: item[0])


def first_last_metric(rows: list[dict[str, Any]], column: str) -> tuple[float | None, float | None]:
    values = [
        parse_float(row.get(column))
        for row in rows
        if parse_float(row.get(column)) is not None
    ]
    if not values:
        return None, None
    return values[0], values[-1]


def recommendation_for_symbol(
    *,
    row_count: int,
    split_factor_event_count: int,
    large_raw_return_count: int,
    severe_raw_return_count: int,
    raw_adj_ratio_jump_count: int,
    load_warning: str = "",
) -> tuple[bool, str, str]:
    if load_warning:
        return False, "inspect_manually", load_warning
    if row_count == 0:
        return False, "inspect_manually", "no rows available in diagnostic date range"
    if split_factor_event_count:
        return (
            True,
            "build_split_adjusted_from_raw",
            "split_factor values indicate Tiingo raw should not be used directly",
        )
    if raw_adj_ratio_jump_count:
        return (
            True,
            "build_split_adjusted_from_raw",
            "raw/adjusted ratio jumps indicate likely historical split adjustment needs",
        )
    if severe_raw_return_count:
        return (
            False,
            "inspect_manually",
            "severe one-day raw return spikes need manual inspection",
        )
    if large_raw_return_count:
        return (
            False,
            "inspect_manually",
            "large one-day raw return spikes need review before research use",
        )
    return False, "use_raw_compatible_as_is", "no split discontinuity flags detected"


def build_symbol_summary_row(
    *,
    symbol: str,
    rows: list[dict[str, Any]],
    load_warning: str = "",
) -> dict[str, Any]:
    dates = [str(row["date"]) for row in rows if row.get("date")]
    split_factor_event_count = sum(
        1 for row in rows if is_split_factor_event(row.get("split_factor"))
    )
    large_raw_return_count = sum(
        1
        for row in rows
        if (value := parse_float(row.get("raw_return_1d"))) is not None
        and abs(value) >= LARGE_RAW_RETURN_THRESHOLD
    )
    severe_raw_return_count = sum(
        1
        for row in rows
        if (value := parse_float(row.get("raw_return_1d"))) is not None
        and abs(value) >= SEVERE_RAW_RETURN_THRESHOLD
    )
    raw_adj_ratio_jump_count = sum(
        1
        for row in rows
        if (value := parse_float(row.get("raw_adj_ratio_change"))) is not None
        and abs(value) >= RAW_ADJ_RATIO_JUMP_THRESHOLD
    )
    max_raw_return, max_raw_return_date = max_abs_metric(rows, "raw_return_1d")
    max_ratio_change, max_ratio_change_date = max_abs_metric(rows, "raw_adj_ratio_change")
    ratio_start, ratio_end = first_last_metric(rows, "raw_adj_ratio")
    needs_split_adjustment, recommendation, reason = recommendation_for_symbol(
        row_count=len(rows),
        split_factor_event_count=split_factor_event_count,
        large_raw_return_count=large_raw_return_count,
        severe_raw_return_count=severe_raw_return_count,
        raw_adj_ratio_jump_count=raw_adj_ratio_jump_count,
        load_warning=load_warning,
    )
    return {
        "symbol": symbol,
        "first_date": min(dates) if dates else "",
        "last_date": max(dates) if dates else "",
        "row_count": len(rows),
        "split_factor_event_count": split_factor_event_count,
        "large_raw_return_count": large_raw_return_count,
        "severe_raw_return_count": severe_raw_return_count,
        "raw_adj_ratio_jump_count": raw_adj_ratio_jump_count,
        "max_abs_raw_return": max_raw_return,
        "max_abs_raw_return_date": max_raw_return_date,
        "max_abs_raw_adj_ratio_change": max_ratio_change,
        "max_abs_raw_adj_ratio_change_date": max_ratio_change_date,
        "raw_adj_ratio_start": ratio_start,
        "raw_adj_ratio_end": ratio_end,
        "likely_split_adjustment_needed": needs_split_adjustment,
        "raw_compatible_usage_recommendation": recommendation,
        "reason": reason,
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
    if not rows:
        lines.append("| " + " | ".join("" for _ in columns) + " |")
        return lines
    for row in rows:
        values = [str(row.get(column, "")).replace("|", "\\|") for column in columns]
        lines.append("| " + " | ".join(values) + " |")
    return lines


def write_markdown_report(
    path: Path,
    *,
    config: TiingoRawSplitDiscontinuityConfig,
    event_rows: list[dict[str, Any]],
    symbol_rows: list[dict[str, Any]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    split_symbols = [
        row for row in symbol_rows if int(row["split_factor_event_count"]) > 0
    ]
    severe_symbols = [
        row for row in symbol_rows if int(row["severe_raw_return_count"]) > 0
    ]
    unsafe_symbols = [
        row
        for row in symbol_rows
        if row["raw_compatible_usage_recommendation"] != "use_raw_compatible_as_is"
    ]
    recommendation_counts: dict[str, int] = {}
    for row in symbol_rows:
        recommendation = str(row["raw_compatible_usage_recommendation"])
        recommendation_counts[recommendation] = recommendation_counts.get(recommendation, 0) + 1

    lines = [
        "# Tiingo Raw Split Discontinuity Diagnostics",
        "",
        f"Generated at: {generated_at}",
        "",
        "## Purpose",
        (
            "Detect split discontinuities, large one-day raw return spikes, and "
            "raw-versus-adjusted close ratio jumps in already-downloaded Tiingo CSV files "
            "before using Tiingo raw-compatible history for long-run research or promotion."
        ),
        "",
        "## Local-only note",
        "- This report reads local Tiingo CSV files only.",
        "- No Tiingo API call happened during this run.",
        "- No S3 write or promotion happened during this run.",
        "- Massive automation was not modified.",
        "",
        "## Inputs",
        f"- Tiingo raw dir: `{config.tiingo_raw_dir}`",
        f"- Symbols file: `{config.symbols_file}`",
        f"- Date range: `{config.start_date or ''}` through `{config.end_date or ''}`",
        f"- Pre-Massive only: `{config.pre_massive_only}`",
        f"- Massive boundary report: `{config.massive_boundary_report}`",
        "",
        "## Summary counts",
        f"- Symbols evaluated: {len(symbol_rows)}",
        f"- Flagged event rows: {len(event_rows)}",
        f"- Symbols with split_factor events: {len(split_symbols)}",
        f"- Symbols with severe raw return spikes: {len(severe_symbols)}",
        "- Usage recommendations: "
        + ", ".join(f"{key}={value}" for key, value in sorted(recommendation_counts.items())),
        "",
        "## Symbols with split_factor events",
        *markdown_table(
            split_symbols,
            ["symbol", "split_factor_event_count", "raw_compatible_usage_recommendation"],
        ),
        "",
        "## Symbols with severe raw return spikes",
        *markdown_table(
            severe_symbols,
            [
                "symbol",
                "severe_raw_return_count",
                "max_abs_raw_return",
                "max_abs_raw_return_date",
                "raw_compatible_usage_recommendation",
            ],
        ),
        "",
        "## Symbols where raw-compatible should not be used as-is",
        *markdown_table(
            unsafe_symbols,
            [
                "symbol",
                "split_factor_event_count",
                "large_raw_return_count",
                "severe_raw_return_count",
                "raw_adj_ratio_jump_count",
                "raw_compatible_usage_recommendation",
                "reason",
            ],
        ),
        "",
        "## Interpretation",
        (
            "- Massive appears closer to Tiingo raw in the overlap period, but raw data "
            "may not be split-adjusted historically."
        ),
        (
            "- Split factors or raw/adjusted ratio jumps are direct signs that raw-compatible "
            "history may contain pre-overlap discontinuities."
        ),
        "- Large raw return spikes without split metadata still need manual review.",
        "",
        "## Recommendation",
        (
            "- If split discontinuities are present, build a Tiingo split-adjusted-only "
            "source from raw prices plus split_factor before any overnight ML run."
        ),
        "- Do not use this diagnostic report as an S3 promotion artifact.",
    ]
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
        f.write("\n")


def run_diagnostics(
    config: TiingoRawSplitDiscontinuityConfig,
) -> TiingoRawSplitDiscontinuityResult:
    pre_massive_end_dates = (
        load_pre_massive_end_dates(config.massive_boundary_report)
        if config.pre_massive_only
        else {}
    )
    event_rows: list[dict[str, Any]] = []
    symbol_summary_rows: list[dict[str, Any]] = []

    for symbol in config.symbols:
        normalized_symbol = symbol.upper()
        path = config.tiingo_raw_dir / f"{normalized_symbol}.csv"
        symbol_end_date = effective_end_date(
            config_end_date=config.end_date,
            pre_massive_end_date=pre_massive_end_dates.get(normalized_symbol),
        )
        try:
            rows = compute_metrics(
                load_tiingo_rows(
                    path,
                    start_date=config.start_date,
                    end_date=symbol_end_date,
                )
            )
            symbol_event_rows = build_event_rows(normalized_symbol, rows)
            event_rows.extend(symbol_event_rows)
            symbol_summary_rows.append(
                build_symbol_summary_row(symbol=normalized_symbol, rows=rows)
            )
        except Exception as exc:  # noqa: BLE001 - keep per-symbol diagnostics moving.
            symbol_summary_rows.append(
                build_symbol_summary_row(
                    symbol=normalized_symbol,
                    rows=[],
                    load_warning=str(exc),
                )
            )

    event_report_path = config.output_dir / EVENT_REPORT_FILENAME
    symbol_summary_path = config.output_dir / SYMBOL_SUMMARY_FILENAME
    markdown_report_path = config.output_dir / MARKDOWN_REPORT_FILENAME
    write_csv_report(event_report_path, event_rows, EVENT_COLUMNS)
    write_csv_report(symbol_summary_path, symbol_summary_rows, SUMMARY_COLUMNS)
    write_markdown_report(
        markdown_report_path,
        config=config,
        event_rows=event_rows,
        symbol_rows=symbol_summary_rows,
    )
    return TiingoRawSplitDiscontinuityResult(
        event_rows=event_rows,
        symbol_summary_rows=symbol_summary_rows,
        event_report_path=event_report_path,
        symbol_summary_path=symbol_summary_path,
        markdown_report_path=markdown_report_path,
    )


def main() -> None:
    try:
        config = config_from_args(parse_args())
        result = run_diagnostics(config)
    except Exception as exc:  # noqa: BLE001 - CLI should fail clearly.
        print(f"Tiingo raw split discontinuity diagnostics failed: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc

    unsafe_count = sum(
        1
        for row in result.symbol_summary_rows
        if row["raw_compatible_usage_recommendation"] != "use_raw_compatible_as_is"
    )
    print(
        "Tiingo raw split discontinuity diagnostics complete: "
        f"symbols={len(result.symbol_summary_rows)} events={len(result.event_rows)} "
        f"not_as_is={unsafe_count}"
    )
    print(f"event report: {result.event_report_path}")
    print(f"symbol summary: {result.symbol_summary_path}")
    print(f"markdown report: {result.markdown_report_path}")


if __name__ == "__main__":
    main()
