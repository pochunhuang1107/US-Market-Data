#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any

DEFAULT_STOOQ_VALIDATION_DIR = "data/manual_backfill/validation"
DEFAULT_TIINGO_VALIDATION_DIR = "data/manual_backfill_tiingo/validation"
DEFAULT_OUTPUT_DIR = "data/manual_backfill_source_comparison"
DEFAULT_SOURCE_A_NAME = "stooq"
DEFAULT_SOURCE_B_NAME = "tiingo"
DEFAULT_PREFERRED_SOURCE = "tiingo"

REQUIRED_REPORTS = [
    "coverage_report.csv",
    "rebased_export_report.csv",
    "stitched_dataset_report.csv",
    "massive_boundary_report.csv",
    "symbol_quality_report.csv",
]
OPTIONAL_SUMMARIES = [
    "manual_backfill_summary.md",
    "rebased_export_summary.md",
    "stitched_dataset_summary.md",
]
TIINGO_DOWNLOAD_MANIFEST = "tiingo_download_manifest.csv"

BOUNDARY_CLOSE_JUMP_WARNING_THRESHOLD = 0.02

SYMBOL_SUMMARY_FILENAME = "source_comparison_symbol_summary.csv"
DECISION_MATRIX_FILENAME = "source_comparison_decision_matrix.csv"
MARKDOWN_REPORT_FILENAME = "source_comparison_report.md"

BASE_SYMBOL_SUMMARY_COLUMNS = [
    "symbol",
    "{a}_first_date",
    "{b}_first_date",
    "better_coverage_source",
    "{a}_rebased_rows",
    "{b}_rebased_rows",
    "{a}_stitched_rows",
    "{b}_stitched_rows",
    "{a}_boundary_jump_pct",
    "{b}_boundary_jump_pct",
    "{a}_status",
    "{b}_status",
    "recommended_source",
    "recommendation_label",
    "reason",
]

DECISION_MATRIX_COLUMNS = [
    "source_provider",
    "symbols_available",
    "symbols_written",
    "symbols_skipped",
    "total_rebased_rows",
    "total_stitched_rows",
    "duplicate_failure_count",
    "boundary_gap_warning_count",
    "high_boundary_jump_warning_count",
    "earliest_first_date",
    "latest_first_date",
    "median_boundary_jump_pct",
    "max_boundary_jump_pct",
    "final_role",
]


@dataclass(frozen=True)
class SourceComparisonConfig:
    stooq_validation_dir: Path
    tiingo_validation_dir: Path
    output_dir: Path
    source_a_name: str
    source_b_name: str
    preferred_source: str


@dataclass
class SourceReports:
    source_name: str
    validation_dir: Path
    coverage_rows: list[dict[str, str]]
    rebased_rows: list[dict[str, str]]
    stitched_rows: list[dict[str, str]]
    boundary_rows: list[dict[str, str]]
    quality_rows: list[dict[str, str]]
    optional_summary_paths: list[Path]
    tiingo_manifest_rows: list[dict[str, str]]


@dataclass
class SourceSymbol:
    source_name: str
    symbol: str
    first_date: str
    rebased_rows: int
    stitched_rows: int
    boundary_jump_pct: float | None
    status: str
    duplicate_rows: int
    ohlc_bad_rows: int
    boundary_status: str
    has_pre_massive_rows: bool
    warning: str


@dataclass
class SourceComparisonResult:
    symbol_summary_rows: list[dict[str, Any]]
    decision_matrix_rows: list[dict[str, Any]]
    symbol_summary_path: Path
    decision_matrix_path: Path
    markdown_report_path: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create a local-only Tiingo vs Stooq historical ETF backfill comparison "
            "and promotion-readiness report."
        )
    )
    parser.add_argument("--stooq-validation-dir", default=DEFAULT_STOOQ_VALIDATION_DIR)
    parser.add_argument("--tiingo-validation-dir", default=DEFAULT_TIINGO_VALIDATION_DIR)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--source-a-name", default=DEFAULT_SOURCE_A_NAME)
    parser.add_argument("--source-b-name", default=DEFAULT_SOURCE_B_NAME)
    parser.add_argument("--preferred-source", default=DEFAULT_PREFERRED_SOURCE)
    return parser.parse_args()


def config_from_args(args: argparse.Namespace) -> SourceComparisonConfig:
    return SourceComparisonConfig(
        stooq_validation_dir=Path(args.stooq_validation_dir),
        tiingo_validation_dir=Path(args.tiingo_validation_dir),
        output_dir=Path(args.output_dir),
        source_a_name=str(args.source_a_name).strip().lower(),
        source_b_name=str(args.source_b_name).strip().lower(),
        preferred_source=str(args.preferred_source).strip().lower(),
    )


def read_csv_report(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"required report does not exist: {path}")
    with open(path, "r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def read_optional_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def write_csv_report(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def parse_int(value: object) -> int:
    text = str(value).strip() if value is not None else ""
    if not text:
        return 0
    try:
        return int(float(text))
    except ValueError:
        return 0


def parse_float(value: object) -> float | None:
    text = str(value).strip() if value is not None else ""
    if not text:
        return None
    try:
        number = float(text)
    except ValueError:
        return None
    return number if math.isfinite(number) else None


def parse_bool(value: object) -> bool:
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


def symbol_from_row(row: dict[str, Any]) -> str:
    return str(row.get("symbol", "")).strip().upper()


def rows_by_symbol(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {symbol_from_row(row): row for row in rows if symbol_from_row(row)}


def prefixed_columns(source_a_name: str, source_b_name: str) -> list[str]:
    return [
        column.format(a=source_a_name, b=source_b_name)
        for column in BASE_SYMBOL_SUMMARY_COLUMNS
    ]


def load_source_reports(source_name: str, validation_dir: Path) -> SourceReports:
    reports = {
        filename: read_csv_report(validation_dir / filename)
        for filename in REQUIRED_REPORTS
    }
    optional_summary_paths = [
        validation_dir / filename
        for filename in OPTIONAL_SUMMARIES
        if (validation_dir / filename).exists()
    ]
    tiingo_manifest_rows = read_optional_csv(validation_dir / TIINGO_DOWNLOAD_MANIFEST)
    return SourceReports(
        source_name=source_name,
        validation_dir=validation_dir,
        coverage_rows=reports["coverage_report.csv"],
        rebased_rows=reports["rebased_export_report.csv"],
        stitched_rows=reports["stitched_dataset_report.csv"],
        boundary_rows=reports["massive_boundary_report.csv"],
        quality_rows=reports["symbol_quality_report.csv"],
        optional_summary_paths=optional_summary_paths,
        tiingo_manifest_rows=tiingo_manifest_rows,
    )


def source_symbols(reports: SourceReports) -> dict[str, SourceSymbol]:
    coverage_by_symbol = rows_by_symbol(reports.coverage_rows)
    rebased_by_symbol = rows_by_symbol(reports.rebased_rows)
    stitched_by_symbol = rows_by_symbol(reports.stitched_rows)
    boundary_by_symbol = rows_by_symbol(reports.boundary_rows)
    symbols = sorted(
        set(coverage_by_symbol)
        | set(rebased_by_symbol)
        | set(stitched_by_symbol)
        | set(boundary_by_symbol)
    )
    result: dict[str, SourceSymbol] = {}
    for symbol in symbols:
        coverage_row = coverage_by_symbol.get(symbol, {})
        rebased_row = rebased_by_symbol.get(symbol, {})
        stitched_row = stitched_by_symbol.get(symbol, {})
        boundary_row = boundary_by_symbol.get(symbol, {})
        rebased_rows = parse_int(rebased_row.get("output_rows"))
        export_rows = parse_int(boundary_row.get("export_row_count"))
        has_pre_massive = rebased_rows > 0 or (
            export_rows > 0 and parse_bool(boundary_row.get("has_pre_massive_history"))
        )
        result[symbol] = SourceSymbol(
            source_name=reports.source_name,
            symbol=symbol,
            first_date=str(coverage_row.get("first_date", "")).strip(),
            rebased_rows=rebased_rows,
            stitched_rows=parse_int(stitched_row.get("stitched_rows")),
            boundary_jump_pct=parse_float(stitched_row.get("boundary_close_jump_pct")),
            status=str(stitched_row.get("status") or rebased_row.get("status") or "").strip(),
            duplicate_rows=parse_int(stitched_row.get("duplicate_rows")),
            ohlc_bad_rows=parse_int(stitched_row.get("ohlc_bad_rows")),
            boundary_status=str(stitched_row.get("boundary_status", "")).strip(),
            has_pre_massive_rows=has_pre_massive,
            warning=str(stitched_row.get("warning") or rebased_row.get("warning") or "").strip(),
        )
    return result


def source_is_usable(source: SourceSymbol | None) -> bool:
    if source is None or not source.has_pre_massive_rows:
        return False
    return (
        source.status in {"written", "warning", "planned"}
        and source.duplicate_rows == 0
        and source.ohlc_bad_rows == 0
        and source.boundary_status in {"pass", "warning"}
    )


def source_has_warning(source: SourceSymbol | None) -> bool:
    if source is None:
        return False
    return (
        source.status == "warning"
        or source.boundary_status == "warning"
        or (
            source.boundary_jump_pct is not None
            and source.boundary_jump_pct > BOUNDARY_CLOSE_JUMP_WARNING_THRESHOLD
        )
        or bool(source.warning)
    )


def first_date_not_later(candidate: SourceSymbol, baseline: SourceSymbol | None) -> bool:
    if baseline is None or not baseline.first_date:
        return True
    if not candidate.first_date:
        return False
    return candidate.first_date <= baseline.first_date


def materially_worse(candidate: SourceSymbol | None, baseline: SourceSymbol | None) -> bool:
    if candidate is None or not source_is_usable(candidate):
        return True
    if baseline is None or not source_is_usable(baseline):
        return False
    if candidate.first_date and baseline.first_date and candidate.first_date > baseline.first_date:
        return True
    return candidate.rebased_rows < baseline.rebased_rows


def approval_label(source_name: str, *, warning: bool) -> str:
    suffix = "_with_warning" if warning else ""
    return f"approve_{source_name}{suffix}"


def better_coverage_source(a: SourceSymbol | None, b: SourceSymbol | None) -> str:
    if a is None and b is None:
        return ""
    if a is None:
        return b.source_name if b else ""
    if b is None:
        return a.source_name
    if a.first_date and b.first_date:
        if a.first_date < b.first_date:
            return a.source_name
        if b.first_date < a.first_date:
            return b.source_name
        return "tie"
    if a.first_date:
        return a.source_name
    if b.first_date:
        return b.source_name
    return ""


def choose_recommendation(
    *,
    source_a: SourceSymbol | None,
    source_b: SourceSymbol | None,
    preferred_source: str,
) -> tuple[str, str, str]:
    sources = {
        source.source_name: source
        for source in [source_a, source_b]
        if source is not None and source.source_name
    }
    preferred = sources.get(preferred_source)
    alternatives = [source for name, source in sources.items() if name != preferred_source]
    other = alternatives[0] if alternatives else None

    if not any(source and source.has_pre_massive_rows for source in [source_a, source_b]):
        return "", "skip", "no pre-Massive rows in either source"

    if preferred and source_is_usable(preferred):
        if first_date_not_later(preferred, other) or not source_is_usable(other):
            warning = source_has_warning(preferred)
            reason = (
                f"{preferred.source_name} has clean stitched validation"
                if not warning
                else f"{preferred.source_name} is usable but carries a boundary/status warning"
            )
            return (
                preferred.source_name,
                approval_label(preferred.source_name, warning=warning),
                reason,
            )

    if other and source_is_usable(other) and materially_worse(preferred, other):
        warning = source_has_warning(other)
        reason = (
            f"{preferred_source} is missing, skipped, or materially worse; "
            f"{other.source_name} is the usable fallback"
        )
        return other.source_name, approval_label(other.source_name, warning=warning), reason

    return "", "inspect_more", "both sources need additional inspection before promotion"


def build_symbol_summary_rows(
    *,
    reports_a: SourceReports,
    reports_b: SourceReports,
    preferred_source: str,
) -> list[dict[str, Any]]:
    symbols_a = source_symbols(reports_a)
    symbols_b = source_symbols(reports_b)
    symbols = sorted(set(symbols_a) | set(symbols_b))
    rows: list[dict[str, Any]] = []
    for symbol in symbols:
        source_a = symbols_a.get(symbol)
        source_b = symbols_b.get(symbol)
        recommended_source, label, reason = choose_recommendation(
            source_a=source_a,
            source_b=source_b,
            preferred_source=preferred_source,
        )
        rows.append(
            {
                "symbol": symbol,
                f"{reports_a.source_name}_first_date": source_a.first_date if source_a else "",
                f"{reports_b.source_name}_first_date": source_b.first_date if source_b else "",
                "better_coverage_source": better_coverage_source(source_a, source_b),
                f"{reports_a.source_name}_rebased_rows": source_a.rebased_rows
                if source_a
                else 0,
                f"{reports_b.source_name}_rebased_rows": source_b.rebased_rows
                if source_b
                else 0,
                f"{reports_a.source_name}_stitched_rows": source_a.stitched_rows
                if source_a
                else 0,
                f"{reports_b.source_name}_stitched_rows": source_b.stitched_rows
                if source_b
                else 0,
                f"{reports_a.source_name}_boundary_jump_pct": ""
                if source_a is None or source_a.boundary_jump_pct is None
                else source_a.boundary_jump_pct,
                f"{reports_b.source_name}_boundary_jump_pct": ""
                if source_b is None or source_b.boundary_jump_pct is None
                else source_b.boundary_jump_pct,
                f"{reports_a.source_name}_status": source_a.status if source_a else "",
                f"{reports_b.source_name}_status": source_b.status if source_b else "",
                "recommended_source": recommended_source,
                "recommendation_label": label,
                "reason": reason,
            }
        )
    return rows


def source_decision_matrix_row(
    reports: SourceReports,
    *,
    preferred_source: str,
) -> dict[str, Any]:
    first_dates = [
        str(row.get("first_date", "")).strip()
        for row in reports.coverage_rows
        if str(row.get("first_date", "")).strip()
    ]
    jumps = [
        jump
        for jump in (
            parse_float(row.get("boundary_close_jump_pct"))
            for row in reports.stitched_rows
        )
        if jump is not None
    ]
    final_role = (
        "preferred_pre_massive_backfill"
        if reports.source_name == preferred_source
        else "backup_validation_source"
    )
    return {
        "source_provider": reports.source_name,
        "symbols_available": sum(
            1 for row in reports.coverage_rows if parse_int(row.get("row_count")) > 0
        ),
        "symbols_written": sum(
            1 for row in reports.stitched_rows if row.get("status") in {"written", "warning"}
        ),
        "symbols_skipped": sum(
            1 for row in reports.stitched_rows if row.get("status") == "skipped"
        ),
        "total_rebased_rows": sum(
            parse_int(row.get("output_rows")) for row in reports.rebased_rows
        ),
        "total_stitched_rows": sum(
            parse_int(row.get("stitched_rows")) for row in reports.stitched_rows
        ),
        "duplicate_failure_count": sum(
            1 for row in reports.stitched_rows if parse_int(row.get("duplicate_rows")) > 0
        ),
        "boundary_gap_warning_count": sum(
            1
            for row in reports.stitched_rows
            if "missing one side of stitched boundary" in str(row.get("warning", ""))
        ),
        "high_boundary_jump_warning_count": sum(
            1
            for jump in jumps
            if jump > BOUNDARY_CLOSE_JUMP_WARNING_THRESHOLD
        ),
        "earliest_first_date": min(first_dates) if first_dates else "",
        "latest_first_date": max(first_dates) if first_dates else "",
        "median_boundary_jump_pct": "" if not jumps else median(jumps),
        "max_boundary_jump_pct": "" if not jumps else max(jumps),
        "final_role": final_role,
    }


def build_decision_matrix_rows(
    reports_a: SourceReports,
    reports_b: SourceReports,
    *,
    preferred_source: str,
) -> list[dict[str, Any]]:
    return [
        source_decision_matrix_row(reports_a, preferred_source=preferred_source),
        source_decision_matrix_row(reports_b, preferred_source=preferred_source),
    ]


def pct_text(value: object) -> str:
    number = parse_float(value)
    if number is None:
        return ""
    return f"{number:.2%}"


def count_labels(rows: list[dict[str, Any]], label: str) -> int:
    return sum(1 for row in rows if row.get("recommendation_label") == label)


def markdown_table(rows: list[dict[str, Any]], columns: list[str]) -> list[str]:
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for row in rows:
        values = [str(row.get(column, "")).replace("|", "\\|") for column in columns]
        lines.append("| " + " | ".join(values) + " |")
    return lines


def write_markdown_report(
    path: Path,
    *,
    config: SourceComparisonConfig,
    reports_a: SourceReports,
    reports_b: SourceReports,
    symbol_rows: list[dict[str, Any]],
    matrix_rows: list[dict[str, Any]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    preferred_rows = [
        row for row in symbol_rows if row["recommended_source"] == config.preferred_source
    ]
    warning_rows = [
        row for row in symbol_rows if row["recommendation_label"].endswith("_with_warning")
    ]
    skip_rows = [row for row in symbol_rows if row["recommendation_label"] == "skip"]
    inspect_rows = [row for row in symbol_rows if row["recommendation_label"] == "inspect_more"]
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    summary_paths = reports_a.optional_summary_paths + reports_b.optional_summary_paths
    optional_summary_line = (
        ", ".join(str(path) for path in summary_paths) if summary_paths else "None present"
    )
    tiingo_manifest_line = (
        f"{len(reports_b.tiingo_manifest_rows)} Tiingo manifest rows loaded"
        if reports_b.tiingo_manifest_rows
        else "No Tiingo download manifest loaded"
    )

    source_table_rows = [
        {
            "source_provider": row["source_provider"],
            "symbols_available": row["symbols_available"],
            "symbols_written": row["symbols_written"],
            "total_rebased_rows": row["total_rebased_rows"],
            "total_stitched_rows": row["total_stitched_rows"],
            "earliest_first_date": row["earliest_first_date"],
            "max_boundary_jump_pct": pct_text(row["max_boundary_jump_pct"]),
            "final_role": row["final_role"],
        }
        for row in matrix_rows
    ]
    symbol_table_rows = [
        {
            "symbol": row["symbol"],
            f"{config.source_a_name}_first_date": row[f"{config.source_a_name}_first_date"],
            f"{config.source_b_name}_first_date": row[f"{config.source_b_name}_first_date"],
            "better_coverage_source": row["better_coverage_source"],
            "recommended_source": row["recommended_source"],
            "recommendation_label": row["recommendation_label"],
            "reason": row["reason"],
        }
        for row in symbol_rows
    ]

    lines = [
        "# Manual Historical ETF Backfill Source Comparison",
        "",
        f"Generated at: {generated_at}",
        "",
        "## Executive summary",
        (
            f"{config.source_b_name} and {config.source_a_name} were compared as local-only "
            "pre-Massive historical ETF backfill candidates. The comparison recommends "
            f"{config.preferred_source} for {len(preferred_rows)} symbols, flags "
            f"{len(warning_rows)} warning-level approvals, skips {len(skip_rows)} symbols, "
            f"and leaves {len(inspect_rows)} symbols for additional inspection."
        ),
        "",
        "## Data sources compared",
        f"- Source A: {config.source_a_name} reports in `{reports_a.validation_dir}`.",
        f"- Source B: {config.source_b_name} reports in `{reports_b.validation_dir}`.",
        f"- Optional summaries loaded: {optional_summary_line}.",
        f"- {tiingo_manifest_line}.",
        "",
        "## Why Massive remains canonical from 2016 onward",
        (
            "Massive remains the canonical current source from each symbol's Massive first "
            "available date onward. These source candidates are evaluated only for rows before "
            "that symbol-specific boundary, and the stitched validation keeps Massive rows "
            "at and after the boundary."
        ),
        "",
        "## Coverage comparison",
        *markdown_table(
            source_table_rows,
            [
                "source_provider",
                "symbols_available",
                "symbols_written",
                "total_rebased_rows",
                "total_stitched_rows",
                "earliest_first_date",
                "max_boundary_jump_pct",
                "final_role",
            ],
        ),
        "",
        "## Stitched validation comparison",
        (
            "Both candidates are checked for duplicate symbol-date rows, OHLC sanity failures, "
            "boundary gaps, and boundary close jumps. Warning-level boundary jumps can be "
            "approved for backfill use only after manual review."
        ),
        "",
        "## Symbol-level recommendation table",
        *markdown_table(
            symbol_table_rows,
            [
                "symbol",
                f"{config.source_a_name}_first_date",
                f"{config.source_b_name}_first_date",
                "better_coverage_source",
                "recommended_source",
                "recommendation_label",
                "reason",
            ],
        ),
        "",
        "## Promotion-readiness decision",
        (
            "The local comparison is promotion-ready as an input to a future promotion design, "
            "but it is not itself a promotion step. No data should be promoted until a separate "
            "S3 promotion tool with same-year merge protection is implemented and reviewed."
        ),
        "",
        "## Risks and caveats",
        "- Boundary close jumps above 2% require manual inspection before approval.",
        "- Rebased pre-Massive rows must be merged into existing boundary-year partitions safely.",
        (
            "- Source quality reports can flag expected adjustment mismatches; "
            "stitched validation is the final local gate here."
        ),
        (
            "- This report does not write to S3, promote historical data, or modify "
            "Massive automation."
        ),
        "",
        "## Final recommendation",
        "- Prefer Tiingo for the one-time pre-Massive historical ETF backfill.",
        "- Keep Massive as canonical for existing Massive-covered periods.",
        "- Use rebased Tiingo rows only before each symbol's Massive first available date.",
        "- Skip IBIT for historical backfill because there are no pre-Massive rows.",
        "- ARKK should be approve_with_warning / inspect boundary manually due to its ~3% jump.",
        (
            "- Do not promote to S3 until a separate promotion tool with same-year "
            "merge protection is implemented and reviewed."
        ),
        "",
        "## Next engineering step",
        (
            "Build and review a dedicated local-to-S3 promotion tool that reads the selected "
            "rebased Tiingo pre-Massive export, refuses unsafe overwrites, and performs "
            "same-year partition merges when the source and Massive boundary share a year."
        ),
    ]
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
        f.write("\n")


def run_source_comparison(config: SourceComparisonConfig) -> SourceComparisonResult:
    reports_a = load_source_reports(config.source_a_name, config.stooq_validation_dir)
    reports_b = load_source_reports(config.source_b_name, config.tiingo_validation_dir)
    symbol_summary_rows = build_symbol_summary_rows(
        reports_a=reports_a,
        reports_b=reports_b,
        preferred_source=config.preferred_source,
    )
    decision_matrix_rows = build_decision_matrix_rows(
        reports_a,
        reports_b,
        preferred_source=config.preferred_source,
    )

    symbol_summary_path = config.output_dir / SYMBOL_SUMMARY_FILENAME
    decision_matrix_path = config.output_dir / DECISION_MATRIX_FILENAME
    markdown_report_path = config.output_dir / MARKDOWN_REPORT_FILENAME
    write_csv_report(
        symbol_summary_path,
        symbol_summary_rows,
        prefixed_columns(config.source_a_name, config.source_b_name),
    )
    write_csv_report(decision_matrix_path, decision_matrix_rows, DECISION_MATRIX_COLUMNS)
    write_markdown_report(
        markdown_report_path,
        config=config,
        reports_a=reports_a,
        reports_b=reports_b,
        symbol_rows=symbol_summary_rows,
        matrix_rows=decision_matrix_rows,
    )
    return SourceComparisonResult(
        symbol_summary_rows=symbol_summary_rows,
        decision_matrix_rows=decision_matrix_rows,
        symbol_summary_path=symbol_summary_path,
        decision_matrix_path=decision_matrix_path,
        markdown_report_path=markdown_report_path,
    )


def main() -> None:
    config = config_from_args(parse_args())
    result = run_source_comparison(config)
    print(
        "manual backfill source comparison complete: "
        f"symbols={len(result.symbol_summary_rows)} "
        f"sources={len(result.decision_matrix_rows)}"
    )
    print(f"symbol summary: {result.symbol_summary_path}")
    print(f"decision matrix: {result.decision_matrix_path}")
    print(f"markdown report: {result.markdown_report_path}")


if __name__ == "__main__":
    main()
