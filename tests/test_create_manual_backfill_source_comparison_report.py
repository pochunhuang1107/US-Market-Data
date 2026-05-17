from __future__ import annotations

import csv
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from scripts import create_manual_backfill_source_comparison_report as comparison  # noqa: E402


def write_csv(
    path: Path,
    rows: list[dict[str, object]],
    fieldnames: list[str] | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = fieldnames or list(rows[0])
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with open(path, "r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def coverage_row(
    symbol: str,
    source: str,
    *,
    first_date: str,
    row_count: int = 100,
    backfill_row_count: int = 10,
    quality_status: str = "pass",
) -> dict[str, object]:
    return {
        "symbol": symbol,
        "source_provider": source,
        "first_date": first_date,
        "last_date": "2026-05-15",
        "row_count": row_count,
        "backfill_row_count": backfill_row_count,
        "overlap_row_count": 20,
        "exported_years": "2015;2016" if backfill_row_count else "",
        "has_pre_massive_history": bool(backfill_row_count),
        "massive_first_available_date": "2016-03-28",
        "source_backfill_end_date": "2016-03-27",
        "backfill_boundary_mode": "infer_from_massive",
        "requires_merge_with_existing_partition": bool(backfill_row_count),
        "quality_status": quality_status,
    }


def rebased_row(
    symbol: str,
    source: str,
    *,
    output_rows: int = 10,
    status: str = "written",
) -> dict[str, object]:
    return {
        "symbol": symbol,
        "source_provider": source,
        "action": "rebase" if output_rows else "skip",
        "rebase_factor": 1.0 if output_rows else "",
        "massive_first_available_date": "2016-03-28",
        "source_backfill_end_date": "2016-03-27",
        "input_rows": output_rows,
        "output_rows": output_rows,
        "first_output_date": "2015-01-02" if output_rows else "",
        "last_output_date": "2016-03-24" if output_rows else "",
        "output_years": "2015;2016" if output_rows else "",
        "output_path_count": 2 if output_rows else 0,
        "requires_merge_with_existing_partition": bool(output_rows),
        "status": status,
        "warning": "",
    }


def stitched_row(
    symbol: str,
    source: str,
    *,
    rebased_rows_used: int = 10,
    stitched_rows: int = 30,
    status: str = "written",
    boundary_status: str = "pass",
    boundary_jump_pct: float = 0.001,
    duplicate_rows: int = 0,
    ohlc_bad_rows: int = 0,
    warning: str = "",
) -> dict[str, object]:
    return {
        "symbol": symbol,
        "source_provider": source,
        "massive_first_available_date": "2016-03-28",
        "rebased_rows_used": rebased_rows_used,
        "massive_rows_used": stitched_rows - rebased_rows_used,
        "stitched_rows": stitched_rows,
        "first_stitched_date": "2015-01-02" if stitched_rows else "",
        "last_stitched_date": "2026-05-15" if stitched_rows else "",
        "stitched_years": "2015;2016;2026" if stitched_rows else "",
        "duplicate_rows": duplicate_rows,
        "ohlc_bad_rows": ohlc_bad_rows,
        "boundary_year": 2016 if stitched_rows else "",
        "has_rebased_boundary_year_rows": bool(rebased_rows_used),
        "has_massive_boundary_year_rows": bool(stitched_rows),
        "same_year_merge_required": bool(rebased_rows_used),
        "same_year_merge_validated": True,
        "last_rebased_date": "2016-03-24" if rebased_rows_used else "",
        "first_massive_date": "2016-03-28" if stitched_rows else "",
        "last_rebased_close": 100 if rebased_rows_used else "",
        "first_massive_close": 100.1 if stitched_rows else "",
        "boundary_close_jump_pct": boundary_jump_pct if stitched_rows else "",
        "boundary_status": boundary_status,
        "status": status,
        "warning": warning,
    }


def boundary_row(
    symbol: str,
    source: str,
    *,
    export_row_count: int = 10,
    first_export_date: str = "2015-01-02",
) -> dict[str, object]:
    return {
        "symbol": symbol,
        "source_provider": source,
        "massive_first_available_date": "2016-03-28",
        "massive_last_available_date": "2026-05-15",
        "source_first_date": first_export_date,
        "source_last_date": "2026-05-15",
        "global_backfill_end_date": "2017-12-31",
        "source_backfill_end_date": "2016-03-27",
        "backfill_boundary_mode": "infer_from_massive",
        "export_row_count": export_row_count,
        "exported_years": "2015;2016" if export_row_count else "",
        "first_export_date": first_export_date if export_row_count else "",
        "last_export_date": "2016-03-24" if export_row_count else "",
        "has_pre_massive_history": bool(export_row_count),
        "boundary_status": "inferred_from_massive",
        "requires_merge_with_existing_partition": bool(export_row_count),
        "warning": "",
    }


def quality_row(
    symbol: str,
    *,
    ohlc_bad_rows: int = 0,
    duplicate_rows_removed: int = 0,
) -> dict[str, object]:
    return {
        "symbol": symbol,
        "quality_status": "pass",
        "issue_count": 0,
        "issues": "",
        "ohlc_bad_rows": ohlc_bad_rows,
        "duplicate_rows_removed": duplicate_rows_removed,
        "event_row_count": 0,
        "event_warning": "",
        "overlap_quality_status": "pass",
        "adjustment_diagnostic_status": "pass",
        "suggested_action": "use_as_is",
        "boundary_rebase_status": "pass_no_rebase_needed",
        "boundary_rebase_factor": 1.0,
        "boundary_suggested_action": "use_as_is",
    }


def write_source_reports(
    validation_dir: Path,
    source: str,
    *,
    first_dates: dict[str, str],
    rebased_counts: dict[str, int],
    stitched_counts: dict[str, int],
    statuses: dict[str, str] | None = None,
    boundary_statuses: dict[str, str] | None = None,
    boundary_jumps: dict[str, float] | None = None,
) -> None:
    statuses = statuses or {}
    boundary_statuses = boundary_statuses or {}
    boundary_jumps = boundary_jumps or {}
    symbols = sorted(first_dates)
    write_csv(
        validation_dir / "coverage_report.csv",
        [
            coverage_row(
                symbol,
                source,
                first_date=first_dates[symbol],
                backfill_row_count=rebased_counts.get(symbol, 0),
            )
            for symbol in symbols
        ],
    )
    write_csv(
        validation_dir / "rebased_export_report.csv",
        [
            rebased_row(
                symbol,
                source,
                output_rows=rebased_counts.get(symbol, 0),
                status=statuses.get(
                    symbol,
                    "written" if rebased_counts.get(symbol, 0) else "skipped",
                ),
            )
            for symbol in symbols
        ],
    )
    write_csv(
        validation_dir / "stitched_dataset_report.csv",
        [
            stitched_row(
                symbol,
                source,
                rebased_rows_used=rebased_counts.get(symbol, 0),
                stitched_rows=stitched_counts.get(symbol, 0),
                status=statuses.get(
                    symbol,
                    "written" if stitched_counts.get(symbol, 0) else "skipped",
                ),
                boundary_status=boundary_statuses.get(
                    symbol,
                    "pass" if stitched_counts.get(symbol, 0) else "warning",
                ),
                boundary_jump_pct=boundary_jumps.get(symbol, 0.001),
                warning=(
                    "boundary close jump exceeds 2% warning threshold"
                    if boundary_jumps.get(symbol, 0) > 0.02
                    else ""
                ),
            )
            for symbol in symbols
        ],
    )
    write_csv(
        validation_dir / "massive_boundary_report.csv",
        [
            boundary_row(
                symbol,
                source,
                export_row_count=rebased_counts.get(symbol, 0),
                first_export_date=first_dates[symbol],
            )
            for symbol in symbols
        ],
    )
    write_csv(
        validation_dir / "symbol_quality_report.csv",
        [quality_row(symbol) for symbol in symbols],
    )


def run_basic_comparison(tmp_path: Path) -> comparison.SourceComparisonResult:
    stooq_dir = tmp_path / "stooq_validation"
    tiingo_dir = tmp_path / "tiingo_validation"
    symbols = {
        "ARKK": "2014-11-05",
        "IBIT": "2024-01-11",
        "SPY": "2005-02-25",
    }
    write_source_reports(
        stooq_dir,
        "stooq",
        first_dates=symbols,
        rebased_counts={"ARKK": 3, "IBIT": 0, "SPY": 8},
        stitched_counts={"ARKK": 20, "IBIT": 0, "SPY": 28},
        statuses={"ARKK": "warning", "IBIT": "skipped", "SPY": "written"},
        boundary_statuses={"ARKK": "warning", "IBIT": "warning", "SPY": "pass"},
        boundary_jumps={"ARKK": 0.031, "IBIT": 0.0, "SPY": 0.001},
    )
    write_source_reports(
        tiingo_dir,
        "tiingo",
        first_dates={"ARKK": "2014-10-31", "IBIT": "2024-01-11", "SPY": "1999-01-29"},
        rebased_counts={"ARKK": 4, "IBIT": 0, "SPY": 15},
        stitched_counts={"ARKK": 21, "IBIT": 0, "SPY": 35},
        statuses={"ARKK": "warning", "IBIT": "skipped", "SPY": "written"},
        boundary_statuses={"ARKK": "warning", "IBIT": "warning", "SPY": "pass"},
        boundary_jumps={"ARKK": 0.031, "IBIT": 0.0, "SPY": 0.001},
    )
    return comparison.run_source_comparison(
        comparison.SourceComparisonConfig(
            stooq_validation_dir=stooq_dir,
            tiingo_validation_dir=tiingo_dir,
            output_dir=tmp_path / "comparison",
            source_a_name="stooq",
            source_b_name="tiingo",
            preferred_source="tiingo",
        )
    )


def test_reads_stooq_and_tiingo_report_csvs(tmp_path: Path) -> None:
    result = run_basic_comparison(tmp_path)

    assert {row["source_provider"] for row in result.decision_matrix_rows} == {
        "stooq",
        "tiingo",
    }


def test_creates_symbol_summary_with_coverage_comparison(tmp_path: Path) -> None:
    result = run_basic_comparison(tmp_path)

    rows = read_csv_rows(result.symbol_summary_path)
    spy = next(row for row in rows if row["symbol"] == "SPY")
    assert spy["stooq_first_date"] == "2005-02-25"
    assert spy["tiingo_first_date"] == "1999-01-29"
    assert spy["better_coverage_source"] == "tiingo"


def test_prefers_tiingo_when_earlier_first_date_and_clean_stitched_status(
    tmp_path: Path,
) -> None:
    result = run_basic_comparison(tmp_path)

    spy = next(row for row in result.symbol_summary_rows if row["symbol"] == "SPY")
    assert spy["recommended_source"] == "tiingo"
    assert spy["recommendation_label"] == "approve_tiingo"


def test_marks_skip_when_both_sources_have_no_pre_massive_rows(tmp_path: Path) -> None:
    result = run_basic_comparison(tmp_path)

    ibit = next(row for row in result.symbol_summary_rows if row["symbol"] == "IBIT")
    assert ibit["recommended_source"] == ""
    assert ibit["recommendation_label"] == "skip"


def test_marks_approve_with_warning_when_boundary_jump_exceeds_threshold(
    tmp_path: Path,
) -> None:
    result = run_basic_comparison(tmp_path)

    arkk = next(row for row in result.symbol_summary_rows if row["symbol"] == "ARKK")
    assert arkk["recommended_source"] == "tiingo"
    assert arkk["recommendation_label"] == "approve_tiingo_with_warning"


def test_creates_decision_matrix(tmp_path: Path) -> None:
    result = run_basic_comparison(tmp_path)

    rows = read_csv_rows(result.decision_matrix_path)
    tiingo = next(row for row in rows if row["source_provider"] == "tiingo")
    assert tiingo["symbols_available"] == "3"
    assert tiingo["symbols_written"] == "2"
    assert tiingo["symbols_skipped"] == "1"
    assert tiingo["total_rebased_rows"] == "19"
    assert tiingo["final_role"] == "preferred_pre_massive_backfill"


def test_creates_markdown_report(tmp_path: Path) -> None:
    result = run_basic_comparison(tmp_path)

    report = result.markdown_report_path.read_text(encoding="utf-8")
    assert "## Executive summary" in report
    assert "Prefer Tiingo for the one-time pre-Massive historical ETF backfill." in report
    assert "Do not promote to S3" in report


def test_handles_missing_optional_summaries_without_crashing(tmp_path: Path) -> None:
    result = run_basic_comparison(tmp_path)

    report = result.markdown_report_path.read_text(encoding="utf-8")
    assert "Optional summaries loaded: None present." in report
