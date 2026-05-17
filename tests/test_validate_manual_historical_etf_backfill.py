from __future__ import annotations
# ruff: noqa: E402,I001

import csv
import sys
from argparse import Namespace
from pathlib import Path

import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src"))

from scripts import validate_manual_historical_etf_backfill as validator  # noqa: E402


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


def write_symbols(path: Path, symbols: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(symbols) + "\n", encoding="utf-8")


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with open(path, "r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def args_for(
    tmp_path: Path,
    *,
    raw_dir: Path,
    massive_curated_dir: Path,
    output_dir: Path,
    symbols_file: Path,
    dry_run: bool = False,
    boundary_window_days: str = "2",
) -> Namespace:
    return Namespace(
        raw_dir=str(raw_dir),
        source_provider="stooq",
        massive_curated_dir=str(massive_curated_dir),
        output_dir=str(output_dir),
        symbols_file=str(symbols_file),
        backfill_end_date="2017-12-31",
        overlap_start_date="2018-01-01",
        overlap_end_date="2018-01-03",
        boundary_window_days=boundary_window_days,
        start_date=None,
        dry_run=dry_run,
    )


def sample_raw_rows() -> list[dict[str, object]]:
    return [
        {"Date": "2017-12-29", "Open": 100, "High": 103, "Low": 99, "Close": 102, "Volume": 10},
        {"Date": "2018-01-02", "Open": 102, "High": 105, "Low": 101, "Close": 104, "Volume": 11},
        {"Date": "2018-01-03", "Open": 104, "High": 106, "Low": 103, "Close": 105, "Volume": 12},
    ]


def write_massive_sample(root: Path, symbol: str, rows: list[dict[str, object]]) -> Path:
    table = validator.rows_to_curated_table(rows)
    year = rows[0]["year"]
    path = root / f"symbol={symbol}" / f"year={year}" / "data.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)
    return path


def test_csv_normalization_accepts_common_title_case_columns(tmp_path: Path) -> None:
    csv_path = tmp_path / "SPY.csv"
    write_csv(csv_path, sample_raw_rows())

    result = validator.normalize_csv_file(csv_path, symbol="spy")
    rows = result.table.to_pylist()

    assert result.quality_status == "pass"
    assert result.table.schema == validator.CURATED_ETF_SCHEMA
    assert rows[0] == {
        "date": "2017-12-29",
        "year": 2017,
        "month": 12,
        "symbol": "SPY",
        "open": 100.0,
        "high": 103.0,
        "low": 99.0,
        "close": 102.0,
        "volume": 10.0,
        "vwap": None,
        "transactions": None,
    }


def test_event_rows_are_quarantined_and_excluded_from_price_outputs(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw" / "stooq"
    massive_dir = tmp_path / "massive"
    output_dir = tmp_path / "out"
    symbols_file = tmp_path / "symbols.txt"
    write_symbols(symbols_file, ["SPY"])
    write_csv(
        raw_dir / "SPY.csv",
        [
            {
                "Date": "2017-12-29",
                "Open": 100,
                "High": 103,
                "Low": 99,
                "Close": 102,
                "Volume": 10,
                "Action": "",
                "Amount": "",
            },
            {
                "Date": "2017-12-30",
                "Open": "",
                "High": "",
                "Low": "",
                "Close": "",
                "Volume": "",
                "Action": "Dividend",
                "Amount": "0.24",
            },
            {
                "Date": "2017-12-31",
                "Open": "",
                "High": "",
                "Low": "",
                "Close": "",
                "Volume": "",
                "Action": "Split",
                "Amount": "2:1",
            },
            {
                "Date": "2018-01-02",
                "Open": 102,
                "High": 105,
                "Low": 101,
                "Close": 104,
                "Volume": 11,
                "Action": "",
                "Amount": "",
            },
        ],
        fieldnames=["Date", "Open", "High", "Low", "Close", "Volume", "Action", "Amount"],
    )

    result = validator.run_validation(
        args_for(
            tmp_path,
            raw_dir=raw_dir,
            massive_curated_dir=massive_dir,
            output_dir=output_dir,
            symbols_file=symbols_file,
        )
    )

    event_path = output_dir / "events" / "source=stooq" / "SPY_events.csv"
    assert event_path.exists()
    event_rows = read_csv_rows(event_path)
    assert [row["Date"] for row in event_rows] == ["2017-12-30", "2017-12-31"]
    assert event_rows[0]["_event_types_detected"] == "action;dividend"
    assert event_rows[1]["_event_types_detected"] == "action;split"

    normalized_rows = pq.ParquetFile(
        output_dir / "normalized" / "stooq" / "SPY.parquet"
    ).read().to_pylist()
    assert [row["date"] for row in normalized_rows] == ["2017-12-29", "2018-01-02"]

    export_path = (
        output_dir
        / "curated_export"
        / "source=stooq"
        / "etf_daily_prices"
        / "symbol=SPY"
        / "year=2017"
        / "data.parquet"
    )
    exported_rows = pq.ParquetFile(export_path).read().to_pylist()
    assert [row["date"] for row in exported_rows] == ["2017-12-29"]

    event_report_rows = read_csv_rows(
        output_dir / "validation" / "event_rows_report.csv"
    )
    assert event_report_rows[0]["raw_row_count"] == "4"
    assert event_report_rows[0]["price_row_count"] == "2"
    assert event_report_rows[0]["event_row_count"] == "2"
    assert event_report_rows[0]["event_output_path"] == str(event_path)
    assert event_report_rows[0]["event_types_detected"] == "action;dividend;split"
    assert result.event_rows[0]["event_row_count"] == 2

    quality_rows = read_csv_rows(
        output_dir / "validation" / "symbol_quality_report.csv"
    )
    assert quality_rows[0]["event_row_count"] == "2"
    assert "quarantined" in quality_rows[0]["event_warning"]


def test_duplicate_symbol_date_rows_are_removed_deterministically(tmp_path: Path) -> None:
    csv_path = tmp_path / "SPY.csv"
    write_csv(
        csv_path,
        [
            {"date": "2017-01-03", "open": 11, "high": 12, "low": 10, "close": 11, "volume": 2},
            {"date": "2017-01-03", "open": 99, "high": 100, "low": 98, "close": 99, "volume": 3},
            {"date": "2017-01-04", "open": 12, "high": 13, "low": 11, "close": 12, "volume": 4},
        ],
    )

    result = validator.normalize_csv_file(csv_path, symbol="SPY")
    rows = result.table.to_pylist()

    assert result.duplicate_rows_removed == 1
    assert len(rows) == 2
    assert rows[0]["date"] == "2017-01-03"
    assert rows[0]["open"] == 11.0
    assert result.quality_status == "warning"


def test_ohlc_quality_failures_are_counted(tmp_path: Path) -> None:
    csv_path = tmp_path / "SPY.csv"
    write_csv(
        csv_path,
        [
            {"date": "2017-01-03", "open": 10, "high": 9, "low": 8, "close": 10, "volume": 2},
            {"date": "2017-01-04", "open": 10, "high": 11, "low": 9, "close": -1, "volume": 2},
            {"date": "2017-01-05", "open": 10, "high": 11, "low": 9, "close": 10, "volume": -2},
        ],
    )

    result = validator.normalize_csv_file(csv_path, symbol="SPY")

    assert result.ohlc_bad_rows == 3
    assert result.quality_status == "fail"
    assert "OHLC sanity failures: 3" in result.issues


def test_pre_massive_export_rule_and_partition_output(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw" / "stooq"
    massive_dir = tmp_path / "massive"
    output_dir = tmp_path / "out"
    symbols_file = tmp_path / "symbols.txt"
    write_symbols(symbols_file, ["SPY"])
    write_csv(raw_dir / "SPY.csv", sample_raw_rows())
    write_massive_sample(
        massive_dir,
        "SPY",
        [
            {
                "date": "2018-01-02",
                "year": 2018,
                "month": 1,
                "symbol": "SPY",
                "open": 102.0,
                "high": 105.0,
                "low": 101.0,
                "close": 104.0,
                "volume": 11.0,
                "vwap": None,
                "transactions": None,
            },
            {
                "date": "2018-01-03",
                "year": 2018,
                "month": 1,
                "symbol": "SPY",
                "open": 104.0,
                "high": 106.0,
                "low": 103.0,
                "close": 105.0,
                "volume": 12.0,
                "vwap": None,
                "transactions": None,
            },
        ],
    )

    result = validator.run_validation(
        args_for(
            tmp_path,
            raw_dir=raw_dir,
            massive_curated_dir=massive_dir,
            output_dir=output_dir,
            symbols_file=symbols_file,
        )
    )

    export_path = (
        output_dir
        / "curated_export"
        / "source=stooq"
        / "etf_daily_prices"
        / "symbol=SPY"
        / "year=2017"
        / "data.parquet"
    )
    assert export_path.exists()
    exported_rows = pq.ParquetFile(export_path).read().to_pylist()
    assert [row["date"] for row in exported_rows] == ["2017-12-29"]
    assert not (
        output_dir
        / "curated_export"
        / "source=stooq"
        / "etf_daily_prices"
        / "symbol=SPY"
        / "year=2018"
        / "data.parquet"
    ).exists()
    assert result.coverage_rows[0]["backfill_row_count"] == 1
    assert result.coverage_rows[0]["exported_years"] == "2017"
    event_report_rows = read_csv_rows(
        output_dir / "validation" / "event_rows_report.csv"
    )
    assert event_report_rows[0]["event_row_count"] == "0"
    assert event_report_rows[0]["event_output_path"] == ""
    quality_rows = read_csv_rows(
        output_dir / "validation" / "symbol_quality_report.csv"
    )
    assert quality_rows[0]["event_row_count"] == "0"
    assert quality_rows[0]["event_warning"] == ""


def test_overlap_comparison_against_synthetic_massive_curated_sample(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw" / "stooq"
    massive_dir = tmp_path / "massive"
    output_dir = tmp_path / "out"
    symbols_file = tmp_path / "symbols.txt"
    write_symbols(symbols_file, ["SPY"])
    write_csv(raw_dir / "SPY.csv", sample_raw_rows())
    write_massive_sample(
        massive_dir,
        "SPY",
        [
            {
                "date": "2018-01-02",
                "year": 2018,
                "month": 1,
                "symbol": "SPY",
                "open": 102.0,
                "high": 105.0,
                "low": 101.0,
                "close": 104.0,
                "volume": 11.0,
                "vwap": None,
                "transactions": None,
            },
            {
                "date": "2018-01-03",
                "year": 2018,
                "month": 1,
                "symbol": "SPY",
                "open": 104.0,
                "high": 106.0,
                "low": 103.0,
                "close": 105.0,
                "volume": 12.0,
                "vwap": None,
                "transactions": None,
            },
        ],
    )

    result = validator.run_validation(
        args_for(
            tmp_path,
            raw_dir=raw_dir,
            massive_curated_dir=massive_dir,
            output_dir=output_dir,
            symbols_file=symbols_file,
        )
    )

    overlap = result.overlap_rows[0]
    assert overlap["source_rows"] == 2
    assert overlap["massive_rows"] == 2
    assert overlap["joined_rows"] == 2
    assert overlap["close_abs_diff_max"] == 0.0
    assert overlap["close_pct_diff_max"] == 0.0
    assert overlap["return_1d_abs_diff_max"] == 0.0
    assert overlap["overlap_quality_status"] == "pass"
    diagnostics = result.adjustment_diagnostics_rows[0]
    assert diagnostics["return_alignment_status"] == "pass"
    assert diagnostics["price_level_status"] == "pass"
    assert diagnostics["adjustment_diagnostic_status"] == "pass"
    assert diagnostics["suggested_action"] == "use_as_is"
    assert diagnostics["boundary_rebase_factor"] == 1.0
    assert (output_dir / "validation" / "adjustment_diagnostics_report.csv").exists()
    boundary = result.boundary_rebase_diagnostics_rows[0]
    assert boundary["boundary_window_days"] == 2
    assert boundary["boundary_rows"] == 2
    assert boundary["boundary_rebase_factor"] == 1.0
    assert boundary["boundary_return_alignment_status"] == "pass"
    assert boundary["boundary_ratio_stability_status"] == "pass"
    assert boundary["boundary_rebase_status"] == "pass_no_rebase_needed"
    assert boundary["suggested_action"] == "use_as_is"
    assert (output_dir / "validation" / "boundary_rebase_diagnostics_report.csv").exists()
    assert (
        result.manifest["report_paths"]["boundary_rebase_diagnostics"]
        == str(output_dir / "validation" / "boundary_rebase_diagnostics_report.csv")
    )


def test_adjustment_diagnostics_rebase_candidate_for_stable_price_ratio(
    tmp_path: Path,
) -> None:
    raw_dir = tmp_path / "raw" / "stooq"
    massive_dir = tmp_path / "massive"
    output_dir = tmp_path / "out"
    symbols_file = tmp_path / "symbols.txt"
    write_symbols(symbols_file, ["SPY"])
    write_csv(raw_dir / "SPY.csv", sample_raw_rows())
    write_massive_sample(
        massive_dir,
        "SPY",
        [
            {
                "date": "2018-01-02",
                "year": 2018,
                "month": 1,
                "symbol": "SPY",
                "open": 204.0,
                "high": 210.0,
                "low": 202.0,
                "close": 208.0,
                "volume": 11.0,
                "vwap": None,
                "transactions": None,
            },
            {
                "date": "2018-01-03",
                "year": 2018,
                "month": 1,
                "symbol": "SPY",
                "open": 208.0,
                "high": 212.0,
                "low": 206.0,
                "close": 210.0,
                "volume": 12.0,
                "vwap": None,
                "transactions": None,
            },
        ],
    )

    result = validator.run_validation(
        args_for(
            tmp_path,
            raw_dir=raw_dir,
            massive_curated_dir=massive_dir,
            output_dir=output_dir,
            symbols_file=symbols_file,
        )
    )

    diagnostics = result.adjustment_diagnostics_rows[0]
    assert result.overlap_rows[0]["overlap_quality_status"] == "fail"
    assert diagnostics["return_alignment_status"] == "pass"
    assert diagnostics["price_level_status"] == "adjusted_level_mismatch"
    assert diagnostics["adjustment_diagnostic_status"] == "rebase_candidate"
    assert diagnostics["suggested_action"] == "rebase_pre_massive_history_before_promotion"
    assert diagnostics["close_ratio_mean"] == 2.0
    assert diagnostics["close_ratio_cv"] == 0.0
    assert diagnostics["close_ratio_drift_abs"] == 0.0
    assert diagnostics["boundary_rebase_factor"] == 2.0
    boundary = result.boundary_rebase_diagnostics_rows[0]
    assert boundary["boundary_rebase_factor"] == 2.0
    assert boundary["boundary_close_ratio_cv"] == 0.0
    assert boundary["boundary_close_ratio_drift_abs"] == 0.0
    assert boundary["boundary_return_alignment_status"] == "pass"
    assert boundary["boundary_ratio_stability_status"] == "pass"
    assert boundary["boundary_rebase_status"] == "boundary_rebase_candidate"
    assert (
        boundary["suggested_action"]
        == "rebase_pre_massive_history_before_promotion"
    )

    quality_rows = read_csv_rows(
        output_dir / "validation" / "symbol_quality_report.csv"
    )
    assert quality_rows[0]["adjustment_diagnostic_status"] == "rebase_candidate"
    assert (
        quality_rows[0]["suggested_action"]
        == "rebase_pre_massive_history_before_promotion"
    )
    assert quality_rows[0]["boundary_rebase_status"] == "boundary_rebase_candidate"
    assert quality_rows[0]["boundary_rebase_factor"] == "2.0"
    assert (
        quality_rows[0]["boundary_suggested_action"]
        == "rebase_pre_massive_history_before_promotion"
    )


def test_boundary_rebase_candidate_when_stable_factor_is_not_near_one(
    tmp_path: Path,
) -> None:
    raw_dir = tmp_path / "raw" / "stooq"
    massive_dir = tmp_path / "massive"
    output_dir = tmp_path / "out"
    symbols_file = tmp_path / "symbols.txt"
    write_symbols(symbols_file, ["SPY"])
    write_csv(raw_dir / "SPY.csv", sample_raw_rows())
    write_massive_sample(
        massive_dir,
        "SPY",
        [
            {
                "date": "2018-01-02",
                "year": 2018,
                "month": 1,
                "symbol": "SPY",
                "open": 103.02,
                "high": 106.05,
                "low": 102.01,
                "close": 105.04,
                "volume": 11.0,
                "vwap": None,
                "transactions": None,
            },
            {
                "date": "2018-01-03",
                "year": 2018,
                "month": 1,
                "symbol": "SPY",
                "open": 105.04,
                "high": 107.06,
                "low": 104.03,
                "close": 106.05,
                "volume": 12.0,
                "vwap": None,
                "transactions": None,
            },
        ],
    )

    result = validator.run_validation(
        args_for(
            tmp_path,
            raw_dir=raw_dir,
            massive_curated_dir=massive_dir,
            output_dir=output_dir,
            symbols_file=symbols_file,
        )
    )

    diagnostics = result.adjustment_diagnostics_rows[0]
    boundary = result.boundary_rebase_diagnostics_rows[0]
    assert result.overlap_rows[0]["overlap_quality_status"] == "pass"
    assert diagnostics["adjustment_diagnostic_status"] == "pass"
    assert boundary["boundary_rebase_factor"] == 1.01
    assert boundary["boundary_return_alignment_status"] == "pass"
    assert boundary["boundary_ratio_stability_status"] == "pass"
    assert boundary["boundary_rebase_status"] == "boundary_rebase_candidate"
    assert (
        boundary["suggested_action"]
        == "rebase_pre_massive_history_before_promotion"
    )


def test_adjustment_diagnostics_fail_when_returns_do_not_align(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw" / "stooq"
    massive_dir = tmp_path / "massive"
    output_dir = tmp_path / "out"
    symbols_file = tmp_path / "symbols.txt"
    write_symbols(symbols_file, ["SPY"])
    write_csv(raw_dir / "SPY.csv", sample_raw_rows())
    write_massive_sample(
        massive_dir,
        "SPY",
        [
            {
                "date": "2018-01-02",
                "year": 2018,
                "month": 1,
                "symbol": "SPY",
                "open": 102.0,
                "high": 105.0,
                "low": 101.0,
                "close": 104.0,
                "volume": 11.0,
                "vwap": None,
                "transactions": None,
            },
            {
                "date": "2018-01-03",
                "year": 2018,
                "month": 1,
                "symbol": "SPY",
                "open": 118.0,
                "high": 121.0,
                "low": 117.0,
                "close": 120.0,
                "volume": 12.0,
                "vwap": None,
                "transactions": None,
            },
        ],
    )

    result = validator.run_validation(
        args_for(
            tmp_path,
            raw_dir=raw_dir,
            massive_curated_dir=massive_dir,
            output_dir=output_dir,
            symbols_file=symbols_file,
        )
    )

    diagnostics = result.adjustment_diagnostics_rows[0]
    assert diagnostics["return_alignment_status"] == "fail"
    assert diagnostics["adjustment_diagnostic_status"] == "fail"
    assert diagnostics["suggested_action"] == "inspect_source_quality"
    boundary = result.boundary_rebase_diagnostics_rows[0]
    assert boundary["boundary_return_alignment_status"] == "fail"
    assert boundary["boundary_rebase_status"] == "fail"
    assert boundary["suggested_action"] == "inspect_source_quality"


def test_missing_massive_overlap_sample_warns_without_crashing(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw" / "stooq"
    massive_dir = tmp_path / "massive"
    output_dir = tmp_path / "out"
    symbols_file = tmp_path / "symbols.txt"
    write_symbols(symbols_file, ["SPY"])
    write_csv(raw_dir / "SPY.csv", sample_raw_rows())

    result = validator.run_validation(
        args_for(
            tmp_path,
            raw_dir=raw_dir,
            massive_curated_dir=massive_dir,
            output_dir=output_dir,
            symbols_file=symbols_file,
            dry_run=True,
        )
    )

    assert result.overlap_rows[0]["overlap_quality_status"] == "warning"
    assert "missing local Massive curated sample for SPY" in result.overlap_rows[0]["warning"]
    assert result.adjustment_diagnostics_rows[0]["adjustment_diagnostic_status"] == "warning"
    assert result.adjustment_diagnostics_rows[0]["suggested_action"] == "missing_massive_sample"
    assert result.boundary_rebase_diagnostics_rows[0]["boundary_rebase_status"] == "warning"
    assert result.boundary_rebase_diagnostics_rows[0]["suggested_action"] == "missing_massive_sample"
    assert result.manifest["warning_count"] >= 1
    assert (output_dir / "validation" / "overlap_report.csv").exists()
    assert (output_dir / "validation" / "adjustment_diagnostics_report.csv").exists()
    assert (output_dir / "validation" / "boundary_rebase_diagnostics_report.csv").exists()
    assert not (output_dir / "normalized" / "stooq" / "SPY.parquet").exists()


def test_boundary_diagnostics_warn_when_boundary_window_is_insufficient(
    tmp_path: Path,
) -> None:
    raw_dir = tmp_path / "raw" / "stooq"
    massive_dir = tmp_path / "massive"
    output_dir = tmp_path / "out"
    symbols_file = tmp_path / "symbols.txt"
    write_symbols(symbols_file, ["SPY"])
    write_csv(raw_dir / "SPY.csv", sample_raw_rows())
    write_massive_sample(
        massive_dir,
        "SPY",
        [
            {
                "date": "2018-01-02",
                "year": 2018,
                "month": 1,
                "symbol": "SPY",
                "open": 102.0,
                "high": 105.0,
                "low": 101.0,
                "close": 104.0,
                "volume": 11.0,
                "vwap": None,
                "transactions": None,
            },
            {
                "date": "2018-01-03",
                "year": 2018,
                "month": 1,
                "symbol": "SPY",
                "open": 104.0,
                "high": 106.0,
                "low": 103.0,
                "close": 105.0,
                "volume": 12.0,
                "vwap": None,
                "transactions": None,
            },
        ],
    )

    result = validator.run_validation(
        args_for(
            tmp_path,
            raw_dir=raw_dir,
            massive_curated_dir=massive_dir,
            output_dir=output_dir,
            symbols_file=symbols_file,
            boundary_window_days="3",
        )
    )

    boundary = result.boundary_rebase_diagnostics_rows[0]
    assert boundary["boundary_window_days"] == 3
    assert boundary["boundary_rows"] == 2
    assert boundary["boundary_rebase_status"] == "warning"
    assert boundary["suggested_action"] == "insufficient_boundary_rows"
    assert "insufficient boundary rows" in boundary["warning"]


def test_boundary_window_days_parsing() -> None:
    assert validator.parse_boundary_window_days("20,60, 120") == [20, 60, 120]
    assert validator.parse_boundary_window_days(None) == [20, 60, 120]


def test_summary_markdown_includes_adjustment_diagnostics_section(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw" / "stooq"
    massive_dir = tmp_path / "massive"
    output_dir = tmp_path / "out"
    symbols_file = tmp_path / "symbols.txt"
    write_symbols(symbols_file, ["GLD", "SPY"])
    write_csv(raw_dir / "GLD.csv", sample_raw_rows())
    write_csv(raw_dir / "SPY.csv", sample_raw_rows())
    write_massive_sample(
        massive_dir,
        "GLD",
        [
            {
                "date": "2018-01-02",
                "year": 2018,
                "month": 1,
                "symbol": "GLD",
                "open": 102.0,
                "high": 105.0,
                "low": 101.0,
                "close": 104.0,
                "volume": 11.0,
                "vwap": None,
                "transactions": None,
            },
            {
                "date": "2018-01-03",
                "year": 2018,
                "month": 1,
                "symbol": "GLD",
                "open": 104.0,
                "high": 106.0,
                "low": 103.0,
                "close": 105.0,
                "volume": 12.0,
                "vwap": None,
                "transactions": None,
            },
        ],
    )
    write_massive_sample(
        massive_dir,
        "SPY",
        [
            {
                "date": "2018-01-02",
                "year": 2018,
                "month": 1,
                "symbol": "SPY",
                "open": 204.0,
                "high": 210.0,
                "low": 202.0,
                "close": 208.0,
                "volume": 11.0,
                "vwap": None,
                "transactions": None,
            },
            {
                "date": "2018-01-03",
                "year": 2018,
                "month": 1,
                "symbol": "SPY",
                "open": 208.0,
                "high": 212.0,
                "low": 206.0,
                "close": 210.0,
                "volume": 12.0,
                "vwap": None,
                "transactions": None,
            },
        ],
    )

    validator.run_validation(
        args_for(
            tmp_path,
            raw_dir=raw_dir,
            massive_curated_dir=massive_dir,
            output_dir=output_dir,
            symbols_file=symbols_file,
        )
    )

    summary = (output_dir / "validation" / "manual_backfill_summary.md").read_text(
        encoding="utf-8"
    )
    assert "## Adjustment Diagnostics Summary" in summary
    assert "rebase_candidate=1" in summary
    assert "Rebase candidates: SPY" in summary
    assert "Large price-level differences with aligned one-day returns" in summary
    assert "No rebasing is performed" in summary
    assert "## Boundary Rebase Diagnostics Summary" in summary
    assert "pass_no_rebase_needed=1" in summary
    assert "boundary_rebase_candidate=1" in summary
    assert "Boundary rebase candidates: SPY" in summary
    assert "No rebasing was performed" in summary
    assert "A later task can create a rebased pre-Massive export only for accepted symbols" in summary
