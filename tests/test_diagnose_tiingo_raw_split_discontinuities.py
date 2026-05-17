from __future__ import annotations
# ruff: noqa: E402,I001

import csv
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from scripts import diagnose_tiingo_raw_split_discontinuities as diagnostics  # noqa: E402


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


def config_for(
    tmp_path: Path,
    *,
    symbols: list[str] | None = None,
) -> diagnostics.TiingoRawSplitDiscontinuityConfig:
    return diagnostics.TiingoRawSplitDiscontinuityConfig(
        tiingo_raw_dir=tmp_path / "raw" / "tiingo",
        symbols_file=tmp_path / "symbols.txt",
        symbols=symbols or ["SPY"],
        output_dir=tmp_path / "out",
        massive_boundary_report=tmp_path / "boundary.csv",
    )


def tiingo_row(
    row_date: str,
    *,
    raw_close: object,
    adj_close: object = 100,
    split_factor: object = 1,
    div_cash: object = 0,
) -> dict[str, object]:
    return {
        "date": row_date,
        "close": adj_close,
        "raw_close": raw_close,
        "adj_close": adj_close,
        "split_factor": split_factor,
        "div_cash": div_cash,
    }


def write_tiingo_rows(
    tmp_path: Path,
    symbol: str,
    rows: list[dict[str, object]],
    fieldnames: list[str] | None = None,
) -> None:
    write_csv(tmp_path / "raw" / "tiingo" / f"{symbol}.csv", rows, fieldnames=fieldnames)


def event_types(result: diagnostics.TiingoRawSplitDiscontinuityResult) -> list[str]:
    return [str(row["event_type"]) for row in result.event_rows]


def test_detects_split_factor_not_equal_to_one(tmp_path: Path) -> None:
    write_tiingo_rows(
        tmp_path,
        "SPY",
        [
            tiingo_row("2020-01-02", raw_close=100, adj_close=50),
            tiingo_row("2020-01-03", raw_close=50, adj_close=50, split_factor=2),
        ],
    )

    result = diagnostics.run_diagnostics(config_for(tmp_path))

    assert "split_factor" in event_types(result)
    row = result.symbol_summary_rows[0]
    assert row["split_factor_event_count"] == 1
    assert row["raw_compatible_usage_recommendation"] == "build_split_adjusted_from_raw"


def test_detects_large_raw_return(tmp_path: Path) -> None:
    write_tiingo_rows(
        tmp_path,
        "SPY",
        [
            tiingo_row("2020-01-02", raw_close=100, adj_close=100),
            tiingo_row("2020-01-03", raw_close=125, adj_close=125),
        ],
    )

    result = diagnostics.run_diagnostics(config_for(tmp_path))

    assert event_types(result) == ["large_raw_return"]
    assert result.event_rows[0]["severity"] == "warning"
    assert result.symbol_summary_rows[0]["large_raw_return_count"] == 1
    assert result.symbol_summary_rows[0]["severe_raw_return_count"] == 0


def test_detects_severe_raw_return(tmp_path: Path) -> None:
    write_tiingo_rows(
        tmp_path,
        "SPY",
        [
            tiingo_row("2020-01-02", raw_close=100, adj_close=100),
            tiingo_row("2020-01-03", raw_close=50, adj_close=50),
        ],
    )

    result = diagnostics.run_diagnostics(config_for(tmp_path))

    assert event_types(result) == ["severe_raw_return"]
    assert result.event_rows[0]["severity"] == "severe"
    assert result.symbol_summary_rows[0]["large_raw_return_count"] == 1
    assert result.symbol_summary_rows[0]["severe_raw_return_count"] == 1


def test_detects_raw_adjusted_ratio_jump(tmp_path: Path) -> None:
    write_tiingo_rows(
        tmp_path,
        "SPY",
        [
            tiingo_row("2020-01-02", raw_close=100, adj_close=100),
            tiingo_row("2020-01-03", raw_close=101, adj_close=50.5),
            tiingo_row("2020-01-04", raw_close=102, adj_close=51),
        ],
    )

    result = diagnostics.run_diagnostics(config_for(tmp_path))

    assert "raw_adj_ratio_jump" in event_types(result)
    row = result.symbol_summary_rows[0]
    assert row["raw_adj_ratio_jump_count"] == 1
    assert row["raw_compatible_usage_recommendation"] == "build_split_adjusted_from_raw"


def test_summary_marks_build_split_adjusted_from_raw_when_split_events_exist(
    tmp_path: Path,
) -> None:
    write_tiingo_rows(
        tmp_path,
        "SPY",
        [
            tiingo_row("2020-01-02", raw_close=100, adj_close=50),
            tiingo_row("2020-01-03", raw_close=100, adj_close=100, split_factor=0.5),
        ],
    )

    result = diagnostics.run_diagnostics(config_for(tmp_path))

    row = result.symbol_summary_rows[0]
    assert row["likely_split_adjustment_needed"] is True
    assert row["raw_compatible_usage_recommendation"] == "build_split_adjusted_from_raw"
    assert "split_factor values" in row["reason"]


def test_no_false_positive_for_clean_smooth_data(tmp_path: Path) -> None:
    write_tiingo_rows(
        tmp_path,
        "SPY",
        [
            tiingo_row("2020-01-02", raw_close=100, adj_close=90),
            tiingo_row("2020-01-03", raw_close=101, adj_close=90.9),
            tiingo_row("2020-01-04", raw_close=102, adj_close=91.8),
        ],
    )

    result = diagnostics.run_diagnostics(config_for(tmp_path))

    assert result.event_rows == []
    row = result.symbol_summary_rows[0]
    assert row["raw_compatible_usage_recommendation"] == "use_raw_compatible_as_is"
    assert row["likely_split_adjustment_needed"] is False


def test_handles_missing_adj_close_gracefully(tmp_path: Path) -> None:
    write_tiingo_rows(
        tmp_path,
        "SPY",
        [
            {"date": "2020-01-02", "close": 100, "raw_close": 100, "split_factor": 1},
            {"date": "2020-01-03", "close": 101, "raw_close": 101, "split_factor": 1},
        ],
        fieldnames=["date", "close", "raw_close", "split_factor"],
    )

    result = diagnostics.run_diagnostics(config_for(tmp_path))

    assert result.event_rows == []
    row = result.symbol_summary_rows[0]
    assert row["raw_adj_ratio_start"] is None
    assert row["raw_adj_ratio_end"] is None
    assert row["raw_compatible_usage_recommendation"] == "use_raw_compatible_as_is"


def test_handles_missing_split_factor_gracefully(tmp_path: Path) -> None:
    write_tiingo_rows(
        tmp_path,
        "SPY",
        [
            {"date": "2020-01-02", "close": 100, "raw_close": 100, "adj_close": 90},
            {"date": "2020-01-03", "close": 101, "raw_close": 101, "adj_close": 90.9},
        ],
        fieldnames=["date", "close", "raw_close", "adj_close"],
    )

    result = diagnostics.run_diagnostics(config_for(tmp_path))

    assert result.event_rows == []
    assert result.symbol_summary_rows[0]["split_factor_event_count"] == 0


def test_markdown_report_generated(tmp_path: Path) -> None:
    write_tiingo_rows(
        tmp_path,
        "SPY",
        [
            tiingo_row("2020-01-02", raw_close=100, adj_close=100),
            tiingo_row("2020-01-03", raw_close=101, adj_close=101),
        ],
    )

    result = diagnostics.run_diagnostics(config_for(tmp_path))

    assert result.markdown_report_path.exists()
    report = result.markdown_report_path.read_text(encoding="utf-8")
    assert "# Tiingo Raw Split Discontinuity Diagnostics" in report
    assert "No Tiingo API call happened during this run." in report
    assert "No S3 write or promotion happened during this run." in report
    assert read_csv_rows(result.symbol_summary_path)[0]["symbol"] == "SPY"
    assert read_csv_rows(result.event_report_path) == []
