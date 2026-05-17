from __future__ import annotations

import csv
import sys
from pathlib import Path

import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src"))

from scripts import create_manual_backfill_price_convention_diagnostics as diagnostics  # noqa: E402
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


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with open(path, "r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def write_tiingo(
    root: Path,
    symbol: str,
    *,
    raw_closes: list[float],
    adjusted_closes: list[float],
) -> None:
    rows = []
    for index, (raw_close, adjusted_close) in enumerate(zip(raw_closes, adjusted_closes)):
        rows.append(
            {
                "date": f"2021-01-{4 + index:02d}",
                "close": adjusted_close,
                "raw_close": raw_close,
                "adj_close": adjusted_close,
            }
        )
    write_csv(root / f"{symbol}.csv", rows)


def write_stooq(root: Path, symbol: str, closes: list[float]) -> None:
    rows = [
        {"Date": f"2021-01-{4 + index:02d}", "Close": close}
        for index, close in enumerate(closes)
    ]
    write_csv(root / f"{symbol}.csv", rows)


def curated_row(symbol: str, date: str, close: float) -> dict[str, object]:
    year, month, _ = date.split("-")
    return {
        "date": date,
        "year": int(year),
        "month": int(month),
        "symbol": symbol,
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "volume": 1000.0,
        "vwap": None,
        "transactions": None,
    }


def write_massive(root: Path, symbol: str, closes: list[float]) -> None:
    rows = [
        curated_row(symbol, f"2021-01-{4 + index:02d}", close)
        for index, close in enumerate(closes)
    ]
    path = root / f"symbol={symbol}" / "year=2021" / "data.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(validator.rows_to_curated_table(rows), path)


def config_for(tmp_path: Path, symbols: list[str]) -> diagnostics.PriceConventionConfig:
    return diagnostics.PriceConventionConfig(
        tiingo_raw_dir=tmp_path / "tiingo",
        stooq_raw_dir=tmp_path / "stooq",
        massive_curated_dir=tmp_path / "massive",
        symbols_file=tmp_path / "symbols.txt",
        symbols=symbols,
        output_dir=tmp_path / "out",
        start_date="2021-01-01",
        end_date="2021-01-31",
    )


def write_complete_symbol(
    tmp_path: Path,
    symbol: str,
    *,
    massive: list[float],
    tiingo_adjusted: list[float],
    tiingo_raw: list[float],
    stooq: list[float],
) -> None:
    write_massive(tmp_path / "massive", symbol, massive)
    write_tiingo(
        tmp_path / "tiingo",
        symbol,
        raw_closes=tiingo_raw,
        adjusted_closes=tiingo_adjusted,
    )
    write_stooq(tmp_path / "stooq", symbol, stooq)


def test_tiingo_csv_loading_with_adjusted_and_raw_fields(tmp_path: Path) -> None:
    write_csv(
        tmp_path / "SPY.csv",
        [
            {"date": "2021-01-04", "close": 101, "raw_close": 100, "adj_close": 90},
            {"date": "2021-01-05", "close": 102, "raw_close": "", "adj_close": ""},
        ],
    )

    adjusted, raw = diagnostics.load_tiingo_prices(
        tmp_path,
        "SPY",
        start_date="2021-01-01",
        end_date="2021-01-31",
    )

    assert [row["close"] for row in adjusted.rows] == [90.0, 102.0]
    assert [row["close"] for row in raw.rows] == [100.0, 102.0]


def test_stooq_csv_loading_with_mixed_casing(tmp_path: Path) -> None:
    write_csv(
        tmp_path / "SPY.csv",
        [{"Date": "2021-01-04", "Close": 100}, {"Date": "2021-01-05", "Close": 101}],
    )

    series = diagnostics.load_stooq_prices(
        tmp_path,
        "SPY",
        start_date="2021-01-01",
        end_date="2021-01-31",
    )

    assert series.source == "stooq"
    assert [row["close"] for row in series.rows] == [100.0, 101.0]


def test_massive_parquet_loading(tmp_path: Path) -> None:
    write_massive(tmp_path, "SPY", [100, 101])

    series = diagnostics.load_massive_prices(
        tmp_path,
        "SPY",
        start_date="2021-01-01",
        end_date="2021-01-31",
    )

    assert series.source == "massive"
    assert [row["close"] for row in series.rows] == [100.0, 101.0]


def test_pairwise_close_and_return_metrics() -> None:
    left = diagnostics.PriceSeries(
        "massive",
        [{"date": "2021-01-04", "close": 100.0}, {"date": "2021-01-05", "close": 110.0}],
    )
    right = diagnostics.PriceSeries(
        "tiingo_adjusted",
        [{"date": "2021-01-04", "close": 90.0}, {"date": "2021-01-05", "close": 99.0}],
    )

    row = diagnostics.build_pairwise_row(
        symbol="SPY",
        pair="massive_vs_tiingo_adjusted",
        left=left,
        right=right,
        start_date="2021-01-01",
        end_date="2021-01-31",
    )

    assert row["joined_rows"] == 2
    assert row["close_abs_diff_mean"] == 10.5
    assert round(row["close_pct_diff_mean"], 6) == 0.1
    assert round(row["close_ratio_mean"], 6) == 1.111111
    assert row["return_1d_abs_diff_mean"] == 0.0


def test_symbol_summary_chooses_tiingo_raw_when_raw_is_closer_to_massive(
    tmp_path: Path,
) -> None:
    write_complete_symbol(
        tmp_path,
        "SPY",
        massive=[100, 110, 121],
        tiingo_adjusted=[90, 99, 108.9],
        tiingo_raw=[100, 110, 121],
        stooq=[105, 115.5, 127.05],
    )

    result = diagnostics.run_price_convention_diagnostics(config_for(tmp_path, ["SPY"]))
    row = result.symbol_summary_rows[0]

    assert row["closest_to_massive_by_close_pct_mean"] == "tiingo_raw"
    assert (
        row["likely_convention_explanation"]
        == "massive_closer_to_raw_or_split_adjusted_convention"
    )


def test_symbol_summary_chooses_tiingo_adjusted_when_adjusted_is_closer_to_massive(
    tmp_path: Path,
) -> None:
    write_complete_symbol(
        tmp_path,
        "SPY",
        massive=[90, 99, 108.9],
        tiingo_adjusted=[90, 99, 108.9],
        tiingo_raw=[100, 110, 121],
        stooq=[90, 99, 108.9],
    )

    result = diagnostics.run_price_convention_diagnostics(config_for(tmp_path, ["SPY"]))
    row = result.symbol_summary_rows[0]

    assert row["closest_to_massive_by_close_pct_mean"] == "tiingo_adjusted"
    assert row["likely_convention_explanation"] == "massive_closer_to_adjusted_convention"


def test_detects_tiingo_adjusted_vs_stooq_closeness(tmp_path: Path) -> None:
    write_complete_symbol(
        tmp_path,
        "SPY",
        massive=[100, 110, 121],
        tiingo_adjusted=[90, 99, 108.9],
        tiingo_raw=[100, 110, 121],
        stooq=[90, 99, 108.9],
    )

    result = diagnostics.run_price_convention_diagnostics(config_for(tmp_path, ["SPY"]))
    row = result.symbol_summary_rows[0]

    assert row["tiingo_adjusted_vs_stooq_close_pct_mean"] == 0.0
    assert row["tiingo_adjusted_vs_stooq_return_diff_mean"] == 0.0
    assert (
        row["likely_convention_explanation"]
        == "source_adjustment_method_differs_from_massive"
    )


def test_markdown_report_is_created(tmp_path: Path) -> None:
    write_complete_symbol(
        tmp_path,
        "SPY",
        massive=[100, 110],
        tiingo_adjusted=[100, 110],
        tiingo_raw=[100, 110],
        stooq=[100, 110],
    )

    result = diagnostics.run_price_convention_diagnostics(config_for(tmp_path, ["SPY"]))

    assert result.markdown_report_path.exists()
    report = result.markdown_report_path.read_text(encoding="utf-8")
    assert "# Manual Backfill Price Convention Diagnostics" in report
    assert "Massive adjusted=true note" in report


def test_missing_source_file_produces_warning_not_crash(tmp_path: Path) -> None:
    write_massive(tmp_path / "massive", "SPY", [100, 110])
    write_tiingo(
        tmp_path / "tiingo",
        "SPY",
        raw_closes=[100, 110],
        adjusted_closes=[100, 110],
    )

    result = diagnostics.run_price_convention_diagnostics(config_for(tmp_path, ["SPY"]))
    stooq_pair = next(row for row in result.pairwise_rows if row["pair"] == "massive_vs_stooq")

    assert stooq_pair["status"] == "warning"
    assert "missing Stooq raw CSV" in stooq_pair["warning"]
    assert read_csv_rows(result.pairwise_report_path)
