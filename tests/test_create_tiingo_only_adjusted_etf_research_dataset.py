from __future__ import annotations

# ruff: noqa: E402,I001

import csv
import sys
from argparse import Namespace
from pathlib import Path

import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from scripts import create_tiingo_only_adjusted_etf_research_dataset as materializer  # noqa: E402


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


def read_parquet_rows(path: Path) -> list[dict[str, object]]:
    return pq.ParquetFile(path).read().to_pylist()


def partition_path(root: Path, symbol: str, year: int) -> Path:
    return root / f"symbol={symbol}" / f"year={year}" / "data.parquet"


def config_for(
    tmp_path: Path,
    *,
    symbols: list[str] | None = None,
    overwrite: bool = False,
    dry_run: bool = False,
) -> materializer.TiingoOnlyAdjustedConfig:
    return materializer.TiingoOnlyAdjustedConfig(
        input_dir=tmp_path / "raw" / "tiingo",
        output_dir=tmp_path / "research" / "etf_daily_prices_tiingo_only",
        symbols_file=tmp_path / "symbols.txt",
        symbols=symbols or ["SPY"],
        start_date="1993-01-01",
        end_date="2026-05-17",
        overwrite=overwrite,
        dry_run=dry_run,
        progress_callback=None,
    )


def sample_tiingo_row(
    row_date: str,
    *,
    symbol: str = "SPY",
    open_price: object = 999,
    high: object = 999,
    low: object = 999,
    close: object = 999,
    volume: object = 999,
    raw_open: object = 100,
    raw_high: object = 110,
    raw_low: object = 90,
    raw_close: object = 105,
    raw_volume: object = 1000,
    adj_open: object = 80,
    adj_high: object = 88,
    adj_low: object = 72,
    adj_close: object = 84,
    adj_volume: object = 1250,
    div_cash: object = 0.12,
    split_factor: object = 1,
) -> dict[str, object]:
    return {
        "date": row_date,
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "source_provider": "tiingo",
        "source_symbol": symbol,
        "price_mode": "adjusted",
        "raw_open": raw_open,
        "raw_high": raw_high,
        "raw_low": raw_low,
        "raw_close": raw_close,
        "raw_volume": raw_volume,
        "adj_open": adj_open,
        "adj_high": adj_high,
        "adj_low": adj_low,
        "adj_close": adj_close,
        "adj_volume": adj_volume,
        "div_cash": div_cash,
        "split_factor": split_factor,
    }


def test_writes_adjusted_ohlcv_as_effective_ml_columns(tmp_path: Path) -> None:
    write_csv(
        tmp_path / "raw" / "tiingo" / "SPY.csv",
        [sample_tiingo_row("2020-01-02")],
    )

    result = materializer.run_materialization(config_for(tmp_path))

    output_dir = tmp_path / "research" / "etf_daily_prices_tiingo_only"
    path = partition_path(output_dir, "SPY", 2020)
    rows = read_parquet_rows(path)
    assert result.manifest_rows[0]["status"] == "written"
    assert rows[0]["open"] == 80.0
    assert rows[0]["high"] == 88.0
    assert rows[0]["low"] == 72.0
    assert rows[0]["close"] == 84.0
    assert rows[0]["volume"] == 1250.0
    assert rows[0]["source_provider"] == "tiingo"
    assert rows[0]["price_mode"] == "tiingo_adjusted"


def test_creates_symbol_year_partition_layout_and_required_columns(tmp_path: Path) -> None:
    write_csv(
        tmp_path / "raw" / "tiingo" / "SPY.csv",
        [
            sample_tiingo_row("2019-12-31", adj_open=81, adj_close=85),
            sample_tiingo_row("2020-01-02", adj_open=82, adj_close=86),
        ],
    )

    materializer.run_materialization(config_for(tmp_path))

    output_dir = tmp_path / "research" / "etf_daily_prices_tiingo_only"
    paths = sorted(output_dir.glob("symbol=SPY/year=*/data.parquet"))
    assert [path.parent.name for path in paths] == ["year=2019", "year=2020"]
    rows = [row for path in paths for row in read_parquet_rows(path)]
    required = {
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
    }
    assert required <= set(rows[0])
    assert rows[0]["symbol"] == "SPY"
    assert rows[0]["year"] == 2019
    assert rows[0]["month"] == 12
    assert rows[0]["vwap"] is None
    assert rows[0]["transactions"] is None


def test_preserves_source_debug_columns(tmp_path: Path) -> None:
    write_csv(
        tmp_path / "raw" / "tiingo" / "SPY.csv",
        [sample_tiingo_row("2020-01-02")],
    )

    materializer.run_materialization(config_for(tmp_path))

    output_dir = tmp_path / "research" / "etf_daily_prices_tiingo_only"
    rows = read_parquet_rows(
        partition_path(output_dir, "SPY", 2020)
    )
    assert rows[0]["raw_open"] == 100.0
    assert rows[0]["raw_close"] == 105.0
    assert rows[0]["raw_volume"] == 1000.0
    assert rows[0]["adj_open"] == 80.0
    assert rows[0]["adj_close"] == 84.0
    assert rows[0]["adj_volume"] == 1250.0
    assert rows[0]["div_cash"] == 0.12
    assert rows[0]["split_factor"] == 1.0


def test_writes_manifest_and_summary_reports(tmp_path: Path) -> None:
    write_csv(
        tmp_path / "raw" / "tiingo" / "SPY.csv",
        [sample_tiingo_row("2020-01-02")],
    )

    result = materializer.run_materialization(config_for(tmp_path))

    manifest_rows = read_csv_rows(result.manifest_path)
    summary = result.summary_path.read_text(encoding="utf-8")
    assert result.manifest_path == (
        tmp_path
        / "research"
        / "etf_daily_prices_tiingo_only"
        / "_reports"
        / "tiingo_only_adjusted_manifest.csv"
    )
    assert result.summary_path.exists()
    assert manifest_rows[0]["symbol"] == "SPY"
    assert manifest_rows[0]["status"] == "written"
    assert manifest_rows[0]["row_count"] == "1"
    assert manifest_rows[0]["first_date"] == "2020-01-02"
    assert manifest_rows[0]["last_date"] == "2020-01-02"
    assert manifest_rows[0]["price_mode"] == "tiingo_adjusted"
    assert "Source provider: tiingo" in summary
    assert "Price mode: tiingo_adjusted" in summary
    assert "local-only and was not promoted to S3" in summary
    assert "Massive remains the app/platform data source" in summary


def test_missing_input_file_is_reported_as_skipped_with_download_hint(tmp_path: Path) -> None:
    config = config_for(tmp_path, symbols=["SPY", "QQQ"])
    config.input_dir.mkdir(parents=True)
    write_csv(config.input_dir / "SPY.csv", [sample_tiingo_row("2020-01-02")])

    result = materializer.run_materialization(config)

    rows_by_symbol = {row["symbol"]: row for row in result.manifest_rows}
    assert rows_by_symbol["SPY"]["status"] == "written"
    assert rows_by_symbol["QQQ"]["status"] == "skipped_missing_input"
    assert "download_tiingo_historical_etf_data.py" in rows_by_symbol["QQQ"]["warning"]
    manifest_rows = read_csv_rows(result.manifest_path)
    assert manifest_rows[1]["status"] == "skipped_missing_input"


def test_uses_effective_columns_as_fallback_when_adjusted_fields_are_missing(
    tmp_path: Path,
) -> None:
    write_csv(
        tmp_path / "raw" / "tiingo" / "SPY.csv",
        [
            {
                "date": "2020-01-02",
                "open": 10,
                "high": 11,
                "low": 9,
                "close": 10.5,
                "volume": 100,
            }
        ],
    )

    result = materializer.run_materialization(config_for(tmp_path))

    output_dir = tmp_path / "research" / "etf_daily_prices_tiingo_only"
    rows = read_parquet_rows(
        partition_path(output_dir, "SPY", 2020)
    )
    assert rows[0]["open"] == 10.0
    assert rows[0]["volume"] == 100.0
    warning = result.manifest_rows[0]["warning"]
    assert "adj_open missing; used effective CSV fallback rows: 1" in warning


def test_materialization_writes_only_under_configured_local_output_dir(tmp_path: Path) -> None:
    config = config_for(tmp_path)
    write_csv(config.input_dir / "SPY.csv", [sample_tiingo_row("2020-01-02")])
    before_files = {path for path in tmp_path.rglob("*") if path.is_file()}

    materializer.run_materialization(config)

    after_files = {path for path in tmp_path.rglob("*") if path.is_file()}
    new_files = after_files - before_files
    assert new_files
    assert all(path.is_relative_to(config.output_dir) for path in new_files)
    text_reports = [path for path in new_files if path.suffix in {".csv", ".md"}]
    assert not any(
        "s3://" in path.read_text(encoding="utf-8", errors="ignore")
        for path in text_reports
    )


def test_config_from_args_loads_symbols_file_or_override(tmp_path: Path) -> None:
    symbols_file = tmp_path / "symbols.txt"
    symbols_file.write_text("spy\nqqq, SPY\n", encoding="utf-8")

    config = materializer.config_from_args(
        Namespace(
            input_dir=str(tmp_path / "in"),
            output_dir=str(tmp_path / "out"),
            symbols_file=str(symbols_file),
            symbols=None,
            start_date="1993-01-01",
            end_date="2026-05-17",
            overwrite=False,
            dry_run=False,
        )
    )
    assert config.symbols == ["QQQ", "SPY"]
    assert config.end_date == "2026-05-17"

    override = materializer.config_from_args(
        Namespace(
            input_dir=str(tmp_path / "in"),
            output_dir=str(tmp_path / "out"),
            symbols_file=str(symbols_file),
            symbols="iwm, spy, IWM",
            start_date="2019-01-01",
            end_date=None,
            overwrite=True,
            dry_run=True,
        )
    )
    assert override.symbols == ["IWM", "SPY"]
    assert override.start_date == "2019-01-01"
    assert override.overwrite is True
    assert override.dry_run is True
