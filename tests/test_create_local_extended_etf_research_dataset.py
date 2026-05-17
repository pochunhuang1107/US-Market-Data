from __future__ import annotations

import csv
import json
import sys
from argparse import Namespace
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src"))

from scripts import create_local_extended_etf_research_dataset as exporter  # noqa: E402
from scripts import validate_manual_historical_etf_backfill as validator  # noqa: E402


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with open(path, "r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def curated_row(
    symbol: str,
    date: str,
    *,
    open_price: float = 10,
    high: float = 11,
    low: float = 9,
    close: float = 10,
    volume: float = 1000,
    vwap: float | None = None,
    transactions: float | None = 10,
) -> dict[str, object]:
    year, month, _ = date.split("-")
    return {
        "date": date,
        "year": int(year),
        "month": int(month),
        "symbol": symbol,
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "vwap": vwap,
        "transactions": transactions,
    }


def write_curated_partitions(root: Path, symbol: str, rows: list[dict[str, object]]) -> None:
    by_year: dict[int, list[dict[str, object]]] = {}
    for row in rows:
        by_year.setdefault(int(row["year"]), []).append(row)
    for year, group_rows in by_year.items():
        path = root / f"symbol={symbol}" / f"year={year}" / "data.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(validator.rows_to_curated_table(group_rows), path)


def config_for(
    tmp_path: Path,
    *,
    symbols: list[str] | None = None,
    overwrite: bool = False,
    dry_run: bool = False,
) -> exporter.LocalExtendedDatasetConfig:
    return exporter.LocalExtendedDatasetConfig(
        input_dir=tmp_path / "stitched_validation" / "etf_daily_prices",
        output_dir=tmp_path / "research" / "etf_daily_prices_extended_tiingo_split",
        validation_dir=tmp_path / "validation",
        source_provider="tiingo_split",
        dataset_name="etf_daily_prices_extended_tiingo_split",
        symbols_file=tmp_path / "symbols.txt",
        symbols=symbols or ["SPY"],
        overwrite=overwrite,
        dry_run=dry_run,
        progress_callback=None,
    )


def setup_symbol(tmp_path: Path, symbol: str = "SPY") -> exporter.LocalExtendedDatasetConfig:
    config = config_for(tmp_path, symbols=[symbol])
    write_curated_partitions(
        config.input_dir,
        symbol,
        [
            curated_row(symbol, "2019-12-31"),
            curated_row(symbol, "2020-01-02", open_price=100, high=110, low=95, close=105),
        ],
    )
    return config


def test_copies_partitioned_parquet_files_from_input_to_output(tmp_path: Path) -> None:
    config = setup_symbol(tmp_path)

    result = exporter.run_export(config)

    source_paths = sorted(config.input_dir.glob("symbol=SPY/year=*/data.parquet"))
    output_paths = sorted(config.output_dir.glob("symbol=SPY/year=*/data.parquet"))
    assert [path.relative_to(config.input_dir) for path in source_paths] == [
        path.relative_to(config.output_dir) for path in output_paths
    ]
    assert [path.read_bytes() for path in source_paths] == [
        path.read_bytes() for path in output_paths
    ]
    assert result.symbol_rows[0]["status"] == "written"
    assert result.symbol_rows[0]["row_count"] == 2


def test_fails_when_output_exists_with_files_and_overwrite_false(tmp_path: Path) -> None:
    config = setup_symbol(tmp_path)
    stale_file = config.output_dir / "symbol=SPY" / "year=1999" / "data.parquet"
    stale_file.parent.mkdir(parents=True, exist_ok=True)
    stale_file.write_text("stale", encoding="utf-8")

    with pytest.raises(exporter.LocalExtendedDatasetError, match="already exists with files"):
        exporter.run_export(config)

    assert stale_file.read_text(encoding="utf-8") == "stale"


def test_overwrite_replaces_target_output_dir_only(tmp_path: Path) -> None:
    config = setup_symbol(tmp_path)
    sentinel = config.output_dir.parent / "do_not_delete.txt"
    sentinel.parent.mkdir(parents=True, exist_ok=True)
    sentinel.write_text("keep", encoding="utf-8")
    stale_file = config.output_dir / "old.txt"
    stale_file.parent.mkdir(parents=True, exist_ok=True)
    stale_file.write_text("remove", encoding="utf-8")

    exporter.run_export(config_for(tmp_path, overwrite=True))

    assert not stale_file.exists()
    assert sentinel.read_text(encoding="utf-8") == "keep"
    assert (config.output_dir / "symbol=SPY" / "year=2020" / "data.parquet").exists()


def test_dry_run_writes_no_parquet_files(tmp_path: Path) -> None:
    config = setup_symbol(tmp_path)

    result = exporter.run_export(config_for(tmp_path, dry_run=True))

    assert result.symbol_rows[0]["status"] == "planned"
    assert not list(config.output_dir.glob("**/*.parquet"))
    assert result.manifest_path.exists()
    assert result.summary_path.exists()


def test_validates_schema(tmp_path: Path) -> None:
    config = config_for(tmp_path)
    path = config.input_dir / "symbol=SPY" / "year=2020" / "data.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    bad_schema = pa.schema([pa.field("date", pa.string()), pa.field("symbol", pa.string())])
    pq.write_table(
        pa.Table.from_pylist([{"date": "2020-01-02", "symbol": "SPY"}], bad_schema),
        path,
    )

    with pytest.raises(exporter.LocalExtendedDatasetError, match="SPY"):
        exporter.run_export(config)

    rows = read_csv_rows(config.output_dir / "_reports" / exporter.SYMBOL_SUMMARY_FILENAME)
    assert rows[0]["status"] == "fail"
    assert "schema does not match" in rows[0]["warning"]


def test_detects_duplicate_symbol_date_rows(tmp_path: Path) -> None:
    config = config_for(tmp_path)
    write_curated_partitions(
        config.input_dir,
        "SPY",
        [
            curated_row("SPY", "2020-01-02"),
            curated_row("SPY", "2020-01-02", open_price=10.5, high=11.5, low=9.5, close=10.5),
        ],
    )

    with pytest.raises(exporter.LocalExtendedDatasetError, match="SPY"):
        exporter.run_export(config)

    rows = read_csv_rows(config.output_dir / "_reports" / exporter.SYMBOL_SUMMARY_FILENAME)
    assert rows[0]["duplicate_rows"] == "1"
    assert rows[0]["status"] == "fail"


def test_detects_ohlc_sanity_issue(tmp_path: Path) -> None:
    config = config_for(tmp_path)
    write_curated_partitions(
        config.input_dir,
        "SPY",
        [curated_row("SPY", "2020-01-02", open_price=10, high=9, low=8, close=10)],
    )

    with pytest.raises(exporter.LocalExtendedDatasetError, match="SPY"):
        exporter.run_export(config)

    rows = read_csv_rows(config.output_dir / "_reports" / exporter.SYMBOL_SUMMARY_FILENAME)
    assert rows[0]["ohlc_bad_rows"] == "1"
    assert "OHLC sanity" in rows[0]["warning"]


def test_writes_manifest_json_symbol_summary_csv_and_markdown_summary(tmp_path: Path) -> None:
    config = setup_symbol(tmp_path)
    stitched_report = config.validation_dir / "stitched_dataset_report.csv"
    stitched_report.parent.mkdir(parents=True, exist_ok=True)
    stitched_report.write_text(
        "symbol,status,warning\nSPY,warning,boundary close jump exceeds 2% warning threshold\n",
        encoding="utf-8",
    )

    result = exporter.run_export(config)

    assert result.manifest_path.exists()
    assert result.symbol_summary_path.exists()
    assert result.summary_path.exists()
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["dataset_name"] == "etf_daily_prices_extended_tiingo_split"
    assert manifest["source_provider"] == "tiingo_split"
    assert manifest["symbols_written"] == ["SPY"]
    assert manifest["total_rows"] == 2
    assert manifest["source_validation_reports_used"] == [
        str(config.validation_dir / "stitched_dataset_report.csv")
    ]
    summary = result.summary_path.read_text(encoding="utf-8")
    assert "No S3 write happened during this run." in summary
    assert "source stitched validation warning" in summary
    assert "data/research/etf_daily_prices_extended_tiingo_split/**/*.parquet" in summary
    assert "Point US-Market-ML-Research to this local path" in summary


def test_supports_symbols_subset(tmp_path: Path) -> None:
    config = config_for(tmp_path, symbols=["SPY", "QQQ"])
    write_curated_partitions(config.input_dir, "SPY", [curated_row("SPY", "2020-01-02")])
    write_curated_partitions(config.input_dir, "QQQ", [curated_row("QQQ", "2020-01-02")])

    result = exporter.run_export(config_for(tmp_path, symbols=["QQQ"]))

    assert result.manifest["symbols_requested"] == ["QQQ"]
    assert result.manifest["symbols_written"] == ["QQQ"]
    assert not (config.output_dir / "symbol=SPY").exists()
    assert (config.output_dir / "symbol=QQQ" / "year=2020" / "data.parquet").exists()


def test_config_from_args_loads_symbols_file_or_override(tmp_path: Path) -> None:
    symbols_file = tmp_path / "symbols.txt"
    symbols_file.write_text("spy\nqqq, SPY\n", encoding="utf-8")

    config = exporter.config_from_args(
        Namespace(
            input_dir=str(tmp_path / "in"),
            output_dir=str(tmp_path / "out"),
            validation_dir=str(tmp_path / "validation"),
            source_provider="tiingo_split",
            dataset_name="dataset",
            symbols_file=str(symbols_file),
            symbols=None,
            overwrite=False,
            dry_run=False,
        )
    )
    assert config.symbols == ["QQQ", "SPY"]

    override = exporter.config_from_args(
        Namespace(
            input_dir=str(tmp_path / "in"),
            output_dir=str(tmp_path / "out"),
            validation_dir=str(tmp_path / "validation"),
            source_provider="tiingo_split",
            dataset_name="dataset",
            symbols_file=str(symbols_file),
            symbols="iwm, spy, IWM",
            overwrite=True,
            dry_run=True,
        )
    )
    assert override.symbols == ["IWM", "SPY"]
    assert override.overwrite is True
    assert override.dry_run is True
