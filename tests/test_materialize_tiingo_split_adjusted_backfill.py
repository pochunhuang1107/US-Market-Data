from __future__ import annotations
# ruff: noqa: E402,I001

import csv
import sys
from argparse import Namespace
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from scripts import materialize_tiingo_split_adjusted_backfill as materializer  # noqa: E402


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
    overwrite: bool = False,
    dry_run: bool = False,
) -> materializer.TiingoSplitAdjustedConfig:
    return materializer.TiingoSplitAdjustedConfig(
        input_dir=tmp_path / "raw" / "tiingo",
        output_dir=tmp_path / "raw" / "tiingo_split_adjusted",
        symbols_file=tmp_path / "symbols.txt",
        symbols=symbols or ["SPY"],
        validation_dir=tmp_path / "validation",
        overwrite=overwrite,
        dry_run=dry_run,
        progress_callback=None,
    )


def sample_tiingo_row(
    row_date: str,
    *,
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
    div_cash: object = 0,
    split_factor: object = 1,
) -> dict[str, object]:
    return {
        "date": row_date,
        "open": 999,
        "high": 999,
        "low": 999,
        "close": 999,
        "volume": 999,
        "source_provider": "tiingo",
        "source_symbol": "SPY",
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


def test_split_factor_adjusts_prior_rows_only_not_split_date(tmp_path: Path) -> None:
    write_csv(
        tmp_path / "raw" / "tiingo" / "SPY.csv",
        [
            sample_tiingo_row("2020-01-01", raw_open=100, raw_close=100, raw_volume=1000),
            sample_tiingo_row(
                "2020-01-02",
                raw_open=50,
                raw_close=50,
                raw_volume=2000,
                split_factor=2,
            ),
            sample_tiingo_row("2020-01-03", raw_open=51, raw_close=51, raw_volume=2100),
        ],
    )

    result = materializer.run_materialization(config_for(tmp_path))

    rows = read_csv_rows(tmp_path / "raw" / "tiingo_split_adjusted" / "SPY.csv")
    assert [row["date"] for row in rows] == ["2020-01-01", "2020-01-02", "2020-01-03"]
    assert [float(row["close"]) for row in rows] == [50.0, 50.0, 51.0]
    assert [float(row["volume"]) for row in rows] == [2000.0, 2000.0, 2100.0]
    assert result.manifest_rows[0]["split_event_count"] == 1
    assert result.manifest_rows[0]["cumulative_split_factor_start"] == 2.0
    assert result.manifest_rows[0]["cumulative_split_factor_end"] == 1.0


def test_handles_multiple_splits_with_compounded_future_factor(tmp_path: Path) -> None:
    write_csv(
        tmp_path / "raw" / "tiingo" / "SPY.csv",
        [
            sample_tiingo_row("2020-01-01", raw_close=600, raw_volume=100),
            sample_tiingo_row("2020-01-02", raw_close=300, raw_volume=200, split_factor=2),
            sample_tiingo_row("2020-01-03", raw_close=330, raw_volume=220),
            sample_tiingo_row("2020-01-04", raw_close=110, raw_volume=660, split_factor=3),
        ],
    )

    materializer.run_materialization(config_for(tmp_path))

    rows = read_csv_rows(tmp_path / "raw" / "tiingo_split_adjusted" / "SPY.csv")
    assert [float(row["close"]) for row in rows] == [100.0, 100.0, 110.0, 110.0]
    assert [float(row["volume"]) for row in rows] == [600.0, 600.0, 660.0, 660.0]


def test_uses_raw_fields_not_adjusted_or_dividend_adjusted_values(tmp_path: Path) -> None:
    write_csv(
        tmp_path / "raw" / "tiingo" / "SPY.csv",
        [
            sample_tiingo_row(
                "2020-01-01",
                raw_open=100,
                raw_high=120,
                raw_low=90,
                raw_close=110,
                raw_volume=1000,
                adj_open=70,
                adj_high=84,
                adj_low=63,
                adj_close=77,
                adj_volume=1428,
                div_cash=10,
            )
        ],
    )

    materializer.run_materialization(config_for(tmp_path))

    rows = read_csv_rows(tmp_path / "raw" / "tiingo_split_adjusted" / "SPY.csv")
    assert rows[0]["open"] == "100.0"
    assert rows[0]["high"] == "120.0"
    assert rows[0]["low"] == "90.0"
    assert rows[0]["close"] == "110.0"
    assert rows[0]["volume"] == "1000.0"
    assert rows[0]["price_mode"] == "split_adjusted"


def test_preserves_raw_adjusted_split_and_dividend_diagnostic_fields(tmp_path: Path) -> None:
    write_csv(
        tmp_path / "raw" / "tiingo" / "SPY.csv",
        [
            sample_tiingo_row(
                "2020-01-01",
                raw_open=100,
                raw_close=110,
                raw_volume=1000,
                adj_open=75,
                adj_close=82.5,
                adj_volume=1333,
                div_cash=0.42,
                split_factor=2,
            )
        ],
    )

    materializer.run_materialization(config_for(tmp_path))

    rows = read_csv_rows(tmp_path / "raw" / "tiingo_split_adjusted" / "SPY.csv")
    assert rows[0]["raw_open"] == "100.0"
    assert rows[0]["raw_close"] == "110.0"
    assert rows[0]["raw_volume"] == "1000.0"
    assert rows[0]["adj_open"] == "75.0"
    assert rows[0]["adj_close"] == "82.5"
    assert rows[0]["adj_volume"] == "1333.0"
    assert rows[0]["div_cash"] == "0.42"
    assert rows[0]["split_factor"] == "2.0"
    assert list(rows[0]) == materializer.OUTPUT_COLUMNS


def test_missing_split_factor_column_is_treated_as_one(tmp_path: Path) -> None:
    rows = [
        {
            "date": "2020-01-01",
            "open": 100,
            "high": 110,
            "low": 90,
            "close": 105,
            "volume": 1000,
        }
    ]
    write_csv(tmp_path / "raw" / "tiingo" / "SPY.csv", rows)

    result = materializer.run_materialization(config_for(tmp_path))

    output_rows = read_csv_rows(tmp_path / "raw" / "tiingo_split_adjusted" / "SPY.csv")
    assert output_rows[0]["close"] == "105.0"
    assert output_rows[0]["volume"] == "1000.0"
    assert output_rows[0]["split_factor"] == "1.0"
    assert result.manifest_rows[0]["split_event_count"] == 0


def test_dry_run_writes_reports_but_no_symbol_csv(tmp_path: Path) -> None:
    write_csv(tmp_path / "raw" / "tiingo" / "SPY.csv", [sample_tiingo_row("2020-01-01")])

    result = materializer.run_materialization(config_for(tmp_path, dry_run=True))

    assert result.manifest_rows[0]["status"] == "planned"
    assert not (tmp_path / "raw" / "tiingo_split_adjusted" / "SPY.csv").exists()
    assert result.manifest_path.exists()
    assert result.summary_path.exists()


def test_writes_manifest_and_summary(tmp_path: Path) -> None:
    write_csv(
        tmp_path / "raw" / "tiingo" / "SPY.csv",
        [sample_tiingo_row("2020-01-01", split_factor=2)],
    )

    result = materializer.run_materialization(config_for(tmp_path))

    manifest_rows = read_csv_rows(result.manifest_path)
    summary = result.summary_path.read_text(encoding="utf-8")
    assert manifest_rows[0]["symbol"] == "SPY"
    assert manifest_rows[0]["status"] == "written"
    assert manifest_rows[0]["row_count"] == "1"
    assert manifest_rows[0]["first_date"] == "2020-01-01"
    assert manifest_rows[0]["last_date"] == "2020-01-01"
    assert manifest_rows[0]["split_event_count"] == "1"
    assert "No API call happened during this run." in summary
    assert "No S3 write happened during this run." in summary
    assert "Dividend cash fields were preserved only as diagnostics" in summary
    assert "--raw-dir data/manual_backfill/raw/tiingo_split_adjusted" in summary
    assert "--source-provider tiingo_split" in summary


def test_config_from_args_loads_symbols_file_or_override(tmp_path: Path) -> None:
    symbols_file = tmp_path / "symbols.txt"
    symbols_file.write_text("spy\nqqq, SPY\n", encoding="utf-8")

    config = materializer.config_from_args(
        Namespace(
            input_dir=str(tmp_path / "in"),
            output_dir=str(tmp_path / "out"),
            symbols_file=str(symbols_file),
            symbols=None,
            validation_dir=str(tmp_path / "validation"),
            overwrite=False,
            dry_run=False,
        )
    )
    assert config.symbols == ["QQQ", "SPY"]

    override_config = materializer.config_from_args(
        Namespace(
            input_dir=str(tmp_path / "in"),
            output_dir=str(tmp_path / "out"),
            symbols_file=str(symbols_file),
            symbols="iwm, spy, IWM",
            validation_dir=str(tmp_path / "validation"),
            overwrite=True,
            dry_run=True,
        )
    )
    assert override_config.symbols == ["IWM", "SPY"]
    assert override_config.overwrite is True
    assert override_config.dry_run is True
