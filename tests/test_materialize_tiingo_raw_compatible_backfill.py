from __future__ import annotations
# ruff: noqa: E402,I001

import csv
import sys
from argparse import Namespace
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from scripts import materialize_tiingo_raw_compatible_backfill as materializer  # noqa: E402


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
) -> materializer.TiingoRawCompatibleConfig:
    return materializer.TiingoRawCompatibleConfig(
        input_dir=tmp_path / "raw" / "tiingo",
        output_dir=tmp_path / "raw" / "tiingo_raw_compatible",
        symbols_file=tmp_path / "symbols.txt",
        symbols=symbols or ["SPY"],
        validation_dir=tmp_path / "validation",
        overwrite=overwrite,
        dry_run=dry_run,
        progress_callback=None,
    )


def sample_tiingo_row(
    date: str,
    *,
    open_price: object = 100,
    raw_open: object = 90,
    adj_open: object = 80,
    close: object = 101,
    raw_close: object = 91,
    adj_close: object = 81,
    volume: object = 1000,
    raw_volume: object = 900,
    adj_volume: object = 1100,
) -> dict[str, object]:
    return {
        "date": date,
        "open": open_price,
        "high": 105,
        "low": 95,
        "close": close,
        "volume": volume,
        "source_provider": "tiingo",
        "source_symbol": "SPY",
        "price_mode": "adjusted",
        "raw_open": raw_open,
        "raw_high": 95,
        "raw_low": 85,
        "raw_close": raw_close,
        "raw_volume": raw_volume,
        "adj_open": adj_open,
        "adj_high": 85,
        "adj_low": 75,
        "adj_close": adj_close,
        "adj_volume": adj_volume,
        "div_cash": 0.12,
        "split_factor": 1,
    }


def test_reads_tiingo_csv_and_writes_raw_compatible_price_columns(tmp_path: Path) -> None:
    write_csv(tmp_path / "raw" / "tiingo" / "SPY.csv", [sample_tiingo_row("2020-01-02")])

    result = materializer.run_materialization(config_for(tmp_path))

    rows = read_csv_rows(tmp_path / "raw" / "tiingo_raw_compatible" / "SPY.csv")
    assert rows[0]["date"] == "2020-01-02"
    assert rows[0]["open"] == "90.0"
    assert rows[0]["high"] == "95.0"
    assert rows[0]["low"] == "85.0"
    assert rows[0]["close"] == "91.0"
    assert rows[0]["volume"] == "900.0"
    assert rows[0]["source_provider"] == "tiingo"
    assert rows[0]["source_symbol"] == "SPY"
    assert rows[0]["price_mode"] == "raw_compatible"
    assert result.manifest_rows[0]["status"] == "written"


def test_falls_back_to_effective_price_columns_when_raw_fields_are_missing(
    tmp_path: Path,
) -> None:
    write_csv(
        tmp_path / "raw" / "tiingo" / "SPY.csv",
        [
            {
                "date": "2020-01-02",
                "open": 100,
                "high": 105,
                "low": 95,
                "close": 101,
                "volume": 1000,
            }
        ],
    )

    materializer.run_materialization(config_for(tmp_path))

    rows = read_csv_rows(tmp_path / "raw" / "tiingo_raw_compatible" / "SPY.csv")
    assert rows[0]["open"] == "100.0"
    assert rows[0]["high"] == "105.0"
    assert rows[0]["low"] == "95.0"
    assert rows[0]["close"] == "101.0"
    assert rows[0]["volume"] == "1000.0"
    assert rows[0]["raw_open"] == ""


def test_preserves_adjusted_and_raw_diagnostic_columns(tmp_path: Path) -> None:
    write_csv(tmp_path / "raw" / "tiingo" / "SPY.csv", [sample_tiingo_row("2020-01-02")])

    materializer.run_materialization(config_for(tmp_path))

    rows = read_csv_rows(tmp_path / "raw" / "tiingo_raw_compatible" / "SPY.csv")
    assert rows[0]["raw_open"] == "90.0"
    assert rows[0]["raw_close"] == "91.0"
    assert rows[0]["raw_volume"] == "900.0"
    assert rows[0]["adj_open"] == "80.0"
    assert rows[0]["adj_close"] == "81.0"
    assert rows[0]["adj_volume"] == "1100.0"
    assert rows[0]["div_cash"] == "0.12"
    assert rows[0]["split_factor"] == "1.0"


def test_sorts_and_deduplicates_by_date_last_row_wins(tmp_path: Path) -> None:
    write_csv(
        tmp_path / "raw" / "tiingo" / "SPY.csv",
        [
            sample_tiingo_row("2020-01-03", raw_open=30, raw_close=31),
            sample_tiingo_row("2020-01-02", raw_open=20, raw_close=21),
            sample_tiingo_row("2020-01-02T12:30:00Z", raw_open=99, raw_close=100),
        ],
    )

    result = materializer.run_materialization(config_for(tmp_path))

    rows = read_csv_rows(tmp_path / "raw" / "tiingo_raw_compatible" / "SPY.csv")
    assert [row["date"] for row in rows] == ["2020-01-02", "2020-01-03"]
    assert rows[0]["open"] == "99.0"
    assert rows[0]["close"] == "100.0"
    assert result.manifest_rows[0]["warning"] == "duplicate date rows deduplicated: 1"


def test_skips_existing_output_when_overwrite_false(tmp_path: Path) -> None:
    write_csv(tmp_path / "raw" / "tiingo" / "SPY.csv", [sample_tiingo_row("2020-01-02")])
    output_path = tmp_path / "raw" / "tiingo_raw_compatible" / "SPY.csv"
    output_path.parent.mkdir(parents=True)
    output_path.write_text("sentinel\n", encoding="utf-8")

    result = materializer.run_materialization(config_for(tmp_path))

    assert result.manifest_rows[0]["status"] == "skipped_existing"
    assert output_path.read_text(encoding="utf-8") == "sentinel\n"


def test_dry_run_does_not_write_symbol_csvs(tmp_path: Path) -> None:
    write_csv(tmp_path / "raw" / "tiingo" / "SPY.csv", [sample_tiingo_row("2020-01-02")])

    result = materializer.run_materialization(config_for(tmp_path, dry_run=True))

    assert result.manifest_rows[0]["status"] == "planned"
    assert not (tmp_path / "raw" / "tiingo_raw_compatible" / "SPY.csv").exists()


def test_writes_manifest_and_summary(tmp_path: Path) -> None:
    write_csv(tmp_path / "raw" / "tiingo" / "SPY.csv", [sample_tiingo_row("2020-01-02")])

    result = materializer.run_materialization(config_for(tmp_path))

    manifest_rows = read_csv_rows(result.manifest_path)
    summary = result.summary_path.read_text(encoding="utf-8")
    assert manifest_rows[0]["symbol"] == "SPY"
    assert manifest_rows[0]["status"] == "written"
    assert manifest_rows[0]["row_count"] == "1"
    assert manifest_rows[0]["first_date"] == "2020-01-02"
    assert manifest_rows[0]["last_date"] == "2020-01-02"
    assert "No API call happened during this run." in summary
    assert "No S3 write happened during this run." in summary
    assert "--raw-dir data/manual_backfill/raw/tiingo_raw_compatible" in summary
    assert "--source-provider tiingo_raw" in summary


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
