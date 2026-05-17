from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

import pyarrow.parquet as pq
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src"))

from scripts import validate_manual_historical_etf_backfill as validator  # noqa: E402
from scripts import validate_stitched_manual_historical_etf_dataset as stitched  # noqa: E402


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


def curated_row(
    symbol: str,
    date: str,
    *,
    open_price: float,
    high: float,
    low: float,
    close: float,
    volume: float = 1000.0,
    vwap: float | None = None,
    transactions: float | None = 10.0,
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


def boundary_row(
    symbol: str,
    massive_first_available_date: str = "2016-03-28",
) -> dict[str, object]:
    return {
        "symbol": symbol,
        "source_provider": "stooq",
        "massive_first_available_date": massive_first_available_date,
        "massive_last_available_date": "2026-05-08",
        "source_first_date": "2015-01-02",
        "source_last_date": "2016-03-28",
        "global_backfill_end_date": "2017-12-31",
        "source_backfill_end_date": "2016-03-27",
        "backfill_boundary_mode": "infer_from_massive",
        "export_row_count": 2,
        "exported_years": "2016",
        "first_export_date": "2016-03-25",
        "last_export_date": "2016-03-27",
        "has_pre_massive_history": True,
        "boundary_status": "inferred_from_massive",
        "requires_merge_with_existing_partition": True,
        "warning": "",
    }


def rebased_report_row(
    symbol: str,
    *,
    output_rows: int = 2,
    status: str = "written",
    massive_first_available_date: str = "2016-03-28",
) -> dict[str, object]:
    return {
        "symbol": symbol,
        "source_provider": "stooq",
        "action": "use_as_is",
        "rebase_factor": 1.0,
        "massive_first_available_date": massive_first_available_date,
        "source_backfill_end_date": "2016-03-27",
        "input_rows": output_rows,
        "output_rows": output_rows,
        "first_output_date": "2016-03-25" if output_rows else "",
        "last_output_date": "2016-03-27" if output_rows else "",
        "output_years": "2016" if output_rows else "",
        "output_path_count": 1 if output_rows else 0,
        "requires_merge_with_existing_partition": True,
        "status": status,
        "warning": "",
    }


def write_reports(
    validation_dir: Path,
    *,
    rebased_rows: list[dict[str, object]],
    boundary_rows: list[dict[str, object]],
) -> None:
    write_csv(validation_dir / "rebased_export_report.csv", rebased_rows)
    write_csv(validation_dir / "massive_boundary_report.csv", boundary_rows)


def config_for(
    tmp_path: Path,
    *,
    dry_run: bool = False,
    fail_on_duplicates: bool = True,
    fail_on_boundary_gap: bool = False,
) -> stitched.StitchedValidationConfig:
    return stitched.StitchedValidationConfig(
        rebased_export_dir=tmp_path
        / "curated_export_rebased"
        / "source=stooq"
        / "etf_daily_prices",
        massive_curated_dir=tmp_path / "massive_curated_sample",
        validation_dir=tmp_path / "validation",
        output_dir=tmp_path / "stitched_validation" / "etf_daily_prices",
        source_provider="stooq",
        dry_run=dry_run,
        fail_on_duplicates=fail_on_duplicates,
        fail_on_boundary_gap=fail_on_boundary_gap,
    )


def setup_basic_symbol(tmp_path: Path, symbol: str = "SPY") -> None:
    config = config_for(tmp_path)
    write_curated_partitions(
        config.rebased_export_dir,
        symbol,
        [
            curated_row(symbol, "2016-03-25", open_price=10, high=11, low=9, close=10),
            curated_row(symbol, "2016-03-27", open_price=11, high=12, low=10, close=11),
            curated_row(symbol, "2016-03-28", open_price=99, high=100, low=98, close=99),
        ],
    )
    write_curated_partitions(
        config.massive_curated_dir,
        symbol,
        [
            curated_row(symbol, "2016-03-27", open_price=88, high=89, low=87, close=88),
            curated_row(symbol, "2016-03-28", open_price=11.1, high=12.1, low=10.1, close=11.1),
            curated_row(symbol, "2016-03-29", open_price=12, high=13, low=11, close=12),
        ],
    )
    write_reports(
        config.validation_dir,
        rebased_rows=[rebased_report_row(symbol)],
        boundary_rows=[boundary_row(symbol)],
    )


def test_stitch_combines_rebased_pre_boundary_and_massive_boundary_plus_rows(
    tmp_path: Path,
) -> None:
    setup_basic_symbol(tmp_path)

    result = stitched.run_stitched_validation(config_for(tmp_path))

    output_path = (
        config_for(tmp_path).output_dir / "symbol=SPY" / "year=2016" / "data.parquet"
    )
    rows = pq.ParquetFile(output_path).read().to_pylist()
    assert [row["date"] for row in rows] == [
        "2016-03-25",
        "2016-03-27",
        "2016-03-28",
        "2016-03-29",
    ]
    assert rows[2]["close"] == 11.1
    assert result.report_rows[0]["rebased_rows_used"] == 2
    assert result.report_rows[0]["massive_rows_used"] == 2
    assert result.report_rows[0]["stitched_rows"] == 4


def test_duplicate_symbol_date_detection_fails_by_default(tmp_path: Path) -> None:
    config = config_for(tmp_path)
    write_curated_partitions(
        config.rebased_export_dir,
        "SPY",
        [curated_row("SPY", "2016-03-27", open_price=10, high=11, low=9, close=10)],
    )
    write_curated_partitions(
        config.massive_curated_dir,
        "SPY",
        [
            curated_row("SPY", "2016-03-28", open_price=10, high=11, low=9, close=10),
            curated_row("SPY", "2016-03-28", open_price=10, high=11, low=9, close=10),
        ],
    )
    write_reports(
        config.validation_dir,
        rebased_rows=[rebased_report_row("SPY", output_rows=1)],
        boundary_rows=[boundary_row("SPY")],
    )

    with pytest.raises(stitched.StitchedValidationError, match="SPY"):
        stitched.run_stitched_validation(config)

    rows = read_csv_rows(config.validation_dir / "stitched_dataset_report.csv")
    assert rows[0]["duplicate_rows"] == "1"
    assert rows[0]["status"] == "fail"


def test_same_year_merge_is_validated_when_boundary_year_contains_both_sources(
    tmp_path: Path,
) -> None:
    setup_basic_symbol(tmp_path)

    result = stitched.run_stitched_validation(config_for(tmp_path))

    row = result.report_rows[0]
    assert row["boundary_year"] == 2016
    assert row["has_rebased_boundary_year_rows"] is True
    assert row["has_massive_boundary_year_rows"] is True
    assert row["same_year_merge_required"] is True
    assert row["same_year_merge_validated"] is True


def test_boundary_close_jump_calculation(tmp_path: Path) -> None:
    setup_basic_symbol(tmp_path)

    result = stitched.run_stitched_validation(config_for(tmp_path))

    assert result.report_rows[0]["last_rebased_close"] == 11.0
    assert result.report_rows[0]["first_massive_close"] == 11.1
    assert result.report_rows[0]["boundary_close_jump_pct"] == pytest.approx(
        abs(11.1 / 11.0 - 1.0)
    )


def test_ohlc_sanity_check_fails_bad_rows(tmp_path: Path) -> None:
    config = config_for(tmp_path)
    write_curated_partitions(
        config.rebased_export_dir,
        "SPY",
        [curated_row("SPY", "2016-03-27", open_price=10, high=9, low=8, close=10)],
    )
    write_curated_partitions(
        config.massive_curated_dir,
        "SPY",
        [curated_row("SPY", "2016-03-28", open_price=10, high=11, low=9, close=10)],
    )
    write_reports(
        config.validation_dir,
        rebased_rows=[rebased_report_row("SPY", output_rows=1)],
        boundary_rows=[boundary_row("SPY")],
    )

    with pytest.raises(stitched.StitchedValidationError, match="SPY"):
        stitched.run_stitched_validation(config)

    rows = read_csv_rows(config.validation_dir / "stitched_dataset_report.csv")
    assert rows[0]["ohlc_bad_rows"] == "1"
    assert rows[0]["status"] == "fail"


def test_skipped_symbol_in_rebased_report_is_skipped(tmp_path: Path) -> None:
    config = config_for(tmp_path)
    write_reports(
        config.validation_dir,
        rebased_rows=[rebased_report_row("SPY", output_rows=0, status="skipped")],
        boundary_rows=[boundary_row("SPY")],
    )

    result = stitched.run_stitched_validation(config)

    assert result.report_rows[0]["status"] == "skipped"
    assert not config.output_dir.exists()


def test_dry_run_does_not_write_parquet(tmp_path: Path) -> None:
    setup_basic_symbol(tmp_path)
    config = config_for(tmp_path, dry_run=True)

    result = stitched.run_stitched_validation(config)

    assert result.report_rows[0]["status"] == "planned"
    assert not config.output_dir.exists()
    assert (config.validation_dir / "stitched_dataset_report.csv").exists()


def test_report_csv_manifest_and_summary_are_generated(tmp_path: Path) -> None:
    setup_basic_symbol(tmp_path)

    result = stitched.run_stitched_validation(config_for(tmp_path))

    assert result.report_path.exists()
    assert result.manifest_path.exists()
    assert result.summary_path.exists()
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["symbols_processed"] == 1
    assert manifest["symbols_written"] == 1
    assert manifest["total_stitched_rows"] == 4
    summary = result.summary_path.read_text(encoding="utf-8")
    assert "validation output only" in summary.lower()
    assert "Inspect boundary jumps" in summary


def test_output_schema_matches_curated_etf_schema(tmp_path: Path) -> None:
    setup_basic_symbol(tmp_path)

    stitched.run_stitched_validation(config_for(tmp_path))

    output_path = (
        config_for(tmp_path).output_dir / "symbol=SPY" / "year=2016" / "data.parquet"
    )
    assert pq.ParquetFile(output_path).read().schema == validator.CURATED_ETF_SCHEMA
