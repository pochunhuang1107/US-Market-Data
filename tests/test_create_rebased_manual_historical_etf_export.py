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

from scripts import create_rebased_manual_historical_etf_export as rebased  # noqa: E402
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


def write_curated_partition(root: Path, symbol: str, rows: list[dict[str, object]]) -> None:
    by_year: dict[int, list[dict[str, object]]] = {}
    for row in rows:
        by_year.setdefault(int(row["year"]), []).append(row)
    for year, group_rows in by_year.items():
        path = root / f"symbol={symbol}" / f"year={year}" / "data.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(validator.rows_to_curated_table(group_rows), path)


def write_validation_reports(
    validation_dir: Path,
    *,
    boundary_rows: list[dict[str, object]],
    quality_rows: list[dict[str, object]],
) -> None:
    write_csv(
        validation_dir / "massive_boundary_report.csv",
        boundary_rows,
        [
            "symbol",
            "source_provider",
            "massive_first_available_date",
            "massive_last_available_date",
            "source_first_date",
            "source_last_date",
            "global_backfill_end_date",
            "source_backfill_end_date",
            "backfill_boundary_mode",
            "export_row_count",
            "exported_years",
            "first_export_date",
            "last_export_date",
            "has_pre_massive_history",
            "boundary_status",
            "requires_merge_with_existing_partition",
            "warning",
        ],
    )
    write_csv(
        validation_dir / "symbol_quality_report.csv",
        quality_rows,
        [
            "symbol",
            "quality_status",
            "issue_count",
            "issues",
            "ohlc_bad_rows",
            "duplicate_rows_removed",
            "event_row_count",
            "event_warning",
            "overlap_quality_status",
            "adjustment_diagnostic_status",
            "suggested_action",
            "boundary_rebase_status",
            "boundary_rebase_factor",
            "boundary_suggested_action",
        ],
    )


def boundary_row(
    symbol: str,
    *,
    export_row_count: int = 1,
    source_backfill_end_date: str = "2016-03-27",
    massive_first_available_date: str = "2016-03-28",
    has_pre_massive_history: bool = True,
    requires_merge: bool = True,
) -> dict[str, object]:
    return {
        "symbol": symbol,
        "source_provider": "stooq",
        "massive_first_available_date": massive_first_available_date,
        "massive_last_available_date": "2026-05-08",
        "source_first_date": "2015-01-02",
        "source_last_date": "2016-03-28",
        "global_backfill_end_date": "2017-12-31",
        "source_backfill_end_date": source_backfill_end_date,
        "backfill_boundary_mode": "infer_from_massive",
        "export_row_count": export_row_count,
        "exported_years": "2016" if export_row_count else "",
        "first_export_date": "2016-03-25" if export_row_count else "",
        "last_export_date": source_backfill_end_date if export_row_count else "",
        "has_pre_massive_history": has_pre_massive_history,
        "boundary_status": "inferred_from_massive",
        "requires_merge_with_existing_partition": requires_merge,
        "warning": "",
    }


def quality_row(symbol: str, action: str, factor: object = "") -> dict[str, object]:
    return {
        "symbol": symbol,
        "quality_status": "warning" if action != "use_as_is" else "pass",
        "issue_count": 0,
        "issues": "",
        "ohlc_bad_rows": 0,
        "duplicate_rows_removed": 0,
        "event_row_count": 0,
        "event_warning": "",
        "overlap_quality_status": "pass",
        "adjustment_diagnostic_status": "pass",
        "suggested_action": action,
        "boundary_rebase_status": "boundary_rebase_candidate",
        "boundary_rebase_factor": factor,
        "boundary_suggested_action": action,
    }


def run_export(
    tmp_path: Path,
    *,
    dry_run: bool = False,
    fail_on_missing_factor: bool = True,
) -> rebased.RebasedExportResult:
    return rebased.run_rebased_export(
        rebased.RebasedExportConfig(
            input_export_dir=tmp_path / "curated_export" / "source=stooq" / "etf_daily_prices",
            validation_dir=tmp_path / "validation",
            output_dir=tmp_path
            / "curated_export_rebased"
            / "source=stooq"
            / "etf_daily_prices",
            source_provider="stooq",
            dry_run=dry_run,
            fail_on_missing_factor=fail_on_missing_factor,
        )
    )


def test_use_as_is_symbol_writes_unchanged_price_rows(tmp_path: Path) -> None:
    input_dir = tmp_path / "curated_export" / "source=stooq" / "etf_daily_prices"
    validation_dir = tmp_path / "validation"
    write_curated_partition(
        input_dir,
        "GLD",
        [curated_row("GLD", "2016-03-27", open_price=10, high=11, low=9, close=10, vwap=10.5)],
    )
    write_validation_reports(
        validation_dir,
        boundary_rows=[boundary_row("GLD")],
        quality_rows=[quality_row("GLD", "use_as_is", 1.0)],
    )

    run_export(tmp_path)

    output_path = (
        tmp_path
        / "curated_export_rebased"
        / "source=stooq"
        / "etf_daily_prices"
        / "symbol=GLD"
        / "year=2016"
        / "data.parquet"
    )
    rows = pq.ParquetFile(output_path).read().to_pylist()
    assert rows[0]["open"] == 10.0
    assert rows[0]["high"] == 11.0
    assert rows[0]["low"] == 9.0
    assert rows[0]["close"] == 10.0
    assert rows[0]["vwap"] == 10.5


def test_rebase_symbol_multiplies_prices_but_not_volume_or_transactions(
    tmp_path: Path,
) -> None:
    input_dir = tmp_path / "curated_export" / "source=stooq" / "etf_daily_prices"
    validation_dir = tmp_path / "validation"
    write_curated_partition(
        input_dir,
        "SPY",
        [
            curated_row(
                "SPY",
                "2016-03-27",
                open_price=10,
                high=11,
                low=9,
                close=10,
                volume=1234,
                vwap=10.25,
                transactions=99,
            )
        ],
    )
    write_validation_reports(
        validation_dir,
        boundary_rows=[boundary_row("SPY")],
        quality_rows=[
            quality_row("SPY", "rebase_pre_massive_history_before_promotion", 2.0)
        ],
    )

    run_export(tmp_path)

    output_path = (
        tmp_path
        / "curated_export_rebased"
        / "source=stooq"
        / "etf_daily_prices"
        / "symbol=SPY"
        / "year=2016"
        / "data.parquet"
    )
    row = pq.ParquetFile(output_path).read().to_pylist()[0]
    assert row["open"] == 20.0
    assert row["high"] == 22.0
    assert row["low"] == 18.0
    assert row["close"] == 20.0
    assert row["vwap"] == 20.5
    assert row["volume"] == 1234.0
    assert row["transactions"] == 99.0


def test_rows_after_source_backfill_end_date_are_excluded(tmp_path: Path) -> None:
    input_dir = tmp_path / "curated_export" / "source=stooq" / "etf_daily_prices"
    validation_dir = tmp_path / "validation"
    write_curated_partition(
        input_dir,
        "SPY",
        [
            curated_row("SPY", "2016-03-27", open_price=10, high=11, low=9, close=10),
            curated_row("SPY", "2016-03-28", open_price=12, high=13, low=11, close=12),
        ],
    )
    write_validation_reports(
        validation_dir,
        boundary_rows=[boundary_row("SPY", export_row_count=1)],
        quality_rows=[
            quality_row("SPY", "rebase_pre_massive_history_before_promotion", 2.0)
        ],
    )

    run_export(tmp_path)

    output_path = (
        tmp_path
        / "curated_export_rebased"
        / "source=stooq"
        / "etf_daily_prices"
        / "symbol=SPY"
        / "year=2016"
        / "data.parquet"
    )
    rows = pq.ParquetFile(output_path).read().to_pylist()
    assert [row["date"] for row in rows] == ["2016-03-27"]


def test_missing_factor_fails_when_fail_on_missing_factor_is_true(tmp_path: Path) -> None:
    input_dir = tmp_path / "curated_export" / "source=stooq" / "etf_daily_prices"
    validation_dir = tmp_path / "validation"
    write_curated_partition(
        input_dir,
        "SPY",
        [curated_row("SPY", "2016-03-27", open_price=10, high=11, low=9, close=10)],
    )
    write_validation_reports(
        validation_dir,
        boundary_rows=[boundary_row("SPY")],
        quality_rows=[
            quality_row("SPY", "rebase_pre_massive_history_before_promotion", "")
        ],
    )

    with pytest.raises(ValueError, match="requires rebasing"):
        run_export(tmp_path)


def test_skipped_symbol_is_reported_when_no_pre_massive_rows_exist(tmp_path: Path) -> None:
    validation_dir = tmp_path / "validation"
    write_validation_reports(
        validation_dir,
        boundary_rows=[boundary_row("SPY", export_row_count=0, has_pre_massive_history=False)],
        quality_rows=[
            quality_row("SPY", "rebase_pre_massive_history_before_promotion", 2.0)
        ],
    )

    result = run_export(tmp_path)

    assert result.report_rows[0]["action"] == "skip"
    assert result.report_rows[0]["status"] == "skipped"
    assert "no pre-Massive" in result.report_rows[0]["warning"]


def test_same_year_merge_flag_is_preserved_in_report(tmp_path: Path) -> None:
    input_dir = tmp_path / "curated_export" / "source=stooq" / "etf_daily_prices"
    validation_dir = tmp_path / "validation"
    write_curated_partition(
        input_dir,
        "SPY",
        [curated_row("SPY", "2016-03-27", open_price=10, high=11, low=9, close=10)],
    )
    write_validation_reports(
        validation_dir,
        boundary_rows=[boundary_row("SPY", requires_merge=True)],
        quality_rows=[quality_row("SPY", "use_as_is", 1.0)],
    )

    result = run_export(tmp_path)

    assert result.report_rows[0]["requires_merge_with_existing_partition"] == "True"
    report_rows = read_csv_rows(validation_dir / "rebased_export_report.csv")
    assert report_rows[0]["requires_merge_with_existing_partition"] == "True"


def test_dry_run_does_not_write_parquet_but_writes_reports(tmp_path: Path) -> None:
    input_dir = tmp_path / "curated_export" / "source=stooq" / "etf_daily_prices"
    validation_dir = tmp_path / "validation"
    write_curated_partition(
        input_dir,
        "SPY",
        [curated_row("SPY", "2016-03-27", open_price=10, high=11, low=9, close=10)],
    )
    write_validation_reports(
        validation_dir,
        boundary_rows=[boundary_row("SPY")],
        quality_rows=[
            quality_row("SPY", "rebase_pre_massive_history_before_promotion", 2.0)
        ],
    )

    result = run_export(tmp_path, dry_run=True)

    assert result.report_rows[0]["status"] == "planned"
    assert not (
        tmp_path / "curated_export_rebased" / "source=stooq" / "etf_daily_prices"
    ).exists()
    assert (validation_dir / "rebased_export_report.csv").exists()
    assert (validation_dir / "rebased_export_manifest.json").exists()
    assert (validation_dir / "rebased_export_summary.md").exists()


def test_generated_report_manifest_and_markdown_exist(tmp_path: Path) -> None:
    input_dir = tmp_path / "curated_export" / "source=stooq" / "etf_daily_prices"
    validation_dir = tmp_path / "validation"
    write_curated_partition(
        input_dir,
        "SPY",
        [curated_row("SPY", "2016-03-27", open_price=10, high=11, low=9, close=10)],
    )
    write_validation_reports(
        validation_dir,
        boundary_rows=[boundary_row("SPY")],
        quality_rows=[quality_row("SPY", "use_as_is", 1.0)],
    )

    result = run_export(tmp_path)

    assert result.report_path.exists()
    assert result.manifest_path.exists()
    assert result.summary_path.exists()
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["symbols_used_as_is"] == 1
    assert manifest["total_output_rows"] == 1


def test_output_parquet_schema_matches_curated_etf_schema(tmp_path: Path) -> None:
    input_dir = tmp_path / "curated_export" / "source=stooq" / "etf_daily_prices"
    validation_dir = tmp_path / "validation"
    write_curated_partition(
        input_dir,
        "SPY",
        [curated_row("SPY", "2016-03-27", open_price=10, high=11, low=9, close=10)],
    )
    write_validation_reports(
        validation_dir,
        boundary_rows=[boundary_row("SPY")],
        quality_rows=[
            quality_row("SPY", "rebase_pre_massive_history_before_promotion", 2.0)
        ],
    )

    run_export(tmp_path)

    output_path = (
        tmp_path
        / "curated_export_rebased"
        / "source=stooq"
        / "etf_daily_prices"
        / "symbol=SPY"
        / "year=2016"
        / "data.parquet"
    )
    assert pq.ParquetFile(output_path).read().schema == validator.CURATED_ETF_SCHEMA
