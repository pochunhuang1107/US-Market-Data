from __future__ import annotations
# ruff: noqa: E402,I001

import sys
from argparse import Namespace
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT))

from scripts import ingest_massive_ticker_overview as ingest_script  # noqa: E402
from us_market_data import ticker_overview  # noqa: E402
from us_market_data.s3_paths import (  # noqa: E402
    curated_ticker_overview_key,
    raw_ticker_overview_key,
)


def sample_overview_payload() -> dict[str, object]:
    return {
        "status": "OK",
        "results": {
            "ticker": "ionq",
            "name": "IonQ, Inc.",
            "market": "stocks",
            "locale": "us",
            "primary_exchange": "XNYS",
            "type": "CS",
            "active": True,
            "currency_name": "usd",
            "cik": "0001824920",
            "composite_figi": "BBG00Y91R9T3",
            "share_class_figi": "BBG00Y91R9V1",
            "market_cap": 111.25,
            "weighted_shares_outstanding": 200000000,
            "share_class_shares_outstanding": 199000000,
            "sic_code": "7372",
            "sic_description": "Services-Prepackaged Software",
            "description": "Quantum computing company.",
            "homepage_url": "https://ionq.com",
            "total_employees": 324,
            "list_date": "2021-10-01",
            "branding": {
                "logo_url": "https://api.massive.com/v1/reference/company-branding/logo.svg",
                "icon_url": "https://api.massive.com/v1/reference/company-branding/icon.png",
            },
        },
    }


def test_build_ticker_overview_url() -> None:
    url = ticker_overview.build_ticker_overview_url("secret", " ionq ")
    parsed = urlparse(url)

    assert parsed.scheme == "https"
    assert parsed.netloc == "api.massive.com"
    assert parsed.path == "/v3/reference/tickers/IONQ"
    assert parse_qs(parsed.query) == {"apiKey": ["secret"]}


def test_normalize_massive_ticker_overview_payload_with_branding() -> None:
    row = ticker_overview.normalize_massive_ticker_overview_payload(
        sample_overview_payload(),
        "IONQ",
        ingested_at="2026-05-09T16:00:00+00:00",
    )

    assert row == {
        "symbol": "IONQ",
        "name": "IonQ, Inc.",
        "market": "stocks",
        "locale": "us",
        "primary_exchange": "XNYS",
        "type": "CS",
        "active": True,
        "currency_name": "usd",
        "cik": "0001824920",
        "composite_figi": "BBG00Y91R9T3",
        "share_class_figi": "BBG00Y91R9V1",
        "market_cap": 111.25,
        "weighted_shares_outstanding": 200000000.0,
        "share_class_shares_outstanding": 199000000.0,
        "sic_code": "7372",
        "sic_description": "Services-Prepackaged Software",
        "description": "Quantum computing company.",
        "homepage_url": "https://ionq.com",
        "total_employees": 324,
        "list_date": "2021-10-01",
        "branding_logo_url": "https://api.massive.com/v1/reference/company-branding/logo.svg",
        "branding_icon_url": "https://api.massive.com/v1/reference/company-branding/icon.png",
        "source": "massive",
        "ingested_at": "2026-05-09T16:00:00+00:00",
    }


def test_normalize_massive_ticker_overview_payload_missing_optional_fields() -> None:
    row = ticker_overview.normalize_massive_ticker_overview_payload(
        {"status": "OK", "results": {"ticker": "NVDA", "active": "true"}},
        "NVDA",
        ingested_at="2026-05-09T16:00:00+00:00",
    )

    assert row["symbol"] == "NVDA"
    assert row["active"] is True
    assert row["branding_logo_url"] is None
    assert row["branding_icon_url"] is None
    assert row["market_cap"] is None
    assert row["description"] is None


def test_rows_to_table_schema() -> None:
    rows = [
        ticker_overview.normalize_massive_ticker_overview_payload(
            sample_overview_payload(),
            "IONQ",
            ingested_at="2026-05-09T16:00:00+00:00",
        )
    ]

    table = ticker_overview.rows_to_table(rows)

    assert table.schema.names == ticker_overview.NORMALIZED_COLUMNS
    assert table.schema == ticker_overview.NORMALIZED_SCHEMA
    assert table.num_rows == 1
    assert table["symbol"].to_pylist() == ["IONQ"]
    assert table["total_employees"].type == pa.int64()
    assert table["market_cap"].type == pa.float64()


def overview_row(symbol: str, name: str, ingested_at: str) -> dict[str, object]:
    row = {column: None for column in ticker_overview.NORMALIZED_COLUMNS}
    row.update(
        {
            "symbol": symbol,
            "name": name,
            "source": "massive",
            "ingested_at": ingested_at,
        }
    )
    return row


def test_table_to_rows_keeps_normalized_schema() -> None:
    rows = [overview_row("IONQ", "IonQ", "2026-05-09T16:00:00+00:00")]
    table = ticker_overview.rows_to_table(rows)

    assert ticker_overview.table_to_rows(table) == rows


def test_merge_ticker_overview_rows_replaces_existing_row() -> None:
    existing_rows = [
        overview_row("ionq", "Old IonQ", "2026-05-01T00:00:00+00:00"),
        overview_row("NVDA", "NVIDIA", "2026-05-01T00:00:00+00:00"),
    ]
    fetched_rows = [overview_row("IONQ", "Fresh IonQ", "2026-05-09T00:00:00+00:00")]

    merged = ticker_overview.merge_ticker_overview_rows(existing_rows, fetched_rows)

    assert [row["symbol"] for row in merged] == ["IONQ", "NVDA"]
    assert merged[0]["name"] == "Fresh IonQ"


def test_merge_ticker_overview_rows_keeps_old_row_not_fetched() -> None:
    existing_rows = [
        overview_row("MSFT", "Microsoft", "2026-05-01T00:00:00+00:00"),
    ]
    fetched_rows = [overview_row("AAPL", "Apple", "2026-05-09T00:00:00+00:00")]

    merged = ticker_overview.merge_ticker_overview_rows(existing_rows, fetched_rows)

    assert [row["symbol"] for row in merged] == ["AAPL", "MSFT"]
    assert {row["symbol"]: row["name"] for row in merged}["MSFT"] == "Microsoft"


def test_stale_symbols_detects_missing_old_fresh_and_invalid_rows() -> None:
    now = datetime(2026, 5, 13, tzinfo=timezone.utc)
    existing_rows = [
        overview_row("OLD", "Old", "2026-05-01T00:00:00+00:00"),
        overview_row("FRESH", "Fresh", "2026-05-12T00:00:00+00:00"),
        overview_row("INVALID", "Invalid", "not-a-timestamp"),
    ]

    assert ticker_overview.stale_symbols(
        existing_rows,
        ["missing", "old", "fresh", "invalid"],
        refresh_after_days=7,
        now=now,
    ) == ["MISSING", "OLD", "INVALID"]


def test_stale_symbols_refresh_after_days_zero_selects_all_requested() -> None:
    existing_rows = [overview_row("FRESH", "Fresh", "2026-05-12T00:00:00+00:00")]

    assert ticker_overview.stale_symbols(
        existing_rows,
        ["fresh", "missing"],
        refresh_after_days=0,
        now=datetime(2026, 5, 13, tzinfo=timezone.utc),
    ) == ["FRESH", "MISSING"]


def test_script_collect_symbols_prefers_symbols_arg_and_dedupes() -> None:
    args = Namespace(
        symbols=" nvda,IONQ,nvda ",
        symbols_file="unused",
        from_ticker_reference="unused",
        limit=1,
    )

    assert ingest_script.collect_symbols(args) == ["IONQ"]


def test_script_collect_symbols_from_file_dedupes_and_sorts(tmp_path: Path) -> None:
    symbols_file = tmp_path / "symbols.txt"
    symbols_file.write_text("msft\n\nAAPL\nmsft\n", encoding="utf-8")
    args = Namespace(
        symbols=None,
        symbols_file=symbols_file,
        from_ticker_reference=None,
        limit=None,
    )

    assert ingest_script.collect_symbols(args) == ["AAPL", "MSFT"]


def test_script_collect_symbols_from_ticker_reference(tmp_path: Path) -> None:
    path = tmp_path / "ticker_reference.parquet"
    table = pa.table(
        {
            "symbol": ["TQQQ", "aapl", "NVDA", "MSFT"],
            "is_common_stock_candidate": [False, True, True, True],
        }
    )
    pq.write_table(table, path)
    args = Namespace(
        symbols=None,
        symbols_file=None,
        from_ticker_reference=path,
        limit=2,
    )

    assert ingest_script.collect_symbols(args) == ["AAPL", "MSFT"]


def test_script_collect_symbols_requires_source() -> None:
    args = Namespace(symbols=None, symbols_file=None, from_ticker_reference=None, limit=None)

    with pytest.raises(RuntimeError, match="provide one symbol source"):
        ingest_script.collect_symbols(args)


def test_path_helpers_produce_expected_keys() -> None:
    assert (
        raw_ticker_overview_key("raw/reference/massive_ticker_overview", "2026-05-09", "IONQ")
        == "raw/reference/massive_ticker_overview/date=2026-05-09/symbol=IONQ/data.json"
    )
    assert (
        curated_ticker_overview_key("curated/reference/ticker_overview")
        == "curated/reference/ticker_overview/data.parquet"
    )
    assert (
        curated_ticker_overview_key("curated/reference/ticker_overview/data.parquet")
        == "curated/reference/ticker_overview/data.parquet"
    )


def test_script_merge_existing_fetches_only_missing_and_stale_symbols(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    curated_key = curated_ticker_overview_key("curated/reference/ticker_overview")
    existing_rows = [
        overview_row("IONQ", "Old IonQ", "2000-01-01T00:00:00+00:00"),
        overview_row("MSFT", "Microsoft", "2026-05-12T00:00:00+00:00"),
        overview_row("NVDA", "NVIDIA", "2999-01-01T00:00:00+00:00"),
    ]
    ticker_overview.write_curated_parquet_local(
        tmp_path,
        curated_key,
        ticker_overview.rows_to_table(existing_rows),
    )
    fetched_symbols: list[str] = []

    def fake_fetcher(symbol: str, **_: object) -> dict[str, object]:
        fetched_symbols.append(symbol)
        return {"status": "OK", "results": {"ticker": symbol, "name": f"Fresh {symbol}"}}

    monkeypatch.setattr(ingest_script, "resolve_massive_api_key", lambda: "test-key")
    args = Namespace(
        symbols="IONQ,NVDA,CSCO",
        symbols_file=None,
        from_ticker_reference=None,
        limit=None,
        local_output_dir=tmp_path,
        s3_bucket=None,
        raw_prefix="raw/reference/massive_ticker_overview",
        curated_prefix="curated/reference/ticker_overview",
        request_interval_seconds=0.0,
        max_retries=0,
        base_sleep_seconds=0.0,
        max_sleep_seconds=0.0,
        merge_existing=True,
        refresh_after_days=7,
        fetch_limit=None,
    )

    assert ingest_script.run_ingest(args, fetcher=fake_fetcher) == 0

    table = pq.read_table(tmp_path / curated_key)
    final_rows = ticker_overview.table_to_rows(table)
    names_by_symbol = {row["symbol"]: row["name"] for row in final_rows}

    assert fetched_symbols == ["CSCO", "IONQ"]
    assert sorted(names_by_symbol) == ["CSCO", "IONQ", "MSFT", "NVDA"]
    assert names_by_symbol["IONQ"] == "Fresh IONQ"
    assert names_by_symbol["MSFT"] == "Microsoft"
    assert names_by_symbol["NVDA"] == "NVIDIA"
    raw_paths = list(tmp_path.glob("raw/reference/massive_ticker_overview/*/symbol=*/data.json"))
    assert len(raw_paths) == 2
