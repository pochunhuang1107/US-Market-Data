from __future__ import annotations
# ruff: noqa: E402,I001

import sys
from argparse import Namespace
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
