from __future__ import annotations
# ruff: noqa: E402,I001

import json
import sys
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pyarrow.parquet as pq
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from us_market_data import ticker_reference  # noqa: E402
from us_market_data.s3_paths import (  # noqa: E402
    curated_ticker_reference_key,
    raw_ticker_reference_key,
)


def sample_payload() -> dict[str, object]:
    return {
        "status": "OK",
        "results": [
            {
                "ticker": "aapl",
                "name": "Apple Inc.",
                "market": "stocks",
                "locale": "us",
                "type": "CS",
                "active": True,
                "currency_name": "usd",
                "primary_exchange": "XNAS",
                "cik": "0000320193",
                "composite_figi": "BBG000B9XRY4",
                "share_class_figi": "BBG001S5N8V8",
                "last_updated_utc": "2026-05-09T00:00:00Z",
            },
            {
                "ticker": "tqqq",
                "name": "ProShares UltraPro QQQ",
                "market": "stocks",
                "locale": "us",
                "type": "ETF",
                "active": "true",
                "currency_name": "usd",
                "primary_exchange": "XNAS",
            },
        ],
    }


def test_normalize_sample_massive_ticker_payload_rows() -> None:
    rows = ticker_reference.normalize_massive_ticker_payloads(
        [sample_payload()],
        ingested_at="2026-05-09T16:00:00+00:00",
    )
    table = ticker_reference.rows_to_table(rows)

    assert table.schema.names == ticker_reference.NORMALIZED_COLUMNS
    assert rows[0] == {
        "symbol": "AAPL",
        "name": "Apple Inc.",
        "market": "stocks",
        "locale": "us",
        "type": "CS",
        "active": True,
        "currency_name": "usd",
        "primary_exchange": "XNAS",
        "cik": "0000320193",
        "composite_figi": "BBG000B9XRY4",
        "share_class_figi": "BBG001S5N8V8",
        "last_updated_utc": "2026-05-09T00:00:00Z",
        "ingested_at": "2026-05-09T16:00:00+00:00",
        "source": "massive",
        "is_common_stock_candidate": True,
    }
    assert rows[1]["symbol"] == "TQQQ"
    assert rows[1]["is_common_stock_candidate"] is False


def test_pagination_helper_handles_next_url_and_adds_api_key() -> None:
    seen_urls: list[str] = []

    def fake_fetcher(url: str) -> dict[str, object]:
        seen_urls.append(url)
        if len(seen_urls) == 1:
            return {
                "results": [{"ticker": "AAPL"}],
                "next_url": "https://api.massive.com/v3/reference/tickers?cursor=abc",
            }
        return {"results": [{"ticker": "MSFT"}]}

    pages = ticker_reference.fetch_massive_ticker_pages(
        api_key="secret",
        market="stocks",
        active=True,
        locale="us",
        limit=1000,
        page_fetcher=fake_fetcher,
    )

    first_query = parse_qs(urlparse(seen_urls[0]).query)
    second_query = parse_qs(urlparse(seen_urls[1]).query)

    assert len(pages) == 2
    assert first_query["market"] == ["stocks"]
    assert first_query["active"] == ["true"]
    assert first_query["locale"] == ["us"]
    assert first_query["limit"] == ["1000"]
    assert first_query["apiKey"] == ["secret"]
    assert second_query["cursor"] == ["abc"]
    assert second_query["apiKey"] == ["secret"]


def test_common_stock_classification_defaults_to_cs_only() -> None:
    assert ticker_reference.is_common_stock_type("CS") is True
    assert ticker_reference.is_common_stock_type("cs") is True
    assert ticker_reference.is_common_stock_type("ETF") is False
    assert ticker_reference.is_common_stock_type("FUND") is False
    assert ticker_reference.is_common_stock_type(None) is False
    assert ticker_reference.is_common_stock_type("ADRC", allowed_type_codes={"CS", "ADRC"}) is True


def test_common_stock_candidate_requires_market_locale_active_and_type() -> None:
    base = {"market": "stocks", "locale": "us", "active": True, "type": "CS"}

    assert ticker_reference.is_common_stock_candidate(base) is True
    assert ticker_reference.is_common_stock_candidate({**base, "market": "crypto"}) is False
    assert ticker_reference.is_common_stock_candidate({**base, "locale": "global"}) is False
    assert ticker_reference.is_common_stock_candidate({**base, "active": False}) is False
    assert ticker_reference.is_common_stock_candidate({**base, "type": "ETF"}) is False


def test_local_raw_json_write_works_with_tmp_path(tmp_path: Path) -> None:
    key = raw_ticker_reference_key("raw/reference/massive_tickers", "2026-05-09", 2)

    path = ticker_reference.write_raw_json_local(tmp_path, key, sample_payload())

    assert path == tmp_path / "raw/reference/massive_tickers/date=2026-05-09/page=2/data.json"
    assert json.loads(path.read_text(encoding="utf-8"))["status"] == "OK"


def test_local_curated_parquet_write_works_with_tmp_path(tmp_path: Path) -> None:
    rows = ticker_reference.normalize_massive_ticker_payloads(
        [sample_payload()],
        ingested_at="2026-05-09T16:00:00+00:00",
    )
    table = ticker_reference.rows_to_table(rows)
    key = curated_ticker_reference_key("curated/reference/ticker_reference/data.parquet")

    path = ticker_reference.write_curated_parquet_local(tmp_path, key, table)
    round_trip = pq.read_table(path)

    assert path == tmp_path / "curated/reference/ticker_reference/data.parquet"
    assert round_trip.schema.names == ticker_reference.NORMALIZED_COLUMNS
    assert round_trip.num_rows == 2
    assert round_trip["symbol"].to_pylist() == ["AAPL", "TQQQ"]


def test_missing_api_key_raises_clear_error() -> None:
    with pytest.raises(RuntimeError, match="MASSIVE_API_KEY or POLYGON_API_KEY"):
        ticker_reference.resolve_massive_api_key({})


def test_path_helpers_produce_expected_keys() -> None:
    assert (
        raw_ticker_reference_key("raw/reference/massive_tickers", "2026-05-09", 1)
        == "raw/reference/massive_tickers/date=2026-05-09/page=1/data.json"
    )
    assert (
        curated_ticker_reference_key("curated/reference")
        == "curated/reference/ticker_reference/data.parquet"
    )
    assert (
        curated_ticker_reference_key("curated/reference/ticker_reference/data.parquet")
        == "curated/reference/ticker_reference/data.parquet"
    )
