from __future__ import annotations
# ruff: noqa: E402,I001

import json
import sys
from pathlib import Path

import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from us_market_data import apewisdom  # noqa: E402
from us_market_data.s3_paths import raw_apewisdom_key, staging_reddit_trends_key  # noqa: E402


def sample_payload() -> dict[str, object]:
    return {
        "count": 2,
        "pages": 1,
        "current_page": 1,
        "results": [
            {
                "rank": "1",
                "ticker": "NVDA",
                "name": "NVIDIA",
                "mentions": "30",
                "upvotes": "120",
                "rank_24h_ago": "3",
                "mentions_24h_ago": "20",
            },
            {
                "rank": 2,
                "ticker": "tsla",
                "name": "Tesla",
                "mentions": 10,
                "upvotes": 40,
                "rank_24h_ago": 5,
                "mentions_24h_ago": 5,
            },
        ],
    }


def test_normalize_sample_apewisdom_payload_into_expected_columns() -> None:
    rows = apewisdom.normalize_apewisdom_payload(
        sample_payload(),
        date_str="2026-05-09",
        hour_str="16",
        filter_name="all-stocks",
        page=1,
        ingested_at="2026-05-09T16:00:00+00:00",
    )

    table = apewisdom.rows_to_table(rows)

    assert table.schema.names == apewisdom.NORMALIZED_COLUMNS
    assert rows[0] == {
        "date": "2026-05-09",
        "hour": "16",
        "source": "apewisdom",
        "filter": "all-stocks",
        "page": 1,
        "rank": 1,
        "ticker": "NVDA",
        "name": "NVIDIA",
        "mentions": 30,
        "upvotes": 120,
        "rank_24h_ago": 3,
        "mentions_24h_ago": 20,
        "mention_change_pct": 50.0,
        "ingested_at": "2026-05-09T16:00:00+00:00",
    }
    assert rows[1]["ticker"] == "TSLA"


def test_normalize_handles_missing_optional_fields() -> None:
    rows = apewisdom.normalize_apewisdom_payload(
        {"results": [{"rank": "7", "ticker": "AMD", "mentions": "12"}]},
        date_str="2026-05-09",
        hour_str="16",
        filter_name="all-stocks",
        page=1,
        ingested_at="2026-05-09T16:00:00+00:00",
    )

    assert rows == [
        {
            "date": "2026-05-09",
            "hour": "16",
            "source": "apewisdom",
            "filter": "all-stocks",
            "page": 1,
            "rank": 7,
            "ticker": "AMD",
            "name": None,
            "mentions": 12,
            "upvotes": None,
            "rank_24h_ago": None,
            "mentions_24h_ago": None,
            "mention_change_pct": None,
            "ingested_at": "2026-05-09T16:00:00+00:00",
        }
    ]


def test_normalize_computes_mention_change_pct_when_possible() -> None:
    rows = apewisdom.normalize_apewisdom_payload(
        {"results": [{"ticker": "AAPL", "mentions": "75", "mentions_24h_ago": "50"}]},
        date_str="2026-05-09",
        hour_str="16",
        filter_name="all-stocks",
        page=1,
        ingested_at="2026-05-09T16:00:00+00:00",
    )

    assert rows[0]["mention_change_pct"] == 50.0


def test_local_raw_json_write_works_with_tmp_path(tmp_path: Path) -> None:
    key = raw_apewisdom_key(
        "raw/reddit/apewisdom",
        "2026-05-09",
        "16",
        "all-stocks",
    )

    path = apewisdom.write_raw_json_local(tmp_path, key, sample_payload())

    assert (
        path
        == tmp_path
        / "raw/reddit/apewisdom/date=2026-05-09/hour=16/filter=all-stocks/data.json"
    )
    assert json.loads(path.read_text(encoding="utf-8"))["count"] == 2


def test_local_staging_parquet_write_works_with_tmp_path(tmp_path: Path) -> None:
    rows = apewisdom.normalize_apewisdom_payload(
        sample_payload(),
        date_str="2026-05-09",
        hour_str="16",
        filter_name="all-stocks",
        page=1,
        ingested_at="2026-05-09T16:00:00+00:00",
    )
    table = apewisdom.rows_to_table(rows)
    key = staging_reddit_trends_key("staging/reddit_trends", "2026-05-09")

    path = apewisdom.write_staging_parquet_local(tmp_path, key, table)
    round_trip = pq.ParquetFile(path).read()

    assert path == tmp_path / "staging/reddit_trends/date=2026-05-09/data.parquet"
    assert round_trip.schema.names == apewisdom.NORMALIZED_COLUMNS
    assert round_trip.num_rows == 2


def test_path_helpers_produce_expected_keys() -> None:
    assert (
        raw_apewisdom_key("raw/reddit/apewisdom", "2026-05-09", "16", "all-stocks")
        == "raw/reddit/apewisdom/date=2026-05-09/hour=16/filter=all-stocks/data.json"
    )
    assert (
        staging_reddit_trends_key("staging/reddit_trends", "2026-05-09")
        == "staging/reddit_trends/date=2026-05-09/data.parquet"
    )
