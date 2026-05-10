from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src"))

from lambdas.apewisdom_reddit_trends_ingest import lambda_function as lambda_module  # noqa: E402


def sample_payload() -> dict[str, object]:
    return {
        "results": [
            {
                "rank": "1",
                "ticker": "NVDA",
                "name": "NVIDIA",
                "mentions": "30",
                "upvotes": "120",
            },
            {
                "rank": "2",
                "ticker": "TSLA",
                "name": "Tesla",
                "mentions": "20",
                "upvotes": "80",
            },
        ],
    }


def configure_handler_mocks(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    calls: dict[str, Any] = {
        "s3_client": object(),
        "fetch": [],
        "raw_writes": [],
        "staging_writes": [],
    }

    monkeypatch.setenv("BUCKET_NAME", "test-bucket")
    monkeypatch.delenv("RAW_PREFIX", raising=False)
    monkeypatch.delenv("STAGING_PREFIX", raising=False)
    monkeypatch.delenv("FILTER_NAME", raising=False)
    monkeypatch.delenv("PAGE", raising=False)
    monkeypatch.delenv("WRITE_RAW", raising=False)
    monkeypatch.delenv("WRITE_STAGING", raising=False)

    def fake_boto3_client(service_name: str) -> object:
        assert service_name == "s3"
        return calls["s3_client"]

    def fake_fetch(filter_name: str, page: int) -> dict[str, object]:
        calls["fetch"].append((filter_name, page))
        return sample_payload()

    def fake_write_raw(
        s3_client: object,
        bucket: str,
        key: str,
        payload: dict[str, Any],
    ) -> str:
        calls["raw_writes"].append((s3_client, bucket, key, payload))
        return key

    def fake_write_staging(
        s3_client: object,
        bucket: str,
        key: str,
        table: Any,
    ) -> str:
        calls["staging_writes"].append((s3_client, bucket, key, table.num_rows))
        return key

    monkeypatch.setattr(lambda_module.boto3, "client", fake_boto3_client)
    monkeypatch.setattr(lambda_module, "fetch_apewisdom_page", fake_fetch)
    monkeypatch.setattr(lambda_module, "write_raw_json_s3", fake_write_raw)
    monkeypatch.setattr(lambda_module, "write_staging_parquet_s3", fake_write_staging)
    return calls


def test_lambda_handler_returns_expected_payload_with_mocked_writes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = configure_handler_mocks(monkeypatch)

    response = lambda_module.lambda_handler(
        {"filter": "all-stocks", "page": 1, "date": "2026-05-08", "hour": "21"},
        None,
    )

    assert response == {
        "statusCode": 200,
        "body": {
            "source": "apewisdom",
            "filter": "all-stocks",
            "page": 1,
            "date": "2026-05-08",
            "hour": "21",
            "row_count": 2,
            "raw_key": (
                "raw/reddit/apewisdom/date=2026-05-08/hour=21/"
                "filter=all-stocks/data.json"
            ),
            "staging_key": "staging/reddit_trends/date=2026-05-08/data.parquet",
            "message": "ApeWisdom Reddit trends ingested as attention signal",
        },
    }
    assert calls["fetch"] == [("all-stocks", 1)]
    assert calls["raw_writes"][0][1:3] == (
        "test-bucket",
        "raw/reddit/apewisdom/date=2026-05-08/hour=21/filter=all-stocks/data.json",
    )
    assert calls["staging_writes"][0] == (
        calls["s3_client"],
        "test-bucket",
        "staging/reddit_trends/date=2026-05-08/data.parquet",
        2,
    )


def test_lambda_event_overrides_environment_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = configure_handler_mocks(monkeypatch)
    monkeypatch.setenv("FILTER_NAME", "env-filter")
    monkeypatch.setenv("PAGE", "3")
    monkeypatch.setenv("RAW_PREFIX", "raw/env")
    monkeypatch.setenv("STAGING_PREFIX", "staging/env")
    monkeypatch.setenv("WRITE_RAW", "true")
    monkeypatch.setenv("WRITE_STAGING", "true")

    response = lambda_module.lambda_handler(
        {
            "filter": "event-filter",
            "page": "2",
            "date": "2026-05-09",
            "hour": "7",
            "write_raw": False,
            "write_staging": "false",
        },
        None,
    )

    body = response["body"]
    assert body["filter"] == "event-filter"
    assert body["page"] == 2
    assert body["date"] == "2026-05-09"
    assert body["hour"] == "07"
    assert body["raw_key"] is None
    assert body["staging_key"] is None
    assert calls["fetch"] == [("event-filter", 2)]
    assert calls["raw_writes"] == []
    assert calls["staging_writes"] == []


def test_lambda_missing_bucket_name_raises_clear_error(monkeypatch: pytest.MonkeyPatch) -> None:
    configure_handler_mocks(monkeypatch)
    monkeypatch.delenv("BUCKET_NAME", raising=False)

    with pytest.raises(ValueError, match="BUCKET_NAME environment variable is required"):
        lambda_module.lambda_handler({}, None)


def test_lambda_invalid_page_raises_clear_error(monkeypatch: pytest.MonkeyPatch) -> None:
    configure_handler_mocks(monkeypatch)

    with pytest.raises(ValueError, match="page must be an integer >= 1"):
        lambda_module.lambda_handler({"page": 0}, None)


def test_lambda_invalid_write_flags_raise_consistent_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure_handler_mocks(monkeypatch)

    with pytest.raises(ValueError, match="invalid boolean value"):
        lambda_module.lambda_handler({"write_raw": "sometimes"}, None)
