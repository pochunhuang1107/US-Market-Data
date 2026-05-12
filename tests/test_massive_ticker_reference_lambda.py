from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src"))

from lambdas.massive_ticker_reference_ingest import lambda_function as lambda_module  # noqa: E402


def sample_payload(symbol: str = "AAPL", type_code: str = "CS") -> dict[str, object]:
    return {
        "status": "OK",
        "results": [
            {
                "ticker": symbol,
                "name": f"{symbol} Inc.",
                "market": "stocks",
                "locale": "us",
                "type": type_code,
                "active": True,
            }
        ],
    }


def clear_config_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in [
        "BUCKET_NAME",
        "RAW_PREFIX",
        "CURATED_KEY",
        "MARKET",
        "ACTIVE",
        "LOCALE",
        "LIMIT",
        "COMMON_STOCK_TYPE_CODES",
        "REQUEST_INTERVAL_SECONDS",
        "MAX_RETRIES",
        "BASE_SLEEP_SECONDS",
        "MAX_SLEEP_SECONDS",
        "MASSIVE_API_KEY",
        "POLYGON_API_KEY",
    ]:
        monkeypatch.delenv(name, raising=False)


def configure_handler_mocks(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    clear_config_env(monkeypatch)
    monkeypatch.setenv("BUCKET_NAME", "test-bucket")
    monkeypatch.setenv("MASSIVE_API_KEY", "test-secret")

    calls: dict[str, Any] = {
        "s3_client": object(),
        "fetch": [],
        "raw_writes": [],
        "curated_writes": [],
    }

    def fake_boto3_client(service_name: str) -> object:
        assert service_name == "s3"
        return calls["s3_client"]

    def fake_fetch(**kwargs: object) -> list[dict[str, object]]:
        calls["fetch"].append(kwargs)
        progress_callback = kwargs["progress_callback"]
        if progress_callback:
            progress_callback("Fetching Massive ticker reference page 1...")
            progress_callback("Fetched page 1: rows=1 next_page=true")
            progress_callback("Sleeping 13.0 seconds before next page to respect rate limits...")
        return [sample_payload("AAPL"), sample_payload("MSFT")]

    def fake_write_raw(
        s3_client: object,
        bucket: str,
        key: str,
        payload: dict[str, Any],
    ) -> str:
        calls["raw_writes"].append((s3_client, bucket, key, payload))
        return key

    def fake_write_curated(
        s3_client: object,
        bucket: str,
        key: str,
        table: Any,
    ) -> str:
        calls["curated_writes"].append((s3_client, bucket, key, table.num_rows))
        return key

    monkeypatch.setattr(lambda_module.boto3, "client", fake_boto3_client)
    monkeypatch.setattr(lambda_module, "fetch_massive_ticker_pages", fake_fetch)
    monkeypatch.setattr(lambda_module, "write_raw_json_s3", fake_write_raw)
    monkeypatch.setattr(lambda_module, "write_curated_parquet_s3", fake_write_curated)
    return calls


def test_lambda_handler_returns_expected_body_with_mocked_fetch_and_writes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = configure_handler_mocks(monkeypatch)

    response = lambda_module.lambda_handler({"date": "2026-05-09"}, None)

    assert response == {
        "statusCode": 200,
        "body": {
            "source": "massive",
            "date": "2026-05-09",
            "market": "stocks",
            "locale": "us",
            "active": True,
            "pages": 2,
            "row_count": 2,
            "curated_key": "curated/reference/ticker_reference/data.parquet",
            "raw_prefix": "raw/reference/massive_tickers",
            "message": "Massive ticker reference ingested",
        },
    }
    assert calls["fetch"][0]["api_key"] == "test-secret"
    assert calls["fetch"][0]["request_interval_seconds"] == 13.0
    assert [write[2] for write in calls["raw_writes"]] == [
        "raw/reference/massive_tickers/date=2026-05-09/page=1/data.json",
        "raw/reference/massive_tickers/date=2026-05-09/page=2/data.json",
    ]
    assert calls["curated_writes"] == [
        (
            calls["s3_client"],
            "test-bucket",
            "curated/reference/ticker_reference/data.parquet",
            2,
        )
    ]


def test_lambda_event_overrides_environment_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = configure_handler_mocks(monkeypatch)
    monkeypatch.setenv("RAW_PREFIX", "raw/env")
    monkeypatch.setenv("CURATED_KEY", "curated/env/data.parquet")
    monkeypatch.setenv("MARKET", "env-market")
    monkeypatch.setenv("ACTIVE", "false")
    monkeypatch.setenv("LOCALE", "global")
    monkeypatch.setenv("LIMIT", "50")
    monkeypatch.setenv("COMMON_STOCK_TYPE_CODES", "ETF")
    monkeypatch.setenv("REQUEST_INTERVAL_SECONDS", "4")
    monkeypatch.setenv("MAX_RETRIES", "2")
    monkeypatch.setenv("BASE_SLEEP_SECONDS", "3")
    monkeypatch.setenv("MAX_SLEEP_SECONDS", "30")

    response = lambda_module.lambda_handler(
        {
            "date": "2026-05-10",
            "market": "stocks",
            "active": "true",
            "locale": "us",
            "limit": "1000",
            "common_stock_type_codes": "CS,ADRC",
            "request_interval_seconds": "13",
            "max_retries": "8",
            "base_sleep_seconds": "15",
            "max_sleep_seconds": "120",
        },
        None,
    )

    fetch_call = calls["fetch"][0]
    assert response["body"]["date"] == "2026-05-10"
    assert response["body"]["market"] == "stocks"
    assert response["body"]["locale"] == "us"
    assert response["body"]["active"] is True
    assert fetch_call["market"] == "stocks"
    assert fetch_call["active"] is True
    assert fetch_call["locale"] == "us"
    assert fetch_call["limit"] == 1000
    assert fetch_call["request_interval_seconds"] == 13.0
    assert fetch_call["max_retries"] == 8
    assert fetch_call["base_sleep_seconds"] == 15.0
    assert fetch_call["max_sleep_seconds"] == 120.0


def test_lambda_missing_bucket_name_raises_clear_error(monkeypatch: pytest.MonkeyPatch) -> None:
    configure_handler_mocks(monkeypatch)
    monkeypatch.delenv("BUCKET_NAME", raising=False)

    with pytest.raises(ValueError, match="BUCKET_NAME environment variable is required"):
        lambda_module.lambda_handler({}, None)


def test_lambda_missing_api_key_raises_clear_error(monkeypatch: pytest.MonkeyPatch) -> None:
    configure_handler_mocks(monkeypatch)
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    monkeypatch.delenv("POLYGON_API_KEY", raising=False)

    with pytest.raises(RuntimeError, match="MASSIVE_API_KEY or POLYGON_API_KEY"):
        lambda_module.lambda_handler({}, None)


def test_common_stock_type_codes_parses_comma_separated_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_config_env(monkeypatch)
    monkeypatch.setenv("BUCKET_NAME", "test-bucket")
    monkeypatch.setenv("COMMON_STOCK_TYPE_CODES", "CS, ADRC")

    config = lambda_module.resolve_config({})

    assert config["common_stock_type_codes"] == ("CS", "ADRC")


def test_lambda_invalid_numeric_config_raises_clear_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure_handler_mocks(monkeypatch)

    with pytest.raises(ValueError, match="limit must be an integer >= 1"):
        lambda_module.lambda_handler({"limit": "zero"}, None)


def test_lambda_invalid_bool_config_raises_clear_error(monkeypatch: pytest.MonkeyPatch) -> None:
    configure_handler_mocks(monkeypatch)

    with pytest.raises(ValueError, match="active must be a boolean value"):
        lambda_module.lambda_handler({"active": "sometimes"}, None)
