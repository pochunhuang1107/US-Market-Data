from __future__ import annotations

import io
import sys
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src"))

from lambdas.massive_ticker_overview_ingest import lambda_function as lambda_module  # noqa: E402
from us_market_data import ticker_overview  # noqa: E402


class FakeS3Error(Exception):
    def __init__(self, code: str, message: str = "fake s3 error") -> None:
        super().__init__(message)
        self.response = {"Error": {"Code": code, "Message": message}}


class FakeS3:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.puts: list[dict[str, Any]] = []

    def get_object(self, Bucket: str, Key: str) -> dict[str, Any]:  # noqa: N803
        body = self.objects.get((Bucket, Key))
        if body is None:
            raise FakeS3Error("NoSuchKey", f"missing s3://{Bucket}/{Key}")
        return {"Body": io.BytesIO(body)}

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        body = kwargs["Body"]
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.objects[(kwargs["Bucket"], kwargs["Key"])] = body
        self.puts.append(kwargs)
        return {"ETag": "fake"}


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


def sample_payload(symbol: str) -> dict[str, object]:
    return {
        "status": "OK",
        "results": {
            "ticker": symbol,
            "name": f"Fresh {symbol}",
            "market": "stocks",
            "locale": "us",
            "type": "CS",
            "active": True,
        },
    }


def clear_config_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in [
        "BUCKET_NAME",
        "SYMBOLS_KEY",
        "RAW_PREFIX",
        "CURATED_KEY",
        "REFRESH_AFTER_DAYS",
        "FETCH_LIMIT",
        "REQUEST_INTERVAL_SECONDS",
        "MAX_RETRIES",
        "BASE_SLEEP_SECONDS",
        "MAX_SLEEP_SECONDS",
        "MASSIVE_API_KEY",
        "POLYGON_API_KEY",
    ]:
        monkeypatch.delenv(name, raising=False)


def configure_handler(
    monkeypatch: pytest.MonkeyPatch,
    fake_s3: FakeS3 | None = None,
) -> FakeS3:
    clear_config_env(monkeypatch)
    monkeypatch.setenv("BUCKET_NAME", "test-bucket")
    monkeypatch.setenv("MASSIVE_API_KEY", "test-secret")
    s3_client = fake_s3 or FakeS3()
    monkeypatch.setattr(lambda_module.boto3, "client", lambda service_name: s3_client)
    return s3_client


def put_text(fake_s3: FakeS3, key: str, text: str, bucket: str = "test-bucket") -> None:
    fake_s3.objects[(bucket, key)] = text.encode("utf-8")


def put_curated_rows(fake_s3: FakeS3, rows: list[dict[str, object]]) -> None:
    ticker_overview.write_curated_parquet_s3(
        fake_s3,
        "test-bucket",
        "curated/reference/ticker_overview/data.parquet",
        ticker_overview.rows_to_table(rows),
    )


def read_curated_rows(fake_s3: FakeS3) -> list[dict[str, Any]]:
    response = fake_s3.get_object(
        Bucket="test-bucket",
        Key="curated/reference/ticker_overview/data.parquet",
    )
    return ticker_overview.rows_from_parquet_bytes(response["Body"].read())


def test_resolve_config_uses_defaults_and_event_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    clear_config_env(monkeypatch)
    monkeypatch.setenv("BUCKET_NAME", "test-bucket")

    config = lambda_module.resolve_config(
        {
            "symbols_key": "curated/custom/symbols.txt",
            "date": "2026-05-13",
            "refresh_after_days": "3",
            "fetch_limit": "25",
            "request_interval_seconds": "1.5",
            "max_retries": "2",
            "base_sleep_seconds": "0.25",
            "max_sleep_seconds": "10",
        }
    )

    assert config == {
        "bucket": "test-bucket",
        "symbols_key": "curated/custom/symbols.txt",
        "raw_prefix": "raw/reference/massive_ticker_overview",
        "curated_key": "curated/reference/ticker_overview/data.parquet",
        "date": "2026-05-13",
        "refresh_after_days": 3,
        "fetch_limit": 25,
        "request_interval_seconds": 1.5,
        "max_retries": 2,
        "base_sleep_seconds": 0.25,
        "max_sleep_seconds": 10.0,
    }


def test_read_symbols_s3_normalizes_dedupes_and_sorts() -> None:
    fake_s3 = FakeS3()
    put_text(fake_s3, "symbols.txt", "msft\n\nAAPL\nmsft\n ionq \n")

    assert lambda_module.read_symbols_s3(fake_s3, "test-bucket", "symbols.txt") == [
        "AAPL",
        "IONQ",
        "MSFT",
    ]


def test_missing_symbols_file_fails_clearly(monkeypatch: pytest.MonkeyPatch) -> None:
    configure_handler(monkeypatch)

    with pytest.raises(RuntimeError, match="symbols file not found"):
        lambda_module.lambda_handler({}, None)


def test_empty_symbols_file_fails_clearly(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_s3 = configure_handler(monkeypatch)
    put_text(fake_s3, "curated/reference/public_profile_symbols/symbols.txt", "\n  \n")

    with pytest.raises(RuntimeError, match="symbols file did not contain any symbols"):
        lambda_module.lambda_handler({}, None)


def test_no_stale_symbols_returns_success_without_fetching(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_s3 = configure_handler(monkeypatch)
    put_text(fake_s3, "curated/reference/public_profile_symbols/symbols.txt", "msft\naapl\n")
    put_curated_rows(
        fake_s3,
        [
            overview_row("AAPL", "Apple", "2999-01-01T00:00:00+00:00"),
            overview_row("MSFT", "Microsoft", "2999-01-01T00:00:00+00:00"),
        ],
    )
    fake_s3.puts.clear()

    def fail_fetch(*_: object, **__: object) -> dict[str, object]:
        raise AssertionError("fetcher should not be called")

    monkeypatch.setattr(lambda_module, "fetch_massive_ticker_overview", fail_fetch)
    response = lambda_module.lambda_handler({"date": "2026-05-13"}, None)

    assert response["body"]["desired_symbols"] == 2
    assert response["body"]["existing_cached_rows"] == 2
    assert response["body"]["symbols_to_fetch"] == 0
    assert response["body"]["fetched"] == 0
    assert response["body"]["final_curated_rows"] == 2
    assert fake_s3.puts == []


def test_missing_existing_cache_starts_empty_and_writes_curated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_s3 = configure_handler(monkeypatch)
    put_text(fake_s3, "curated/reference/public_profile_symbols/symbols.txt", "aapl\n")
    fetched_symbols: list[str] = []

    def fake_fetch(symbol: str, **_: object) -> dict[str, object]:
        fetched_symbols.append(symbol)
        return sample_payload(symbol)

    monkeypatch.setattr(lambda_module, "fetch_massive_ticker_overview", fake_fetch)
    response = lambda_module.lambda_handler({"date": "2026-05-13"}, None)

    assert fetched_symbols == ["AAPL"]
    assert response["body"]["existing_cached_rows"] == 0
    assert response["body"]["symbols_to_fetch"] == 1
    assert response["body"]["fetched"] == 1
    assert response["body"]["final_curated_rows"] == 1
    assert [
        put["Key"]
        for put in fake_s3.puts
        if put["Key"].startswith("raw/reference/massive_ticker_overview/")
    ] == ["raw/reference/massive_ticker_overview/date=2026-05-13/symbol=AAPL/data.json"]
    assert {row["symbol"]: row["name"] for row in read_curated_rows(fake_s3)} == {
        "AAPL": "Fresh AAPL"
    }


def test_fetched_rows_are_merged_into_existing_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_s3 = configure_handler(monkeypatch)
    put_text(fake_s3, "curated/reference/public_profile_symbols/symbols.txt", "ionq\nmsft\naapl\n")
    put_curated_rows(
        fake_s3,
        [
            overview_row("IONQ", "Old IonQ", "2000-01-01T00:00:00+00:00"),
            overview_row("MSFT", "Microsoft", "2999-01-01T00:00:00+00:00"),
        ],
    )
    fetched_symbols: list[str] = []

    def fake_fetch(symbol: str, **_: object) -> dict[str, object]:
        fetched_symbols.append(symbol)
        return sample_payload(symbol)

    monkeypatch.setattr(lambda_module, "fetch_massive_ticker_overview", fake_fetch)
    response = lambda_module.lambda_handler({"date": "2026-05-13"}, None)
    rows_by_symbol = {row["symbol"]: row["name"] for row in read_curated_rows(fake_s3)}

    assert fetched_symbols == ["AAPL", "IONQ"]
    assert response["body"]["existing_cached_rows"] == 2
    assert response["body"]["symbols_to_fetch"] == 2
    assert response["body"]["fetched"] == 2
    assert rows_by_symbol == {
        "AAPL": "Fresh AAPL",
        "IONQ": "Fresh IONQ",
        "MSFT": "Microsoft",
    }


def test_fetch_limit_is_respected(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_s3 = configure_handler(monkeypatch)
    put_text(fake_s3, "curated/reference/public_profile_symbols/symbols.txt", "msft\naapl\nionq\n")
    fetched_symbols: list[str] = []

    def fake_fetch(symbol: str, **_: object) -> dict[str, object]:
        fetched_symbols.append(symbol)
        return sample_payload(symbol)

    monkeypatch.setattr(lambda_module, "fetch_massive_ticker_overview", fake_fetch)
    response = lambda_module.lambda_handler({"date": "2026-05-13", "fetch_limit": 2}, None)

    assert fetched_symbols == ["AAPL", "IONQ"]
    assert response["body"]["desired_symbols"] == 3
    assert response["body"]["symbols_to_fetch"] == 2
    assert response["body"]["fetched"] == 2
    assert response["body"]["final_curated_rows"] == 2
