from __future__ import annotations
# ruff: noqa: E402,I001

import csv
import json
import sys
from argparse import Namespace
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlparse

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from scripts import download_tiingo_historical_etf_data as tiingo  # noqa: E402


class MockResponse:
    def __init__(self, payload: object) -> None:
        self.payload = payload

    def __enter__(self) -> MockResponse:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self) -> bytes:
        return json.dumps(self.payload).encode("utf-8")


class RecordingOpener:
    def __init__(self, payloads: dict[str, object]) -> None:
        self.payloads = payloads
        self.requests = []
        self.timeouts = []

    def __call__(self, request: object, *, timeout: float) -> MockResponse:
        self.requests.append(request)
        self.timeouts.append(timeout)
        url = request.full_url
        for symbol, payload in self.payloads.items():
            if f"/{symbol}/prices" in url:
                if isinstance(payload, Exception):
                    raise payload
                return MockResponse(payload)
        raise AssertionError(f"unexpected URL: {url}")


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with open(path, "r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def args_for(tmp_path: Path, **overrides: object) -> Namespace:
    symbols_file = tmp_path / "symbols.txt"
    symbols_file.write_text("SPY\nQQQ\n", encoding="utf-8")
    values = {
        "symbols_file": str(symbols_file),
        "symbols": None,
        "start_date": "1999-01-01",
        "end_date": "2026-05-15",
        "output_dir": str(tmp_path / "raw" / "tiingo"),
        "validation_dir": str(tmp_path / "validation"),
        "token_env_var": "TIINGO_API_TOKEN",
        "price_mode": "adjusted",
        "request_interval_seconds": 0.0,
        "timeout_seconds": 12.5,
        "max_retries": 3,
        "dry_run": False,
        "overwrite": False,
    }
    values.update(overrides)
    return Namespace(**values)


def config_for(
    tmp_path: Path,
    *,
    symbols: list[str] | None = None,
    price_mode: str = "adjusted",
    opener: object | None = None,
    dry_run: bool = False,
    overwrite: bool = False,
    progress_callback: object | None = None,
) -> tiingo.TiingoDownloadConfig:
    return tiingo.TiingoDownloadConfig(
        symbols=symbols or ["SPY"],
        start_date="1999-01-01",
        end_date="2026-05-15",
        output_dir=tmp_path / "raw" / "tiingo",
        validation_dir=tmp_path / "validation",
        price_mode=price_mode,
        request_interval_seconds=0.0,
        timeout_seconds=12.5,
        max_retries=1,
        dry_run=dry_run,
        overwrite=overwrite,
        token="test-secret",
        opener=opener or RecordingOpener({"SPY": sample_payload()}),
        sleeper=lambda _seconds: None,
        progress_callback=progress_callback,
    )


def sample_payload() -> list[dict[str, object]]:
    return [
        {
            "date": "2020-01-03T00:00:00.000Z",
            "open": 100,
            "high": 110,
            "low": 90,
            "close": 105,
            "volume": 1000,
            "adjOpen": 50,
            "adjHigh": 55,
            "adjLow": 45,
            "adjClose": 52.5,
            "adjVolume": 2000,
            "divCash": 0.1,
            "splitFactor": 1,
        },
        {
            "date": "2020-01-02",
            "open": 90,
            "high": 95,
            "low": 85,
            "close": 92,
            "volume": 900,
            "adjOpen": 45,
            "adjHigh": 47.5,
            "adjLow": 42.5,
            "adjClose": 46,
            "adjVolume": 1800,
            "divCash": 0,
            "splitFactor": 1,
        },
    ]


def test_symbol_parsing_from_symbols_override_and_file(tmp_path: Path) -> None:
    symbols_file = tmp_path / "symbols.txt"
    symbols_file.write_text("# comment\nspy\nqqq, iwm\nSPY\n\n", encoding="utf-8")

    assert tiingo.load_symbols(symbols_file) == ["IWM", "QQQ", "SPY"]

    config = tiingo.config_from_args(
        args_for(tmp_path, symbols_file=str(symbols_file), symbols="gld, spy,GLD")
    )
    assert config.symbols == ["GLD", "SPY"]


def test_request_url_header_and_token_handling_without_leaking_token() -> None:
    request = tiingo.build_tiingo_prices_request(
        symbol="spy",
        start_date="1999-01-01",
        end_date="2026-05-15",
        token="test-secret",
    )

    parsed = urlparse(request.full_url)
    query = parse_qs(parsed.query)
    assert parsed.scheme == "https"
    assert parsed.path == "/tiingo/daily/SPY/prices"
    assert query["startDate"] == ["1999-01-01"]
    assert query["endDate"] == ["2026-05-15"]
    assert request.get_header("Authorization") == "Token test-secret"
    assert "test-secret" not in request.full_url


def test_adjusted_price_mapping_and_preserved_raw_fields(tmp_path: Path) -> None:
    opener = RecordingOpener({"SPY": sample_payload()})

    result = tiingo.run_download(config_for(tmp_path, opener=opener))

    rows = read_csv_rows(tmp_path / "raw" / "tiingo" / "SPY.csv")
    assert rows[0]["date"] == "2020-01-02"
    assert rows[0]["open"] == "45.0"
    assert rows[0]["high"] == "47.5"
    assert rows[0]["low"] == "42.5"
    assert rows[0]["close"] == "46.0"
    assert rows[0]["volume"] == "1800.0"
    assert rows[0]["raw_open"] == "90.0"
    assert rows[0]["adj_open"] == "45.0"
    assert rows[0]["source_provider"] == "tiingo"
    assert rows[0]["source_symbol"] == "SPY"
    assert rows[0]["price_mode"] == "adjusted"
    assert rows[0]["div_cash"] == "0.0"
    assert rows[0]["split_factor"] == "1.0"
    assert result.manifest_rows[0]["status"] == "downloaded"


def test_raw_price_mapping(tmp_path: Path) -> None:
    opener = RecordingOpener({"SPY": sample_payload()})

    tiingo.run_download(config_for(tmp_path, price_mode="raw", opener=opener))

    rows = read_csv_rows(tmp_path / "raw" / "tiingo" / "SPY.csv")
    assert rows[0]["open"] == "90.0"
    assert rows[0]["high"] == "95.0"
    assert rows[0]["low"] == "85.0"
    assert rows[0]["close"] == "92.0"
    assert rows[0]["volume"] == "900.0"
    assert rows[0]["price_mode"] == "raw"


def test_date_normalization_sorting_and_duplicate_date_last_row_wins(tmp_path: Path) -> None:
    payload = [
        {"date": "2020-01-03T00:00:00Z", "open": 3, "high": 4, "low": 2, "close": 3, "volume": 3},
        {"date": "2020-01-02", "open": 1, "high": 2, "low": 1, "close": 1, "volume": 1},
        {"date": "2020-01-02T12:30:00Z", "open": 9, "high": 10, "low": 8, "close": 9, "volume": 9},
    ]
    opener = RecordingOpener({"SPY": payload})

    result = tiingo.run_download(config_for(tmp_path, price_mode="raw", opener=opener))

    rows = read_csv_rows(tmp_path / "raw" / "tiingo" / "SPY.csv")
    assert [row["date"] for row in rows] == ["2020-01-02", "2020-01-03"]
    assert rows[0]["open"] == "9.0"
    assert result.manifest_rows[0]["warning"] == "duplicate date rows deduplicated: 1"


def test_existing_file_skip_behavior_does_not_call_http(tmp_path: Path) -> None:
    output_path = tmp_path / "raw" / "tiingo" / "SPY.csv"
    output_path.parent.mkdir(parents=True)
    output_path.write_text("date,open,high,low,close,volume\n", encoding="utf-8")

    def fail_opener(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("HTTP should not be called")

    result = tiingo.run_download(config_for(tmp_path, opener=fail_opener))

    assert result.manifest_rows[0]["status"] == "skipped_existing"
    assert output_path.read_text(encoding="utf-8") == "date,open,high,low,close,volume\n"


def test_dry_run_does_not_call_http_or_write_symbol_csvs(tmp_path: Path) -> None:
    messages: list[str] = []

    def fail_opener(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("HTTP should not be called")

    result = tiingo.run_download(
        config_for(
            tmp_path,
            symbols=["SPY", "QQQ"],
            opener=fail_opener,
            dry_run=True,
            progress_callback=messages.append,
        )
    )

    assert [row["status"] for row in result.manifest_rows] == ["planned", "planned"]
    assert not (tmp_path / "raw" / "tiingo" / "SPY.csv").exists()
    assert not (tmp_path / "raw" / "tiingo" / "QQQ.csv").exists()
    assert "planned SPY" in messages[0]


def test_manifest_and_summary_generation(tmp_path: Path) -> None:
    result = tiingo.run_download(config_for(tmp_path))

    manifest_rows = read_csv_rows(result.manifest_path)
    summary = result.summary_path.read_text(encoding="utf-8")
    assert manifest_rows[0]["symbol"] == "SPY"
    assert manifest_rows[0]["status"] == "downloaded"
    assert manifest_rows[0]["row_count"] == "2"
    assert manifest_rows[0]["first_date"] == "2020-01-02"
    assert manifest_rows[0]["last_date"] == "2020-01-03"
    assert "No S3 write happened during this run." in summary
    assert "token was read from an environment variable" in summary
    assert "--source-provider tiingo" in summary
    assert "--raw-dir data/manual_backfill/raw/tiingo" in summary


def test_failed_symbol_is_recorded_and_does_not_stop_other_symbols(tmp_path: Path) -> None:
    error = HTTPError(
        "https://api.tiingo.com/tiingo/daily/BAD/prices",
        404,
        "not found",
        None,
        None,
    )
    opener = RecordingOpener({"BAD": error, "SPY": sample_payload()})

    result = tiingo.run_download(config_for(tmp_path, symbols=["BAD", "SPY"], opener=opener))

    rows_by_symbol = {row["symbol"]: row for row in result.manifest_rows}
    assert rows_by_symbol["BAD"]["status"] == "failed"
    assert "HTTP 404" in rows_by_symbol["BAD"]["warning"]
    assert rows_by_symbol["SPY"]["status"] == "downloaded"
    assert (tmp_path / "raw" / "tiingo" / "SPY.csv").exists()
    assert not tiingo.all_requested_symbols_failed(result)


def test_all_requested_symbols_failed_helper(tmp_path: Path) -> None:
    error = HTTPError(
        "https://api.tiingo.com/tiingo/daily/BAD/prices",
        404,
        "not found",
        None,
        None,
    )
    opener = RecordingOpener({"BAD": error})

    result = tiingo.run_download(config_for(tmp_path, symbols=["BAD"], opener=opener))

    assert tiingo.all_requested_symbols_failed(result)
