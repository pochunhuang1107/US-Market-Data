from __future__ import annotations
# ruff: noqa: E402,I001

import io
import json
import sys
import types
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from us_market_data import ticker_reference_diagnostics as diagnostics  # noqa: E402


def sample_rows() -> list[dict[str, object]]:
    return [
        {
            "symbol": "AAPL",
            "name": "Apple Inc.",
            "market": "stocks",
            "locale": "us",
            "type": "CS",
            "active": True,
            "primary_exchange": "XNAS",
            "is_common_stock_candidate": True,
        },
        {
            "symbol": "TQQQ",
            "name": "ProShares UltraPro QQQ",
            "market": "stocks",
            "locale": "us",
            "type": "ETF",
            "active": True,
            "primary_exchange": "XNAS",
            "is_common_stock_candidate": False,
        },
        {
            "symbol": "VRP",
            "name": "Invesco Variable Rate Preferred ETF",
            "market": "stocks",
            "locale": "us",
            "type": "ETF",
            "active": True,
            "primary_exchange": "ARCX",
            "is_common_stock_candidate": False,
        },
        {
            "symbol": "MYST",
            "name": "Unknown Type Co.",
            "market": "stocks",
            "locale": "us",
            "type": None,
            "active": True,
            "primary_exchange": None,
            "is_common_stock_candidate": False,
        },
    ]


def write_sample_parquet(path: Path) -> None:
    table = pa.Table.from_pylist(sample_rows())
    pq.write_table(table, path)


def parquet_bytes() -> bytes:
    out = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(sample_rows()), out)
    return out.getvalue()


def test_local_parquet_diagnostics_with_tmp_path(tmp_path: Path) -> None:
    path = tmp_path / "ticker_reference.parquet"
    write_sample_parquet(path)

    rows = diagnostics.read_parquet_rows(str(path))
    summary = diagnostics.build_diagnostics(rows, symbols=["AAPL", "NOPE"], top_n=2)

    assert summary["total_count"] == 4
    assert summary["common_stock_candidate_count"] == 1
    assert summary["non_common_stock_count"] == 3
    assert summary["missing_unknown_type_count"] == 1
    assert summary["non_common_stock_samples_by_type"]["ETF"] == ["TQQQ", "VRP"]


def test_type_distribution_computed_correctly() -> None:
    summary = diagnostics.build_diagnostics(sample_rows())

    assert summary["type_distribution"] == {"ETF": 2, "<missing>": 1, "CS": 1}
    assert summary["primary_exchange_distribution"] == {"XNAS": 2, "<missing>": 1, "ARCX": 1}


def test_symbol_checks_identify_found_and_missing_symbols() -> None:
    checks = diagnostics.symbol_checks(sample_rows(), ["aapl", "missing"])

    assert checks[0]["status"] == "found"
    assert checks[0]["symbol"] == "AAPL"
    assert checks[0]["name"] == "Apple Inc."
    assert checks[0]["is_common_stock_candidate"] is True
    assert checks[1] == {
        "symbol": "MISSING",
        "name": None,
        "type": None,
        "market": None,
        "locale": None,
        "active": None,
        "primary_exchange": None,
        "is_common_stock_candidate": None,
        "status": "missing",
    }


def test_output_json_writes_expected_structure(tmp_path: Path) -> None:
    summary = diagnostics.build_diagnostics(sample_rows(), symbols=["AAPL", "NOPE"])
    output_path = tmp_path / "nested" / "summary.json"

    diagnostics.write_json_summary(output_path, summary)
    payload = json.loads(output_path.read_text(encoding="utf-8"))

    assert payload.keys() == {
        "total_count",
        "common_stock_candidate_count",
        "non_common_stock_count",
        "type_distribution",
        "primary_exchange_distribution",
        "symbol_checks",
    }
    assert payload["total_count"] == 4
    assert payload["symbol_checks"][0]["symbol"] == "AAPL"
    assert payload["symbol_checks"][1]["status"] == "missing"


def test_s3_read_path_uses_mocked_boto3_get_object(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    calls: list[dict[str, str]] = []

    class FakeBody:
        def read(self) -> bytes:
            return parquet_bytes()

    class FakeS3Client:
        def get_object(self, **kwargs: str) -> dict[str, object]:
            calls.append(kwargs)
            return {"Body": FakeBody()}

    fake_boto3 = types.SimpleNamespace(client=lambda service: FakeS3Client())
    monkeypatch.setitem(sys.modules, "boto3", fake_boto3)

    rows = diagnostics.read_parquet_rows("s3://example-bucket/reference/data.parquet")

    assert calls == [{"Bucket": "example-bucket", "Key": "reference/data.parquet"}]
    assert rows[0]["symbol"] == "AAPL"
