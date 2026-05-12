from __future__ import annotations

import io
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pyarrow.parquet as pq

DEFAULT_INPUT_PATH = "s3://us-market-data-casper/curated/reference/ticker_reference/data.parquet"
DEFAULT_SYMBOLS = [
    "AAPL",
    "MSFT",
    "NVDA",
    "IONQ",
    "HOOD",
    "ASTS",
    "SPY",
    "QQQ",
    "VOO",
    "TQQQ",
    "VRP",
    "TAXX",
    "PSFF",
    "BKUI",
    "PSQA",
]

SYMBOL_CHECK_FIELDS = [
    "symbol",
    "name",
    "type",
    "market",
    "locale",
    "active",
    "primary_exchange",
    "is_common_stock_candidate",
]


def parse_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.lstrip("/"):
        raise ValueError(f"invalid S3 URI: {uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def read_parquet_rows(input_path: str, s3_client: Any | None = None) -> list[dict[str, Any]]:
    if input_path.startswith("s3://"):
        bucket, key = parse_s3_uri(input_path)
        if s3_client is None:
            import boto3

            s3_client = boto3.client("s3")
        response = s3_client.get_object(Bucket=bucket, Key=key)
        body = response["Body"].read()
        table = pq.read_table(io.BytesIO(body))
    else:
        table = pq.read_table(input_path)
    return table.to_pylist()


def normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper()


def label_value(value: Any) -> str:
    if value is None:
        return "<missing>"
    text = str(value).strip()
    return text if text else "<missing>"


def is_missing_or_unknown_type(value: Any) -> bool:
    text = label_value(value)
    return text == "<missing>" or text.upper() == "UNKNOWN"


def distribution(rows: list[dict[str, Any]], column: str) -> dict[str, int]:
    counts = Counter(label_value(row.get(column)) for row in rows)
    return {
        key: value
        for key, value in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    }


def symbol_checks(rows: list[dict[str, Any]], symbols: list[str]) -> list[dict[str, Any]]:
    by_symbol = {
        normalize_symbol(str(row.get("symbol"))): row
        for row in rows
        if row.get("symbol") is not None and normalize_symbol(str(row.get("symbol")))
    }
    checks: list[dict[str, Any]] = []

    for raw_symbol in symbols:
        symbol = normalize_symbol(raw_symbol)
        row = by_symbol.get(symbol)
        if row is None:
            checks.append(
                {
                    "symbol": symbol,
                    "name": None,
                    "type": None,
                    "market": None,
                    "locale": None,
                    "active": None,
                    "primary_exchange": None,
                    "is_common_stock_candidate": None,
                    "status": "missing",
                }
            )
            continue

        checks.append(
            {
                **{field: row.get(field) for field in SYMBOL_CHECK_FIELDS},
                "symbol": symbol,
                "status": "found",
            }
        )

    return checks


def non_common_stock_samples_by_type(
    rows: list[dict[str, Any]],
    top_n: int,
) -> dict[str, list[str]]:
    samples: dict[str, list[str]] = defaultdict(list)
    for row in sorted(rows, key=lambda item: str(item.get("symbol") or "")):
        if row.get("is_common_stock_candidate") is True:
            continue
        type_label = label_value(row.get("type"))
        if len(samples[type_label]) >= top_n:
            continue
        symbol = row.get("symbol")
        if symbol is not None and str(symbol).strip():
            samples[type_label].append(str(symbol).strip().upper())

    return dict(sorted(samples.items()))


def build_diagnostics(
    rows: list[dict[str, Any]],
    symbols: list[str] | None = None,
    top_n: int = 25,
) -> dict[str, Any]:
    symbol_list = symbols if symbols else DEFAULT_SYMBOLS
    total_count = len(rows)
    common_stock_candidate_count = sum(
        1 for row in rows if row.get("is_common_stock_candidate") is True
    )
    missing_unknown_type_count = sum(
        1 for row in rows if is_missing_or_unknown_type(row.get("type"))
    )

    return {
        "total_count": total_count,
        "common_stock_candidate_count": common_stock_candidate_count,
        "non_common_stock_count": total_count - common_stock_candidate_count,
        "type_distribution": distribution(rows, "type"),
        "primary_exchange_distribution": distribution(rows, "primary_exchange"),
        "non_common_stock_samples_by_type": non_common_stock_samples_by_type(rows, top_n),
        "missing_unknown_type_count": missing_unknown_type_count,
        "symbol_checks": symbol_checks(rows, symbol_list),
    }


def json_summary(diagnostics: dict[str, Any]) -> dict[str, Any]:
    return {
        "total_count": diagnostics["total_count"],
        "common_stock_candidate_count": diagnostics["common_stock_candidate_count"],
        "non_common_stock_count": diagnostics["non_common_stock_count"],
        "type_distribution": diagnostics["type_distribution"],
        "primary_exchange_distribution": diagnostics["primary_exchange_distribution"],
        "symbol_checks": diagnostics["symbol_checks"],
    }


def write_json_summary(path: str | Path, diagnostics: dict[str, Any]) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(json_summary(diagnostics), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return output_path
