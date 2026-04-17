from __future__ import annotations


def raw_key_for_date(raw_prefix: str, date_str: str) -> str:
    return f"{raw_prefix}/date={date_str}/data.json"


def staging_key_for_month(staging_prefix: str, date_str: str) -> str:
    year, month, _ = date_str.split("-")
    return f"{staging_prefix}/year={year}/month={month}/data.parquet"


def curated_key(curated_prefix: str, symbol: str, year: int) -> str:
    return f"{curated_prefix}/symbol={symbol}/year={year}/data.parquet"


def etf_metadata_key(reference_prefix: str) -> str:
    return f"{reference_prefix}/etf_symbol_metadata/data.parquet"
