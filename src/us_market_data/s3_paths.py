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


def raw_apewisdom_key(
    raw_prefix: str,
    date_str: str,
    hour_str: str,
    filter_name: str,
) -> str:
    return f"{raw_prefix}/date={date_str}/hour={hour_str}/filter={filter_name}/data.json"


def staging_reddit_trends_key(staging_prefix: str, date_str: str) -> str:
    return f"{staging_prefix}/date={date_str}/data.parquet"


def raw_ticker_reference_key(raw_prefix: str, date_str: str, page: int) -> str:
    return f"{raw_prefix}/date={date_str}/page={page}/data.json"


def curated_ticker_reference_key(reference_prefix_or_key: str) -> str:
    value = reference_prefix_or_key.strip("/")
    if value.endswith(".parquet"):
        return value
    return f"{value}/ticker_reference/data.parquet"
