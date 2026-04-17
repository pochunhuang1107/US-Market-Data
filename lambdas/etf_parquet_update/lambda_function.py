import io
import json
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Tuple

import boto3
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from us_market_data.etf_metadata import (
    load_metadata_table,
    upsert_last_available_dates,
    write_metadata_table,
)
from us_market_data.s3_paths import curated_key, etf_metadata_key, staging_key_for_month

s3 = boto3.client("s3")
sns = boto3.client("sns")

BUCKET_NAME = os.environ["BUCKET_NAME"]
STAGING_PREFIX = os.environ.get("STAGING_PREFIX", "staging/market_bars")
CURATED_PREFIX = os.environ.get("CURATED_PREFIX", "curated/etf_daily_prices")
REFERENCE_PREFIX = os.environ.get("REFERENCE_PREFIX", "curated/reference")
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
ETF_SYMBOLS = {
    s.strip() for s in os.environ["ETF_SYMBOLS"].split(",") if s.strip()
}

SCHEMA = pa.schema(
    [
        pa.field("date", pa.string()),
        pa.field("year", pa.int32()),
        pa.field("month", pa.int32()),
        pa.field("symbol", pa.string()),
        pa.field("open", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("close", pa.float64()),
        pa.field("volume", pa.float64()),
        pa.field("vwap", pa.float64()),
        pa.field("transactions", pa.float64()),
    ]
)


def publish_sns(subject: str, message: str) -> None:
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject[:100],
        Message=message,
    )


def resolve_target_date(event: Any) -> str:
    if isinstance(event, dict) and event.get("date"):
        return event["date"]

    mode = event.get("mode") if isinstance(event, dict) else None
    ny_now = datetime.now(ZoneInfo("America/New_York")).date()

    if mode == "close":
        return ny_now.isoformat()

    if mode == "finalize":
        return (ny_now - timedelta(days=1)).isoformat()

    return (ny_now - timedelta(days=1)).isoformat()


def is_weekend(date_str: str) -> bool:
    dt = datetime.strptime(date_str, "%Y-%m-%d").date()
    return dt.weekday() >= 5


def load_holidays_from_s3(year: int) -> set[str]:
    key = f"raw/reference/market_holidays/year={year}/holidays.json"

    try:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        body = response["Body"].read().decode("utf-8")
        payload = json.loads(body)

        holidays = set()
        for item in payload.get("results", []):
            holiday_date = item.get("date")
            if holiday_date:
                holidays.add(holiday_date)

        return holidays

    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in {"NoSuchKey", "404", "NotFound"}:
            print(f"Holiday file not found for year {year}, skipping holiday check")
            return set()
        raise

    except Exception as e:
        print(f"Could not load holiday file for year {year}: {str(e)}")
        return set()


def is_market_holiday(date_str: str) -> bool:
    year = datetime.strptime(date_str, "%Y-%m-%d").year
    holidays = load_holidays_from_s3(year)
    return date_str in holidays


def object_exists(bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def empty_table() -> pa.Table:
    arrays = [pa.array([], type=field.type) for field in SCHEMA]
    return pa.Table.from_arrays(arrays, schema=SCHEMA)


def align_table_to_schema(table: pa.Table) -> pa.Table:
    arrays = []
    for field in SCHEMA:
        if field.name in table.column_names:
            arrays.append(pc.cast(table[field.name], field.type))
        else:
            arrays.append(pa.array([None] * table.num_rows, type=field.type))
    return pa.Table.from_arrays(arrays, schema=SCHEMA)


def load_staging_month_table(date_str: str) -> pa.Table:
    key = staging_key_for_month(STAGING_PREFIX, date_str)

    if not object_exists(BUCKET_NAME, key):
        raise FileNotFoundError(f"Missing staging parquet: s3://{BUCKET_NAME}/{key}")

    response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    body = response["Body"].read()
    table = pq.read_table(io.BytesIO(body))
    return align_table_to_schema(table)


def filter_table_for_date(table: pa.Table, date_str: str) -> pa.Table:
    if table.num_rows == 0:
        return table
    mask = pc.equal(table["date"], pa.scalar(date_str, type=pa.string()))
    return table.filter(mask)


def filter_table_for_etfs(table: pa.Table) -> pa.Table:
    if table.num_rows == 0:
        return table

    symbols = table["symbol"].to_pylist()
    keep = [symbol in ETF_SYMBOLS for symbol in symbols]
    return table.filter(pa.array(keep, type=pa.bool_()))


def load_existing_curated_table(symbol: str, year: int) -> pa.Table:
    key = curated_key(CURATED_PREFIX, symbol, year)

    if not object_exists(BUCKET_NAME, key):
        return empty_table()

    response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    body = response["Body"].read()
    table = pq.read_table(io.BytesIO(body))
    return align_table_to_schema(table)


def split_table_by_symbol_year(table: pa.Table) -> Dict[Tuple[str, int], pa.Table]:
    if table.num_rows == 0:
        return {}

    symbols = table["symbol"].to_pylist()
    years = table["year"].to_pylist()

    groups: Dict[Tuple[str, int], List[int]] = defaultdict(list)
    for i, (symbol, year) in enumerate(zip(symbols, years)):
        groups[(symbol, int(year))].append(i)

    result: Dict[Tuple[str, int], pa.Table] = {}
    for key, indices in groups.items():
        idx_array = pa.array(indices, type=pa.int64())
        result[key] = table.take(idx_array)

    return result


def drop_target_date(table: pa.Table, date_str: str) -> pa.Table:
    if table.num_rows == 0:
        return table
    mask = pc.not_equal(table["date"], pa.scalar(date_str, type=pa.string()))
    return table.filter(mask)


def dedupe_and_sort(table: pa.Table) -> pa.Table:
    if table.num_rows == 0:
        return table

    sort_indices = pc.sort_indices(
        table,
        sort_keys=[("date", "ascending"), ("symbol", "ascending")]
    )
    sorted_table = table.take(sort_indices)

    seen = set()
    keep = []
    dates = sorted_table["date"].to_pylist()
    symbols = sorted_table["symbol"].to_pylist()

    for key in zip(dates, symbols):
        if key not in seen:
            seen.add(key)
            keep.append(True)
        else:
            keep.append(False)

    return sorted_table.filter(pa.array(keep, type=pa.bool_()))


def merge_curated_data(existing_table: pa.Table, new_table: pa.Table, date_str: str) -> pa.Table:
    existing_without_date = drop_target_date(existing_table, date_str)
    merged = pa.concat_tables([existing_without_date, new_table], promote_options="default")
    return dedupe_and_sort(merged)


def write_curated_table(symbol: str, year: int, table: pa.Table) -> str:
    key = curated_key(CURATED_PREFIX, symbol, year)
    out = io.BytesIO()
    pq.write_table(table, out, compression="snappy")
    out.seek(0)

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=out.getvalue(),
        ContentType="application/octet-stream",
    )
    return key


def update_etf_metadata_for_symbols(symbols_touched: set[str], date_str: str) -> str:
    metadata_key = etf_metadata_key(REFERENCE_PREFIX)
    metadata_table = load_metadata_table(s3, BUCKET_NAME, metadata_key)
    updated_table = upsert_last_available_dates(
        metadata_table,
        symbols=symbols_touched,
        date_str=date_str,
        updated_at=datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    )
    return write_metadata_table(s3, BUCKET_NAME, metadata_key, updated_table)


def lambda_handler(event, context):
    try:
        date_str = resolve_target_date(event)

        if is_weekend(date_str):
            return {
                "statusCode": 200,
                "message": "Skipped weekend date",
                "date": date_str,
                "skipped": True,
                "reason": "weekend",
                "status": "SKIPPED",
            }

        if is_market_holiday(date_str):
            return {
                "statusCode": 200,
                "message": "Skipped market holiday",
                "date": date_str,
                "skipped": True,
                "reason": "market_holiday",
            }

        try:
            staging_table = load_staging_month_table(date_str)
        except FileNotFoundError as e:
            msg = str(e)
            publish_sns("us-market-data staging missing", msg)
            return {
                "statusCode": 200,
                "message": msg,
                "date": date_str,
                "skipped": True,
                "reason": "missing_staging",
                "status": "SKIPPED",
            }

        date_table = filter_table_for_date(staging_table, date_str)
        etf_table = filter_table_for_etfs(date_table)

        if etf_table.num_rows == 0:
            msg = (
                f"No ETF rows found in staging for {date_str} at "
                f"s3://{BUCKET_NAME}/{staging_key_for_month(STAGING_PREFIX, date_str)}"
            )
            publish_sns("us-market-data curated empty", msg)
            return {
                "statusCode": 200,
                "message": msg,
                "date": date_str,
                "skipped": True,
                "reason": "no_etf_rows",
                "status": "SKIPPED",
            }

        grouped = split_table_by_symbol_year(etf_table)

        written_keys = []
        rows_written = 0
        symbols_touched: set[str] = set()

        for (symbol, year), new_table in sorted(grouped.items()):
            existing_table = load_existing_curated_table(symbol, year)
            merged_table = merge_curated_data(existing_table, new_table, date_str)
            key = write_curated_table(symbol, year, merged_table)
            written_keys.append(key)
            rows_written += new_table.num_rows
            symbols_touched.add(symbol)

        metadata_key = update_etf_metadata_for_symbols(symbols_touched, date_str)

        return {
            "statusCode": 200,
            "message": "Curated ETF parquet update completed",
            "date": date_str,
            "skipped": False,
            "symbols_updated": len(grouped),
            "rows_for_date": etf_table.num_rows,
            "rows_written": rows_written,
            "sample_keys": written_keys[:10],
            "metadata_key": metadata_key,
            "status": "SUCCEEDED",
        }

    except Exception as e:
        msg = f"Curated parquet Lambda failed for event={json.dumps(event)}\n\nError: {str(e)}"
        publish_sns("us-market-data curated failure", msg)
        raise
