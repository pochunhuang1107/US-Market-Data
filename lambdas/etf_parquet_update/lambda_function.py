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

s3 = boto3.client("s3")
sns = boto3.client("sns")

BUCKET_NAME = os.environ["BUCKET_NAME"]
STAGING_PREFIX = os.environ.get("STAGING_PREFIX", "staging/market_bars")
CURATED_PREFIX = os.environ.get("CURATED_PREFIX", "curated/etf_daily_prices")
METADATA_KEY = os.environ.get(
    "ETF_METADATA_KEY",
    "curated/reference/etf_symbol_metadata/data.parquet",
)
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
ETF_SYMBOLS = {
    s.strip() for s in os.environ["ETF_SYMBOLS"].split(",") if s.strip()
}

PRICE_SCHEMA = pa.schema(
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

METADATA_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string()),
        pa.field("first_available_date", pa.string()),
        pa.field("last_available_date", pa.string()),
        pa.field("updated_at", pa.string()),
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


def staging_key_for_month(date_str: str) -> str:
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return f"{STAGING_PREFIX}/year={dt.year}/month={dt.month:02d}/data.parquet"


def curated_key(symbol: str, year: int) -> str:
    return f"{CURATED_PREFIX}/symbol={symbol}/year={year}/data.parquet"


def object_exists(bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def empty_price_table() -> pa.Table:
    arrays = [pa.array([], type=field.type) for field in PRICE_SCHEMA]
    return pa.Table.from_arrays(arrays, schema=PRICE_SCHEMA)


def empty_metadata_table() -> pa.Table:
    arrays = [pa.array([], type=field.type) for field in METADATA_SCHEMA]
    return pa.Table.from_arrays(arrays, schema=METADATA_SCHEMA)


def align_price_table_to_schema(table: pa.Table) -> pa.Table:
    arrays = []
    for field in PRICE_SCHEMA:
        if field.name in table.column_names:
            arrays.append(pc.cast(table[field.name], field.type))
        else:
            arrays.append(pa.array([None] * table.num_rows, type=field.type))
    return pa.Table.from_arrays(arrays, schema=PRICE_SCHEMA)


def align_metadata_table_to_schema(table: pa.Table) -> pa.Table:
    arrays = []
    for field in METADATA_SCHEMA:
        if field.name in table.column_names:
            arrays.append(pc.cast(table[field.name], field.type))
        else:
            arrays.append(pa.array([None] * table.num_rows, type=field.type))
    return pa.Table.from_arrays(arrays, schema=METADATA_SCHEMA)


def load_staging_month_table(date_str: str) -> pa.Table:
    key = staging_key_for_month(date_str)

    if not object_exists(BUCKET_NAME, key):
        raise FileNotFoundError(f"Missing staging parquet: s3://{BUCKET_NAME}/{key}")

    response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    body = response["Body"].read()
    table = pq.read_table(io.BytesIO(body))
    return align_price_table_to_schema(table)


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
    key = curated_key(symbol, year)

    if not object_exists(BUCKET_NAME, key):
        return empty_price_table()

    response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    body = response["Body"].read()
    table = pq.read_table(io.BytesIO(body))
    return align_price_table_to_schema(table)


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


def dedupe_and_sort_price_table(table: pa.Table) -> pa.Table:
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
    return dedupe_and_sort_price_table(merged)


def write_curated_table(symbol: str, year: int, table: pa.Table) -> str:
    key = curated_key(symbol, year)
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


def load_metadata_table() -> pa.Table:
    if not object_exists(BUCKET_NAME, METADATA_KEY):
        return empty_metadata_table()

    response = s3.get_object(Bucket=BUCKET_NAME, Key=METADATA_KEY)
    body = response["Body"].read()
    table = pq.read_table(io.BytesIO(body))
    return align_metadata_table_to_schema(table)


def metadata_table_to_rows(table: pa.Table) -> Dict[str, Dict[str, str]]:
    if table.num_rows == 0:
        return {}

    symbols = table["symbol"].to_pylist()
    firsts = table["first_available_date"].to_pylist()
    lasts = table["last_available_date"].to_pylist()
    updateds = table["updated_at"].to_pylist()

    rows: Dict[str, Dict[str, str]] = {}
    for symbol, first_date, last_date, updated_at in zip(symbols, firsts, lasts, updateds):
        rows[str(symbol)] = {
            "symbol": str(symbol),
            "first_available_date": str(first_date),
            "last_available_date": str(last_date),
            "updated_at": str(updated_at),
        }
    return rows


def metadata_rows_to_table(rows: Dict[str, Dict[str, str]]) -> pa.Table:
    if not rows:
        return empty_metadata_table()

    ordered_symbols = sorted(rows.keys())
    return pa.Table.from_arrays(
        [
            pa.array([rows[s]["symbol"] for s in ordered_symbols], type=pa.string()),
            pa.array([rows[s]["first_available_date"] for s in ordered_symbols], type=pa.string()),
            pa.array([rows[s]["last_available_date"] for s in ordered_symbols], type=pa.string()),
            pa.array([rows[s]["updated_at"] for s in ordered_symbols], type=pa.string()),
        ],
        schema=METADATA_SCHEMA,
    )


def update_metadata_table(
    metadata_table: pa.Table,
    *,
    touched_symbols: List[str],
    date_str: str,
) -> pa.Table:
    rows = metadata_table_to_rows(metadata_table)
    now_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    for symbol in sorted(set(touched_symbols)):
        existing = rows.get(symbol)
        if existing is None:
            rows[symbol] = {
                "symbol": symbol,
                "first_available_date": date_str,
                "last_available_date": date_str,
                "updated_at": now_utc,
            }
            continue

        existing_first = existing["first_available_date"]
        existing_last = existing["last_available_date"]

        rows[symbol] = {
            "symbol": symbol,
            "first_available_date": min(existing_first, date_str),
            "last_available_date": max(existing_last, date_str),
            "updated_at": now_utc,
        }

    return metadata_rows_to_table(rows)


def write_metadata_table(table: pa.Table) -> str:
    out = io.BytesIO()
    pq.write_table(table, out, compression="snappy")
    out.seek(0)

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=METADATA_KEY,
        Body=out.getvalue(),
        ContentType="application/octet-stream",
    )
    return METADATA_KEY


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
                f"s3://{BUCKET_NAME}/{staging_key_for_month(date_str)}"
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

        for (symbol, year), new_table in sorted(grouped.items()):
            existing_table = load_existing_curated_table(symbol, year)
            merged_table = merge_curated_data(existing_table, new_table, date_str)
            key = write_curated_table(symbol, year, merged_table)
            written_keys.append(key)
            rows_written += new_table.num_rows

        touched_symbols = sorted(set(etf_table["symbol"].to_pylist()))
        metadata_table = load_metadata_table()
        updated_metadata_table = update_metadata_table(
            metadata_table,
            touched_symbols=touched_symbols,
            date_str=date_str,
        )
        metadata_key = write_metadata_table(updated_metadata_table)

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
            "metadata_symbols_touched": len(touched_symbols),
            "status": "SUCCEEDED",
        }

    except Exception as e:
        msg = f"Curated parquet Lambda failed for event={json.dumps(event)}\n\nError: {str(e)}"
        publish_sns("us-market-data curated failure", msg)
        raise