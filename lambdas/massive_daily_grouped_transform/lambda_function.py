import io
import json
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Any, Dict, List

import boto3
from botocore.exceptions import ClientError

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from us_market_data.s3_paths import raw_key_for_date, staging_key_for_month

s3 = boto3.client("s3")
sns = boto3.client("sns")

BUCKET_NAME = os.environ["BUCKET_NAME"]
RAW_PREFIX = os.environ.get("RAW_PREFIX", "raw/massive/daily_market_summary")
STAGING_PREFIX = os.environ.get("STAGING_PREFIX", "staging/market_bars")
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]


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


def read_json_from_s3(bucket: str, key: str) -> Dict[str, Any]:
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read().decode("utf-8")
    return json.loads(body)


def load_raw_data(date_str: str) -> Dict[str, Any]:
    return read_json_from_s3(BUCKET_NAME, raw_key_for_date(RAW_PREFIX, date_str))


def safe_float(v):
    if v is None:
        return None
    return float(v)


def normalize_rows(payload: Dict[str, Any], date_str: str) -> List[Dict[str, Any]]:
    results = payload.get("results", [])
    dt = datetime.strptime(date_str, "%Y-%m-%d")

    rows: List[Dict[str, Any]] = []
    for r in results:
        symbol = r.get("T")
        if not symbol:
            continue

        rows.append(
            {
                "date": date_str,
                "year": dt.year,
                "month": dt.month,
                "symbol": str(symbol),
                "open": safe_float(r.get("o")),
                "high": safe_float(r.get("h")),
                "low": safe_float(r.get("l")),
                "close": safe_float(r.get("c")),
                "volume": safe_float(r.get("v")),
                "vwap": safe_float(r.get("vw")),
                "transactions": safe_float(r.get("n")),
            }
        )

    return rows


def empty_table() -> pa.Table:
    arrays = [pa.array([], type=field.type) for field in SCHEMA]
    return pa.Table.from_arrays(arrays, schema=SCHEMA)


def rows_to_table(rows: List[Dict[str, Any]]) -> pa.Table:
    if not rows:
        return empty_table()

    columns = {name: [] for name in SCHEMA.names}
    for row in rows:
        for name in SCHEMA.names:
            columns[name].append(row.get(name))

    arrays = [
        pa.array(columns["date"], type=pa.string()),
        pa.array(columns["year"], type=pa.int32()),
        pa.array(columns["month"], type=pa.int32()),
        pa.array(columns["symbol"], type=pa.string()),
        pa.array(columns["open"], type=pa.float64()),
        pa.array(columns["high"], type=pa.float64()),
        pa.array(columns["low"], type=pa.float64()),
        pa.array(columns["close"], type=pa.float64()),
        pa.array(columns["volume"], type=pa.float64()),
        pa.array(columns["vwap"], type=pa.float64()),
        pa.array(columns["transactions"], type=pa.float64()),
    ]
    return pa.Table.from_arrays(arrays, schema=SCHEMA)


def load_existing_month_table(date_str: str) -> pa.Table:
    key = staging_key_for_month(STAGING_PREFIX, date_str)

    if not object_exists(BUCKET_NAME, key):
        return empty_table()

    response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    body = response["Body"].read()
    table = pq.read_table(io.BytesIO(body))

    arrays = []
    for field in SCHEMA:
        if field.name in table.column_names:
            col = table[field.name]
            try:
                arrays.append(pc.cast(col, field.type))
            except Exception:
                if pa.types.is_floating(field.type):
                    arrays.append(pc.cast(col, pa.float64()))
                elif pa.types.is_integer(field.type):
                    arrays.append(pc.cast(pc.floor(col), field.type))
                else:
                    arrays.append(pc.cast(col, field.type))
        else:
            arrays.append(pa.array([None] * table.num_rows, type=field.type))

    return pa.Table.from_arrays(arrays, schema=SCHEMA)


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
    date_col = sorted_table["date"].to_pylist()
    symbol_col = sorted_table["symbol"].to_pylist()

    for i, key in enumerate(zip(date_col, symbol_col)):
        if key not in seen:
            seen.add(key)
            keep.append(True)
        else:
            keep.append(False)

    return sorted_table.filter(pa.array(keep, type=pa.bool_()))


def merge_month_data(existing_table: pa.Table, new_rows: List[Dict[str, Any]], date_str: str) -> pa.Table:
    new_table = rows_to_table(new_rows)
    existing_without_date = drop_target_date(existing_table, date_str)

    merged = pa.concat_tables(
        [existing_without_date, new_table],
        promote_options="default",
    )

    return dedupe_and_sort(merged)


def write_month_table_to_s3(table: pa.Table, date_str: str) -> str:
    key = staging_key_for_month(STAGING_PREFIX, date_str)
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
            raw_payload = load_raw_data(date_str)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in {"NoSuchKey", "404", "NotFound"}:
                msg = (
                    f"No raw data found for {date_str} at "
                    f"s3://{BUCKET_NAME}/{raw_key_for_date(RAW_PREFIX, date_str)}"
                )
                publish_sns("us-market-data staging raw missing", msg)
                return {
                    "statusCode": 200,
                    "message": msg,
                    "date": date_str,
                    "skipped": True,
                    "reason": "missing_raw",
                    "status": "SKIPPED",
                }
            raise

        rows = normalize_rows(raw_payload, date_str)

        if not rows:
            msg = (
                f"No market rows found in raw payload for {date_str} at "
                f"s3://{BUCKET_NAME}/{raw_key_for_date(RAW_PREFIX, date_str)}"
            )
            publish_sns("us-market-data staging empty", msg)
            raise RuntimeError(msg)

        existing_table = load_existing_month_table(date_str)
        merged_table = merge_month_data(existing_table, rows, date_str)
        s3_key = write_month_table_to_s3(merged_table, date_str)

        return {
            "statusCode": 200,
            "message": "Monthly staging parquet updated",
            "date": date_str,
            "rows_for_date": len(rows),
            "rows_in_month_file": merged_table.num_rows,
            "s3_key": s3_key,
            "overwritten_if_exists": True,
            "skipped": False,
            "status": "SUCCEEDED",
        }

    except Exception as e:
        msg = f"Staging Lambda failed for event={json.dumps(event)}\n\nError: {str(e)}"
        publish_sns("us-market-data staging failure", msg)
        raise
