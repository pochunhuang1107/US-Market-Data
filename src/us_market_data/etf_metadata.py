from __future__ import annotations

import io
from datetime import datetime, timezone
from typing import Iterable

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

METADATA_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string()),
        pa.field("first_available_date", pa.string()),
        pa.field("last_available_date", pa.string()),
        pa.field("updated_at", pa.string()),
    ]
)


def empty_metadata_table() -> pa.Table:
    arrays = [pa.array([], type=field.type) for field in METADATA_SCHEMA]
    return pa.Table.from_arrays(arrays, schema=METADATA_SCHEMA)


def object_exists(s3_client, bucket: str, key: str) -> bool:
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def load_metadata_table(s3_client, bucket: str, key: str) -> pa.Table:
    if not object_exists(s3_client, bucket, key):
        return empty_metadata_table()

    response = s3_client.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read()
    table = pq.read_table(io.BytesIO(body))
    arrays = []
    for field in METADATA_SCHEMA:
        if field.name in table.column_names:
            arrays.append(pc.cast(table[field.name], field.type))
        else:
            arrays.append(pa.array([None] * table.num_rows, type=field.type))
    return pa.Table.from_arrays(arrays, schema=METADATA_SCHEMA)


def write_metadata_table(s3_client, bucket: str, key: str, table: pa.Table) -> str:
    out = io.BytesIO()
    pq.write_table(table, out, compression="snappy")
    out.seek(0)
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=out.getvalue(),
        ContentType="application/octet-stream",
    )
    return key


def _to_rows(table: pa.Table) -> dict[str, dict[str, str]]:
    if table.num_rows == 0:
        return {}
    rows: dict[str, dict[str, str]] = {}
    for row in table.to_pylist():
        symbol = row["symbol"]
        if symbol:
            rows[str(symbol)] = {
                "symbol": str(symbol),
                "first_available_date": str(row["first_available_date"]),
                "last_available_date": str(row["last_available_date"]),
                "updated_at": str(row["updated_at"]),
            }
    return rows


def _from_rows(rows: Iterable[dict[str, str]]) -> pa.Table:
    ordered = sorted(rows, key=lambda r: r["symbol"])
    return pa.Table.from_pylist(ordered, schema=METADATA_SCHEMA) if ordered else empty_metadata_table()


def upsert_last_available_dates(
    table: pa.Table,
    *,
    symbols: Iterable[str],
    date_str: str,
    updated_at: str | None = None,
) -> pa.Table:
    now_utc = updated_at or datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    existing = _to_rows(table)

    for symbol in sorted(set(symbols)):
        current = existing.get(symbol)
        if current is None:
            existing[symbol] = {
                "symbol": symbol,
                "first_available_date": date_str,
                "last_available_date": date_str,
                "updated_at": now_utc,
            }
            continue

        current["last_available_date"] = max(current["last_available_date"], date_str)
        current["updated_at"] = now_utc

    return _from_rows(existing.values())


def build_metadata_table_from_ranges(symbol_ranges: dict[str, tuple[str, str]]) -> pa.Table:
    now_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    rows = []
    for symbol, (first_date, last_date) in sorted(symbol_ranges.items()):
        rows.append(
            {
                "symbol": symbol,
                "first_available_date": first_date,
                "last_available_date": last_date,
                "updated_at": now_utc,
            }
        )
    return _from_rows(rows)
