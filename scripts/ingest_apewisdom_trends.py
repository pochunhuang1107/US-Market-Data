#!/usr/bin/env python3
# ruff: noqa: E402
from __future__ import annotations

import argparse
import io
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from us_market_data.s3_paths import raw_apewisdom_key, staging_reddit_trends_key

APEWISDOM_BASE_URL = "https://apewisdom.io/api/v1.0"
SOURCE = "apewisdom"

NORMALIZED_COLUMNS = [
    "date",
    "hour",
    "source",
    "filter",
    "page",
    "rank",
    "ticker",
    "name",
    "mentions",
    "upvotes",
    "rank_24h_ago",
    "mentions_24h_ago",
    "mention_change_pct",
    "ingested_at",
]

NORMALIZED_SCHEMA = pa.schema(
    [
        pa.field("date", pa.string()),
        pa.field("hour", pa.string()),
        pa.field("source", pa.string()),
        pa.field("filter", pa.string()),
        pa.field("page", pa.int32()),
        pa.field("rank", pa.int32()),
        pa.field("ticker", pa.string()),
        pa.field("name", pa.string()),
        pa.field("mentions", pa.int64()),
        pa.field("upvotes", pa.int64()),
        pa.field("rank_24h_ago", pa.int32()),
        pa.field("mentions_24h_ago", pa.int64()),
        pa.field("mention_change_pct", pa.float64()),
        pa.field("ingested_at", pa.string()),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch ApeWisdom Reddit trend data and write raw JSON plus staging parquet. "
            "ApeWisdom is treated as an attention signal, not a buy/sell signal."
        )
    )
    parser.add_argument("--bucket", default="us-market-data-casper")
    parser.add_argument("--raw-prefix", default="raw/reddit/apewisdom")
    parser.add_argument("--staging-prefix", default="staging/reddit_trends")
    parser.add_argument("--filter", default="all-stocks", dest="filter_name")
    parser.add_argument("--page", default=1, type=int)
    parser.add_argument("--date", default=None, help="UTC date YYYY-MM-DD; defaults to today")
    parser.add_argument("--hour", default=None, help="UTC hour HH; defaults to current hour")
    parser.add_argument("--write-raw", default=True, type=parse_bool)
    parser.add_argument("--write-staging", default=True, type=parse_bool)
    parser.add_argument(
        "--local-output-dir",
        default=None,
        help="Write under this local directory instead of S3, mirroring S3 key layout",
    )
    return parser.parse_args()


def parse_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "t", "yes", "y"}:
        return True
    if text in {"0", "false", "f", "no", "n"}:
        return False
    raise argparse.ArgumentTypeError(f"invalid boolean value: {value!r}")


def utc_date_hour(now: datetime | None = None) -> tuple[str, str]:
    current = now or datetime.now(timezone.utc)
    return current.date().isoformat(), f"{current.hour:02d}"


def fetch_apewisdom_page(filter_name: str, page: int) -> dict[str, Any]:
    if page < 1:
        raise ValueError("--page must be >= 1")

    encoded_filter = quote(filter_name.strip(), safe="")
    url = f"{APEWISDOM_BASE_URL}/filter/{encoded_filter}/page/{page}"
    request = Request(
        url,
        method="GET",
        headers={
            "Accept": "application/json",
            "User-Agent": "us-market-data-apewisdom-ingest/0.1",
        },
    )
    try:
        with urlopen(request, timeout=30) as response:
            body = response.read().decode("utf-8")
    except HTTPError as exc:
        raise RuntimeError(f"HTTP {exc.code} fetching ApeWisdom URL {url}") from exc
    except URLError as exc:
        raise RuntimeError(f"failed fetching ApeWisdom URL {url}: {exc.reason}") from exc

    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError("ApeWisdom returned non-JSON response") from exc

    if not isinstance(payload, dict):
        raise RuntimeError("ApeWisdom response was not a JSON object")
    return payload


def first_present(row: dict[str, Any], names: list[str]) -> Any:
    lower_to_key = {key.lower(): key for key in row}
    for name in names:
        key = lower_to_key.get(name.lower())
        if key is not None:
            return row.get(key)
    return None


def parse_int(value: Any) -> int | None:
    number = parse_number(value)
    if number is None:
        return None
    return int(number)


def parse_float(value: Any) -> float | None:
    return parse_number(value)


def parse_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)

    text = str(value).strip()
    if not text or text.upper() in {"N/A", "NA", "NONE", "NULL", "-"}:
        return None

    cleaned = text.replace(",", "").replace("%", "")
    match = re.search(r"-?\d+(?:\.\d+)?", cleaned)
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


def compute_mention_change_pct(mentions: int | None, mentions_24h_ago: int | None) -> float | None:
    if mentions is None or mentions_24h_ago is None or mentions_24h_ago == 0:
        return None
    return ((mentions - mentions_24h_ago) / mentions_24h_ago) * 100.0


def payload_results(payload: dict[str, Any]) -> list[dict[str, Any]]:
    results = payload.get("results", [])
    if not isinstance(results, list):
        return []
    return [row for row in results if isinstance(row, dict)]


def normalize_apewisdom_payload(
    payload: dict[str, Any],
    date_str: str,
    hour_str: str,
    filter_name: str,
    page: int,
    ingested_at: str | None = None,
) -> list[dict[str, Any]]:
    ingested_at = ingested_at or datetime.now(timezone.utc).isoformat()
    normalized = []

    for raw in payload_results(payload):
        ticker = first_present(raw, ["ticker", "symbol", "asset", "code"])
        ticker_text = str(ticker).strip().upper() if ticker is not None else ""
        if not ticker_text:
            continue

        mentions = parse_int(first_present(raw, ["mentions", "mention_count", "mentions_count"]))
        mentions_24h_ago = parse_int(
            first_present(raw, ["mentions_24h_ago", "mentions24hago", "mentions_24h"])
        )
        mention_change_pct = parse_float(
            first_present(
                raw,
                [
                    "mention_change_pct",
                    "mentions_change_pct",
                    "mention_change_24h",
                    "mentions_change_24h",
                    "change_pct",
                    "change_24h",
                ],
            )
        )
        if mention_change_pct is None:
            mention_change_pct = compute_mention_change_pct(mentions, mentions_24h_ago)

        normalized.append(
            {
                "date": date_str,
                "hour": hour_str,
                "source": SOURCE,
                "filter": filter_name,
                "page": page,
                "rank": parse_int(first_present(raw, ["rank", "position"])),
                "ticker": ticker_text,
                "name": str(first_present(raw, ["name", "company", "title"]) or "").strip() or None,
                "mentions": mentions,
                "upvotes": parse_int(first_present(raw, ["upvotes", "upvote_count", "score"])),
                "rank_24h_ago": parse_int(
                    first_present(raw, ["rank_24h_ago", "rank24hago", "rank_24h"])
                ),
                "mentions_24h_ago": mentions_24h_ago,
                "mention_change_pct": mention_change_pct,
                "ingested_at": ingested_at,
            }
        )

    return normalized


def rows_to_table(rows: list[dict[str, Any]]) -> pa.Table:
    arrays = []
    for field in NORMALIZED_SCHEMA:
        values = [row.get(field.name) for row in rows]
        arrays.append(pa.array(values, type=field.type))
    return pa.Table.from_arrays(arrays, schema=NORMALIZED_SCHEMA)


def table_to_parquet_bytes(table: pa.Table) -> bytes:
    out = io.BytesIO()
    pq.write_table(table, out, compression="snappy")
    return out.getvalue()


def local_path_for_key(local_output_dir: str | Path, key: str) -> Path:
    return Path(local_output_dir) / key


def write_bytes_local(local_output_dir: str | Path, key: str, body: bytes) -> Path:
    path = local_path_for_key(local_output_dir, key)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)
    return path


def write_raw_json_local(local_output_dir: str | Path, key: str, payload: dict[str, Any]) -> Path:
    body = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
    return write_bytes_local(local_output_dir, key, body)


def write_staging_parquet_local(local_output_dir: str | Path, key: str, table: pa.Table) -> Path:
    return write_bytes_local(local_output_dir, key, table_to_parquet_bytes(table))


def write_raw_json_s3(s3_client: Any, bucket: str, key: str, payload: dict[str, Any]) -> str:
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload).encode("utf-8"),
        ContentType="application/json",
    )
    return key


def write_staging_parquet_s3(s3_client: Any, bucket: str, key: str, table: pa.Table) -> str:
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=table_to_parquet_bytes(table),
        ContentType="application/octet-stream",
    )
    return key


def main() -> int:
    args = parse_args()
    default_date, default_hour = utc_date_hour()
    date_str = args.date or default_date
    hour_str = args.hour or default_hour

    payload = fetch_apewisdom_page(args.filter_name, args.page)
    rows = normalize_apewisdom_payload(
        payload,
        date_str=date_str,
        hour_str=hour_str,
        filter_name=args.filter_name,
        page=args.page,
    )
    table = rows_to_table(rows)

    raw_key = raw_apewisdom_key(args.raw_prefix, date_str, hour_str, args.filter_name)
    staging_key = staging_reddit_trends_key(args.staging_prefix, date_str)

    if args.local_output_dir:
        if args.write_raw:
            path = write_raw_json_local(args.local_output_dir, raw_key, payload)
            print(f"wrote raw local: {path}")
        if args.write_staging:
            path = write_staging_parquet_local(args.local_output_dir, staging_key, table)
            print(f"wrote staging local: {path} rows={table.num_rows}")
    else:
        s3_client = boto3.client("s3")
        if args.write_raw:
            write_raw_json_s3(s3_client, args.bucket, raw_key, payload)
            print(f"wrote raw s3: s3://{args.bucket}/{raw_key}")
        if args.write_staging:
            write_staging_parquet_s3(s3_client, args.bucket, staging_key, table)
            print(f"wrote staging s3: s3://{args.bucket}/{staging_key} rows={table.num_rows}")

    print(
        "apewisdom ingest complete: "
        f"filter={args.filter_name} page={args.page} "
        f"date={date_str} hour={hour_str} rows={table.num_rows}"
    )
    print("note: ApeWisdom Reddit trends are an attention signal, not a buy/sell signal")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
