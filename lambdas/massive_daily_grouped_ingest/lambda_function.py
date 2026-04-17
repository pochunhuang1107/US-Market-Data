import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import boto3
from botocore.exceptions import ClientError

s3 = boto3.client("s3")
secrets = boto3.client("secretsmanager")
sns = boto3.client("sns")

BUCKET_NAME = os.environ["BUCKET_NAME"]
SECRET_NAME = os.environ["SECRET_NAME"]
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]


def publish_sns(subject: str, message: str) -> None:
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject[:100],
        Message=message,
    )


def get_api_key() -> str:
    response = secrets.get_secret_value(SecretId=SECRET_NAME)
    secret = json.loads(response["SecretString"])
    return secret["api_key"]


def resolve_target_date(event) -> str:
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


def call_massive_daily_market_summary(date_str: str, api_key: str) -> dict:
    base_url = f"https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/{date_str}"
    params = {
        "adjusted": "true",
        "apiKey": api_key,
    }
    url = f"{base_url}?{urllib.parse.urlencode(params)}"

    req = urllib.request.Request(
        url,
        method="GET",
        headers={
            "Accept": "application/json",
            "User-Agent": "aws-lambda-us-market-data/1.0",
        },
    )

    with urllib.request.urlopen(req, timeout=30) as response:
        body = response.read().decode("utf-8")
        return json.loads(body)


def payload_has_results(payload: dict) -> bool:
    results_count = payload.get("resultsCount", 0)
    results = payload.get("results", [])
    status = payload.get("status")
    return (status == "OK" or status == "DELAYED") and results_count and len(results) > 0


def save_raw_to_s3(payload: dict, date_str: str) -> str:
    key = f"raw/massive/daily_market_summary/date={date_str}/data.json"
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(payload).encode("utf-8"),
        ContentType="application/json",
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
            }

        if is_market_holiday(date_str):
            return {
                "statusCode": 200,
                "message": "Skipped market holiday",
                "date": date_str,
                "skipped": True,
                "reason": "market_holiday",
            }

        api_key = get_api_key()
        payload = call_massive_daily_market_summary(date_str, api_key)

        if not payload_has_results(payload):
            msg = f"No usable market data returned for expected trading day {date_str}"
            publish_sns("us-market-data raw empty", msg)
            raise ValueError(msg)

        s3_key = save_raw_to_s3(payload, date_str)

        return {
            "statusCode": 200,
            "message": "Daily market summary ingestion completed",
            "date": date_str,
            "s3_key": s3_key,
            "skipped": False,
            "api_status": payload.get("status"),
            "results_count": payload.get("resultsCount"),
        }

    except Exception as e:
        msg = f"Raw ingest Lambda failed for event={json.dumps(event)}\n\nError: {str(e)}"
        publish_sns("us-market-data raw failure", msg)
        raise
