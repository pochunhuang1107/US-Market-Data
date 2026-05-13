# Massive Ticker Overview Lambda Deployment

## Purpose

`massive_ticker_overview_ingest` upserts Massive company profile data for the selected public stock symbols used by US Stock Picker. It reads a plain-text symbol list from S3, fetches only missing or stale profiles, writes raw Massive responses for fetched symbols, and rewrites the curated profile cache.

This is a selected public-profile cache upsert. It is not a full-universe ingestion job.

## S3 inputs and outputs

Input:

- `curated/reference/public_profile_symbols/symbols.txt`

Outputs:

- `raw/reference/massive_ticker_overview/date=YYYY-MM-DD/symbol=SYMBOL/data.json`
- `curated/reference/ticker_overview/data.parquet`

All paths are configurable through environment variables.

## Package

From the repo root:

```bash
./scripts/package_lambda_linux_x86_64.sh massive_ticker_overview_ingest
```

The Linux x86_64 ZIP artifact is created under `dist/`:

```bash
aws s3 cp dist/massive_ticker_overview_ingest_linux_x86_64.zip s3://us-market-data-casper/lambda-artifacts/massive_ticker_overview_ingest.zip
```

Generated ZIP files and build artifacts under `dist/` and `build/` should not be committed.

## Lambda configuration

- AWS Lambda name: `massive-ticker-overview-ingest`
- Handler: `lambda_function.lambda_handler`
- Runtime: Python 3.14, or the same Python runtime used by the other Python Lambdas in this repo
- Architecture: use the same architecture as the rest of the repo's Python Lambdas unless there is a reason to change it
- Timeout: 10 minutes
- Memory: 512 MB

## IAM permissions

Grant the Lambda execution role permission to read the symbol list and curated cache, write the configured S3 outputs, and emit CloudWatch logs.

Required:

- `s3:GetObject`
- `s3:PutObject`
- `logs:CreateLogGroup`
- `logs:CreateLogStream`
- `logs:PutLogEvents`

Scope S3 permissions to the target bucket and prefixes, for example:

- `arn:aws:s3:::us-market-data-casper/curated/reference/public_profile_symbols/*`
- `arn:aws:s3:::us-market-data-casper/curated/reference/ticker_overview/*`
- `arn:aws:s3:::us-market-data-casper/raw/reference/massive_ticker_overview/*`

## Environment variables

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `BUCKET_NAME` | Yes | None | S3 bucket for symbol input, raw output, and curated output. |
| `MASSIVE_API_KEY` | Yes, unless `POLYGON_API_KEY` is set | None | Massive API key. |
| `POLYGON_API_KEY` | Yes, unless `MASSIVE_API_KEY` is set | None | Fallback API key name accepted for compatibility. |
| `SYMBOLS_KEY` | No | `curated/reference/public_profile_symbols/symbols.txt` | Plain-text symbol list, one symbol per line. |
| `RAW_PREFIX` | No | `raw/reference/massive_ticker_overview` | Prefix for raw Massive JSON payloads. |
| `CURATED_KEY` | No | `curated/reference/ticker_overview/data.parquet` | Key for normalized curated cache parquet. |
| `REFRESH_AFTER_DAYS` | No | `7` | Refetch cached symbols older than this many days. Set `0` to refresh all requested symbols. |
| `FETCH_LIMIT` | No | `300` | Maximum missing/stale symbols to fetch per run. |
| `REQUEST_INTERVAL_SECONDS` | No | `0` | Sleep between symbol requests. |
| `MAX_RETRIES` | No | `5` | Retry attempts for retryable HTTP/network failures. |
| `BASE_SLEEP_SECONDS` | No | `2` | Base exponential-backoff sleep. |
| `MAX_SLEEP_SECONDS` | No | `60` | Maximum retry sleep. |

## Console test

Use this test event in the AWS Lambda console:

```json
{
  "date": "2026-05-13",
  "symbols_key": "curated/reference/public_profile_symbols/symbols.txt",
  "refresh_after_days": 7,
  "fetch_limit": 300,
  "request_interval_seconds": 0,
  "max_retries": 5,
  "base_sleep_seconds": 2,
  "max_sleep_seconds": 60
}
```

Expected response shape:

```json
{
  "statusCode": 200,
  "body": {
    "source": "massive",
    "date": "2026-05-13",
    "desired_symbols": 120,
    "existing_cached_rows": 500,
    "symbols_to_fetch": 20,
    "fetched": 20,
    "final_curated_rows": 510,
    "curated_key": "curated/reference/ticker_overview/data.parquet",
    "symbols_key": "curated/reference/public_profile_symbols/symbols.txt",
    "raw_prefix": "raw/reference/massive_ticker_overview",
    "message": "Massive ticker overview upsert complete"
  }
}
```

Exact counts vary with the input symbols and existing cache.

## EventBridge schedule

Suggested cadence: once daily after the stock-picker public symbol refresh has uploaded `symbols.txt`.

This runbook does not add EventBridge infrastructure. Create or update the schedule manually in the AWS Console when you are ready to deploy.
