# Massive Ticker Reference Lambda Deployment

## Purpose

`massive_ticker_reference_ingest` refreshes Massive ticker reference metadata for active US stock-market tickers. It writes raw paginated Massive responses and a normalized curated parquet file used as common-stock filtering metadata.

## S3 outputs

The Lambda writes to:

- `raw/reference/massive_tickers/date=YYYY-MM-DD/page=N/data.json`
- `curated/reference/ticker_reference/data.parquet`

Both paths are configurable through environment variables.

## Package

From the repo root:

```bash
./scripts/package_lambda.sh massive_ticker_reference_ingest
```

The ZIP artifact is created under `dist/`. Generated ZIP files and build artifacts under `dist/` and `build/` should not be committed.

## Lambda configuration

- Handler: `lambda_function.lambda_handler`
- Runtime: Python 3.14, or the same Python runtime used by the other Python Lambdas in this repo
- Architecture: use the same architecture as the rest of the repo's Python Lambdas unless there is a reason to change it
- Timeout: 10 minutes
- Memory: 512 MB

## IAM permissions

Grant the Lambda execution role permission to write the configured S3 outputs and emit CloudWatch logs.

Required:

- `s3:PutObject`
- `logs:CreateLogGroup`
- `logs:CreateLogStream`
- `logs:PutLogEvents`

Scope S3 permissions to the target bucket and prefixes, for example:

- `arn:aws:s3:::us-market-data-casper/raw/reference/massive_tickers/*`
- `arn:aws:s3:::us-market-data-casper/curated/reference/ticker_reference/*`

## Environment variables

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `BUCKET_NAME` | Yes | None | S3 bucket for raw and curated outputs. |
| `MASSIVE_API_KEY` | Yes, unless `POLYGON_API_KEY` is set | None | Massive API key. |
| `POLYGON_API_KEY` | Yes, unless `MASSIVE_API_KEY` is set | None | Fallback API key name accepted for compatibility. |
| `RAW_PREFIX` | No | `raw/reference/massive_tickers` | Prefix for raw Massive JSON pages. |
| `CURATED_KEY` | No | `curated/reference/ticker_reference/data.parquet` | Key for normalized curated parquet. |
| `MARKET` | No | `stocks` | Massive market parameter. |
| `ACTIVE` | No | `true` | Massive active ticker filter. |
| `LOCALE` | No | `us` | Massive locale parameter. |
| `LIMIT` | No | `1000` | Massive page size. |
| `COMMON_STOCK_TYPE_CODES` | No | `CS` | Comma-separated Massive type codes treated as common-stock candidates. |
| `REQUEST_INTERVAL_SECONDS` | No | `13` | Sleep between paginated requests. |
| `MAX_RETRIES` | No | `8` | Retry attempts for retryable HTTP/network failures. |
| `BASE_SLEEP_SECONDS` | No | `15` | Base exponential-backoff sleep. |
| `MAX_SLEEP_SECONDS` | No | `120` | Maximum retry sleep. |

## Console test

Use this test event in the AWS Lambda console:

```json
{
  "date": "2026-05-09",
  "market": "stocks",
  "active": true,
  "locale": "us",
  "limit": 1000,
  "common_stock_type_codes": "CS",
  "request_interval_seconds": 13,
  "max_retries": 8,
  "base_sleep_seconds": 15,
  "max_sleep_seconds": 120
}
```

Expected response shape:

```json
{
  "statusCode": 200,
  "body": {
    "source": "massive",
    "date": "2026-05-09",
    "market": "stocks",
    "locale": "us",
    "active": true,
    "pages": 13,
    "row_count": 12582,
    "curated_key": "curated/reference/ticker_reference/data.parquet",
    "raw_prefix": "raw/reference/massive_tickers",
    "message": "Massive ticker reference ingested"
  }
}
```

Exact page and row counts vary with the Massive response.

## EventBridge schedule

Suggested cadence: weekly on Saturday morning after Friday close and after Friday market data has finalized.

This runbook does not add EventBridge infrastructure. Create or update the schedule manually in the AWS Console when you are ready to deploy.

## Request pacing

For 5-requests-per-minute Massive plans, keep `REQUEST_INTERVAL_SECONDS` at `13` or higher. The Lambda also logs page fetch progress, retry sleeps, and pacing sleeps to CloudWatch without printing API keys.
