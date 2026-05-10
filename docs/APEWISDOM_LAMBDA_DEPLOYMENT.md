# ApeWisdom Lambda Deployment

## Purpose

`apewisdom_reddit_trends_ingest` fetches ApeWisdom Reddit trend rankings and writes them to S3 as both raw source data and normalized staging parquet.

ApeWisdom is an attention signal, not a buy/sell signal. Use it to understand where retail discussion is concentrated, then validate any market decision with price, volume, fundamentals, and risk controls.

## S3 outputs

The Lambda writes to:

- `raw/reddit/apewisdom/date=YYYY-MM-DD/hour=HH/filter=<filter>/data.json`
- `staging/reddit_trends/date=YYYY-MM-DD/data.parquet`

The prefixes are configurable through environment variables.

## Package

From the repo root:

```bash
./scripts/package_lambda.sh apewisdom_reddit_trends_ingest
```

The ZIP artifact is created under `dist/`. Generated ZIP files and build artifacts under `dist/` and `build/` should not be committed.

## Lambda configuration

- Handler: `lambda_function.lambda_handler`
- Runtime: Python 3.11
- Architecture: use the same architecture as the rest of the repo's Python Lambdas unless there is a reason to change it.
- Timeout: start with a modest timeout such as 1 minute, then adjust from CloudWatch duration metrics if needed.
- Memory: start with 512 MB, then adjust from CloudWatch memory metrics if needed.

## IAM permissions

Grant the Lambda execution role permission to write the configured S3 outputs and emit CloudWatch logs.

Required:

- `s3:PutObject`
- `logs:CreateLogGroup`
- `logs:CreateLogStream`
- `logs:PutLogEvents`

May be needed depending on how the Lambda reads or verifies existing objects:

- `s3:GetObject`

Scope S3 permissions to the target bucket and prefixes, for example:

- `arn:aws:s3:::us-market-data-casper/raw/reddit/apewisdom/*`
- `arn:aws:s3:::us-market-data-casper/staging/reddit_trends/*`

## Environment variables

Example values are available at `config/lambda_env_examples/apewisdom_reddit_trends_ingest.env.example`.

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `BUCKET_NAME` | Yes | None | S3 bucket for raw and staging outputs. |
| `RAW_PREFIX` | No | `raw/reddit/apewisdom` | Prefix for raw ApeWisdom JSON payloads. |
| `STAGING_PREFIX` | No | `staging/reddit_trends` | Prefix for normalized staging parquet. |
| `FILTER_NAME` | No | `all-stocks` | ApeWisdom filter to ingest. |
| `PAGE` | No | `1` | ApeWisdom page number to ingest. |
| `WRITE_RAW` | No | `true` | Whether to write the raw JSON output. |
| `WRITE_STAGING` | No | `true` | Whether to write the staging parquet output. |

## Console test

Use this test event in the AWS Lambda console:

```json
{
  "filter": "all-stocks",
  "page": 1
}
```

Expected response shape:

```json
{
  "statusCode": 200,
  "body": {
    "filter": "all-stocks",
    "page": 1,
    "raw_key": "raw/reddit/apewisdom/date=YYYY-MM-DD/hour=HH/filter=all-stocks/data.json",
    "staging_key": "staging/reddit_trends/date=YYYY-MM-DD/data.parquet",
    "record_count": 100
  }
}
```

Exact counts and timestamps vary by ApeWisdom response and invocation time. If raw or staging writes are disabled, the corresponding output key may be omitted or empty.

## EventBridge schedule

Suggested cadence: hourly or every few hours in UTC.

Start with a conservative schedule, review CloudWatch logs and S3 outputs, then adjust cadence based on how much intraday signal you need.

This runbook does not add EventBridge infrastructure. Create or update the schedule manually in the AWS Console when you are ready to deploy.
