# US Market Data

Data infrastructure repo for the US market data pipeline that feeds the ETF-first simulation platform.

This repo is based on your current production flow:
- raw grouped daily market summary ingestion
- monthly full-market staging parquet transform
- ETF-only curated parquet update
- ETF symbol metadata maintenance

## Scope

Current scope:
- Full-market raw ingestion
- Full-market staging transform
- ETF-only curation to app-facing parquet
- ETF symbol metadata parquet for app lookup
- Massive ticker reference ingestion for common-stock filtering metadata
- Reddit/social trend ingestion from ApeWisdom as an attention signal
- Manual/backfill scripts for curation and metadata bootstrap

Deferred on purpose:
- Daily all-stock curated lambda
- Request-time parquet scans for symbol history
- Mixing app repo and data infra repo

## Current S3 layout

- `raw/massive/daily_market_summary/date=YYYY-MM-DD/data.json`
- `raw/reference/massive_tickers/date=YYYY-MM-DD/page=N/data.json`
- `raw/reddit/apewisdom/date=YYYY-MM-DD/hour=HH/filter=<filter>/data.json`
- `staging/market_bars/year=YYYY/month=MM/data.parquet`
- `staging/reddit_trends/date=YYYY-MM-DD/data.parquet`
- `curated/etf_daily_prices/symbol=SYMBOL/year=YYYY/data.parquet`
- `curated/reference/etf_symbol_metadata/data.parquet`
- `curated/reference/ticker_reference/data.parquet`

## Repo structure

```text
lambdas/
  massive_daily_grouped_ingest/
  massive_daily_grouped_transform/
  etf_parquet_update/
  apewisdom_reddit_trends_ingest/
scripts/
  backfill_curated_etf_from_staging.py
  bootstrap_etf_symbol_metadata.py
  ingest_apewisdom_trends.py
  ingest_massive_ticker_reference.py
  inspect_ticker_reference.py
  validate_etf_symbol_metadata.py
  package_lambda.sh
src/us_market_data/
  s3_paths.py
  etf_metadata.py
  apewisdom.py
  ticker_reference.py
  ticker_reference_diagnostics.py
requirements/
  lambda.txt
  scripts.txt
config/
  etf_symbols.example.txt
  lambda_env_examples/
docs/
  APEWISDOM_LAMBDA_DEPLOYMENT.md
  DATA_CONTRACT.md
```

## Metadata design

Metadata parquet path:
- `curated/reference/etf_symbol_metadata/data.parquet`

Schema:
- `symbol`
- `first_available_date`
- `last_available_date`
- `updated_at`

Operational model:
- bootstrap script computes first + last dates from existing curated ETF parquet
- daily ETF curation lambda updates:
  - `last_available_date`
  - `updated_at`
  - inserts new symbols if needed
- `first_available_date` is preserved after bootstrap unless a brand-new symbol appears

## Recommended workflow

### 1. Bootstrap curated ETF parquet if needed

Use existing staging monthly parquet as source.

```bash
python scripts/backfill_curated_etf_from_staging.py \
  --bucket us-market-data-casper \
  --staging-prefix staging/market_bars \
  --curated-prefix curated/etf_daily_prices \
  --symbols-file config/etf_symbols.example.txt \
  --start-month 2018-01 \
  --end-month 2026-04
```

### 2. Bootstrap ETF metadata parquet

```bash
python scripts/bootstrap_etf_symbol_metadata.py \
  --bucket us-market-data-casper \
  --curated-prefix curated/etf_daily_prices \
  --metadata-key curated/reference/etf_symbol_metadata/data.parquet
```

### 3. Validate metadata against curated ETF files

```bash
python scripts/validate_etf_symbol_metadata.py \
  --bucket us-market-data-casper \
  --curated-prefix curated/etf_daily_prices \
  --metadata-key curated/reference/etf_symbol_metadata/data.parquet
```

### 4. Manually ingest ApeWisdom Reddit trends

Fetch the current ApeWisdom `all-stocks` page, write the raw payload, and write a normalized
daily staging parquet file:

```bash
python scripts/ingest_apewisdom_trends.py \
  --bucket us-market-data-casper \
  --filter all-stocks
```

For local testing without S3, mirror the S3 layout under a local directory:

```bash
python scripts/ingest_apewisdom_trends.py \
  --filter all-stocks \
  --local-output-dir data/local_apewisdom
```

ApeWisdom tracks stock/crypto mentions across Reddit and 4chan boards. Treat this data as an
attention signal, not a buy/sell signal.

### 5. Manually ingest Massive ticker reference

Fetch active US stock-market ticker reference metadata from Massive, write each raw response page,
and write a normalized curated parquet dataset:

```bash
MASSIVE_API_KEY=... python scripts/ingest_massive_ticker_reference.py \
  --bucket us-market-data-casper
```

For local testing without S3, mirror the S3 layout under a local directory:

```bash
MASSIVE_API_KEY=... python scripts/ingest_massive_ticker_reference.py \
  --local-output-dir data/local_ticker_reference
```

Massive reference ingestion retries rate limits and temporary server/network failures with
configurable `--max-retries`, `--base-sleep-seconds`, and `--max-sleep-seconds` options. If Massive
continues to rate limit the run, rerun it later or increase the retry/backoff values.

For free/basic-tier Massive accounts, pace page requests proactively to stay under the typical
5-requests-per-minute limit:

```bash
MASSIVE_API_KEY=... python scripts/ingest_massive_ticker_reference.py \
  --local-output-dir data/local_ticker_reference \
  --request-interval-seconds 13 \
  --max-retries 8
```

The curated output includes a conservative `is_common_stock_candidate` flag. In v1, the default
candidate logic is active US stock-market tickers with Massive type `CS`.

Inspect the curated ticker reference parquet before downstream consumers use it:

```bash
python scripts/inspect_ticker_reference.py
```

To inspect the canonical S3 object explicitly:

```bash
python scripts/inspect_ticker_reference.py \
  --input-path s3://us-market-data-casper/curated/reference/ticker_reference/data.parquet
```

### 6. Deploy lambdas

Package each lambda with:

```bash
./scripts/package_lambda.sh massive_daily_grouped_ingest
./scripts/package_lambda.sh massive_daily_grouped_transform
./scripts/package_lambda.sh etf_parquet_update
./scripts/package_lambda.sh apewisdom_reddit_trends_ingest
```

Artifacts will be created under `dist/`.

For the ApeWisdom Lambda AWS Console setup, see `docs/APEWISDOM_LAMBDA_DEPLOYMENT.md`.
Use `config/lambda_env_examples/apewisdom_reddit_trends_ingest.env.example` as the environment variable reference.

## Lambda schedules

Recommended mapping from your current schedule:

- `massive-daily-grouped-ingest`
  - weekday 4:30 PM New York time for `close`
  - Tue-Sat 4:30 AM New York time for `finalize`

- `massive-daily-grouped-transform`
  - weekday 4:35 PM New York time for `close`
  - Tue-Sat 4:35 AM New York time for `finalize`

- `etf-parquet-update`
  - weekday 4:40 PM New York time for `close`
  - Tue-Sat 4:40 AM New York time for `finalize`

- `apewisdom-reddit-trends-ingest`
  - suggested future EventBridge cadence: hourly or every few hours in UTC
  - not deployed or scheduled by this repo change

## Dependencies

Lambda dependencies are intentionally small.

- `boto3` is available in Lambda runtime
- `pyarrow` is required for transform/curation and scripts

See:
- `requirements/lambda.txt`
- `requirements/scripts.txt`

## Notes on stock support

This repo keeps full-market data in raw + staging but does **not** add a full daily stock-curation lambda yet.
That is intentional.

Reason:
- ETF per-symbol/year rewrite pattern is acceptable for ETF volume
- The same pattern for 10k+ stocks would create much heavier S3 request and rewrite overhead

If stock curation is added later, use a different storage layout than ETF per-symbol/year.

## Git setup

For the data repo:

```bash
git clone https://github.com/pochunhuang1107/US-Market-Data.git
```

For the renamed web app repo, update your existing local origin:

```bash
git remote -v
git remote set-url origin https://github.com/pochunhuang1107/US-Market-Simulation-Platform.git
git remote -v
```

GitHub may redirect the old repo URL for a while, but updating `origin` is cleaner.
