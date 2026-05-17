# US Market Data

Data infrastructure repo for the US market data pipeline that feeds the ETF-first simulation platform.

This repo is based on your current production flow:
- raw grouped daily market summary ingestion
- monthly full-market staging parquet transform
- ETF-only curated parquet update
- ETF symbol metadata maintenance
- Massive ticker overview company profile enrichment

## Scope

Current scope:
- Full-market raw ingestion
- Full-market staging transform
- ETF-only curation to app-facing parquet
- ETF symbol metadata parquet for app lookup
- Massive ticker reference ingestion for common-stock filtering metadata
- Massive ticker overview ingestion for public stock profile enrichment in US Stock Picker
- Reddit/social trend ingestion from ApeWisdom as an attention signal
- Manual/backfill scripts for curation and metadata bootstrap

Deferred on purpose:
- Daily all-stock curated lambda
- Request-time parquet scans for symbol history
- Mixing app repo and data infra repo

## Current S3 layout

- `raw/massive/daily_market_summary/date=YYYY-MM-DD/data.json`
- `raw/reference/massive_tickers/date=YYYY-MM-DD/page=N/data.json`
- `raw/reference/massive_ticker_overview/date=YYYY-MM-DD/symbol=SYMBOL/data.json`
- `raw/reddit/apewisdom/date=YYYY-MM-DD/hour=HH/filter=<filter>/data.json`
- `staging/market_bars/year=YYYY/month=MM/data.parquet`
- `staging/reddit_trends/date=YYYY-MM-DD/data.parquet`
- `curated/etf_daily_prices/symbol=SYMBOL/year=YYYY/data.parquet`
- `curated/reference/etf_symbol_metadata/data.parquet`
- `curated/reference/ticker_reference/data.parquet`
- `curated/reference/ticker_overview/data.parquet`
- `curated/reference/public_profile_symbols/symbols.txt`

## Repo structure

```text
lambdas/
  massive_daily_grouped_ingest/
  massive_daily_grouped_transform/
  massive_ticker_reference_ingest/
  massive_ticker_overview_ingest/
  etf_parquet_update/
  apewisdom_reddit_trends_ingest/
scripts/
  backfill_curated_etf_from_staging.py
  bootstrap_etf_symbol_metadata.py
  validate_manual_historical_etf_backfill.py
  ingest_apewisdom_trends.py
  ingest_massive_ticker_reference.py
  ingest_massive_ticker_overview.py
  inspect_ticker_reference.py
  validate_etf_symbol_metadata.py
  package_lambda.sh
  package_lambda_linux_x86_64.sh
src/us_market_data/
  s3_paths.py
  etf_metadata.py
  apewisdom.py
  ticker_reference.py
  ticker_overview.py
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
  MASSIVE_TICKER_REFERENCE_LAMBDA_DEPLOYMENT.md
  MASSIVE_TICKER_OVERVIEW_LAMBDA_DEPLOYMENT.md
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

### 4. Validate local manual historical ETF backfills

The manual historical ETF backfill validator is local-only. It reads manually downloaded ETF CSVs,
normalizes valid OHLCV price rows, stages curated-style parquet under
`data/manual_backfill/curated_export/source=<source>/etf_daily_prices/`, and writes validation
reports under `data/manual_backfill/validation/`.

Dividend, split, distribution, and other event/non-price rows are quarantined before price
normalization. They are written to `data/manual_backfill/events/source=<source>/<SYMBOL>_events.csv`,
summarized in `validation/event_rows_report.csv`, and excluded from normalized and curated-style ETF
price parquet. If these events are needed later, handle them as a separate corporate-action/event
dataset rather than mixing them into ETF price history.

Overlap diagnostics also write `validation/adjustment_diagnostics_report.csv` and
`validation/boundary_rebase_diagnostics_report.csv`. These reports compare Massive/source close
ratios and one-day return differences so stable price-level mismatches can be flagged as possible
adjustment convention differences. Boundary diagnostics focus on the first joined overlap trading
days because Stooq is intended only for pre-Massive historical backfill. The validator only
diagnoses these cases; it does not rebase prices or change the curated-style export.

By default, the validator preserves the historical fixed boundary behavior and exports rows with
`date <= --backfill-end-date`. For safer local ETF backfill staging after downloading a Massive
sample, use `--backfill-boundary-mode infer_from_massive`. In that mode the validator infers each
symbol's local Massive first available date from `--massive-curated-dir` and exports only source
rows before that symbol-specific date. Symbols without a local Massive sample export no source rows
in `infer_from_massive` mode and are marked `missing_massive_sample` in
`validation/massive_boundary_report.csv`; this avoids accidentally exporting all historical source
rows.

To compare Tiingo against the local Stooq manual backfill before any promotion decision, use the
local-only Tiingo downloader. It reads `TIINGO_API_TOKEN` from the environment, sends the token only
in the API request header, writes normalized source CSVs under
`data/manual_backfill/raw/tiingo/`, and writes `tiingo_download_manifest.csv` plus
`tiingo_download_summary.md` under `data/manual_backfill/validation/`. It does not write to S3,
does not promote historical data, and does not change Massive automation.

```bash
python scripts/download_tiingo_historical_etf_data.py \
  --symbols-file config/etf_symbols.example.txt \
  --start-date 1999-01-01 \
  --output-dir data/manual_backfill/raw/tiingo \
  --price-mode adjusted
```

Then validate Tiingo with the same manual backfill validator:

```bash
python scripts/validate_manual_historical_etf_backfill.py \
  --raw-dir data/manual_backfill/raw/tiingo \
  --source-provider tiingo \
  --massive-curated-dir data/manual_backfill/massive_curated_sample \
  --output-dir data/manual_backfill \
  --symbols-file config/etf_symbols.example.txt \
  --backfill-boundary-mode infer_from_massive
```

If Tiingo has already been downloaded and you need to evaluate a raw-compatible price convention
without calling Tiingo again, materialize a separate local source from the preserved `raw_*` columns:

```bash
python scripts/materialize_tiingo_raw_compatible_backfill.py \
  --input-dir data/manual_backfill/raw/tiingo \
  --output-dir data/manual_backfill/raw/tiingo_raw_compatible \
  --symbols-file config/etf_symbols.example.txt \
  --overwrite
```

This writes `tiingo_raw_compatible_materialization_manifest.csv` and
`tiingo_raw_compatible_materialization_summary.md` under `data/manual_backfill/validation/`. It
does not call external APIs, does not write to S3, does not change Massive automation, and does not
overwrite the existing Tiingo adjusted source directory. Validate the materialized source with
`--raw-dir data/manual_backfill/raw/tiingo_raw_compatible` and `--source-provider tiingo_raw`.

Before relying on Tiingo raw-compatible history for long-run research, run the local split
discontinuity diagnostic against the already-downloaded Tiingo CSVs:

```bash
python scripts/diagnose_tiingo_raw_split_discontinuities.py \
  --tiingo-raw-dir data/manual_backfill/raw/tiingo \
  --symbols-file config/etf_symbols.example.txt \
  --output-dir data/manual_backfill_tiingo_raw_diagnostics
```

This writes `split_discontinuity_event_report.csv`,
`split_discontinuity_symbol_summary.csv`, and `split_discontinuity_report.md`. It does not call
Tiingo, write to S3, promote data, or change Massive automation. If split discontinuities are
present, build a separate Tiingo split-adjusted source from raw prices plus `split_factor` before
any overnight ML run.

Reference series such as VIX and DGS10 belong to the existing `curated/reference_series` workflow,
not the manual ETF price backfill.

```bash
python scripts/validate_manual_historical_etf_backfill.py \
  --raw-dir data/manual_backfill/raw/stooq \
  --source-provider stooq \
  --massive-curated-dir data/manual_backfill/massive_curated_sample \
  --output-dir data/manual_backfill \
  --symbols-file config/etf_symbols.example.txt \
  --backfill-boundary-mode infer_from_massive \
  --backfill-end-date 2017-12-31 \
  --overlap-start-date 2018-01-01 \
  --overlap-end-date 2026-05-08 \
  --boundary-window-days 20,60,120
```

### 5. Manually ingest ApeWisdom Reddit trends

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

### 6. Manually ingest Massive ticker reference

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

### 7. Manually ingest Massive ticker overview profiles

Fetch company profile fields from Massive Ticker Overview for selected symbols, write one raw JSON
payload per symbol, and write a normalized curated parquet file for US Stock Picker public stock
profile enrichment:

```bash
MASSIVE_API_KEY=... python scripts/ingest_massive_ticker_overview.py \
  --symbols IONQ,NVDA \
  --local-output-dir data/local_ticker_overview
```

To select common-stock candidates from an existing ticker reference parquet, apply a safety limit:

```bash
MASSIVE_API_KEY=... python scripts/ingest_massive_ticker_overview.py \
  --from-ticker-reference data/local_ticker_reference/curated/reference/ticker_reference/data.parquet \
  --limit 100 \
  --local-output-dir data/local_ticker_overview
```

The raw profile path is
`raw/reference/massive_ticker_overview/date=YYYY-MM-DD/symbol=SYMBOL/data.json`; the curated output is
`curated/reference/ticker_overview/data.parquet`. Overview ingestion retries rate limits and
temporary server/network failures with configurable retry/backoff options, and `--request-interval-seconds`
can pace symbol requests.

Use `--merge-existing` when ingesting an incremental picker symbol list. In this mode,
`curated/reference/ticker_overview/data.parquet` is treated as a current profile cache: the script
reads the existing curated parquet, fetches only missing or stale requested symbols, upserts fresh
rows by symbol, and keeps cached rows for symbols that were not part of the latest run.

```bash
python scripts/ingest_massive_ticker_overview.py \
  --symbols-file ../US-Stock-Picker/data/public_profile_symbols/symbols.txt \
  --s3-bucket us-market-data-casper \
  --merge-existing \
  --refresh-after-days 7 \
  --fetch-limit 300
```

### 8. Deploy lambdas

Package each lambda with:

```bash
./scripts/package_lambda.sh massive_daily_grouped_ingest
./scripts/package_lambda.sh massive_daily_grouped_transform
./scripts/package_lambda.sh massive_ticker_reference_ingest
./scripts/package_lambda.sh massive_ticker_overview_ingest
./scripts/package_lambda.sh etf_parquet_update
./scripts/package_lambda.sh apewisdom_reddit_trends_ingest
```

Artifacts will be created under `dist/`.

For Python 3.14 Lambdas deployed as zip packages on x86_64, build native dependencies in Docker
instead of packaging Mac wheels locally:

```bash
./scripts/package_lambda_linux_x86_64.sh massive_ticker_overview_ingest
aws s3 cp dist/massive_ticker_overview_ingest_linux_x86_64.zip s3://us-market-data-casper/lambda-artifacts/massive_ticker_overview_ingest.zip
```

For the ApeWisdom Lambda AWS Console setup, see `docs/APEWISDOM_LAMBDA_DEPLOYMENT.md`.
Use `config/lambda_env_examples/apewisdom_reddit_trends_ingest.env.example` as the environment variable reference.

For the Massive ticker reference Lambda AWS Console setup, see `docs/MASSIVE_TICKER_REFERENCE_LAMBDA_DEPLOYMENT.md`.

For the Massive ticker overview upsert Lambda AWS Console setup, see `docs/MASSIVE_TICKER_OVERVIEW_LAMBDA_DEPLOYMENT.md`.

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

- `massive-ticker-reference-ingest`
  - suggested future EventBridge cadence: weekly Saturday morning after Friday market data is finalized
  - not deployed or scheduled by this repo change

- `massive-ticker-overview-ingest`
  - suggested future EventBridge cadence: once daily after the stock-picker public symbol refresh
  - upserts the selected public-profile cache only; it is not a full-universe ingestion
  - not deployed or scheduled by this repo change

- `apewisdom-reddit-trends-ingest`
  - suggested future EventBridge cadence: hourly or every few hours in UTC
  - not deployed or scheduled by this repo change

## Dependencies

Lambda dependencies are intentionally small.

- `boto3` is available in Lambda runtime
- `pyarrow` is required for transform/curation and scripts
- Lambda `pyarrow` versions must provide Python 3.14 Linux x86_64 wheels for zip packaging

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
