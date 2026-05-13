# Data Contract

## Layers

### Raw
Path:
- `raw/massive/daily_market_summary/date=YYYY-MM-DD/data.json`
- `raw/reference/massive_tickers/date=YYYY-MM-DD/page=N/data.json`
- `raw/reference/massive_ticker_overview/date=YYYY-MM-DD/symbol=SYMBOL/data.json`
- `raw/reddit/apewisdom/date=YYYY-MM-DD/hour=HH/filter=<filter>/data.json`

Purpose:
- preserve source payloads for replay and debugging
- preserve Massive ticker reference pages for replay and type-classification audits
- preserve Massive ticker overview payloads for public stock profile enrichment audits
- preserve ApeWisdom Reddit/social trend payloads as attention-signal source data

### Staging
Path:
- `staging/market_bars/year=YYYY/month=MM/data.parquet`
- `staging/reddit_trends/date=YYYY-MM-DD/data.parquet`

Purpose:
- full-market monthly parquet
- stable transform layer before curated outputs
- stable Reddit/social trends table for downstream feature jobs

Market bars schema:
- `date`
- `year`
- `month`
- `symbol`
- `open`
- `high`
- `low`
- `close`
- `volume`
- `vwap`
- `transactions`

Reddit trends schema:
- `date`
- `hour`
- `source`
- `filter`
- `page`
- `rank`
- `ticker`
- `name`
- `mentions`
- `upvotes`
- `rank_24h_ago`
- `mentions_24h_ago`
- `mention_change_pct`
- `ingested_at`

Note:
- ApeWisdom data is an attention signal, not a buy/sell signal.

### Curated ETF Prices
Path:
- `curated/etf_daily_prices/symbol=SYMBOL/year=YYYY/data.parquet`

Purpose:
- app-facing ETF-only daily price store
- backtest engine reads from this layer, not staging

Schema:
- same logical schema as staging price rows

### Curated Reference Metadata
Path:
- `curated/reference/etf_symbol_metadata/data.parquet`
- `curated/reference/ticker_reference/data.parquet`
- `curated/reference/ticker_overview/data.parquet`

Purpose:
- cheap app lookup for symbol coverage window
- avoid request-time history scans against curated ETF parquet
- normalized Massive ticker metadata for downstream universe filtering
- normalized Massive company profile metadata for US Stock Picker public stock detail pages

ETF metadata schema:
- `symbol` (string)
- `first_available_date` (string, ISO date)
- `last_available_date` (string, ISO date)
- `updated_at` (string, ISO timestamp UTC)

Ticker reference schema:
- `symbol` (string)
- `name` (string)
- `market` (string)
- `locale` (string)
- `type` (string)
- `active` (boolean)
- `currency_name` (string)
- `primary_exchange` (string)
- `cik` (string)
- `composite_figi` (string)
- `share_class_figi` (string)
- `last_updated_utc` (string)
- `ingested_at` (string, ISO timestamp UTC)
- `source` (string)
- `is_common_stock_candidate` (boolean)

Ticker reference v1 note:
- `is_common_stock_candidate` is conservative and defaults to active US stock-market tickers with Massive type `CS`.
- Ticker reference diagnostics can be run with `python scripts/inspect_ticker_reference.py`.

Ticker overview schema:
- `symbol` (string)
- `name` (string)
- `market` (string)
- `locale` (string)
- `primary_exchange` (string)
- `type` (string)
- `active` (boolean)
- `currency_name` (string)
- `cik` (string)
- `composite_figi` (string)
- `share_class_figi` (string)
- `market_cap` (double)
- `weighted_shares_outstanding` (double)
- `share_class_shares_outstanding` (double)
- `sic_code` (string)
- `sic_description` (string)
- `description` (string)
- `homepage_url` (string)
- `total_employees` (int64)
- `list_date` (string, ISO date when present)
- `branding_logo_url` (string)
- `branding_icon_url` (string)
- `source` (string)
- `ingested_at` (string, ISO timestamp UTC)

Ticker overview manual ingest examples:
```bash
python scripts/ingest_massive_ticker_overview.py --symbols IONQ,NVDA --local-output-dir data/local_ticker_overview
python scripts/ingest_massive_ticker_overview.py --from-ticker-reference data/.../ticker_reference/data.parquet --limit 100
python scripts/ingest_massive_ticker_overview.py \
  --symbols-file ../US-Stock-Picker/data/public_profile_symbols/symbols.txt \
  --s3-bucket us-market-data-casper \
  --merge-existing \
  --refresh-after-days 7 \
  --fetch-limit 300
```

Ticker overview cache note:
- `curated/reference/ticker_overview/data.parquet` is a current profile cache when ingested with
  `--merge-existing`, not only the latest run's selected symbols. Incremental runs read the existing
  curated parquet, fetch missing or stale requested symbols, replace matching symbol rows, and keep
  cached rows for symbols not fetched.

## App assumptions

The app may assume:
- ETF backtests use `curated/etf_daily_prices`
- symbol coverage defaults use `curated/reference/etf_symbol_metadata/data.parquet`
- ticker universe filtering may use `curated/reference/ticker_reference/data.parquet`
- public stock profile pages may use `curated/reference/ticker_overview/data.parquet`
- unsupported symbols should be rejected before simulation

The app should not assume:
- staging is stable for end-user requests
- stock-curated data exists
- raw payloads are directly queryable by the product
- Reddit trend rows are investment recommendations or sentiment scores

## Idempotency rules

- ingest may overwrite the same raw date key
- transform may overwrite the same monthly staging parquet
- ETF curation may rewrite the same symbol/year parquet repeatedly
- metadata parquet may be overwritten on each ETF curation run
- ApeWisdom raw files may be overwritten for the same date/hour/filter
- ApeWisdom staging parquet may be overwritten for the same date
- Massive ticker reference raw files may be overwritten for the same date/page
- Massive ticker reference curated parquet may be overwritten on each manual ingestion
- Massive ticker overview raw files may be overwritten for the same date/symbol
- Massive ticker overview curated parquet may be overwritten as a current cache snapshot; with
  `--merge-existing`, rows for symbols not fetched are preserved

## Close vs finalize

- `close` updates same-day data path
- `finalize` typically targets previous trading day
- downstream logic must be safe for reprocessing the same date
