# Data Contract

## Layers

### Raw
Path:
- `raw/massive/daily_market_summary/date=YYYY-MM-DD/data.json`
- `raw/reddit/apewisdom/date=YYYY-MM-DD/hour=HH/filter=<filter>/data.json`

Purpose:
- preserve source payloads for replay and debugging
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

Purpose:
- cheap app lookup for symbol coverage window
- avoid request-time history scans against curated ETF parquet

Schema:
- `symbol` (string)
- `first_available_date` (string, ISO date)
- `last_available_date` (string, ISO date)
- `updated_at` (string, ISO timestamp UTC)

## App assumptions

The app may assume:
- ETF backtests use `curated/etf_daily_prices`
- symbol coverage defaults use `curated/reference/etf_symbol_metadata/data.parquet`
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

## Close vs finalize

- `close` updates same-day data path
- `finalize` typically targets previous trading day
- downstream logic must be safe for reprocessing the same date
