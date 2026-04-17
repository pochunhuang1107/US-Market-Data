# Data Contract

## Layers

### Raw
Path:
- `raw/massive/daily_market_summary/date=YYYY-MM-DD/data.json`

Purpose:
- preserve source payloads for replay and debugging

### Staging
Path:
- `staging/market_bars/year=YYYY/month=MM/data.parquet`

Purpose:
- full-market monthly parquet
- stable transform layer before curated outputs

Schema:
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

## Idempotency rules

- ingest may overwrite the same raw date key
- transform may overwrite the same monthly staging parquet
- ETF curation may rewrite the same symbol/year parquet repeatedly
- metadata parquet may be overwritten on each ETF curation run

## Close vs finalize

- `close` updates same-day data path
- `finalize` typically targets previous trading day
- downstream logic must be safe for reprocessing the same date
