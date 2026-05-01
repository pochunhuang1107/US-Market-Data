# Reference Series MVP

Reference series are market regime inputs that are useful beside ETF daily prices, but are not tradeable ETF price history. This folder keeps them as a data-only S3 staging path for later research and app work.

This branch does not enable VIX rules, strategy conditions, backend APIs, frontend UI, builder behavior, or simulation-engine logic. It writes no `curated/reference_series` output.

This MVP writes normalized monthly Parquet to `staging/reference_series/` only. Raw provider snapshots are intentionally not written here to avoid unnecessary S3 objects and cost. The existing Massive raw/staging design is different because Massive provides daily market-wide snapshots by date; Cboe and FRED reference-series data is fetched as historical/date-range CSV per symbol.

## Supported Symbols

| Symbol | Source | Type | Unit |
| --- | --- | --- | --- |
| `VIX` | Cboe | volatility index | index level |
| `VIX9D` | Cboe | volatility index | index level |
| `VXEEM` | Cboe | volatility index | index level |
| `DGS10` | FRED | Treasury yield | percent |
| `DGS2` | FRED | Treasury yield | percent |
| `T10Y2Y` | FRED | yield spread | percent |
| `DFF` | FRED | policy rate | percent |

Future candidates pending source confirmation:

- `VVIX`
- `OVX`
- `GVZ`

## Provider Notes

Cboe CSVs use the official historical-data CSV endpoint pattern:

```text
https://cdn.cboe.com/api/global/us_indices/daily_prices/<SYMBOL>_History.csv
```

This was checked for the active Phase 1 Cboe symbols when the MVP was added. Cboe exposes these files from its historical VIX and volatility-index pages, but the exact CDN path is still centralized in `CBOE_CSV_URLS` so it can be updated in one place if Cboe changes its site.

FRED data uses the no-key graph CSV download:

```text
https://fred.stlouisfed.org/graph/fredgraph.csv?id=<SERIES_ID>&cosd=<START_DATE>&coed=<END_DATE>
```

No FRED API key is required for the current implementation.

## Normalized Schema

All staging parquet files use exactly these columns:

```text
date
symbol
source
series_type
open
high
low
close
value
unit
```

Rules:

- `date` is an ISO `YYYY-MM-DD` string.
- `symbol` is uppercase.
- `source` is `cboe` or `fred`.
- `open`, `high`, `low`, `close`, and `value` are nullable numeric fields.
- Rows with missing dates or missing values are dropped.
- Rows are sorted ascending by date.
- Duplicate dates are de-duplicated, keeping the latest row seen from the provider file.

For Cboe rows, `value` equals `close`. For FRED rows, `open`, `high`, and `low` are null, while `close` and `value` both equal the observation value.

## S3 Layout

Staging parquet is the only MVP output:

```text
staging/reference_series/source=<source>/symbol=<symbol>/year=<YYYY>/month=<MM>/data.parquet
```

Each staging object contains one symbol, one source, and one calendar month. Parquet files are written with Snappy compression.

## Backfill Examples

Dry-run fetch and normalization:

```bash
python scripts/reference_series/backfill_reference_series.py \
  --symbols VIX,DGS10 \
  --start-date 2010-01-01 \
  --end-date 2026-04-30 \
  --bucket us-market-data-casper \
  --region us-west-2 \
  --dry-run
```

Write monthly staging parquet:

```bash
python scripts/reference_series/backfill_reference_series.py \
  --symbols VIX,VIX9D,VXEEM,DGS10,DGS2,T10Y2Y,DFF \
  --start-date 2010-01-01 \
  --end-date 2026-04-30 \
  --bucket us-market-data-casper \
  --region us-west-2 \
  --write-staging
```

Existing staging objects are skipped unless `--overwrite` is supplied.

## Validation

After writing staging data, print row counts, date ranges, and value ranges:

```bash
python scripts/reference_series/check_reference_series.py \
  --symbols VIX,VIX9D,VXEEM,DGS10,DGS2,T10Y2Y,DFF \
  --bucket us-market-data-casper \
  --region us-west-2
```

The checker lists objects under:

```text
staging/reference_series/source=<source>/symbol=<symbol>/year=*/month=*/data.parquet
```

## Athena

Athena can query this layout if an external table is added later, but Athena is optional and is not required by app runtime behavior. This branch only creates data-ingestion scripts and documentation.
