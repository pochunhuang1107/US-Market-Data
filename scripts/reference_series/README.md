# Reference Series MVP

Reference series are market regime inputs that are useful beside ETF daily prices, but are not
tradeable ETF price history. This folder keeps the data-only reference-series curation scripts and
documentation.

The app-ready source of truth is:

- `curated/reference_series`: app-ready yearly reference-series data.

Future Simulation Platform integration should read only from `curated/reference_series`, not from
`staging/reference_series`. Curated is the stable runtime contract.

`staging/reference_series` is not part of the active reference-series workflow. Future scheduled
reference-series automation can fetch Cboe/FRED data, normalize it, merge it directly into
`curated/reference_series` yearly files, and update metadata without writing staging first.

The Massive pipeline still needs raw/staging layers because Massive provides daily market-wide
snapshots. Reference series from Cboe and FRED are small, clean, symbol-level historical/date-range
series, so raw snapshots and a staging layer are not necessary here.

This branch does not enable VIX rules, strategy conditions, backend APIs, frontend UI, builder
behavior, simulation-engine logic, Lambda, or EventBridge automation. Raw provider snapshots are
intentionally not written for this MVP to avoid unnecessary S3 objects and cost.

## Supported Symbols

Current active supported symbols:

| Symbol | Source | Type | Unit |
| --- | --- | --- | --- |
| `VIX` | Cboe | volatility index | index level |
| `VIX9D` | Cboe | volatility index | index level |
| `VXEEM` | Cboe | volatility index | index level |
| `DGS10` | FRED | Treasury yield | percent |
| `DGS2` | FRED | Treasury yield | percent |
| `T10Y2Y` | FRED | yield spread | percent |
| `DFF` | FRED | policy rate | percent |

Future candidates pending source confirmation remain unsupported by the scripts:

- `VVIX`
- `OVX`
- `GVZ`

## Provider Notes

Cboe CSVs use the official historical-data CSV endpoint pattern:

```text
https://cdn.cboe.com/api/global/us_indices/daily_prices/<SYMBOL>_History.csv
```

FRED data uses the no-key graph CSV download:

```text
https://fred.stlouisfed.org/graph/fredgraph.csv?id=<SERIES_ID>&cosd=<START_DATE>&coed=<END_DATE>
```

No FRED API key is required for the current implementation.

## Normalized Schema

Curated reference-series parquet files use exactly these columns:

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
- Rows with missing dates or missing values are dropped during normalization.
- Rows are sorted ascending by date.
- Duplicate dates are de-duplicated, keeping the latest row seen.

For Cboe rows, `value` equals `close`. For FRED rows, `open`, `high`, and `low` are null, while
`close` and `value` both equal the observation value.

## S3 Layout

App-ready curated parquet:

```text
curated/reference_series/symbol=<symbol>/year=<YYYY>/data.parquet
```

Curated metadata:

```text
curated/reference/reference_series_metadata/data.parquet
```

The metadata file includes `symbol`, `source`, `series_type`, `first_available_date`,
`last_available_date`, `row_count`, and `updated_at`. Parquet files are written with Snappy
compression.

## Example Commands

Backfill dry-run:

```bash
python scripts/reference_series/backfill_reference_series.py \
  --symbols VIX,VIX9D,VXEEM,DGS10,DGS2,T10Y2Y,DFF \
  --start-date 2010-01-01 \
  --end-date 2026-04-30 \
  --bucket us-market-data-casper \
  --region us-west-2 \
  --dry-run
```

Write curated:

```bash
python scripts/reference_series/backfill_reference_series.py \
  --symbols VIX,VIX9D,VXEEM,DGS10,DGS2,T10Y2Y,DFF \
  --start-date 2010-01-01 \
  --end-date 2026-04-30 \
  --bucket us-market-data-casper \
  --region us-west-2 \
  --overwrite
```

Validate curated:

```bash
python scripts/reference_series/check_reference_series.py \
  --symbols VIX,VIX9D,VXEEM,DGS10,DGS2,T10Y2Y,DFF \
  --bucket us-market-data-casper \
  --region us-west-2
```

## Athena

Athena can query these layouts if external tables are added later, but Athena is optional and is not
required by app runtime behavior. This branch only creates manual curation scripts and
documentation; Lambda/EventBridge automation is intentionally out of scope here.
