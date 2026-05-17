#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

INPUT_DIR="${INPUT_DIR:-data/manual_backfill/raw/tiingo}"
OUTPUT_DIR="${OUTPUT_DIR:-data/research/etf_daily_prices_tiingo_only}"
SYMBOLS_FILE="${SYMBOLS_FILE:-config/etf_symbols.example.txt}"
SYMBOLS="${SYMBOLS:-}"
START_DATE="${START_DATE:-1993-01-01}"
END_DATE="${END_DATE:-}"
OVERWRITE="${OVERWRITE:-1}"

args=(
  --input-dir "$INPUT_DIR"
  --output-dir "$OUTPUT_DIR"
  --symbols-file "$SYMBOLS_FILE"
  --start-date "$START_DATE"
)

if [ -n "$SYMBOLS" ]; then
  args+=(--symbols "$SYMBOLS")
fi

if [ -n "$END_DATE" ]; then
  args+=(--end-date "$END_DATE")
fi

if [ "$OVERWRITE" = "1" ] || [ "$OVERWRITE" = "true" ] || [ "$OVERWRITE" = "yes" ]; then
  args+=(--overwrite)
fi

python scripts/create_tiingo_only_adjusted_etf_research_dataset.py "${args[@]}"

case "$OUTPUT_DIR" in
  /*) output_abs="$OUTPUT_DIR" ;;
  *) output_abs="$ROOT_DIR/$OUTPUT_DIR" ;;
esac

echo "Final parquet glob: $output_abs/**/*.parquet"
