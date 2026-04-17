#!/usr/bin/env bash
set -euo pipefail

if [ $# -ne 1 ]; then
  echo "Usage: $0 <lambda_name>"
  echo "Example: $0 etf_parquet_update"
  exit 1
fi

LAMBDA_NAME="$1"
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_DIR="$ROOT_DIR/build/$LAMBDA_NAME"
DIST_DIR="$ROOT_DIR/dist"
LAMBDA_DIR="$ROOT_DIR/lambdas/$LAMBDA_NAME"

rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR" "$DIST_DIR"

python -m pip install -r "$ROOT_DIR/requirements/lambda.txt" -t "$BUILD_DIR"
cp "$LAMBDA_DIR/lambda_function.py" "$BUILD_DIR/"
cp -R "$ROOT_DIR/src/us_market_data" "$BUILD_DIR/"

(cd "$BUILD_DIR" && zip -rq "$DIST_DIR/$LAMBDA_NAME.zip" .)

echo "Created $DIST_DIR/$LAMBDA_NAME.zip"
