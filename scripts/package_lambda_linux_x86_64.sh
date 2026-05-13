#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 <lambda_name>"
  echo "Example: $0 massive_ticker_overview_ingest"
}

die() {
  echo "Error: $*" >&2
  exit 1
}

if [ $# -ne 1 ]; then
  usage
  exit 1
fi

LAMBDA_NAME="$1"
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_DIR="$ROOT_DIR/build/${LAMBDA_NAME}_linux_x86_64"
DIST_DIR="$ROOT_DIR/dist"
ZIP_PATH="$DIST_DIR/${LAMBDA_NAME}_linux_x86_64.zip"
LAMBDA_DIR="$ROOT_DIR/lambdas/$LAMBDA_NAME"
REQUIREMENTS_FILE="$ROOT_DIR/requirements/lambda.txt"
IMAGE="${LAMBDA_BUILD_IMAGE:-public.ecr.aws/sam/build-python3.14:latest-x86_64}"
PLATFORM="linux/amd64"

[ -d "$LAMBDA_DIR" ] || die "Lambda folder not found: $LAMBDA_DIR"
[ -f "$LAMBDA_DIR/lambda_function.py" ] || die "Missing handler file: $LAMBDA_DIR/lambda_function.py"
[ -d "$ROOT_DIR/src/us_market_data" ] || die "Missing package directory: $ROOT_DIR/src/us_market_data"
[ -f "$REQUIREMENTS_FILE" ] || die "Missing requirements file: $REQUIREMENTS_FILE"

command -v docker >/dev/null 2>&1 || die "Docker is required to build Linux x86_64 Lambda packages."

if ! docker info >/dev/null 2>&1; then
  die "Docker is installed but the daemon is not available. Start Docker and rerun this script."
fi

rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR" "$DIST_DIR"
rm -f "$ZIP_PATH"

echo "Building $LAMBDA_NAME for Python 3.14 Lambda on $PLATFORM"
echo "Using Docker image: $IMAGE"

if ! docker run --rm \
  --platform "$PLATFORM" \
  --user "$(id -u):$(id -g)" \
  -e LAMBDA_NAME="$LAMBDA_NAME" \
  -e HOME=/tmp \
  -e PIP_CACHE_DIR=/tmp/pip-cache \
  -w /tmp \
  -v "$ROOT_DIR:/repo:ro" \
  -v "$BUILD_DIR:/build" \
  -v "$DIST_DIR:/dist" \
  "$IMAGE" \
  bash -euo pipefail -c '
    python3.14 -m pip install \
      --disable-pip-version-check \
      --no-cache-dir \
      --only-binary=:all: \
      -r /repo/requirements/lambda.txt \
      -t /build

    cp /repo/lambdas/"$LAMBDA_NAME"/lambda_function.py /build/
    cp -R /repo/src/us_market_data /build/

    python3.14 - <<'"'"'PY'"'"'
import os
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

lambda_name = os.environ["LAMBDA_NAME"]
build_dir = Path("/build")
zip_path = Path("/dist") / f"{lambda_name}_linux_x86_64.zip"

with ZipFile(zip_path, "w", ZIP_DEFLATED) as package:
    for path in sorted(build_dir.rglob("*")):
        if path.is_file():
            package.write(path, path.relative_to(build_dir).as_posix())
PY
  '; then
  cat >&2 <<'EOF'
Error: Linux x86_64 Lambda packaging failed inside Docker.

The package is built with Python 3.14 on linux/amd64 so native dependencies must publish
compatible Linux x86_64 wheels. If pip reported "No matching distribution found" for
pyarrow or another native package, update requirements/lambda.txt to the smallest version
range that provides CPython 3.14 manylinux x86_64 wheels, then rerun this script.
EOF
  exit 1
fi

[ -f "$ZIP_PATH" ] || die "Expected zip was not created: $ZIP_PATH"

FIRST_SO="$(find "$BUILD_DIR" -type f -name "*.so" | head -n 1 || true)"
if [ -n "$FIRST_SO" ]; then
  echo "Verifying native extension architecture: $FIRST_SO"

  if command -v file >/dev/null 2>&1; then
    FILE_OUTPUT="$(file "$FIRST_SO")"
    echo "$FILE_OUTPUT"
  else
    FILE_OUTPUT=""
    echo "Warning: 'file' command not found; falling back to ELF header verification."
  fi

  python3 - "$FIRST_SO" <<'PY'
import struct
import sys
from pathlib import Path

path = Path(sys.argv[1])
data = path.read_bytes()[:20]
if len(data) < 20 or data[:4] != b"\x7fELF":
    raise SystemExit(f"Error: {path} is not an ELF shared object.")

elf_class = data[4]
endianness = data[5]
if elf_class != 2:
    raise SystemExit(f"Error: {path} is ELF but not 64-bit.")
if endianness != 1:
    raise SystemExit(f"Error: {path} is ELF but not little-endian.")

machine = struct.unpack("<H", data[18:20])[0]
if machine != 0x3E:
    raise SystemExit(f"Error: {path} is ELF but not x86-64; machine={machine}.")
PY

  if [ -n "$FILE_OUTPUT" ] && [[ "$FILE_OUTPUT" != *"ELF 64-bit"* || "$FILE_OUTPUT" != *"x86-64"* ]]; then
    die "Native extension verification failed: expected ELF 64-bit x86-64 output."
  fi
else
  echo "No .so files found; skipping native extension architecture verification."
fi

echo "Created $ZIP_PATH"
