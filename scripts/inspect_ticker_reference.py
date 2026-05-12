#!/usr/bin/env python3
# ruff: noqa: E402
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from us_market_data.ticker_reference_diagnostics import (
    DEFAULT_INPUT_PATH,
    build_diagnostics,
    read_parquet_rows,
    write_json_summary,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inspect the curated Massive ticker reference parquet dataset."
    )
    parser.add_argument("--input-path", default=DEFAULT_INPUT_PATH)
    parser.add_argument(
        "--symbol",
        action="append",
        default=None,
        help="Symbol to inspect; repeatable",
    )
    parser.add_argument("--top-n", default=25, type=int)
    parser.add_argument("--output-json", default=None)
    return parser.parse_args()


def print_distribution(title: str, values: dict[str, int]) -> None:
    print(f"\n{title}:")
    if not values:
        print("  <none>: 0")
        return
    for label, count in values.items():
        print(f"  {label}: {count}")


def print_non_common_samples(samples: dict[str, list[str]]) -> None:
    print("\nTop non-common-stock sample symbols by type:")
    if not samples:
        print("  <none>")
        return
    for type_label, symbols in samples.items():
        print(f"  {type_label}: {', '.join(symbols) if symbols else '<none>'}")


def print_symbol_checks(checks: list[dict[str, Any]]) -> None:
    print("\nSymbol checks:")
    for check in checks:
        fields = [
            f"symbol={check.get('symbol')}",
            f"status={check.get('status')}",
            f"name={check.get('name')}",
            f"type={check.get('type')}",
            f"market={check.get('market')}",
            f"locale={check.get('locale')}",
            f"active={check.get('active')}",
            f"primary_exchange={check.get('primary_exchange')}",
            f"is_common_stock_candidate={check.get('is_common_stock_candidate')}",
        ]
        print("  " + " | ".join(fields))


def print_diagnostics(diagnostics: dict[str, Any]) -> None:
    print(f"total row count: {diagnostics['total_count']}")
    print(f"common-stock candidate count: {diagnostics['common_stock_candidate_count']}")
    print(f"non-common-stock count: {diagnostics['non_common_stock_count']}")
    print_distribution("type distribution", diagnostics["type_distribution"])
    print_distribution(
        "primary_exchange distribution",
        diagnostics["primary_exchange_distribution"],
    )
    print_non_common_samples(diagnostics["non_common_stock_samples_by_type"])
    print(f"\nmissing/unknown type count: {diagnostics['missing_unknown_type_count']}")
    print_symbol_checks(diagnostics["symbol_checks"])


def main() -> int:
    args = parse_args()
    if args.top_n < 1:
        raise ValueError("--top-n must be >= 1")

    rows = read_parquet_rows(args.input_path)
    diagnostics = build_diagnostics(rows, symbols=args.symbol, top_n=args.top_n)
    print_diagnostics(diagnostics)

    if args.output_json:
        output_path = write_json_summary(args.output_json, diagnostics)
        print(f"\nwrote JSON summary: {output_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
