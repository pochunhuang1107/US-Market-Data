from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "scripts" / "reference_series"))

import backfill_reference_series as reference_series  # noqa: E402
import check_reference_series  # noqa: E402


class ReferenceSeriesTests(unittest.TestCase):
    def test_provider_mapping(self) -> None:
        self.assertEqual(
            reference_series.PROVIDER_BY_SYMBOL,
            {
                "VIX": "cboe",
                "VIX9D": "cboe",
                "VXEEM": "cboe",
                "DGS10": "fred",
                "DGS2": "fred",
                "T10Y2Y": "fred",
                "DFF": "fred",
            },
        )

    def test_pending_cboe_symbols_are_rejected(self) -> None:
        with self.assertRaises(ValueError):
            reference_series.parse_symbols("VVIX")
        with self.assertRaises(ValueError):
            reference_series.parse_symbols("OVX,GVZ")

    def test_normalized_schema_columns(self) -> None:
        self.assertEqual(
            reference_series.NORMALIZED_SCHEMA.names,
            reference_series.NORMALIZED_COLUMNS,
        )

    def test_cboe_normalization(self) -> None:
        rows = [
            {"DATE": "2026-04-01", "OPEN": "20", "HIGH": "22", "LOW": "19", "CLOSE": "21"},
            {"DATE": "2026-04-02", "OPEN": "21", "HIGH": "23", "LOW": "20", "CLOSE": ""},
            {"DATE": "2026-04-01", "OPEN": "30", "HIGH": "32", "LOW": "29", "CLOSE": "31"},
        ]
        normalized = reference_series.normalize_cboe_rows(
            rows,
            "vix",
            date(2026, 4, 1),
            date(2026, 4, 30),
        )

        self.assertEqual(len(normalized), 1)
        self.assertEqual(normalized[0]["date"], "2026-04-01")
        self.assertEqual(normalized[0]["symbol"], "VIX")
        self.assertEqual(normalized[0]["source"], "cboe")
        self.assertEqual(normalized[0]["series_type"], "volatility_index")
        self.assertEqual(normalized[0]["close"], 31.0)
        self.assertEqual(normalized[0]["value"], 31.0)
        self.assertEqual(normalized[0]["unit"], "index_level")

    def test_fred_normalization(self) -> None:
        rows = [
            {"observation_date": "2026-04-01", "DGS10": "4.33"},
            {"observation_date": "2026-04-02", "DGS10": "."},
            {"observation_date": "2026-04-03", "DGS10": "4.35"},
        ]
        normalized = reference_series.normalize_fred_rows(
            rows,
            "DGS10",
            date(2026, 4, 1),
            date(2026, 4, 30),
        )

        self.assertEqual(len(normalized), 2)
        self.assertEqual(normalized[0]["source"], "fred")
        self.assertEqual(normalized[0]["series_type"], "treasury_yield")
        self.assertIsNone(normalized[0]["open"])
        self.assertEqual(normalized[0]["close"], 4.33)
        self.assertEqual(normalized[0]["value"], 4.33)
        self.assertEqual(normalized[0]["unit"], "percent")

    def test_curated_reference_series_key(self) -> None:
        self.assertEqual(
            reference_series.curated_key("vix", 2024),
            "curated/reference_series/symbol=VIX/year=2024/data.parquet",
        )

    def test_reference_series_metadata_key(self) -> None:
        self.assertEqual(
            reference_series.metadata_key(),
            "curated/reference/reference_series_metadata/data.parquet",
        )

    def test_sort_dedupe_rows_keeps_latest_row_encountered(self) -> None:
        rows = [
            {
                "date": "2024-01-02",
                "symbol": "VIX",
                "source": "cboe",
                "series_type": "volatility_index",
                "open": 12.0,
                "high": 14.0,
                "low": 11.0,
                "close": 13.0,
                "value": 13.0,
                "unit": "index_level",
            },
            {
                "date": "2024-01-01",
                "symbol": "VIX",
                "source": "cboe",
                "series_type": "volatility_index",
                "open": 10.0,
                "high": 12.0,
                "low": 9.0,
                "close": 11.0,
                "value": 11.0,
                "unit": "index_level",
            },
            {
                "date": "2024-01-02",
                "symbol": "VIX",
                "source": "cboe",
                "series_type": "volatility_index",
                "open": 20.0,
                "high": 22.0,
                "low": 19.0,
                "close": 21.0,
                "value": 21.0,
                "unit": "index_level",
            },
        ]

        curated = reference_series.sort_dedupe_rows(rows)

        self.assertEqual([row["date"] for row in curated], ["2024-01-01", "2024-01-02"])
        self.assertEqual(curated[1]["close"], 21.0)
        self.assertEqual(curated[1]["value"], 21.0)

    def test_group_rows_by_year(self) -> None:
        rows = [
            {"date": "2023-12-29"},
            {"date": "2024-01-02"},
            {"date": "2024-01-03"},
        ]

        grouped = reference_series.group_rows_by_year(rows)

        self.assertEqual(
            {year: len(year_rows) for year, year_rows in grouped.items()},
            {2023: 1, 2024: 2},
        )

    def test_check_reference_series_path_logic_uses_curated(self) -> None:
        self.assertEqual(
            check_reference_series.curated_prefix("vix"),
            "curated/reference_series/symbol=VIX/",
        )

    def test_metadata_merge_replaces_existing_symbol(self) -> None:
        existing = [
            {"symbol": "DGS10", "row_count": 10},
            {"symbol": "VIX", "row_count": 1},
        ]
        updated = [{"symbol": "VIX", "row_count": 2}]

        merged = reference_series.merge_metadata_rows(existing, updated)

        self.assertEqual(
            [(row["symbol"], row["row_count"]) for row in merged],
            [("DGS10", 10), ("VIX", 2)],
        )


if __name__ == "__main__":
    unittest.main()
