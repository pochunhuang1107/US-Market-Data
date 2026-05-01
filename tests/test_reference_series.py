from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "scripts" / "reference_series"))

import backfill_reference_series as reference_series  # noqa: E402


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

    def test_monthly_staging_path(self) -> None:
        self.assertEqual(
            reference_series.staging_key("fred", "dgs10", 2026, 4),
            "staging/reference_series/source=fred/symbol=DGS10/year=2026/month=04/data.parquet",
        )


if __name__ == "__main__":
    unittest.main()
