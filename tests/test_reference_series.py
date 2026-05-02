from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "scripts" / "reference_series"))
sys.path.insert(0, str(REPO_ROOT / "lambdas" / "reference_series_curated_update"))

import backfill_reference_series as reference_series  # noqa: E402
import check_reference_series  # noqa: E402
import lambda_function as reference_series_lambda  # noqa: E402


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

    def test_lambda_default_event_resolution(self) -> None:
        with patch.object(
            reference_series_lambda,
            "current_new_york_date",
            return_value=date(2026, 4, 30),
        ):
            request = reference_series_lambda.resolve_update_request({}, {})

        self.assertEqual(request.symbols, reference_series_lambda.DEFAULT_REFERENCE_SYMBOLS)
        self.assertEqual(request.start_date, date(2026, 1, 1))
        self.assertEqual(request.end_date, date(2026, 4, 30))
        self.assertFalse(request.dry_run)

    def test_lambda_event_overrides(self) -> None:
        request = reference_series_lambda.resolve_update_request(
            {
                "symbols": ["vix", "DGS10"],
                "start_date": "2026-01-01",
                "end_date": "2026-04-30",
                "dry_run": True,
            },
            {},
        )

        self.assertEqual(request.symbols, ["VIX", "DGS10"])
        self.assertEqual(request.start_date, date(2026, 1, 1))
        self.assertEqual(request.end_date, date(2026, 4, 30))
        self.assertTrue(request.dry_run)

    def test_lambda_reference_symbols_environment_override(self) -> None:
        with patch.object(
            reference_series_lambda,
            "current_new_york_date",
            return_value=date(2026, 4, 30),
        ):
            request = reference_series_lambda.resolve_update_request(
                {},
                {"REFERENCE_SYMBOLS": "DGS2,DFF"},
            )

        self.assertEqual(request.symbols, ["DGS2", "DFF"])

    def test_lambda_rejects_unsupported_symbols(self) -> None:
        for symbol in ("VVIX", "OVX", "GVZ"):
            with self.assertRaisesRegex(ValueError, "unsupported symbols"):
                reference_series_lambda.parse_symbols(symbol)

    def test_lambda_curated_key_uses_configurable_prefix(self) -> None:
        self.assertEqual(
            reference_series_lambda.curated_key("vix", 2026),
            "curated/reference_series/symbol=VIX/year=2026/data.parquet",
        )
        self.assertEqual(
            reference_series_lambda.curated_key("vix", 2026, "alt/reference"),
            "alt/reference/symbol=VIX/year=2026/data.parquet",
        )

    def test_lambda_metadata_key_default_and_environment_override(self) -> None:
        self.assertEqual(
            reference_series_lambda.metadata_key({}),
            "curated/reference/reference_series_metadata/data.parquet",
        )
        self.assertEqual(
            reference_series_lambda.metadata_key({"METADATA_KEY": "custom/metadata.parquet"}),
            "custom/metadata.parquet",
        )

    def test_lambda_http_timeout_default_is_30_seconds(self) -> None:
        self.assertEqual(
            reference_series_lambda.resolve_http_timeout_seconds({}),
            30.0,
        )

    def test_lambda_http_timeout_custom_value(self) -> None:
        self.assertEqual(
            reference_series_lambda.resolve_http_timeout_seconds(
                {"HTTP_TIMEOUT_SECONDS": "12.5"}
            ),
            12.5,
        )

    def test_lambda_http_timeout_invalid_value(self) -> None:
        with self.assertRaisesRegex(ValueError, "HTTP_TIMEOUT_SECONDS must be"):
            reference_series_lambda.resolve_http_timeout_seconds(
                {"HTTP_TIMEOUT_SECONDS": "nope"}
            )
        with self.assertRaisesRegex(ValueError, "HTTP_TIMEOUT_SECONDS must be positive"):
            reference_series_lambda.resolve_http_timeout_seconds(
                {"HTTP_TIMEOUT_SECONDS": "0"}
            )

    def test_lambda_urlopen_timeout_falls_back_to_curl(self) -> None:
        class CurlResult:
            returncode = 0
            stdout = b"ok"
            stderr = b""

        with patch.object(
            reference_series_lambda,
            "urlopen",
            side_effect=TimeoutError("urlopen timed out"),
        ):
            with patch.object(
                reference_series_lambda.shutil,
                "which",
                return_value="/usr/bin/curl",
            ):
                with patch.object(
                    reference_series_lambda.subprocess,
                    "run",
                    return_value=CurlResult(),
                ) as run_mock:
                    body = reference_series_lambda.fetch_url(
                        "https://example.test/data.csv",
                        source="fred",
                        symbol="DGS10",
                        timeout_seconds=12,
                    )

        self.assertEqual(body, b"ok")
        command = run_mock.call_args.args[0]
        self.assertEqual(command[:5], ["/usr/bin/curl", "-fsSL", "--http1.1", "--max-time", "12"])
        self.assertEqual(command[-1], "https://example.test/data.csv")

    def test_lambda_urlopen_failure_without_curl_is_clear(self) -> None:
        with patch.object(
            reference_series_lambda,
            "urlopen",
            side_effect=TimeoutError("urlopen timed out"),
        ):
            with patch.object(reference_series_lambda.shutil, "which", return_value=None):
                with self.assertRaisesRegex(
                    RuntimeError,
                    "failed fetching FRED symbol=DGS10 .*timeout_seconds=12",
                ) as raised:
                    reference_series_lambda.fetch_url(
                        "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS10",
                        source="fred",
                        symbol="DGS10",
                        timeout_seconds=12,
                    )

        message = str(raised.exception)
        self.assertIn("urlopen timed out", message)
        self.assertIn("curl fallback failed", message)
        self.assertIn("curl is not available for fallback provider fetch", message)

    def test_lambda_fred_fetch_failure_includes_context(self) -> None:
        class TimeoutResponse:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, traceback):
                return False

            def read(self):
                raise TimeoutError("The read operation timed out")

        with patch.object(
            reference_series_lambda,
            "urlopen",
            return_value=TimeoutResponse(),
        ) as urlopen_mock:
            with patch.dict(
                "os.environ",
                {"HTTP_TIMEOUT_SECONDS": "7"},
                clear=True,
            ):
                with patch.object(reference_series_lambda.shutil, "which", return_value=None):
                    with self.assertRaisesRegex(
                        RuntimeError,
                        "failed fetching FRED symbol=DGS10 .*timeout_seconds=7",
                    ) as raised:
                        reference_series_lambda.fetch_fred_series(
                            "DGS10",
                            date(2026, 1, 1),
                            date(2026, 1, 31),
                        )

        message = str(raised.exception)
        self.assertIn("url=https://fred.stlouisfed.org/graph/fredgraph.csv?", message)
        self.assertIn("The read operation timed out", message)
        self.assertIn("curl is not available for fallback provider fetch", message)
        self.assertEqual(urlopen_mock.call_args.kwargs["timeout"], 7.0)

    def test_lambda_merge_curated_rows_preserves_outside_window_and_overwrites_dates(self) -> None:
        existing = [
            {
                "date": "2025-12-31",
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
                "date": "2026-01-02",
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
        new = [
            {
                "date": "2026-01-01",
                "symbol": "VIX",
                "source": "cboe",
                "series_type": "volatility_index",
                "open": 12.0,
                "high": 13.0,
                "low": 11.0,
                "close": 12.5,
                "value": 12.5,
                "unit": "index_level",
            },
            {
                "date": "2026-01-02",
                "symbol": "VIX",
                "source": "cboe",
                "series_type": "volatility_index",
                "open": 30.0,
                "high": 32.0,
                "low": 29.0,
                "close": 31.0,
                "value": 31.0,
                "unit": "index_level",
            },
        ]

        merged = reference_series_lambda.merge_curated_rows(existing, new)

        self.assertEqual(
            [row["date"] for row in merged],
            ["2025-12-31", "2026-01-01", "2026-01-02"],
        )
        self.assertEqual(merged[-1]["close"], 31.0)

    def test_lambda_dry_run_does_not_touch_s3(self) -> None:
        rows = [
            {
                "date": "2026-01-02",
                "symbol": "VIX",
                "source": "cboe",
                "series_type": "volatility_index",
                "open": 20.0,
                "high": 22.0,
                "low": 19.0,
                "close": 21.0,
                "value": 21.0,
                "unit": "index_level",
            }
        ]

        def fake_fetch(symbol: str, start_date: date, end_date: date):
            return reference_series_lambda.SeriesFetchResult(
                symbol=symbol,
                source="cboe",
                url="https://example.test/vix.csv",
                normalized_rows=rows,
            )

        with patch.object(
            reference_series_lambda,
            "fetch_reference_series",
            side_effect=fake_fetch,
        ):
            with patch.object(
                reference_series_lambda,
                "get_s3_client",
                side_effect=AssertionError("no s3"),
            ):
                with patch.dict(
                    "os.environ",
                    {"BUCKET_NAME": "unit-test-bucket"},
                    clear=True,
                ):
                    result = reference_series_lambda.lambda_handler(
                        {
                            "symbols": ["VIX"],
                            "start_date": "2026-01-01",
                            "end_date": "2026-01-31",
                            "dry_run": True,
                        },
                        None,
                    )

        self.assertEqual(result["statusCode"], 200)
        self.assertEqual(result["status"], "DRY_RUN")
        self.assertEqual(result["symbols_updated"], ["VIX"])
        self.assertEqual(result["rows_fetched"], 1)
        self.assertEqual(result["rows_written"], 0)
        self.assertEqual(
            result["updated_keys"],
            ["curated/reference_series/symbol=VIX/year=2026/data.parquet"],
        )


if __name__ == "__main__":
    unittest.main()
