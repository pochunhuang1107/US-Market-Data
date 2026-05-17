from __future__ import annotations
# ruff: noqa: E402,I001

import csv
import sys
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src"))

from scripts import export_local_massive_curated_sample as exporter  # noqa: E402


def client_error(code: str, key: str) -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": f"{key} failed"}},
        "HeadObject",
    )


class FakeS3Client:
    def __init__(self, objects: dict[str, bytes] | None = None) -> None:
        self.objects = objects or {}
        self.head_calls: list[str] = []
        self.download_calls: list[tuple[str, str, str]] = []

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, int]:
        self.head_calls.append(Key)
        if Key not in self.objects:
            raise client_error("404", Key)
        return {"ContentLength": len(self.objects[Key])}

    def download_file(self, Bucket: str, Key: str, Filename: str) -> None:
        self.download_calls.append((Bucket, Key, Filename))
        if Key not in self.objects:
            raise client_error("404", Key)
        Path(Filename).write_bytes(self.objects[Key])


def write_symbols(path: Path, content: str = "spy\n# comment\nQQQ\nSPY\n\n") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with open(path, "r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def config_for(
    tmp_path: Path,
    *,
    symbols: list[str] | None = None,
    dry_run: bool = False,
    allow_missing: bool = True,
) -> exporter.ExportConfig:
    return exporter.ExportConfig(
        bucket="test-bucket",
        curated_prefix="curated/etf_daily_prices",
        output_dir=tmp_path / "manual_backfill" / "massive_curated_sample",
        symbols=symbols or ["SPY"],
        start_year=2018,
        end_year=2018,
        dry_run=dry_run,
        allow_missing=allow_missing,
    )


def test_symbol_parsing_from_file_and_symbols_override(tmp_path: Path) -> None:
    symbols_file = tmp_path / "symbols.txt"
    write_symbols(symbols_file)

    assert exporter.resolve_symbols(None, str(symbols_file)) == ["QQQ", "SPY"]
    assert exporter.resolve_symbols("iwm, spy, IWM", str(symbols_file)) == ["IWM", "SPY"]


def test_local_path_construction(tmp_path: Path) -> None:
    output_dir = tmp_path / "sample"

    assert exporter.local_curated_path(output_dir, "SPY", 2018) == (
        output_dir / "symbol=SPY" / "year=2018" / "data.parquet"
    )
    assert exporter.manifest_path_for_output_dir(output_dir) == tmp_path / "sample_manifest.csv"
    assert exporter.summary_path_for_output_dir(output_dir) == tmp_path / "sample_summary.md"


def test_successful_object_download_writes_local_file(tmp_path: Path) -> None:
    key = "curated/etf_daily_prices/symbol=SPY/year=2018/data.parquet"
    fake_s3 = FakeS3Client({key: b"parquet bytes"})

    result = exporter.run_export(config_for(tmp_path), fake_s3)
    local_path = (
        tmp_path
        / "manual_backfill"
        / "massive_curated_sample"
        / "symbol=SPY"
        / "year=2018"
        / "data.parquet"
    )

    assert local_path.read_bytes() == b"parquet bytes"
    assert result.downloaded_count == 1
    assert result.missing_count == 0
    assert fake_s3.download_calls == [("test-bucket", key, str(local_path))]


def test_missing_object_with_allow_missing_true_records_missing(tmp_path: Path) -> None:
    result = exporter.run_export(config_for(tmp_path, allow_missing=True), FakeS3Client())

    assert result.downloaded_count == 0
    assert result.missing_count == 1
    assert result.failed_count == 0
    assert result.manifest_rows[0]["status"] == "missing"
    assert result.manifest_rows[0]["warning"] == "missing S3 object"


def test_missing_object_with_allow_missing_false_raises_after_manifest(tmp_path: Path) -> None:
    config = config_for(tmp_path, allow_missing=False)

    with pytest.raises(exporter.MissingS3ObjectsError, match="--no-allow-missing"):
        exporter.run_export(config, FakeS3Client())

    rows = read_csv_rows(exporter.manifest_path_for_output_dir(config.output_dir))
    assert rows[0]["status"] == "missing"


def test_manifest_creation(tmp_path: Path) -> None:
    key = "curated/etf_daily_prices/symbol=SPY/year=2018/data.parquet"
    config = config_for(tmp_path)

    result = exporter.run_export(config, FakeS3Client({key: b"abc"}))
    rows = read_csv_rows(result.manifest_path)

    assert result.manifest_path == (
        tmp_path / "manual_backfill" / "massive_curated_sample_manifest.csv"
    )
    assert rows == [
        {
            "symbol": "SPY",
            "year": "2018",
            "s3_uri": f"s3://test-bucket/{key}",
            "local_path": str(exporter.local_curated_path(config.output_dir, "SPY", 2018)),
            "status": "downloaded",
            "size_bytes": "3",
            "warning": "",
        }
    ]
    assert result.summary_path.read_text(encoding="utf-8").startswith(
        "# Massive Curated ETF Local Sample Summary"
    )


def test_dry_run_does_not_write_parquet_object_content(tmp_path: Path) -> None:
    key = "curated/etf_daily_prices/symbol=SPY/year=2018/data.parquet"
    config = config_for(tmp_path, dry_run=True)
    fake_s3 = FakeS3Client({key: b"parquet bytes"})

    result = exporter.run_export(config, fake_s3)
    local_path = exporter.local_curated_path(config.output_dir, "SPY", 2018)

    assert not local_path.exists()
    assert fake_s3.download_calls == []
    assert result.downloaded_count == 0
    assert result.manifest_rows[0]["status"] == "planned"
    assert "Dry run: parquet files were not downloaded." in result.summary_path.read_text(
        encoding="utf-8"
    )
