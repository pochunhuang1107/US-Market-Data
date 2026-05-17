#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

TIINGO_DAILY_PRICES_BASE_URL = "https://api.tiingo.com/tiingo/daily"
DEFAULT_SYMBOLS_FILE = "config/etf_symbols.example.txt"
DEFAULT_START_DATE = "1999-01-01"
DEFAULT_OUTPUT_DIR = "data/manual_backfill/raw/tiingo"
DEFAULT_VALIDATION_DIR = "data/manual_backfill/validation"
DEFAULT_TOKEN_ENV_VAR = "TIINGO_API_TOKEN"
DEFAULT_PRICE_MODE = "adjusted"
DEFAULT_REQUEST_INTERVAL_SECONDS = 1.0
DEFAULT_TIMEOUT_SECONDS = 30.0
DEFAULT_MAX_RETRIES = 3
SOURCE_PROVIDER = "tiingo"
RETRYABLE_HTTP_CODES = frozenset({429, 500, 502, 503, 504})

OUTPUT_COLUMNS = [
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "source_provider",
    "source_symbol",
    "price_mode",
    "raw_open",
    "raw_high",
    "raw_low",
    "raw_close",
    "raw_volume",
    "adj_open",
    "adj_high",
    "adj_low",
    "adj_close",
    "adj_volume",
    "div_cash",
    "split_factor",
]

MANIFEST_COLUMNS = [
    "symbol",
    "start_date",
    "end_date",
    "output_path",
    "status",
    "row_count",
    "first_date",
    "last_date",
    "price_mode",
    "warning",
]


@dataclass(frozen=True)
class TiingoDownloadConfig:
    symbols: list[str]
    start_date: str
    end_date: str
    output_dir: Path = Path(DEFAULT_OUTPUT_DIR)
    validation_dir: Path = Path(DEFAULT_VALIDATION_DIR)
    token_env_var: str = DEFAULT_TOKEN_ENV_VAR
    price_mode: str = DEFAULT_PRICE_MODE
    request_interval_seconds: float = DEFAULT_REQUEST_INTERVAL_SECONDS
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    max_retries: int = DEFAULT_MAX_RETRIES
    dry_run: bool = False
    overwrite: bool = False
    token: str | None = None
    opener: Callable[..., Any] = urlopen
    sleeper: Callable[[float], Any] = time.sleep
    progress_callback: Callable[[str], Any] | None = print


@dataclass
class TiingoDownloadResult:
    manifest_rows: list[dict[str, Any]]
    manifest_path: Path
    summary_path: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download Tiingo historical EOD ETF data into local normalized CSV files "
            "for manual backfill source comparison."
        )
    )
    parser.add_argument("--symbols-file", default=DEFAULT_SYMBOLS_FILE)
    parser.add_argument("--symbols", help="Optional comma-separated symbol override")
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--end-date")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--validation-dir", default=DEFAULT_VALIDATION_DIR)
    parser.add_argument("--token-env-var", default=DEFAULT_TOKEN_ENV_VAR)
    parser.add_argument(
        "--price-mode",
        choices=["adjusted", "raw"],
        default=DEFAULT_PRICE_MODE,
    )
    parser.add_argument(
        "--request-interval-seconds",
        type=float,
        default=DEFAULT_REQUEST_INTERVAL_SECONDS,
    )
    parser.add_argument("--timeout-seconds", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def parse_symbols_text(value: str) -> list[str]:
    symbols: list[str] = []
    for raw_line in value.splitlines():
        line = raw_line.split("#", 1)[0]
        for part in line.replace(",", " ").split():
            symbol = part.strip().upper()
            if symbol:
                symbols.append(symbol)
    return sorted(dict.fromkeys(symbols))


def load_symbols(symbols_file: str | Path) -> list[str]:
    with open(symbols_file, "r", encoding="utf-8") as f:
        return parse_symbols_text(f.read())


def validate_iso_date(value: str, name: str) -> str:
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise ValueError(f"{name} must be YYYY-MM-DD: {value}") from exc


def today_iso() -> str:
    return date.today().isoformat()


def config_from_args(args: argparse.Namespace) -> TiingoDownloadConfig:
    symbols = parse_symbols_text(args.symbols) if args.symbols else load_symbols(args.symbols_file)
    start_date = validate_iso_date(args.start_date, "--start-date")
    end_date = validate_iso_date(args.end_date or today_iso(), "--end-date")
    return TiingoDownloadConfig(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        output_dir=Path(args.output_dir),
        validation_dir=Path(args.validation_dir),
        token_env_var=str(args.token_env_var),
        price_mode=str(args.price_mode),
        request_interval_seconds=float(args.request_interval_seconds),
        timeout_seconds=float(args.timeout_seconds),
        max_retries=int(args.max_retries),
        dry_run=bool(args.dry_run),
        overwrite=bool(args.overwrite),
    )


def validate_config(config: TiingoDownloadConfig) -> None:
    if not config.symbols:
        raise ValueError("no symbols requested")
    if config.start_date > config.end_date:
        raise ValueError("--start-date must be on or before --end-date")
    if config.price_mode not in {"adjusted", "raw"}:
        raise ValueError("--price-mode must be one of: adjusted, raw")
    if config.request_interval_seconds < 0:
        raise ValueError("--request-interval-seconds must be >= 0")
    if config.timeout_seconds <= 0:
        raise ValueError("--timeout-seconds must be > 0")
    if config.max_retries < 0:
        raise ValueError("--max-retries must be >= 0")
    if not config.dry_run and not resolved_token(config):
        raise ValueError(f"missing Tiingo API token: set {config.token_env_var}")


def resolved_token(config: TiingoDownloadConfig) -> str | None:
    token = config.token if config.token is not None else os.environ.get(config.token_env_var)
    return token.strip() if token and token.strip() else None


def output_path_for_symbol(output_dir: Path, symbol: str) -> Path:
    return output_dir / f"{symbol.upper()}.csv"


def build_tiingo_prices_url(symbol: str, start_date: str, end_date: str) -> str:
    ticker = quote(symbol.upper(), safe="")
    query = urlencode(
        {
            "startDate": start_date,
            "endDate": end_date,
            "format": "json",
        }
    )
    return f"{TIINGO_DAILY_PRICES_BASE_URL}/{ticker}/prices?{query}"


def build_tiingo_prices_request(
    *,
    symbol: str,
    start_date: str,
    end_date: str,
    token: str,
) -> Request:
    return Request(
        build_tiingo_prices_url(symbol, start_date, end_date),
        method="GET",
        headers={
            "Accept": "application/json",
            "Authorization": f"Token {token}",
            "User-Agent": "us-market-data-tiingo-manual-backfill/0.1",
        },
    )


def retry_after_seconds(exc: HTTPError) -> float | None:
    retry_after = exc.headers.get("Retry-After") if exc.headers else None
    if retry_after is None:
        return None
    try:
        seconds = float(str(retry_after).strip())
    except ValueError:
        return None
    return seconds if seconds >= 0 else None


def backoff_seconds(retry_index: int) -> float:
    return min(60.0, 2.0 * (2**retry_index))


def fetch_tiingo_prices(
    *,
    symbol: str,
    config: TiingoDownloadConfig,
    token: str,
) -> list[dict[str, Any]]:
    request = build_tiingo_prices_request(
        symbol=symbol,
        start_date=config.start_date,
        end_date=config.end_date,
        token=token,
    )
    for retry_index in range(config.max_retries + 1):
        try:
            with config.opener(request, timeout=config.timeout_seconds) as response:
                body = response.read().decode("utf-8")
            payload = json.loads(body)
            if not isinstance(payload, list):
                raise RuntimeError("Tiingo daily prices response was not a JSON array")
            return payload
        except HTTPError as exc:
            message = f"HTTP {exc.code} fetching Tiingo daily prices for {symbol}"
            if exc.code not in RETRYABLE_HTTP_CODES:
                raise RuntimeError(message) from exc
            if retry_index >= config.max_retries:
                raise RuntimeError(f"{message} after {retry_index + 1} attempts") from exc
            sleep_seconds = retry_after_seconds(exc)
            if sleep_seconds is None:
                sleep_seconds = backoff_seconds(retry_index)
            if config.progress_callback:
                config.progress_callback(
                    f"HTTP {exc.code} fetching {symbol}; retry "
                    f"{retry_index + 1}/{config.max_retries} after {sleep_seconds:.1f}s"
                )
            config.sleeper(sleep_seconds)
        except (URLError, TimeoutError) as exc:
            reason = getattr(exc, "reason", exc)
            message = f"failed fetching Tiingo daily prices for {symbol}: {reason}"
            if retry_index >= config.max_retries:
                raise RuntimeError(f"{message} after {retry_index + 1} attempts") from exc
            sleep_seconds = backoff_seconds(retry_index)
            if config.progress_callback:
                config.progress_callback(
                    f"Error fetching {symbol}: {reason}; retry "
                    f"{retry_index + 1}/{config.max_retries} after {sleep_seconds:.1f}s"
                )
            config.sleeper(sleep_seconds)
        except json.JSONDecodeError as exc:
            raise RuntimeError("Tiingo daily prices endpoint returned non-JSON response") from exc
    raise RuntimeError(f"failed fetching Tiingo daily prices for {symbol}")


def parse_date_value(value: object) -> str | None:
    text = str(value).strip() if value is not None else ""
    if not text:
        return None
    candidate = text[:10]
    try:
        return date.fromisoformat(candidate).isoformat()
    except ValueError:
        return None


def parse_number(value: object) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text.replace(",", ""))
    except ValueError:
        return None


def choose_adjusted(adjusted_value: object, raw_value: object) -> float | None:
    adjusted = parse_number(adjusted_value)
    return adjusted if adjusted is not None else parse_number(raw_value)


def normalize_tiingo_rows(
    payload: list[dict[str, Any]],
    *,
    symbol: str,
    price_mode: str,
) -> tuple[list[dict[str, Any]], str]:
    rows_by_date: dict[str, dict[str, Any]] = {}
    invalid_dates = 0
    for raw in payload:
        normalized_date = parse_date_value(raw.get("date"))
        if normalized_date is None:
            invalid_dates += 1
            continue

        raw_open = parse_number(raw.get("open"))
        raw_high = parse_number(raw.get("high"))
        raw_low = parse_number(raw.get("low"))
        raw_close = parse_number(raw.get("close"))
        raw_volume = parse_number(raw.get("volume"))
        adj_open = parse_number(raw.get("adjOpen"))
        adj_high = parse_number(raw.get("adjHigh"))
        adj_low = parse_number(raw.get("adjLow"))
        adj_close = parse_number(raw.get("adjClose"))
        adj_volume = parse_number(raw.get("adjVolume"))

        if price_mode == "adjusted":
            open_value = choose_adjusted(raw.get("adjOpen"), raw.get("open"))
            high_value = choose_adjusted(raw.get("adjHigh"), raw.get("high"))
            low_value = choose_adjusted(raw.get("adjLow"), raw.get("low"))
            close_value = choose_adjusted(raw.get("adjClose"), raw.get("close"))
            volume_value = choose_adjusted(raw.get("adjVolume"), raw.get("volume"))
        else:
            open_value = raw_open
            high_value = raw_high
            low_value = raw_low
            close_value = raw_close
            volume_value = raw_volume

        rows_by_date[normalized_date] = {
            "date": normalized_date,
            "open": open_value,
            "high": high_value,
            "low": low_value,
            "close": close_value,
            "volume": volume_value,
            "source_provider": SOURCE_PROVIDER,
            "source_symbol": symbol.upper(),
            "price_mode": price_mode,
            "raw_open": raw_open,
            "raw_high": raw_high,
            "raw_low": raw_low,
            "raw_close": raw_close,
            "raw_volume": raw_volume,
            "adj_open": adj_open,
            "adj_high": adj_high,
            "adj_low": adj_low,
            "adj_close": adj_close,
            "adj_volume": adj_volume,
            "div_cash": parse_number(raw.get("divCash")),
            "split_factor": parse_number(raw.get("splitFactor")),
        }

    rows = [rows_by_date[key] for key in sorted(rows_by_date)]
    warnings: list[str] = []
    duplicate_count = len(payload) - invalid_dates - len(rows)
    if duplicate_count:
        warnings.append(f"duplicate date rows deduplicated: {duplicate_count}")
    if invalid_dates:
        warnings.append(f"rows skipped for invalid date: {invalid_dates}")
    return rows, "; ".join(warnings)


def write_normalized_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def first_last_dates(rows: list[dict[str, Any]]) -> tuple[str, str]:
    dates = [str(row["date"]) for row in rows if row.get("date")]
    if not dates:
        return "", ""
    return min(dates), max(dates)


def build_manifest_row(
    *,
    symbol: str,
    config: TiingoDownloadConfig,
    output_path: Path,
    status: str,
    rows: list[dict[str, Any]] | None = None,
    warning: str = "",
) -> dict[str, Any]:
    first_date, last_date = first_last_dates(rows or [])
    return {
        "symbol": symbol,
        "start_date": config.start_date,
        "end_date": config.end_date,
        "output_path": str(output_path),
        "status": status,
        "row_count": len(rows or []),
        "first_date": first_date,
        "last_date": last_date,
        "price_mode": config.price_mode,
        "warning": warning,
    }


def write_csv_report(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_summary(
    path: Path,
    *,
    config: TiingoDownloadConfig,
    rows: list[dict[str, Any]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    downloaded_count = sum(1 for row in rows if row["status"] == "downloaded")
    skipped_existing_count = sum(1 for row in rows if row["status"] == "skipped_existing")
    failed_count = sum(1 for row in rows if row["status"] == "failed")
    planned_count = sum(1 for row in rows if row["status"] == "planned")
    lines = [
        "# Tiingo Historical ETF Download Summary",
        "",
        "## Purpose",
        (
            "Download Tiingo historical EOD ETF data into local normalized CSV files "
            "for comparison against the manual Stooq backfill."
        ),
        "",
        "## Local-Only Notes",
        "- This is a local/manual one-time source adapter for source comparison.",
        "- No S3 write happened during this run.",
        "- No Massive automation changes happened during this run.",
        "- The Tiingo token was read from an environment variable and was not written to reports.",
    ]
    if config.dry_run:
        lines.append("- Dry run: Tiingo was not called and raw symbol CSV files were not written.")
    lines.extend(
        [
            "",
            "## Counts",
            f"- Symbol count: {len(rows)}",
            f"- Downloaded count: {downloaded_count}",
            f"- Skipped existing count: {skipped_existing_count}",
            f"- Failed count: {failed_count}",
        ]
    )
    if planned_count:
        lines.append(f"- Planned count: {planned_count}")
    lines.extend(
        [
            "",
            "## Output",
            f"- Output dir: {config.output_dir}",
            "",
            "## Next Validation Command",
            "```bash",
            "python scripts/validate_manual_historical_etf_backfill.py \\",
            "  --raw-dir data/manual_backfill/raw/tiingo \\",
            "  --source-provider tiingo \\",
            "  --massive-curated-dir data/manual_backfill/massive_curated_sample \\",
            "  --output-dir data/manual_backfill \\",
            "  --symbols-file config/etf_symbols.example.txt \\",
            "  --backfill-boundary-mode infer_from_massive",
            "```",
        ]
    )
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
        f.write("\n")


def run_download(config: TiingoDownloadConfig) -> TiingoDownloadResult:
    validate_config(config)
    token = resolved_token(config)
    manifest_rows: list[dict[str, Any]] = []
    made_request = False

    for symbol in config.symbols:
        output_path = output_path_for_symbol(config.output_dir, symbol)
        if config.dry_run:
            if config.progress_callback:
                config.progress_callback(f"planned {symbol}: {output_path}")
            manifest_rows.append(
                build_manifest_row(
                    symbol=symbol,
                    config=config,
                    output_path=output_path,
                    status="planned",
                    warning="dry run: Tiingo was not called",
                )
            )
            continue

        if output_path.exists() and not config.overwrite:
            manifest_rows.append(
                build_manifest_row(
                    symbol=symbol,
                    config=config,
                    output_path=output_path,
                    status="skipped_existing",
                    warning="output file exists; rerun with --overwrite to replace it",
                )
            )
            continue

        try:
            assert token is not None
            if made_request and config.request_interval_seconds:
                config.sleeper(config.request_interval_seconds)
            payload = fetch_tiingo_prices(symbol=symbol, config=config, token=token)
            made_request = True
            normalized_rows, warning = normalize_tiingo_rows(
                payload,
                symbol=symbol,
                price_mode=config.price_mode,
            )
            if not normalized_rows:
                raise RuntimeError("Tiingo returned no normalized price rows")
            write_normalized_csv(output_path, normalized_rows)
            manifest_rows.append(
                build_manifest_row(
                    symbol=symbol,
                    config=config,
                    output_path=output_path,
                    status="downloaded",
                    rows=normalized_rows,
                    warning=warning,
                )
            )
        except Exception as exc:  # noqa: BLE001 - per-symbol failure should continue.
            manifest_rows.append(
                build_manifest_row(
                    symbol=symbol,
                    config=config,
                    output_path=output_path,
                    status="failed",
                    warning=str(exc),
                )
            )
            if config.progress_callback:
                config.progress_callback(f"failed {symbol}: {exc}")

    manifest_path = config.validation_dir / "tiingo_download_manifest.csv"
    summary_path = config.validation_dir / "tiingo_download_summary.md"
    write_csv_report(manifest_path, manifest_rows, MANIFEST_COLUMNS)
    write_summary(summary_path, config=config, rows=manifest_rows)

    return TiingoDownloadResult(
        manifest_rows=manifest_rows,
        manifest_path=manifest_path,
        summary_path=summary_path,
    )


def all_requested_symbols_failed(result: TiingoDownloadResult) -> bool:
    return bool(result.manifest_rows) and all(
        row["status"] == "failed" for row in result.manifest_rows
    )


def main() -> None:
    try:
        config = config_from_args(parse_args())
        result = run_download(config)
    except Exception as exc:  # noqa: BLE001 - CLI should fail clearly.
        print(f"Tiingo historical ETF download failed: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc

    downloaded = sum(1 for row in result.manifest_rows if row["status"] == "downloaded")
    skipped = sum(1 for row in result.manifest_rows if row["status"] == "skipped_existing")
    failed = sum(1 for row in result.manifest_rows if row["status"] == "failed")
    planned = sum(1 for row in result.manifest_rows if row["status"] == "planned")
    print(
        "Tiingo historical ETF download complete: "
        f"symbols={len(result.manifest_rows)} "
        f"downloaded={downloaded} skipped_existing={skipped} "
        f"failed={failed} planned={planned}"
    )
    print(f"manifest: {result.manifest_path}")
    print(f"summary: {result.summary_path}")
    if all_requested_symbols_failed(result):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
