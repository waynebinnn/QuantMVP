from __future__ import annotations

import argparse
import time
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from quant_mvp.config import load_download_config


def _load_akshare():
    try:
        import akshare as ak  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover - runtime guard
        raise SystemExit(
            "akshare is not installed. Install it with: pip install akshare"
        ) from exc
    return ak


def _call_with_retry(func, retries: int = 3, delay_seconds: float = 1.5):
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return func()
        except Exception as exc:  # pragma: no cover - network instability
            last_exc = exc
            if attempt == retries:
                break
            time.sleep(delay_seconds * attempt)
    raise RuntimeError(f"Data download failed after {retries} attempts: {last_exc}") from last_exc


def _download_intraday_bars_chunked(
    ak,
    symbol: str,
    period: str,
    start_date: str,
    end_date: str,
    adjust: str,
    retries: int,
    chunk_days: int,
) -> pd.DataFrame:
    start_ts = pd.to_datetime(start_date)
    end_ts = pd.to_datetime(end_date)
    if start_ts >= end_ts:
        raise ValueError("start-date must be earlier than end-date")

    chunks: list[pd.DataFrame] = []
    cursor = start_ts
    while cursor < end_ts:
        chunk_end = min(cursor + pd.Timedelta(days=chunk_days), end_ts)
        chunk_start_str = cursor.strftime("%Y-%m-%d %H:%M:%S")
        chunk_end_str = chunk_end.strftime("%Y-%m-%d %H:%M:%S")

        chunk_df = _call_with_retry(
            lambda: ak.stock_zh_a_hist_min_em(
                symbol=symbol,
                period=period,
                start_date=chunk_start_str,
                end_date=chunk_end_str,
                adjust=adjust,
            ),
            retries=retries,
        )

        if not chunk_df.empty:
            chunks.append(chunk_df)

        cursor = chunk_end + pd.Timedelta(minutes=1)

    if not chunks:
        raise ValueError(f"No data returned for symbol={symbol}")
    return pd.concat(chunks, ignore_index=True)


def _download_daily_bars_chunked(
    ak,
    symbol: str,
    start_date: str,
    end_date: str,
    adjust: str,
    retries: int,
    chunk_days: int,
) -> pd.DataFrame:
    if chunk_days <= 0:
        raise ValueError("chunk_days must be positive")

    start_ts = pd.to_datetime(start_date).normalize()
    end_ts = pd.to_datetime(end_date).normalize()
    if start_ts > end_ts:
        raise ValueError("start_date must be earlier than or equal to end_date")

    def _fetch_range(left: pd.Timestamp, right: pd.Timestamp) -> list[pd.DataFrame]:
        chunk_start_str = left.strftime("%Y%m%d")
        chunk_end_str = right.strftime("%Y%m%d")
        try:
            chunk_df = _call_with_retry(
                lambda: ak.stock_zh_a_hist(
                    symbol=symbol,
                    period="daily",
                    start_date=chunk_start_str,
                    end_date=chunk_end_str,
                    adjust=adjust,
                    timeout=20,
                ),
                retries=retries,
            )
            return [chunk_df] if not chunk_df.empty else []
        except Exception as exc:
            if left >= right:
                print(f"[warn] Skip failed daily slice {chunk_start_str}-{chunk_end_str}: {exc}")
                return []
            mid = left + (right - left) / 2
            mid = pd.Timestamp(mid).normalize()
            left_chunks = _fetch_range(left, mid)
            right_start = mid + pd.Timedelta(days=1)
            right_chunks = _fetch_range(right_start, right)
            return left_chunks + right_chunks

    chunks: list[pd.DataFrame] = []
    cursor = start_ts
    while cursor <= end_ts:
        chunk_end = min(cursor + pd.Timedelta(days=chunk_days - 1), end_ts)
        chunks.extend(_fetch_range(cursor, chunk_end))

        cursor = chunk_end + pd.Timedelta(days=1)

    if not chunks:
        raise ValueError(f"No data returned for symbol={symbol}")
    return pd.concat(chunks, ignore_index=True)


def _to_sina_symbol(symbol: str) -> str:
    normalized = symbol.lower()
    if normalized.startswith("sh") or normalized.startswith("sz"):
        return normalized
    if normalized.startswith("6"):
        return f"sh{normalized}"
    return f"sz{normalized}"


def _normalize_symbol(symbol: object) -> str:
    raw = str(symbol).strip()
    if not raw:
        return ""

    lowered = raw.lower()
    if lowered.startswith("sh") or lowered.startswith("sz"):
        market = lowered[:2]
        code = lowered[2:]
        if code.isdigit() and len(code) < 6:
            code = code.zfill(6)
        return f"{market}{code}"

    if raw.isdigit() and len(raw) < 6:
        return raw.zfill(6)
    return raw


def _to_sina_adjust(adjust: str) -> str:
    if adjust.lower() in {"none", "", "raw"}:
        return ""
    return adjust


def _download_intraday_bars_sina(
    ak,
    symbol: str,
    period: str,
    start_date: str,
    end_date: str,
    adjust: str,
    retries: int,
) -> pd.DataFrame:
    sina_symbol = _to_sina_symbol(symbol)
    sina_adjust = _to_sina_adjust(adjust)
    raw = _call_with_retry(
        lambda: ak.stock_zh_a_minute(symbol=sina_symbol, period=period, adjust=sina_adjust),
        retries=retries,
    )
    if raw.empty:
        raise ValueError(
            f"No intraday data returned from Sina for symbol={sina_symbol}, period={period}. "
            "The symbol may be unavailable on this provider or outside the requested range."
        )

    df = raw.rename(
        columns={
            "day": "datetime",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
            "amount": "amount",
        }
    ).copy()
    df["datetime"] = pd.to_datetime(df["datetime"])

    start_ts = pd.to_datetime(start_date)
    end_ts = pd.to_datetime(end_date)
    df = df[(df["datetime"] >= start_ts) & (df["datetime"] <= end_ts)].reset_index(drop=True)
    return df


def download_a_share_bars(
    symbol: str,
    period: str,
    start_date: str,
    end_date: str,
    adjust: str,
    retries: int = 3,
    chunk_days: int = 7,
    timestamp_mode: str = "end",
) -> pd.DataFrame:
    ak = _load_akshare()

    if period not in {"daily", "60min"}:
        raise ValueError("download.period must be 'daily' or '60min' in the config file")

    if period == "daily":
        try:
            df = _download_daily_bars_chunked(
                ak=ak,
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                adjust=adjust,
                retries=retries,
                chunk_days=chunk_days,
            )
        except Exception as em_exc:
            print(f"[warn] Eastmoney daily API failed: {em_exc}")
            market_symbol = _to_sina_symbol(symbol)
            try:
                print("[warn] Falling back to Tencent daily API...")
                df = _call_with_retry(
                    lambda: ak.stock_zh_a_hist_tx(
                        symbol=market_symbol,
                        start_date=start_date.replace("-", ""),
                        end_date=end_date.replace("-", ""),
                        adjust=adjust,
                        timeout=20,
                    ),
                    retries=retries,
                )
            except Exception as tx_exc:
                print(f"[warn] Tencent daily API failed: {tx_exc}")
                try:
                    print("[warn] Retrying Tencent daily API with raw adjust...")
                    df = _call_with_retry(
                        lambda: ak.stock_zh_a_hist_tx(
                            symbol=market_symbol,
                            start_date=start_date.replace("-", ""),
                            end_date=end_date.replace("-", ""),
                            adjust="",
                            timeout=20,
                        ),
                        retries=retries,
                    )
                except Exception as tx_raw_exc:
                    print(f"[warn] Tencent raw-adjust daily API failed: {tx_raw_exc}")
                    print("[warn] Falling back to Sina daily API...")
                    try:
                        df = _call_with_retry(
                            lambda: ak.stock_zh_a_daily(
                                symbol=market_symbol,
                                start_date=start_date.replace("-", ""),
                                end_date=end_date.replace("-", ""),
                                adjust=_to_sina_adjust(adjust),
                            ),
                            retries=retries,
                        )
                    except Exception as sina_exc:
                        raise RuntimeError(
                            "All daily data sources failed: "
                            f"eastmoney={em_exc}; tencent={tx_exc}; tencent_raw={tx_raw_exc}; sina={sina_exc}"
                        ) from sina_exc
        rename_map = {
            "日期": "datetime",
            "date": "datetime",
            "Date": "datetime",
            "开盘": "open",
            "open": "open",
            "Open": "open",
            "收盘": "close",
            "close": "close",
            "Close": "close",
            "最高": "high",
            "high": "high",
            "High": "high",
            "最低": "low",
            "low": "low",
            "Low": "low",
            "成交量": "volume",
            "volume": "volume",
            "Volume": "volume",
            "成交额": "amount",
            "amount": "amount",
            "Amount": "amount",
            "振幅": "amplitude_pct",
            "amplitude_pct": "amplitude_pct",
            "涨跌幅": "pct_change",
            "pct_change": "pct_change",
            "涨跌额": "change_amount",
            "change_amount": "change_amount",
            "换手率": "turnover_rate_pct",
            "turnover_rate_pct": "turnover_rate_pct",
        }
    else:
        ak_period = "60"
        try:
            df = _download_intraday_bars_chunked(
                ak=ak,
                symbol=symbol,
                period=ak_period,
                start_date=start_date,
                end_date=end_date,
                adjust=adjust,
                retries=retries,
                chunk_days=chunk_days,
            )
        except Exception as em_exc:
            print(f"[warn] Eastmoney intraday API failed: {em_exc}")
            print("[warn] Falling back to Sina intraday API...")
            try:
                df = _download_intraday_bars_sina(
                    ak=ak,
                    symbol=symbol,
                    period=ak_period,
                    start_date=start_date,
                    end_date=end_date,
                    adjust=adjust,
                    retries=retries,
                )
            except Exception as sina_exc:
                raise RuntimeError(
                    "All intraday data sources failed: "
                    f"eastmoney={em_exc}; sina={sina_exc}"
                ) from sina_exc
        rename_map = {
            "时间": "datetime",
            "开盘": "open",
            "收盘": "close",
            "最高": "high",
            "最低": "low",
            "成交量": "volume",
            "成交额": "amount",
            "均价": "vwap",
            "datetime": "datetime",
            "open": "open",
            "close": "close",
            "high": "high",
            "low": "low",
            "volume": "volume",
            "amount": "amount",
        }

    if df.empty:
        raise ValueError(f"No data returned for symbol={symbol}")

    required_target_columns = {"datetime", "open", "close", "high", "low", "volume"}
    missing_targets = [
        target
        for target in required_target_columns
        if not any(source in df.columns and mapped == target for source, mapped in rename_map.items())
    ]
    if missing_targets:
        raise ValueError(f"Unexpected response columns, missing required targets: {missing_targets}")

    available_columns = [column for column in rename_map if column in df.columns]
    result = df.rename(columns=rename_map)[[rename_map[column] for column in available_columns]].copy()

    result.insert(0, "symbol", symbol)
    result.insert(1, "bar_period", period)
    result.insert(2, "adjust", adjust)
    result["datetime"] = pd.to_datetime(result["datetime"])

    if period != "daily" and timestamp_mode == "start":
        bar_minutes = 60 if period == "60min" else int(period)
        result["datetime"] = result["datetime"] - pd.to_timedelta(bar_minutes, unit="m")

    result = result.sort_values("datetime").reset_index(drop=True)

    for column in ["open", "high", "low", "close", "volume"]:
        result[column] = pd.to_numeric(result[column], errors="coerce")

    optional_numeric_columns = [
        "amount",
        "vwap",
        "amplitude_pct",
        "pct_change",
        "change_amount",
        "turnover_rate_pct",
    ]
    for column in optional_numeric_columns:
        if column in result.columns:
            result[column] = pd.to_numeric(result[column], errors="coerce")

    if result[["open", "high", "low", "close", "volume"]].isna().any().any():
        raise ValueError("Downloaded data contains invalid numeric values")

    result = result.drop_duplicates(subset=["datetime"]).reset_index(drop=True)

    return result


def download_a_share_daily(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    return download_a_share_bars(symbol=symbol, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")


def _resolve_output_path(output_path: str, symbol: str, period: str) -> Path:
    base_output = output_path.strip() if output_path else ""
    period_suffix = "60min" if period == "60min" else "day"
    auto_filename = f"stock_{symbol}_{period_suffix}.csv"

    if not base_output:
        return Path("data") / symbol / auto_filename

    path = Path(base_output)
    looks_like_directory = (
        base_output.endswith("/")
        or base_output.endswith("\\")
        or path.suffix == ""
    )
    if looks_like_directory:
        return path / symbol / auto_filename

    return path


def _try_load_local_cache(symbol: str, period: str) -> pd.DataFrame | None:
    period_suffix = "60min" if period == "60min" else "day"
    candidates = sorted(Path("data").glob(f"**/stock_{symbol}_{period_suffix}.csv"))
    if not candidates:
        return None
    latest = max(candidates, key=lambda p: p.stat().st_mtime)
    try:
        cached = pd.read_csv(latest)
    except Exception:
        return None
    if cached.empty:
        return None
    print(f"[warn] Using local cached data: {latest}")
    return cached


def main() -> None:
    parser = argparse.ArgumentParser(description="Download A-share daily or 60-minute data from config to CSV")
    parser.add_argument("--config", required=True, help="Path to YAML config")
    args = parser.parse_args()

    download = load_download_config(args.config)
    symbol = _normalize_symbol(download.symbol)
    if not symbol:
        raise ValueError("symbol is required in the download config file")
    if not download.start_date or not download.end_date:
        raise ValueError("start_date and end_date are required in the download config file")

    print(
        "[info] download config "
        f"path={args.config}, symbol={symbol}, period={download.period}, "
        f"start_date={download.start_date}, end_date={download.end_date}, "
        f"adjust={download.adjust}, retries={download.retries}, chunk_days={download.chunk_days}"
    )

    try:
        df = download_a_share_bars(
            symbol,
            download.period,
            download.start_date,
            download.end_date,
            download.adjust,
            retries=download.retries,
            chunk_days=download.chunk_days,
            timestamp_mode=download.timestamp_mode,
        )
    except Exception as exc:
        cached = _try_load_local_cache(symbol, download.period)
        if cached is None:
            raise
        print(f"[warn] Download failed, fallback to cache: {exc}")
        df = cached
    output_path = _resolve_output_path(download.output_path, symbol, download.period)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Saved {len(df)} rows to {output_path}")


if __name__ == "__main__":
    main()