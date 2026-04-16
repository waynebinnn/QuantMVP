from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(slots=True)
class CostConfig:
    fee_bps: float = 1.0
    slippage_bps: float = 1.0


@dataclass(slots=True)
class BacktestConfig:
    initial_capital: float = 1_000_000.0
    hourly60_periods_per_year: int = 968


@dataclass(slots=True)
class DataConfig:
    csv_path: str
    datetime_col: str = "datetime"


@dataclass(slots=True)
class ReportConfig:
    output_dir: str = "artifacts"


@dataclass(slots=True)
class DownloadConfig:
    symbol: str = ""
    period: str = "60min"
    start_date: str = ""
    end_date: str = ""
    adjust: str = "qfq"
    output_path: str = ""
    retries: int = 5
    chunk_days: int = 5
    timestamp_mode: str = "end"


@dataclass(slots=True)
class StrategyConfig:
    name: str = "sma_cross"
    mode: str = "60min"
    hourly60_fast_window: int = 5
    hourly60_slow_window: int = 20
    hourly60_confirm_bars: int = 1
    hourly60_trend_window: int = 30
    hourly60_use_volume_filter: bool = False
    hourly60_volume_window: int = 20
    hourly60_volume_multiplier: float = 1.05


@dataclass(slots=True)
class AppConfig:
    data: DataConfig
    strategy: StrategyConfig
    cost: CostConfig
    backtest: BacktestConfig
    report: ReportConfig
    download: DownloadConfig



def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}



def load_config(path: str) -> AppConfig:
    raw = _load_yaml(Path(path))
    data = DataConfig(**raw.get("data", {}))
    strategy = StrategyConfig(**raw.get("strategy", {}))
    cost = CostConfig(**raw.get("cost", {}))
    backtest = BacktestConfig(**raw.get("backtest", {}))
    report = ReportConfig(**raw.get("report", {}))
    download = DownloadConfig(**raw.get("download", {}))
    return AppConfig(data=data, strategy=strategy, cost=cost, backtest=backtest, report=report, download=download)


def load_download_config(path: str) -> DownloadConfig:
    raw = _load_yaml(Path(path))
    payload = raw.get("download", raw)
    if not isinstance(payload, dict):
        raise ValueError("Download config must be a mapping")
    return DownloadConfig(**payload)
