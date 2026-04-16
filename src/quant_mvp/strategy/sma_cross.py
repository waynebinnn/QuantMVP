from __future__ import annotations

import pandas as pd

from quant_mvp.strategy.base import Strategy


def _validate_windows(fast_window: int, slow_window: int, confirm_bars: int, trend_window: int) -> None:
    if fast_window <= 0 or slow_window <= 0:
        raise ValueError("Window sizes must be positive")
    if fast_window >= slow_window:
        raise ValueError("fast_window must be smaller than slow_window")
    if confirm_bars <= 0:
        raise ValueError("confirm_bars must be positive")
    if trend_window < slow_window:
        raise ValueError("trend_window must be larger than or equal to slow_window")


def _validate_volume_filter(volume_window: int, volume_multiplier: float) -> None:
    if volume_window <= 0:
        raise ValueError("volume_window must be positive")
    if volume_multiplier <= 0:
        raise ValueError("volume_multiplier must be positive")


def _stateful_sma_signal(
    close: pd.Series,
    fast_window: int,
    slow_window: int,
    confirm_bars: int,
    trend_window: int,
    volume: pd.Series | None = None,
    use_volume_filter: bool = False,
    volume_window: int = 20,
    volume_multiplier: float = 1.0,
) -> pd.Series:
    fast_ma = close.rolling(fast_window, min_periods=fast_window).mean()
    slow_ma = close.rolling(slow_window, min_periods=slow_window).mean()
    trend_ma = close.rolling(trend_window, min_periods=trend_window).mean()

    bullish = (fast_ma > slow_ma) & (close > slow_ma) & (close > trend_ma)
    bearish = (fast_ma < slow_ma) & (close < slow_ma) & (close < trend_ma)

    if use_volume_filter:
        if volume is None:
            raise ValueError("volume filter requires volume series")
        _validate_volume_filter(volume_window, volume_multiplier)
        vol_ma = volume.rolling(volume_window, min_periods=volume_window).mean()
        strong_volume = volume > (vol_ma * volume_multiplier)
        bullish = bullish & strong_volume

    bullish_confirmed = bullish.rolling(confirm_bars, min_periods=confirm_bars).sum() == confirm_bars
    bearish_confirmed = bearish.rolling(confirm_bars, min_periods=confirm_bars).sum() == confirm_bars

    positions: list[float] = []
    current_position = 0.0
    for is_bullish, is_bearish in zip(bullish_confirmed.fillna(False), bearish_confirmed.fillna(False)):
        if current_position == 0.0 and is_bullish:
            current_position = 1.0
        elif current_position == 1.0 and is_bearish:
            current_position = 0.0
        positions.append(current_position)

    return pd.Series(positions, index=close.index, dtype=float)


class SMACross60MinuteStrategy(Strategy):
    def __init__(
        self,
        fast_window: int = 5,
        slow_window: int = 20,
        confirm_bars: int = 1,
        trend_window: int = 30,
        use_volume_filter: bool = False,
        volume_window: int = 20,
        volume_multiplier: float = 1.05,
    ) -> None:
        _validate_windows(fast_window, slow_window, confirm_bars, trend_window)
        if use_volume_filter:
            _validate_volume_filter(volume_window, volume_multiplier)
        self.fast_window = fast_window
        self.slow_window = slow_window
        self.confirm_bars = confirm_bars
        self.trend_window = trend_window
        self.use_volume_filter = use_volume_filter
        self.volume_window = volume_window
        self.volume_multiplier = volume_multiplier

    def generate_signals(self, data: pd.DataFrame) -> pd.Series:
        close = data["close"]
        volume = data.get("volume")
        return _stateful_sma_signal(
            close=close,
            fast_window=self.fast_window,
            slow_window=self.slow_window,
            confirm_bars=self.confirm_bars,
            trend_window=self.trend_window,
            volume=volume,
            use_volume_filter=self.use_volume_filter,
            volume_window=self.volume_window,
            volume_multiplier=self.volume_multiplier,
        )

