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


def _stateful_sma_signal(
    close: pd.Series,
    fast_window: int,
    slow_window: int,
    confirm_bars: int,
    trend_window: int,
    require_trend_slope: bool,
) -> pd.Series:
    fast_ma = close.rolling(fast_window, min_periods=fast_window).mean()
    slow_ma = close.rolling(slow_window, min_periods=slow_window).mean()
    trend_ma = close.rolling(trend_window, min_periods=trend_window).mean()

    bullish = (fast_ma > slow_ma) & (close > slow_ma) & (close > trend_ma)
    bearish = (fast_ma < slow_ma) & (close < slow_ma) & (close < trend_ma)

    if require_trend_slope:
        bullish = bullish & (trend_ma > trend_ma.shift(1))
        bearish = bearish & (trend_ma < trend_ma.shift(1))

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


class SMACrossDailyStrategy(Strategy):
    def __init__(
        self,
        fast_window: int = 5,
        slow_window: int = 20,
        confirm_bars: int = 2,
        trend_window: int = 30,
    ) -> None:
        _validate_windows(fast_window, slow_window, confirm_bars, trend_window)
        self.fast_window = fast_window
        self.slow_window = slow_window
        self.confirm_bars = confirm_bars
        self.trend_window = trend_window

    def generate_signals(self, data: pd.DataFrame) -> pd.Series:
        close = data["close"]
        return _stateful_sma_signal(
            close=close,
            fast_window=self.fast_window,
            slow_window=self.slow_window,
            confirm_bars=self.confirm_bars,
            trend_window=self.trend_window,
            require_trend_slope=True,
        )


class SMACross60MinuteStrategy(Strategy):
    def __init__(
        self,
        fast_window: int = 5,
        slow_window: int = 20,
        confirm_bars: int = 1,
        trend_window: int = 30,
    ) -> None:
        _validate_windows(fast_window, slow_window, confirm_bars, trend_window)
        self.fast_window = fast_window
        self.slow_window = slow_window
        self.confirm_bars = confirm_bars
        self.trend_window = trend_window

    def generate_signals(self, data: pd.DataFrame) -> pd.Series:
        close = data["close"]
        return _stateful_sma_signal(
            close=close,
            fast_window=self.fast_window,
            slow_window=self.slow_window,
            confirm_bars=self.confirm_bars,
            trend_window=self.trend_window,
            require_trend_slope=False,
        )

