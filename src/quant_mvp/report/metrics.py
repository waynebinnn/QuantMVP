from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from quant_mvp.backtest.engine import BacktestResult


@dataclass(slots=True)
class PerformanceMetrics:
    start_datetime: pd.Timestamp
    end_datetime: pd.Timestamp
    initial_capital: float
    ending_capital: float
    total_pnl: float
    total_return: float
    annual_return: float
    annual_volatility: float
    sharpe: float
    max_drawdown: float
    max_drawdown_amount: float
    avg_turnover: float
    invested_ratio: float
    positive_bar_ratio: float
    trade_count: int


def _max_drawdown(equity_curve: pd.Series) -> float:
    running_max = equity_curve.cummax()
    drawdown = (equity_curve / running_max) - 1.0
    return float(drawdown.min())


def _max_drawdown_amount(equity_curve: pd.Series) -> float:
    running_max = equity_curve.cummax()
    drawdown = equity_curve - running_max
    return float(drawdown.min())


def calculate_metrics(
    result: BacktestResult,
    periods_per_year: int = 252,
) -> PerformanceMetrics:
    ending_capital = float(result.equity_curve.iloc[-1])
    total_pnl = ending_capital - result.initial_capital
    total_return = float(ending_capital / result.initial_capital - 1.0)

    n_periods = max(len(result.returns), 1)
    annual_return = float((1.0 + total_return) ** (periods_per_year / n_periods) - 1.0)

    vol = float(result.returns.std(ddof=0) * np.sqrt(periods_per_year))
    mean_ret = float(result.returns.mean() * periods_per_year)
    sharpe = float(mean_ret / vol) if vol > 0 else 0.0

    max_dd = _max_drawdown(result.equity_curve)
    max_dd_amount = _max_drawdown_amount(result.equity_curve)
    avg_turnover = float(result.turnover.mean())
    invested_ratio = float((result.position > 0).mean())
    positive_bar_ratio = float((result.returns > 0).mean())
    trade_count = int((result.position.diff() > 0).sum())

    return PerformanceMetrics(
        start_datetime=result.start_datetime,
        end_datetime=result.end_datetime,
        initial_capital=result.initial_capital,
        ending_capital=ending_capital,
        total_pnl=total_pnl,
        total_return=total_return,
        annual_return=annual_return,
        annual_volatility=vol,
        sharpe=sharpe,
        max_drawdown=max_dd,
        max_drawdown_amount=max_dd_amount,
        avg_turnover=avg_turnover,
        invested_ratio=invested_ratio,
        positive_bar_ratio=positive_bar_ratio,
        trade_count=trade_count,
    )
