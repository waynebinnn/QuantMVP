from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from quant_mvp.strategy.base import Strategy


@dataclass(slots=True)
class BacktestResult:
    start_datetime: pd.Timestamp
    end_datetime: pd.Timestamp
    initial_capital: float
    open_curve: pd.Series
    price_curve: pd.Series
    equity_curve: pd.Series
    returns: pd.Series
    position: pd.Series
    turnover: pd.Series



def run_backtest(
    data: pd.DataFrame,
    strategy: Strategy,
    initial_capital: float = 1_000_000.0,
    fee_bps: float = 1.0,
    slippage_bps: float = 1.0,
) -> BacktestResult:
    indexed_data = data.set_index("datetime")
    signal = strategy.generate_signals(data).clip(lower=0.0, upper=1.0)
    signal.index = indexed_data.index

    position = signal.shift(1).fillna(0.0)
    asset_ret = indexed_data["close"].pct_change().fillna(0.0)

    gross_ret = position * asset_ret

    turnover = position.diff().abs().fillna(position.abs())
    trading_cost = turnover * ((fee_bps + slippage_bps) / 10_000.0)

    net_ret = gross_ret - trading_cost
    equity = float(initial_capital) * (1.0 + net_ret).cumprod()

    return BacktestResult(
        start_datetime=pd.Timestamp(data["datetime"].iloc[0]),
        end_datetime=pd.Timestamp(data["datetime"].iloc[-1]),
        initial_capital=float(initial_capital),
        open_curve=indexed_data["open"],
        price_curve=indexed_data["close"],
        equity_curve=equity,
        returns=net_ret,
        position=position,
        turnover=turnover,
    )
