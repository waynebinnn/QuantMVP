from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from quant_mvp.backtest.engine import BacktestResult


@dataclass(slots=True)
class TradeEvent:
    date: pd.Timestamp
    action: str
    price: float
    position_before: float
    position_after: float
    equity: float


def extract_trade_events(result: BacktestResult) -> list[TradeEvent]:
    events: list[TradeEvent] = []
    position = result.position.fillna(0.0)
    changes = position.diff().fillna(position.iloc[0])
    opens = result.open_curve.reindex(position.index)
    position_before = position.shift(1).fillna(0.0)
    equity_before_trade = result.equity_curve.shift(1).fillna(result.initial_capital)

    for date, change in changes.items():
        if change > 0:
            action = "BUY"
        elif change < 0:
            action = "SELL"
        else:
            continue

        events.append(
            TradeEvent(
                date=pd.Timestamp(date),
                action=action,
                price=float(opens.loc[date]),
                position_before=float(position_before.loc[date]),
                position_after=float(position.loc[date]),
                equity=float(equity_before_trade.loc[date]),
            )
        )

    return events