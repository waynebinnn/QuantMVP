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
    dates = list(position.index)

    for index, (date, change) in enumerate(changes.items()):
        if change > 0:
            action = "BUY"
        elif change < 0:
            action = "SELL"
        else:
            continue

        display_index = min(index + 1, len(dates) - 1)
        display_date = pd.Timestamp(dates[display_index])

        events.append(
            TradeEvent(
                date=display_date,
                action=action,
                price=float(opens.iloc[display_index]),
                position_before=float(position.shift(1).loc[date]) if date in position.index else 0.0,
                position_after=float(position.loc[date]),
                equity=float(result.equity_curve.iloc[display_index]),
            )
        )

    return events