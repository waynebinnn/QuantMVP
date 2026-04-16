from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from quant_mvp.backtest.engine import run_backtest
from quant_mvp.config import load_config
from quant_mvp.data.csv_loader import load_ohlcv_csv
from quant_mvp.report.html import save_backtest_report
from quant_mvp.report.metrics import calculate_metrics
from quant_mvp.report.plots import save_price_chart, save_return_chart
from quant_mvp.strategy.sma_cross import SMACross60MinuteStrategy, SMACrossDailyStrategy


def resolve_symbol_from_csv_path(csv_path: str) -> str:
    stem = Path(csv_path).stem
    matches = re.findall(r"\d{6}", stem)
    if matches:
        return matches[-1]
    return stem


def build_strategy(name: str, mode: str, cfg_strategy):
    if name != "sma_cross":
        raise ValueError(f"Unsupported strategy: {name}")

    if mode == "daily":
        return SMACrossDailyStrategy(
            fast_window=cfg_strategy.daily_fast_window,
            slow_window=cfg_strategy.daily_slow_window,
            confirm_bars=cfg_strategy.daily_confirm_bars,
            trend_window=cfg_strategy.daily_trend_window,
            use_volume_filter=cfg_strategy.daily_use_volume_filter,
            volume_window=cfg_strategy.daily_volume_window,
            volume_multiplier=cfg_strategy.daily_volume_multiplier,
        )

    if mode == "60min":
        return SMACross60MinuteStrategy(
            fast_window=cfg_strategy.hourly60_fast_window,
            slow_window=cfg_strategy.hourly60_slow_window,
            confirm_bars=cfg_strategy.hourly60_confirm_bars,
            trend_window=cfg_strategy.hourly60_trend_window,
            use_volume_filter=cfg_strategy.hourly60_use_volume_filter,
            volume_window=cfg_strategy.hourly60_volume_window,
            volume_multiplier=cfg_strategy.hourly60_volume_multiplier,
        )

    raise ValueError(f"Unsupported strategy mode: {mode}. Use 'daily' or '60min'.")


def resolve_periods_per_year(cfg, mode: str, override: int | None) -> int:
    if override is not None:
        return override
    if mode == "60min":
        return cfg.backtest.hourly60_periods_per_year
    return cfg.backtest.periods_per_year



def main() -> None:
    parser = argparse.ArgumentParser(description="Run a quant MVP backtest")
    parser.add_argument("--config", required=True, help="Path to YAML config")
    args = parser.parse_args()

    cfg = load_config(args.config)
    strategy_mode = str(cfg.strategy.mode).lower()
    periods_per_year = resolve_periods_per_year(cfg, strategy_mode, None)

    data = load_ohlcv_csv(cfg.data.csv_path, datetime_col=cfg.data.datetime_col)

    strategy = build_strategy(
        name=cfg.strategy.name,
        mode=strategy_mode,
        cfg_strategy=cfg.strategy,
    )

    result = run_backtest(
        data=data,
        strategy=strategy,
        initial_capital=cfg.backtest.initial_capital,
        fee_bps=cfg.cost.fee_bps,
        slippage_bps=cfg.cost.slippage_bps,
    )

    metrics = calculate_metrics(result, periods_per_year=periods_per_year)

    def fmt_money(value: float) -> str:
        return f"¥{value:,.2f}"

    def fmt_signed_money(value: float) -> str:
        prefix = "+" if value >= 0 else "-"
        return f"{prefix}¥{abs(value):,.2f}"

    print("=== 回测概览 ===")
    print(f"回测区间: {metrics.start_datetime:%Y-%m-%d} -> {metrics.end_datetime:%Y-%m-%d}")
    print(f"样本数量: {len(data)}")
    print(f"初始资金: {fmt_money(metrics.initial_capital)}")
    print(f"结束资金: {fmt_money(metrics.ending_capital)}")
    print(f"总盈亏: {fmt_signed_money(metrics.total_pnl)}")
    print()

    print("=== 收益表现 ===")
    print(f"总收益率: {metrics.total_return:.2%}")
    print(f"年化收益率: {metrics.annual_return:.2%}")
    print(f"年化波动率: {metrics.annual_volatility:.2%}")
    print(f"夏普比率: {metrics.sharpe:.3f}")
    print(f"最大回撤: {metrics.max_drawdown:.2%}")
    print(f"最大回撤金额: {fmt_signed_money(metrics.max_drawdown_amount)}")
    print()

    print("=== 交易与持仓 ===")
    print(f"交易次数: {metrics.trade_count}")
    print(f"持仓占比: {metrics.invested_ratio:.2%}")
    print(f"上涨日占比: {metrics.positive_day_ratio:.2%}")
    print(f"平均换手率: {metrics.avg_turnover:.4f}")

    symbol = resolve_symbol_from_csv_path(cfg.data.csv_path)
    output_dir = Path(cfg.report.output_dir) / symbol / strategy_mode
    price_chart_path = save_price_chart(result, metrics, output_dir)
    return_chart_path = save_return_chart(result, metrics, output_dir)
    report_path = save_backtest_report(result, metrics, price_chart_path, return_chart_path, output_dir)
    print()
    print("=== 图表输出 ===")
    print(f"股票价格图: {price_chart_path}")
    print(f"收益曲线图: {return_chart_path}")
    print(f"HTML 报告: {report_path}")


if __name__ == "__main__":
    main()
