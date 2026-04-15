from __future__ import annotations

from pathlib import Path
from xml.sax.saxutils import escape

from quant_mvp.backtest.engine import BacktestResult
from quant_mvp.report.events import extract_trade_events
from quant_mvp.report.metrics import PerformanceMetrics


def _metric_card(title: str, value: str, hint: str = "") -> str:
    hint_html = f'<div class="hint">{escape(hint)}</div>' if hint else ""
    return (
        '<div class="card">'
        f'<div class="label">{escape(title)}</div>'
        f'<div class="value">{escape(value)}</div>'
        f'{hint_html}'
        '</div>'
    )


def _trade_rows(result: BacktestResult) -> str:
    events = extract_trade_events(result)
    if not events:
        return '<tr><td colspan="5" class="empty">没有检测到交易点位</td></tr>'

    rows = []
    for event in events:
        rows.append(
            '<tr>'
            f'<td>{event.date:%Y-%m-%d}</td>'
            f'<td class="action {event.action.lower()}">{event.action}</td>'
            f'<td>{event.price:,.2f}</td>'
            f'<td>{event.position_before:.0f} -> {event.position_after:.0f}</td>'
            f'<td>{event.equity:,.2f}</td>'
            '</tr>'
        )
    return "".join(rows)


def save_backtest_report(
    result: BacktestResult,
    metrics: PerformanceMetrics,
    price_chart_path: str | Path,
    return_chart_path: str | Path,
    output_dir: str | Path,
) -> Path:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    price_chart_name = Path(price_chart_path).name
    return_chart_name = Path(return_chart_path).name
    report_path = output_path / "report.html"

    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Quant MVP 回测报告</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f8fafc;
      --panel: #ffffff;
      --border: #e2e8f0;
      --text: #0f172a;
      --muted: #64748b;
      --green: #16a34a;
      --red: #dc2626;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: linear-gradient(180deg, #eef2ff 0%, var(--bg) 240px);
      color: var(--text);
    }}
    .wrap {{ max-width: 1280px; margin: 0 auto; padding: 32px 20px 40px; }}
    .hero {{ display: flex; flex-wrap: wrap; gap: 18px; align-items: end; justify-content: space-between; margin-bottom: 20px; }}
    h1 {{ margin: 0; font-size: 32px; letter-spacing: -0.02em; }}
    .subtitle {{ color: var(--muted); margin-top: 8px; }}
    .meta {{ color: var(--muted); font-size: 14px; text-align: right; }}
    .grid {{ display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 14px; margin-bottom: 20px; }}
    .card {{ background: var(--panel); border: 1px solid var(--border); border-radius: 18px; padding: 18px; box-shadow: 0 10px 35px rgba(15, 23, 42, 0.04); }}
    .label {{ color: var(--muted); font-size: 13px; margin-bottom: 10px; }}
    .value {{ font-size: 24px; font-weight: 700; letter-spacing: -0.02em; }}
    .hint {{ color: var(--muted); font-size: 12px; margin-top: 8px; }}
    .panel {{ background: var(--panel); border: 1px solid var(--border); border-radius: 22px; padding: 18px; box-shadow: 0 12px 40px rgba(15, 23, 42, 0.05); margin-bottom: 20px; }}
    .panel h2 {{ margin: 0 0 14px; font-size: 18px; }}
    .charts {{ display: flex; flex-direction: column; gap: 18px; }}
    .chart h3 {{ margin: 0 0 10px; font-size: 15px; color: var(--muted); font-weight: 600; }}
    .chart img {{ width: 100%; height: auto; display: block; border-radius: 14px; border: 1px solid var(--border); }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th, td {{ padding: 12px 10px; border-bottom: 1px solid var(--border); text-align: left; }}
    th {{ color: var(--muted); font-weight: 600; }}
    tr:hover td {{ background: #f8fafc; }}
    .action.buy {{ color: var(--green); font-weight: 700; }}
    .action.sell {{ color: var(--red); font-weight: 700; }}
    .empty {{ text-align: center; color: var(--muted); padding: 18px 10px; }}
    @media (max-width: 1100px) {{ .grid {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }} }}
    @media (max-width: 700px) {{ .grid {{ grid-template-columns: 1fr; }} .meta {{ text-align: left; }} }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <div>
        <h1>Quant MVP 回测报告</h1>
        <div class="subtitle">{escape(f"区间 {metrics.start_datetime:%Y-%m-%d} -> {metrics.end_datetime:%Y-%m-%d}")}</div>
      </div>
      <div class="meta">
        <div>初始资金 {metrics.initial_capital:,.2f}</div>
        <div>结束资金 {metrics.ending_capital:,.2f}</div>
        <div>总盈亏 {metrics.total_pnl:+,.2f}</div>
      </div>
    </div>

    <div class="grid">
      {_metric_card("总收益率", f"{metrics.total_return:.2%}", f"年化 {metrics.annual_return:.2%}")}
      {_metric_card("夏普比率", f"{metrics.sharpe:.3f}", f"年化波动 {metrics.annual_volatility:.2%}")}
      {_metric_card("最大回撤", f"{metrics.max_drawdown:.2%}", f"回撤金额 {metrics.max_drawdown_amount:+,.2f}")}
      {_metric_card("交易次数", str(metrics.trade_count), f"持仓占比 {metrics.invested_ratio:.2%}")}
    </div>

    <div class="panel">
      <h2>曲线总览</h2>
      <div class="charts">
        <div class="chart">
          <h3>股票价格曲线与买卖点（下一根K线生效）</h3>
          <img src="{escape(price_chart_name)}" alt="price curve" />
        </div>
        <div class="chart">
          <h3>收益曲线（下一根K线生效）</h3>
          <img src="{escape(return_chart_name)}" alt="return curve" />
        </div>
      </div>
    </div>

    <div class="panel">
      <h2>交易点位</h2>
      <div style="color: var(--muted); font-size: 13px; margin: -6px 0 12px;">成交价按下一根K线生效后的价格展示。</div>
      <table>
        <thead>
          <tr>
            <th>日期</th>
            <th>动作</th>
            <th>成交价</th>
            <th>仓位变化</th>
            <th>对应净值</th>
          </tr>
        </thead>
        <tbody>
          {_trade_rows(result)}
        </tbody>
      </table>
    </div>
  </div>
</body>
</html>"""

    report_path.write_text(html, encoding="utf-8")
    return report_path