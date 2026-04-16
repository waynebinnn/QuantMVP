from __future__ import annotations

from pathlib import Path
from xml.sax.saxutils import escape

import pandas as pd

from quant_mvp.backtest.engine import BacktestResult
from quant_mvp.report.events import extract_trade_events
from quant_mvp.report.metrics import PerformanceMetrics


def _format_date(value: pd.Timestamp) -> str:
    return f"{pd.Timestamp(value):%Y-%m-%d}"


def _scale_points(values: list[float], x0: int, y0: int, width: int, height: int) -> list[tuple[float, float]]:
    if len(values) == 1:
        return [(x0 + width / 2.0, y0 + height / 2.0)]

    min_value = min(values)
    max_value = max(values)
    if max_value == min_value:
        max_value = min_value + 1.0

    points: list[tuple[float, float]] = []
    for index, value in enumerate(values):
        x = x0 + (width * index / (len(values) - 1))
        normalized = (value - min_value) / (max_value - min_value)
        y = y0 + (height * (1.0 - normalized))
        points.append((x, y))
    return points


def _polyline(points: list[tuple[float, float]], color: str, stroke_width: int = 3) -> str:
    point_text = " ".join(f"{x:.1f},{y:.1f}" for x, y in points)
    return (
        f'<polyline fill="none" stroke="{color}" stroke-width="{stroke_width}" '
        f'stroke-linecap="round" stroke-linejoin="round" points="{point_text}" />'
    )


def _fill_path(points: list[tuple[float, float]], baseline_y: float, color: str) -> str:
    if not points:
        return ""
    path_points = [f"M {points[0][0]:.1f},{baseline_y:.1f}"]
    path_points.append(f"L {points[0][0]:.1f},{points[0][1]:.1f}")
    for x, y in points[1:]:
        path_points.append(f"L {x:.1f},{y:.1f}")
    path_points.append(f"L {points[-1][0]:.1f},{baseline_y:.1f} Z")
    return f'<path d="{" ".join(path_points)}" fill="{color}" opacity="0.22" />'


def _trade_marker(x: float, y: float, action: str) -> str:
    if action == "BUY":
        color = "#16a34a"
        label_y = y - 10
    else:
        color = "#dc2626"
        label_y = y + 18

    return (
        f'<g>'
        f'<circle cx="{x:.1f}" cy="{y:.1f}" r="6" fill="{color}" stroke="#ffffff" stroke-width="2" />'
        f'<text x="{x + 8:.1f}" y="{label_y:.1f}" font-size="11" font-weight="700" fill="{color}">{action}</text>'
        f'</g>'
    )


def _value_to_y(value: float, min_value: float, max_value: float, top: int, height: int) -> float:
    if max_value == min_value:
        return top + height / 2.0
    normalized = (value - min_value) / (max_value - min_value)
    return top + (height * (1.0 - normalized))


def _build_timeseries_svg(
    values: pd.Series,
    metrics: PerformanceMetrics,
    title: str,
    subtitle: str,
    output_path: Path,
    *,
    line_color: str,
    fill_color: str,
    value_label: str,
    value_formatter,
    trade_events,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    dates = list(pd.to_datetime(values.index))
    series_values = [float(value) for value in values.values]
    chart_width = 1070
    width = 1200
    height = 760
    margin_left = 90
    margin_top = 70
    chart_height = 320
    chart_x = margin_left
    chart_y = margin_top + 28
    axis_bottom = chart_y + chart_height

    points = _scale_points(series_values, chart_x, chart_y, chart_width, chart_height)
    series_min = min(series_values)
    series_max = max(series_values)

    date_to_index = {pd.Timestamp(date): index for index, date in enumerate(dates)}

    svg_parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#f8fafc" />',
        f'<text x="40" y="36" font-size="24" font-weight="700" fill="#111827">{escape(title)}</text>',
        f'<text x="40" y="58" font-size="13" fill="#6b7280">{escape(subtitle)}</text>',
        '<g transform="translate(930,18)">',
        '<rect x="0" y="0" width="220" height="56" rx="10" fill="#ffffff" stroke="#e2e8f0" />',
        '<circle cx="18" cy="20" r="5" fill="#16a34a" />',
        '<text x="30" y="24" font-size="12" fill="#334155">BUY: 开仓</text>',
        '<circle cx="18" cy="40" r="5" fill="#dc2626" />',
        '<text x="30" y="44" font-size="12" fill="#334155">SELL: 平仓</text>',
        '</g>',
        f'<text x="40" y="{chart_y - 14}" font-size="16" font-weight="700" fill="#111827">{escape(value_label)}</text>',
        f'<line x1="{chart_x}" y1="{axis_bottom}" x2="{chart_x + chart_width}" y2="{axis_bottom}" stroke="#94a3b8" stroke-width="1.5" />',
        f'<line x1="{chart_x}" y1="{chart_y}" x2="{chart_x}" y2="{axis_bottom}" stroke="#cbd5e1" stroke-width="1" />',
        f'<line x1="{chart_x}" y1="{axis_bottom}" x2="{chart_x + chart_width}" y2="{axis_bottom}" stroke="#cbd5e1" stroke-width="1" />',
        _fill_path(points, axis_bottom, fill_color),
        _polyline(points, line_color, 3),
        f'<text x="{chart_x}" y="{axis_bottom + 24}" font-size="12" fill="#64748b">{_format_date(dates[0]) if dates else ""}</text>',
        f'<text x="{chart_x + chart_width / 2.0 - 42}" y="{axis_bottom + 24}" font-size="12" fill="#64748b">{_format_date(dates[len(dates) // 2]) if dates else ""}</text>',
        f'<text x="{chart_x + chart_width - 72}" y="{axis_bottom + 24}" font-size="12" fill="#64748b">{_format_date(dates[-1]) if dates else ""}</text>',
        f'<text x="8" y="{chart_y + 14}" font-size="12" fill="#64748b">{value_formatter(series_max)}</text>',
        f'<text x="8" y="{axis_bottom}" font-size="12" fill="#64748b">{value_formatter(series_min)}</text>',
    ]

    for event in trade_events:
        index = date_to_index.get(pd.Timestamp(event.date))
        if index is None:
            continue
        x = points[index][0]
        y = _value_to_y(event.price, series_min, series_max, chart_y, chart_height)
        svg_parts.append(_trade_marker(x, y, event.action))

    svg_parts.extend([
        f'<text x="40" y="{height - 28}" font-size="13" fill="#334155">初始资金 {metrics.initial_capital:,.2f} | 结束资金 {metrics.ending_capital:,.2f} | 总盈亏 {metrics.total_pnl:+,.2f}</text>',
        '</svg>',
    ])

    output_path.write_text("\n".join(svg_parts), encoding="utf-8")
    return output_path


def save_price_chart(
    result: BacktestResult,
    metrics: PerformanceMetrics,
    output_dir: str | Path,
) -> Path:
    output_path = Path(output_dir) / "price_curve.svg"
    subtitle = f"区间 {result.start_datetime:%Y-%m-%d} -> {result.end_datetime:%Y-%m-%d} | 股票价格曲线与买卖点（下一根开盘成交）"
    return _build_timeseries_svg(
        result.price_curve,
        metrics,
        title="股票价格曲线",
        subtitle=subtitle,
        output_path=output_path,
        line_color="#0f766e",
        fill_color="#14b8a6",
        value_label="价格",
        value_formatter=lambda value: f"{value:,.2f}",
        trade_events=extract_trade_events(result),
    )


def save_return_chart(
    result: BacktestResult,
    metrics: PerformanceMetrics,
    output_dir: str | Path,
) -> Path:
    output_path = Path(output_dir) / "return_curve.svg"
    cumulative_return = result.equity_curve / result.initial_capital - 1.0
    subtitle = f"区间 {result.start_datetime:%Y-%m-%d} -> {result.end_datetime:%Y-%m-%d} | 累计收益曲线（按开盘到开盘收益计算）"
    return _build_timeseries_svg(
        cumulative_return,
        metrics,
        title="收益曲线",
        subtitle=subtitle,
        output_path=output_path,
        line_color="#2563eb",
        fill_color="#60a5fa",
        value_label="累计收益",
        value_formatter=lambda value: f"{value:.2%}",
        trade_events=extract_trade_events(result),
    )


def save_backtest_chart(
    result: BacktestResult,
    metrics: PerformanceMetrics,
    output_dir: str | Path,
) -> Path:
    return save_return_chart(result, metrics, output_dir)