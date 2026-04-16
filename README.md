# Quant Trading MVP

一个以 YAML 配置驱动的量化回测小玩具，当前仅支持 60 分钟级别（60min）。

## 功能概览

- CSV 行情加载与字段校验
- SMA 状态机策略（60min）
- 向量化回测引擎
- 绩效指标：收益率、年化、Sharpe、最大回撤、交易次数等
- SVG 图表与 HTML 报告
- 全流程配置化运行（下载 + 回测）

## 环境与安装

```bash
pip install -r requirements.txt
```

当前主要依赖：

- pandas
- numpy
- pyyaml
- akshare

## 一分钟上手

1. 先下载数据（按下载配置）

```bash
python scripts/download_stock_data.py --config config/download.yaml
```

2. 跑 60 分钟回测

```bash
python scripts/run_backtest.py --config config/backtest_60min.yaml
```

## 配置文件说明

### 回测配置

- `config/backtest_60min.yaml`：60 分钟回测

关键项：

- `data.csv_path`：回测数据 CSV
- `strategy.mode`：固定为 `60min`
- `backtest.hourly60_periods_per_year`：60min 年化周期（常用 968）

### 下载配置

- `config/download.yaml`：下载脚本专用配置

关键项：

- `symbol`
- `period`：固定为 `60min`
- `start_date` / `end_date`
- `adjust`：`qfq` / `hfq` / `none`
- `retries`
- `chunk_days`
- `timestamp_mode`：`end` 或 `start`（仅分钟线生效）
- `output_path`：输出根目录或文件路径

## 重要坑位：symbol 一定要加引号

YAML 对无引号数字有自动类型推断。像 `002625` 如果不加引号，可能被解释为数字，进而被归一化成错误代码（例如 `001429`）。

推荐写法：

```yaml
symbol: "002625"
```

## 输出目录规则

### 下载输出

当 `output_path` 是目录（例如 `data/`）或为空时：

- `data/{symbol}/stock_{symbol}_60min.csv`

例如 symbol 为 002241：

- `data/002241/stock_002241_60min.csv`

### 回测输出

回测输出目录按 `symbol/mode` 自动分层：

- `artifacts/{symbol}/60min/price_curve.svg`
- `artifacts/{symbol}/60min/return_curve.svg`
- `artifacts/{symbol}/60min/report.html`

## 结果展示（artifacts）

当前结果目录：`/Users/binnn/workspace/量化/artifacts`

可在 README 中直接点击查看：

### 000938（60min）

- [HTML 报告](artifacts/000938/60min/report.html)

### 002241（60min）

- [HTML 报告](artifacts/002241/60min/report.html)

### 002625（60min）

- [HTML 报告](artifacts/002625/60min/report.html)

## 策略说明（当前实现）

策略文件：`src/quant_mvp/strategy/sma_cross.py`

- `SMACross60MinuteStrategy`

核心逻辑：

- 快慢均线关系 + 价格相对均线位置
- 连续确认 `confirm_bars`
- 状态机持仓切换（0/1）
- 可选量能过滤（`hourly60_use_volume_filter`）

## 下载机制与回退顺序

1. Eastmoney（`stock_zh_a_hist_min_em`）
2. Sina（`stock_zh_a_minute`）

脚本会在启动时打印实际生效参数，方便排查“配置看起来对但运行不对”的问题。

## 常见问题

### 1) 报错里 symbol 不是你写的值

先看启动日志 `[info] download config ... symbol=...`，这是实际生效值。

若不一致，通常是：

- YAML 未保存
- 运行了别的配置文件
- symbol 没有加引号导致类型转换

### 2) `RemoteDisconnected` / 下载失败

这是上游接口不稳定常见问题，建议：

- `retries` 提高到 8~12
- `chunk_days` 降到 1~2
- 分段时间范围多次下载

### 3) `No intraday data returned from Sina`

表示该 symbol 在 Sina 分钟接口当前不可用，或该时间范围没有返回。

### 4) 下载失败但希望继续

脚本支持本地缓存回退：如果同 symbol 同周期已有历史 CSV，会尝试复用缓存。


## 项目结构

- `scripts/download_stock_data.py`：数据下载入口
- `scripts/run_backtest.py`：回测入口
- `config/backtest_60min.yaml`：60 分钟回测配置
- `config/download.yaml`：下载配置
- `src/quant_mvp/backtest`：回测引擎
- `src/quant_mvp/strategy`：策略
- `src/quant_mvp/report`：指标/图表/报告
- `src/quant_mvp/data`：数据加载

## TODO / 当前问题

- 策略并未进行优化改善