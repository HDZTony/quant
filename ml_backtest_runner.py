#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
ML 策略回测模块：在 1 分钟 Bar 数据上模拟 ML 信号交易，
与简单规则策略对比收益、最大回撤、胜率等指标。

策略说明
--------
- **ML 策略（方案 B）**：当 ML 模型输出买入/卖出概率超过阈值时执行交易。
  保守模式：只在模型高置信度时交易，降低假信号。
- **基准策略**：简单买入持有（Buy & Hold），作为下限对比。
- **规则策略模拟**：基于 MACD 金叉/死叉的简易规则，作为现有策略的近似对比。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import joblib
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from xgboost import XGBClassifier

from ml_feature_builder import build_features, get_feature_columns, FeatureConfig

plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Arial Unicode MS"]
plt.rcParams["axes.unicode_minus"] = False

MODELS_DIR = Path("models")
DATA_PATH = Path("data/159506_1min.parquet")
OUTPUT_DIR = Path("models")
CATALOG_PATH = Path("data/catalog")


# ---------------------------------------------------------------------------
# 回测配置
# ---------------------------------------------------------------------------
@dataclass
class BacktestConfig:
    initial_capital: float = 230_000.0
    trade_size: int = 10_000       # 每次交易股数
    commission_rate: float = 0.0003  # 佣金费率（单边）
    slippage: float = 0.001         # 滑点（元/股）
    buy_prob_threshold: float = 0.45  # ML 买入概率阈值
    sell_prob_threshold: float = 0.45  # ML 卖出概率阈值


# ---------------------------------------------------------------------------
# 交易记录
# ---------------------------------------------------------------------------
@dataclass
class Trade:
    entry_time: pd.Timestamp
    exit_time: pd.Timestamp | None = None
    entry_price: float = 0.0
    exit_price: float = 0.0
    size: int = 0
    side: str = "buy"
    pnl: float = 0.0
    commission: float = 0.0


# ---------------------------------------------------------------------------
# 回测引擎
# ---------------------------------------------------------------------------
@dataclass
class BacktestResult:
    strategy_name: str
    total_return: float = 0.0
    annual_return: float = 0.0
    max_drawdown: float = 0.0
    win_rate: float = 0.0
    total_trades: int = 0
    profit_factor: float = 0.0
    sharpe_ratio: float = 0.0
    equity_curve: pd.Series = field(default_factory=pd.Series)
    trades: list[Trade] = field(default_factory=list)


def _calc_metrics(
    equity_curve: pd.Series, trades: list[Trade], name: str, trading_days: int
) -> BacktestResult:
    total_return = (equity_curve.iloc[-1] / equity_curve.iloc[0]) - 1
    annual_return = (1 + total_return) ** (252 / max(trading_days, 1)) - 1

    peak = equity_curve.cummax()
    drawdown = (equity_curve - peak) / peak
    max_drawdown = drawdown.min()

    wins = [t for t in trades if t.pnl > 0]
    losses = [t for t in trades if t.pnl <= 0]
    win_rate = len(wins) / len(trades) if trades else 0.0

    total_profit = sum(t.pnl for t in wins) if wins else 0.0
    total_loss = abs(sum(t.pnl for t in losses)) if losses else 1e-9
    profit_factor = total_profit / total_loss

    daily_returns = equity_curve.pct_change().dropna()
    sharpe = (daily_returns.mean() / daily_returns.std() * np.sqrt(240)) if len(daily_returns) > 1 and daily_returns.std() > 0 else 0.0

    return BacktestResult(
        strategy_name=name,
        total_return=total_return,
        annual_return=annual_return,
        max_drawdown=max_drawdown,
        win_rate=win_rate,
        total_trades=len(trades),
        profit_factor=profit_factor,
        sharpe_ratio=sharpe,
        equity_curve=equity_curve,
        trades=trades,
    )


# ---------------------------------------------------------------------------
# ML 策略回测
# ---------------------------------------------------------------------------
def backtest_ml_strategy(
    features: pd.DataFrame,
    model: XGBClassifier,
    scaler,
    feature_cols: list[str],
    config: BacktestConfig,
) -> BacktestResult:
    """
    用 ML 模型在特征矩阵上逐行模拟交易。

    模型输出三类概率: [hold, buy, sell]。
    当 buy_prob > threshold 且无持仓时买入；
    当 sell_prob > threshold 且有持仓时卖出。
    """
    X = scaler.transform(features[feature_cols].values)
    probs = model.predict_proba(X)  # (n, 3): [hold, buy, sell]

    capital = config.initial_capital
    position = 0
    entry_price = 0.0
    entry_time = None
    trades: list[Trade] = []
    equity = []

    close_prices = features["datetime"].map(
        dict(zip(features["datetime"], features.index))
    )

    df_raw = pd.read_parquet(DATA_PATH)
    close_map = dict(zip(df_raw["datetime"], df_raw["close"]))
    datetimes = features["datetime"].values

    for i in range(len(features)):
        dt = pd.Timestamp(datetimes[i])
        price = close_map.get(dt, None)
        if price is None:
            equity.append(capital + position * entry_price)
            continue

        buy_prob = probs[i, 1]
        sell_prob = probs[i, 2]

        if position == 0 and buy_prob > config.buy_prob_threshold:
            affordable = int(capital // (price + config.slippage)) // 100 * 100
            size = min(config.trade_size, affordable)
            if size > 0:
                cost = size * (price + config.slippage)
                commission = cost * config.commission_rate
                capital -= cost + commission
                position = size
                entry_price = price + config.slippage
                entry_time = dt

        elif position > 0 and sell_prob > config.sell_prob_threshold:
            revenue = position * (price - config.slippage)
            commission = revenue * config.commission_rate
            capital += revenue - commission
            pnl = (price - config.slippage - entry_price) * position - commission
            trades.append(Trade(
                entry_time=entry_time,
                exit_time=dt,
                entry_price=entry_price,
                exit_price=price - config.slippage,
                size=position,
                pnl=pnl,
                commission=commission,
            ))
            position = 0
            entry_price = 0.0

        mark_to_market = capital + position * price
        equity.append(mark_to_market)

    equity_series = pd.Series(equity, index=features["datetime"].values)
    trading_days = features["datetime"].dt.date.nunique()
    return _calc_metrics(equity_series, trades, "ML 策略", trading_days)


# ---------------------------------------------------------------------------
# 简单规则策略：MACD 金叉买入 / 死叉卖出
# ---------------------------------------------------------------------------
def backtest_rule_strategy(
    features: pd.DataFrame,
    config: BacktestConfig,
) -> BacktestResult:
    df_raw = pd.read_parquet(DATA_PATH)
    close_map = dict(zip(df_raw["datetime"], df_raw["close"]))
    datetimes = features["datetime"].values

    capital = config.initial_capital
    position = 0
    entry_price = 0.0
    entry_time = None
    trades: list[Trade] = []
    equity = []

    golden = features["golden_cross"].values
    death = features["death_cross"].values

    for i in range(len(features)):
        dt = pd.Timestamp(datetimes[i])
        price = close_map.get(dt, None)
        if price is None:
            equity.append(capital + position * entry_price)
            continue

        if position == 0 and golden[i] == 1:
            affordable = int(capital // (price + config.slippage)) // 100 * 100
            size = min(config.trade_size, affordable)
            if size > 0:
                cost = size * (price + config.slippage)
                commission = cost * config.commission_rate
                capital -= cost + commission
                position = size
                entry_price = price + config.slippage
                entry_time = dt

        elif position > 0 and death[i] == 1:
            revenue = position * (price - config.slippage)
            commission = revenue * config.commission_rate
            capital += revenue - commission
            pnl = (price - config.slippage - entry_price) * position - commission
            trades.append(Trade(
                entry_time=entry_time,
                exit_time=dt,
                entry_price=entry_price,
                exit_price=price - config.slippage,
                size=position,
                pnl=pnl,
                commission=commission,
            ))
            position = 0
            entry_price = 0.0

        mark_to_market = capital + position * price
        equity.append(mark_to_market)

    equity_series = pd.Series(equity, index=features["datetime"].values)
    trading_days = features["datetime"].dt.date.nunique()
    return _calc_metrics(equity_series, trades, "MACD 金叉/死叉规则", trading_days)


# ---------------------------------------------------------------------------
# Buy & Hold 基准
# ---------------------------------------------------------------------------
def backtest_buy_hold(
    features: pd.DataFrame,
    config: BacktestConfig,
) -> BacktestResult:
    df_raw = pd.read_parquet(DATA_PATH)
    close_map = dict(zip(df_raw["datetime"], df_raw["close"]))
    datetimes = features["datetime"].values

    first_price = None
    for dt in datetimes:
        p = close_map.get(pd.Timestamp(dt))
        if p is not None:
            first_price = p
            break

    shares = int(config.initial_capital // (first_price + config.slippage)) // 100 * 100
    remaining = config.initial_capital - shares * (first_price + config.slippage)

    equity = []
    for dt in datetimes:
        p = close_map.get(pd.Timestamp(dt))
        if p is not None:
            equity.append(remaining + shares * p)
        else:
            equity.append(equity[-1] if equity else config.initial_capital)

    equity_series = pd.Series(equity, index=datetimes)
    last_price = close_map.get(pd.Timestamp(datetimes[-1]), first_price)
    pnl = (last_price - first_price - config.slippage) * shares
    trades = [Trade(
        entry_time=pd.Timestamp(datetimes[0]),
        exit_time=pd.Timestamp(datetimes[-1]),
        entry_price=first_price + config.slippage,
        exit_price=last_price,
        size=shares,
        pnl=pnl,
    )]
    trading_days = features["datetime"].dt.date.nunique()
    return _calc_metrics(equity_series, trades, "Buy & Hold", trading_days)


# ---------------------------------------------------------------------------
# 可视化对比
# ---------------------------------------------------------------------------
def plot_comparison(results: list[BacktestResult]) -> Path:
    fig, axes = plt.subplots(2, 1, figsize=(14, 10), gridspec_kw={"height_ratios": [3, 1]})

    # 权益曲线
    ax1 = axes[0]
    for r in results:
        normalized = r.equity_curve / r.equity_curve.iloc[0]
        ax1.plot(normalized.index, normalized.values, label=r.strategy_name, linewidth=1.2)
    ax1.set_title("策略权益曲线对比（归一化）")
    ax1.set_ylabel("净值")
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    # 回撤
    ax2 = axes[1]
    for r in results:
        peak = r.equity_curve.cummax()
        dd = (r.equity_curve - peak) / peak
        ax2.fill_between(dd.index, dd.values, alpha=0.3, label=r.strategy_name)
    ax2.set_title("回撤对比")
    ax2.set_ylabel("回撤")
    ax2.legend()
    ax2.grid(True, alpha=0.3)

    plt.tight_layout()
    path = OUTPUT_DIR / "backtest_comparison.png"
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"对比图已保存: {path}")
    return path


def print_comparison_table(results: list[BacktestResult]) -> None:
    print(f"\n{'='*80}")
    print(f"  策略回测对比报告")
    print(f"{'='*80}")
    header = f"{'策略':<20s} {'总收益':>10s} {'年化收益':>10s} {'最大回撤':>10s} {'胜率':>8s} {'交易次数':>8s} {'盈亏比':>8s} {'Sharpe':>8s}"
    print(header)
    print("-" * 80)
    for r in results:
        line = (
            f"{r.strategy_name:<20s} "
            f"{r.total_return:>9.2%} "
            f"{r.annual_return:>9.2%} "
            f"{r.max_drawdown:>9.2%} "
            f"{r.win_rate:>7.1%} "
            f"{r.total_trades:>8d} "
            f"{r.profit_factor:>7.2f} "
            f"{r.sharpe_ratio:>7.2f}"
        )
        print(line)
    print(f"{'='*80}\n")


# ---------------------------------------------------------------------------
# Nautilus BacktestNode 路径
# ---------------------------------------------------------------------------
def backtest_ml_strategy_nautilus(
    date_str: str,
    model_path: str = "models/xgb_signal.json",
    meta_path: str = "models/xgb_signal_meta.joblib",
) -> BacktestResult:
    """
    使用 Nautilus Trader 引擎运行 ML 策略回测。

    利用与实盘完全一致的 ``ETF159506MLStrategy``，
    回测结果可以直接与实盘环境对比。
    """
    import logging
    from datetime import datetime, date as date_cls

    from nautilus_trader.backtest.node import BacktestNode
    from nautilus_trader.config import (
        BacktestDataConfig,
        BacktestEngineConfig,
        BacktestRunConfig,
        BacktestVenueConfig,
        ImportableStrategyConfig,
        LoggingConfig,
    )
    from nautilus_trader.model.identifiers import InstrumentId

    logger = logging.getLogger(__name__)
    target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    instrument_id = InstrumentId.from_str("159506.SZSE")

    catalog_path = CATALOG_PATH
    if not catalog_path.exists():
        raise FileNotFoundError(f"Catalog 不存在: {catalog_path}，请先运行 etf_159506_official_backtest.py 准备数据")

    data_config = BacktestDataConfig(
        catalog_path=str(catalog_path),
        data_cls="nautilus_trader.model.data:Bar",
        instrument_id=instrument_id,
        start_time=datetime.combine(target_date, datetime.min.time()),
        end_time=datetime.combine(target_date, datetime.max.time()),
        bar_spec="1-MINUTE-LAST",
    )

    venue_config = BacktestVenueConfig(
        name="SZSE",
        oms_type="NETTING",
        account_type="MARGIN",
        base_currency="CNY",
        starting_balances=["230000 CNY"],
    )

    engine_config = BacktestEngineConfig(
        strategies=[
            ImportableStrategyConfig(
                strategy_path="etf_159506_ml_strategy:ETF159506MLStrategy",
                config_path="etf_159506_ml_strategy_config:ETF159506MLConfig",
                config={
                    "instrument_id": str(instrument_id),
                    "bar_type": "159506.SZSE-1-MINUTE-LAST-EXTERNAL",
                    "venue": "SZSE",
                    "trade_size": 0,
                    "model_path": model_path,
                    "meta_path": meta_path,
                    "buy_prob_threshold": 0.45,
                    "sell_prob_threshold": 0.45,
                },
            )
        ],
        logging=LoggingConfig(log_level="WARNING"),
    )

    run_config = BacktestRunConfig(
        engine=engine_config,
        venues=[venue_config],
        data=[data_config],
        dispose_on_completion=False,
    )

    logger.info(f"Nautilus ML 回测开始: {date_str}")
    node = BacktestNode(configs=[run_config])
    results = node.run()

    if not results:
        raise RuntimeError("Nautilus 回测没有返回结果")

    result = results[0]

    # 从策略实例提取交易信号
    signals: list[dict] = []
    for engine in node.get_engines():
        if hasattr(engine, "trader"):
            for strategy in engine.trader.strategies():
                if hasattr(strategy, "_saved_trade_signals"):
                    signals.extend(strategy._saved_trade_signals)

    # 简单权益曲线（从 result 中未必能直接取到完整 tick 级别，
    # 但可取 account balance）
    equity_val = 230_000.0
    try:
        for engine in node.get_engines():
            accounts = engine.trader.generate_account_report()
            if accounts is not None and len(accounts) > 0:
                equity_val = float(accounts.iloc[-1].get("total", equity_val))
    except Exception:
        pass

    total_return = (equity_val / 230_000.0) - 1
    bt_result = BacktestResult(
        strategy_name="ML 策略 (Nautilus)",
        total_return=total_return,
        total_trades=result.total_orders // 2,
    )
    bt_result._nautilus_signals = signals
    return bt_result


# ---------------------------------------------------------------------------
# 面向 kline-viewer 的结构化输出
# ---------------------------------------------------------------------------
def run_ml_backtest_for_viewer(date_str: str | None = None) -> dict:
    """
    运行 pandas 版 ML 回测，返回前端所需的结构化数据。

    Returns
    -------
    dict
        {
            "equity_curves": [{"name", "data": [{"time", "value"}]}],
            "metrics": [{"name", "total_return", ...}],
            "signals": [{"time", "position", "color", "shape", "text"}],
            "feature_importance": [{"feature", "importance", "rank"}],
        }
    """
    df_raw = pd.read_parquet(DATA_PATH)
    if date_str:
        df_raw["date_str"] = pd.to_datetime(df_raw["datetime"]).dt.strftime("%Y-%m-%d")
        df_raw = df_raw[df_raw["date_str"] == date_str].drop(columns=["date_str"])
        if df_raw.empty:
            return {"equity_curves": [], "metrics": [], "signals": [], "feature_importance": []}

    features = build_features(df_raw)
    feature_cols = get_feature_columns(features)

    model_path = MODELS_DIR / "xgb_signal.json"
    meta_path = MODELS_DIR / "xgb_signal_meta.joblib"
    if not model_path.exists():
        raise FileNotFoundError("模型未训练，请先运行 ml_signal_trainer.py")

    model = XGBClassifier()
    model.load_model(str(model_path))
    meta = joblib.load(meta_path)
    scaler = meta["scaler"]

    bt_config = BacktestConfig()

    ml_result = backtest_ml_strategy(features, model, scaler, feature_cols, bt_config)
    rule_result = backtest_rule_strategy(features, bt_config)
    bh_result = backtest_buy_hold(features, bt_config)

    def _curve_to_list(r: BacktestResult) -> list[dict]:
        normalized = r.equity_curve / r.equity_curve.iloc[0]
        out = []
        for ts, val in zip(normalized.index, normalized.values):
            t = int(pd.Timestamp(ts).timestamp())
            out.append({"time": t, "value": round(float(val), 6)})
        return out

    equity_curves = [
        {"name": r.strategy_name, "data": _curve_to_list(r)}
        for r in [ml_result, rule_result, bh_result]
    ]

    def _metric_dict(r: BacktestResult) -> dict:
        return {
            "name": r.strategy_name,
            "total_return": round(r.total_return, 6),
            "annual_return": round(r.annual_return, 6),
            "max_drawdown": round(r.max_drawdown, 6),
            "win_rate": round(r.win_rate, 4),
            "total_trades": r.total_trades,
            "profit_factor": round(r.profit_factor, 4),
            "sharpe_ratio": round(r.sharpe_ratio, 4),
        }

    metrics = [_metric_dict(r) for r in [ml_result, rule_result, bh_result]]

    # 将 ML 交易信号转换为 kline-viewer markers
    signals: list[dict] = []
    for t in ml_result.trades:
        if t.entry_time is not None:
            signals.append({
                "time": int(pd.Timestamp(t.entry_time).timestamp()),
                "position": "belowBar",
                "color": "rgba(239,83,80,0.9)",
                "shape": "arrowUp",
                "text": f"ML买 {t.entry_price:.3f}",
            })
        if t.exit_time is not None:
            signals.append({
                "time": int(pd.Timestamp(t.exit_time).timestamp()),
                "position": "aboveBar",
                "color": "rgba(38,166,154,0.9)",
                "shape": "arrowDown",
                "text": f"ML卖 {t.exit_price:.3f}",
            })

    # 特征重要性
    feature_importance = _load_feature_importance()

    return {
        "equity_curves": equity_curves,
        "metrics": metrics,
        "signals": signals,
        "feature_importance": feature_importance,
    }


def _load_feature_importance() -> list[dict]:
    """读取已缓存的特征重要性数据。"""
    meta_path = MODELS_DIR / "xgb_signal_meta.joblib"
    model_path = MODELS_DIR / "xgb_signal.json"
    if not model_path.exists() or not meta_path.exists():
        return []
    try:
        model = XGBClassifier()
        model.load_model(str(model_path))
        meta = joblib.load(meta_path)
        feature_cols = meta["feature_columns"]

        importance = model.feature_importances_
        ranked = sorted(
            zip(feature_cols, importance),
            key=lambda x: x[1],
            reverse=True,
        )
        return [
            {"feature": name, "importance": round(float(imp), 6), "rank": i + 1}
            for i, (name, imp) in enumerate(ranked)
        ]
    except Exception:
        return []


# ---------------------------------------------------------------------------
# 主入口
# ---------------------------------------------------------------------------
def main() -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)

    # 加载数据
    df_raw = pd.read_parquet(DATA_PATH)
    features = build_features(df_raw)
    feature_cols = get_feature_columns(features)

    # 加载 XGBoost 模型
    model_path = MODELS_DIR / "xgb_signal.json"
    meta_path = MODELS_DIR / "xgb_signal_meta.joblib"
    if not model_path.exists():
        raise FileNotFoundError(f"模型不存在: {model_path}，请先运行 ml_signal_trainer.py")

    model = XGBClassifier()
    model.load_model(str(model_path))
    meta = joblib.load(meta_path)
    scaler = meta["scaler"]

    bt_config = BacktestConfig()

    print(f"数据: {len(features)} 行, {features['datetime'].dt.date.nunique()} 交易日")
    print(f"初始资金: {bt_config.initial_capital:,.0f} CNY")
    print(f"每次交易: {bt_config.trade_size} 股")

    # 运行三种策略
    print("\n运行 ML 策略...")
    ml_result = backtest_ml_strategy(features, model, scaler, feature_cols, bt_config)

    print("运行 MACD 规则策略...")
    rule_result = backtest_rule_strategy(features, bt_config)

    print("运行 Buy & Hold...")
    bh_result = backtest_buy_hold(features, bt_config)

    # 输出对比
    results = [ml_result, rule_result, bh_result]
    print_comparison_table(results)
    plot_comparison(results)

    # ML 策略交易明细
    if ml_result.trades:
        print(f"\nML 策略交易明细（共 {len(ml_result.trades)} 笔）:")
        print(f"{'入场时间':<22s} {'出场时间':<22s} {'入场价':>8s} {'出场价':>8s} {'数量':>6s} {'盈亏':>10s}")
        print("-" * 80)
        for t in ml_result.trades[:20]:
            entry_str = str(t.entry_time)[:19] if t.entry_time else ""
            exit_str = str(t.exit_time)[:19] if t.exit_time else ""
            print(f"{entry_str:<22s} {exit_str:<22s} {t.entry_price:>8.3f} {t.exit_price:>8.3f} {t.size:>6d} {t.pnl:>+10.2f}")
        if len(ml_result.trades) > 20:
            print(f"  ... 省略 {len(ml_result.trades) - 20} 笔交易")


if __name__ == "__main__":
    main()
