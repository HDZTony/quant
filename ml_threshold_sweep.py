#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
买卖概率阈值与成本敏感性：在固定已训练模型与特征的前提下，对 buy_prob_threshold、sell_prob_threshold
及可选佣金率、滑点做网格扫描，输出回测指标表（不写回模型文件）。

用法::

    uv run python ml_threshold_sweep.py
    uv run python ml_threshold_sweep.py --buy 0.35 0.45 0.55 --sell 0.35 0.45 0.55
    uv run python ml_threshold_sweep.py --commission 0.0002 0.0003 --slippage 0.0005 0.001
"""

from __future__ import annotations

import argparse
import itertools
from pathlib import Path

import joblib
import pandas as pd
from xgboost import XGBClassifier

from ml_backtest_runner import BacktestConfig, DATA_PATH, backtest_ml_strategy
from ml_feature_builder import build_features, get_feature_columns


MODELS_DIR = Path("models")


def sweep(
    buy_thresholds: list[float],
    sell_thresholds: list[float],
    commission_rates: list[float],
    slippages: list[float],
    data_path: Path,
) -> pd.DataFrame:
    df_raw = pd.read_parquet(data_path)
    features = build_features(df_raw)
    feature_cols = get_feature_columns(features)

    model_path = MODELS_DIR / "xgb_signal.json"
    meta_path = MODELS_DIR / "xgb_signal_meta.joblib"
    if not model_path.exists() or not meta_path.exists():
        raise FileNotFoundError("请先运行 ml_signal_trainer.py 生成 models/xgb_signal.json 与 meta")

    model = XGBClassifier()
    model.load_model(str(model_path))
    meta = joblib.load(meta_path)
    scaler = meta["scaler"]

    rows: list[dict] = []
    for comm, slip, bt, st in itertools.product(
        commission_rates, slippages, buy_thresholds, sell_thresholds
    ):
        cfg = BacktestConfig(
            commission_rate=comm,
            slippage=slip,
            buy_prob_threshold=bt,
            sell_prob_threshold=st,
        )
        r = backtest_ml_strategy(features, model, scaler, feature_cols, cfg)
        rows.append(
            {
                "commission_rate": comm,
                "slippage": slip,
                "buy_prob_threshold": bt,
                "sell_prob_threshold": st,
                "total_return": r.total_return,
                "annual_return": r.annual_return,
                "max_drawdown": r.max_drawdown,
                "sharpe_ratio": r.sharpe_ratio,
                "win_rate": r.win_rate,
                "total_trades": r.total_trades,
                "profit_factor": r.profit_factor,
            }
        )

    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="ML 回测：阈值与成本网格")
    parser.add_argument(
        "--buy",
        type=float,
        nargs="+",
        default=[0.35, 0.45, 0.55],
        help="买入概率阈值候选",
    )
    parser.add_argument(
        "--sell",
        type=float,
        nargs="+",
        default=[0.35, 0.45, 0.55],
        help="卖出概率阈值候选",
    )
    parser.add_argument(
        "--commission",
        type=float,
        nargs="+",
        default=[0.0003],
        help="单边佣金费率候选（与 BacktestConfig 一致）",
    )
    parser.add_argument(
        "--slippage",
        type=float,
        nargs="+",
        default=[0.001],
        help="滑点（元/股）候选",
    )
    parser.add_argument(
        "--data",
        type=str,
        default=str(DATA_PATH),
        help="1 分钟 parquet",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="models/threshold_cost_sweep.csv",
        help="输出 CSV",
    )
    args = parser.parse_args()

    df = sweep(
        args.buy,
        args.sell,
        args.commission,
        args.slippage,
        Path(args.data),
    )
    outp = Path(args.output)
    outp.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(outp, index=False, encoding="utf-8-sig")
    print(f"已写入: {outp.resolve()}  （共 {len(df)} 行）")
    # 按总收益排序展示前几行
    show = df.sort_values("total_return", ascending=False).head(15)
    print(show.to_string(index=False))


if __name__ == "__main__":
    main()
