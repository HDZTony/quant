#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
标签敏感性实验：固定特征工程与 XGBoost 超参，仅改变 label_forward_minutes / label_threshold，
对每组参数做 Walk-Forward 评估，并用该组下最佳折的模型跑全样本 ML 回测，便于对比分类指标与资金曲线。

用法::

    uv run python ml_label_sensitivity.py
    uv run python ml_label_sensitivity.py --forwards 3 5 10 --thresholds 0.001 0.002 0.003
"""

from __future__ import annotations

import argparse
from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd

from ml_backtest_runner import BacktestConfig, DATA_PATH, backtest_ml_strategy
from ml_feature_builder import FeatureConfig, build_features, get_feature_columns
from ml_signal_trainer import walk_forward_evaluate, xgb_factory


def run_grid(
    forwards: list[int],
    thresholds: list[float],
    data_path: str,
) -> pd.DataFrame:
    p = Path(data_path)
    if not p.exists():
        raise FileNotFoundError(f"数据文件不存在: {p}")

    df_raw = pd.read_parquet(p)
    rows: list[dict] = []

    base = FeatureConfig()

    for fwd in forwards:
        for thr in thresholds:
            cfg = replace(
                base,
                label_forward_minutes=fwd,
                label_threshold=thr,
            )
            features = build_features(df_raw, cfg)
            feature_cols = get_feature_columns(features)
            X = features[feature_cols].values
            y_raw = features["label"].values.copy()
            y = np.where(y_raw == -1, 2, y_raw)

            wf = walk_forward_evaluate(X, y, xgb_factory, f"fwd={fwd}_thr={thr}")

            model = wf["best_model"]
            scaler = wf["best_scaler"]
            avg = wf["avg_metrics"]
            best_fold = wf["best_fold_idx"] + 1

            bt_cfg = BacktestConfig()
            bt = backtest_ml_strategy(features, model, scaler, feature_cols, bt_cfg)

            rows.append(
                {
                    "label_forward_minutes": fwd,
                    "label_threshold": thr,
                    "n_samples": len(features),
                    "wf_avg_precision": avg["precision"],
                    "wf_avg_recall": avg["recall"],
                    "wf_avg_f1": avg["f1"],
                    "wf_best_fold": best_fold,
                    "bt_total_return": bt.total_return,
                    "bt_max_drawdown": bt.max_drawdown,
                    "bt_sharpe": bt.sharpe_ratio,
                    "bt_total_trades": bt.total_trades,
                }
            )

            print(
                f"[fwd={fwd} thr={thr}] WF F1={avg['f1']:.4f} | "
                f"BT ret={bt.total_return:.4f} MDD={bt.max_drawdown:.4f} trades={bt.total_trades}"
            )

    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="标签参数网格：Walk-Forward + 全样本回测")
    parser.add_argument(
        "--forwards",
        type=int,
        nargs="+",
        default=[5, 10, 20],
        help="label_forward_minutes 候选（分钟）",
    )
    parser.add_argument(
        "--thresholds",
        type=float,
        nargs="+",
        default=[0.001, 0.002, 0.003],
        help="label_threshold 候选（远期收益绝对阈值）",
    )
    parser.add_argument(
        "--data",
        type=str,
        default=str(DATA_PATH),
        help="1 分钟 parquet 路径",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="models/label_sensitivity_report.csv",
        help="结果 CSV 路径",
    )
    args = parser.parse_args()

    df = run_grid(args.forwards, args.thresholds, args.data)
    outp = Path(args.output)
    outp.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(outp, index=False, encoding="utf-8-sig")
    print(f"\n已写入: {outp.resolve()}")
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
