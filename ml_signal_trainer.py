#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
模型训练模块：使用 Walk-Forward TimeSeriesSplit 训练 XGBoost / LightGBM 信号分类器。

输出：
- 训练好的模型（JSON / txt）
- 每折的评估指标（precision / recall / f1）
- 最终选定模型保存到 models/ 目录
"""

from __future__ import annotations

from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import classification_report, precision_recall_fscore_support
from sklearn.model_selection import TimeSeriesSplit
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier
from lightgbm import LGBMClassifier

from ml_feature_builder import build_features, get_feature_columns, FeatureConfig


# ---------------------------------------------------------------------------
# 训练配置
# ---------------------------------------------------------------------------
MODELS_DIR = Path("models")
FEATURE_DATA = Path("data/159506_1min.parquet")
N_SPLITS = 5
GAP = 30  # 训练集与测试集之间的间隔（分钟数），防止数据泄露


# ---------------------------------------------------------------------------
# Walk-Forward 评估
# ---------------------------------------------------------------------------
def walk_forward_evaluate(
    X: np.ndarray,
    y: np.ndarray,
    model_factory,
    model_name: str,
    n_splits: int = N_SPLITS,
    gap: int = GAP,
) -> dict:
    """
    Walk-Forward 时序交叉验证。

    Returns
    -------
    dict 包含：
        fold_reports: 每折的 classification_report 字符串
        avg_metrics: 加权平均指标 (precision, recall, f1)
        best_fold_idx: 最佳折（按 macro-f1 排序）
        best_model: 最佳折训练的模型
    """
    tscv = TimeSeriesSplit(n_splits=n_splits, gap=gap)
    scaler = StandardScaler()

    fold_reports = []
    fold_metrics = []
    best_f1 = -1.0
    best_model = None
    best_scaler = None
    best_fold_idx = -1

    print(f"\n{'='*70}")
    print(f"  Walk-Forward 验证: {model_name}  ({n_splits} 折, gap={gap})")
    print(f"{'='*70}")

    for fold_idx, (train_idx, test_idx) in enumerate(tscv.split(X)):
        X_train, X_test = X[train_idx], X[test_idx]
        y_train, y_test = y[train_idx], y[test_idx]

        fold_scaler = StandardScaler()
        X_train_scaled = fold_scaler.fit_transform(X_train)
        X_test_scaled = fold_scaler.transform(X_test)

        model = model_factory()
        model.fit(X_train_scaled, y_train)

        y_pred = model.predict(X_test_scaled)

        report = classification_report(y_test, y_pred, zero_division=0)
        precision, recall, f1, support = precision_recall_fscore_support(
            y_test, y_pred, average="macro", zero_division=0
        )

        fold_reports.append(report)
        fold_metrics.append({"precision": precision, "recall": recall, "f1": f1})

        print(f"\n--- Fold {fold_idx + 1}/{n_splits} ---")
        print(f"  训练集: {len(train_idx)} 样本, 测试集: {len(test_idx)} 样本")
        print(f"  Macro  P={precision:.4f}  R={recall:.4f}  F1={f1:.4f}")

        if f1 > best_f1:
            best_f1 = f1
            best_model = model
            best_scaler = fold_scaler
            best_fold_idx = fold_idx

    avg_metrics = {
        k: np.mean([m[k] for m in fold_metrics]) for k in ("precision", "recall", "f1")
    }
    print(f"\n{'='*70}")
    print(f"  {model_name} 平均指标  P={avg_metrics['precision']:.4f}  "
          f"R={avg_metrics['recall']:.4f}  F1={avg_metrics['f1']:.4f}")
    print(f"  最佳折: Fold {best_fold_idx + 1} (F1={best_f1:.4f})")
    print(f"{'='*70}\n")

    return {
        "fold_reports": fold_reports,
        "fold_metrics": fold_metrics,
        "avg_metrics": avg_metrics,
        "best_fold_idx": best_fold_idx,
        "best_model": best_model,
        "best_scaler": best_scaler,
    }


# ---------------------------------------------------------------------------
# 模型工厂
# ---------------------------------------------------------------------------
def xgb_factory() -> XGBClassifier:
    return XGBClassifier(
        n_estimators=300,
        max_depth=5,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        objective="multi:softprob",
        eval_metric="mlogloss",
        use_label_encoder=False,
        verbosity=0,
        random_state=42,
    )


def lgbm_factory() -> LGBMClassifier:
    return LGBMClassifier(
        n_estimators=300,
        max_depth=5,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        objective="multiclass",
        num_class=3,
        verbose=-1,
        random_state=42,
    )


# ---------------------------------------------------------------------------
# 保存模型
# ---------------------------------------------------------------------------
def save_model(model, scaler, feature_cols: list[str], model_name: str) -> Path:
    MODELS_DIR.mkdir(exist_ok=True)

    if isinstance(model, XGBClassifier):
        model_path = MODELS_DIR / f"{model_name}.json"
        model.save_model(str(model_path))
    else:
        model_path = MODELS_DIR / f"{model_name}.txt"
        model.booster_.save_model(str(model_path))

    meta = {
        "scaler": scaler,
        "feature_columns": feature_cols,
        "label_map": {0: "hold", 1: "buy", 2: "sell"},
    }
    meta_path = MODELS_DIR / f"{model_name}_meta.joblib"
    joblib.dump(meta, meta_path)

    print(f"模型已保存: {model_path}")
    print(f"元数据已保存: {meta_path}")
    return model_path


# ---------------------------------------------------------------------------
# 主入口
# ---------------------------------------------------------------------------
def main() -> None:
    if not FEATURE_DATA.exists():
        raise FileNotFoundError(f"数据文件不存在: {FEATURE_DATA}")

    df_raw = pd.read_parquet(FEATURE_DATA)
    features = build_features(df_raw)
    feature_cols = get_feature_columns(features)

    X = features[feature_cols].values
    X_df = features[feature_cols]  # 保留列名，供 LightGBM 使用
    # XGBoost multi:softprob 需要标签从 0 开始：-1 → 2
    y_raw = features["label"].values.copy()
    y = np.where(y_raw == -1, 2, y_raw)

    print(f"数据集: {X.shape[0]} 样本, {X.shape[1]} 特征")
    unique, counts = np.unique(y, return_counts=True)
    for label, count in zip(unique, counts):
        name = {0: "hold", 1: "buy", 2: "sell"}[label]
        print(f"  类别 {label}({name}): {count} ({count / len(y) * 100:.1f}%)")

    # --- XGBoost ---
    xgb_result = walk_forward_evaluate(X, y, xgb_factory, "XGBoost")

    # --- LightGBM ---
    lgbm_result = walk_forward_evaluate(X, y, lgbm_factory, "LightGBM")

    # --- 比较并选择最佳模型 ---
    xgb_f1 = xgb_result["avg_metrics"]["f1"]
    lgbm_f1 = lgbm_result["avg_metrics"]["f1"]

    print(f"\n{'='*70}")
    print(f"  模型对比")
    print(f"{'='*70}")
    print(f"  XGBoost  平均 Macro-F1 = {xgb_f1:.4f}")
    print(f"  LightGBM 平均 Macro-F1 = {lgbm_f1:.4f}")

    if xgb_f1 >= lgbm_f1:
        winner_name, winner_result = "xgb_signal", xgb_result
        print(f"  选择: XGBoost")
    else:
        winner_name, winner_result = "lgbm_signal", lgbm_result
        print(f"  选择: LightGBM")

    print(f"{'='*70}\n")

    save_model(
        winner_result["best_model"],
        winner_result["best_scaler"],
        feature_cols,
        winner_name,
    )

    # 同时保存两个模型供后续对比
    save_model(xgb_result["best_model"], xgb_result["best_scaler"], feature_cols, "xgb_signal")
    save_model(lgbm_result["best_model"], lgbm_result["best_scaler"], feature_cols, "lgbm_signal")

    # 输出最终测试集（最后一折）的详细报告
    print("\n--- XGBoost 最后一折详细报告 ---")
    print(xgb_result["fold_reports"][-1])
    print("\n--- LightGBM 最后一折详细报告 ---")
    print(lgbm_result["fold_reports"][-1])


if __name__ == "__main__":
    main()
