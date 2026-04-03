#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
特征重要性分析模块：使用 SHAP 和 XGBoost feature_importances_ 分析
哪些技术指标对买卖决策贡献最大，输出可视化报告和优化建议。
"""

from __future__ import annotations

from pathlib import Path

import joblib
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import shap
from xgboost import XGBClassifier

from ml_feature_builder import build_features, get_feature_columns, FeatureConfig

plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Arial Unicode MS"]
plt.rcParams["axes.unicode_minus"] = False

MODELS_DIR = Path("models")
OUTPUT_DIR = Path("models")
FEATURE_DATA = Path("data/159506_1min.parquet")


# ---------------------------------------------------------------------------
# 加载模型
# ---------------------------------------------------------------------------
def load_xgb_model() -> tuple[XGBClassifier, dict]:
    model_path = MODELS_DIR / "xgb_signal.json"
    meta_path = MODELS_DIR / "xgb_signal_meta.joblib"
    if not model_path.exists():
        raise FileNotFoundError(f"模型不存在: {model_path}，请先运行 ml_signal_trainer.py")
    model = XGBClassifier()
    model.load_model(str(model_path))
    meta = joblib.load(meta_path)
    return model, meta


# ---------------------------------------------------------------------------
# 内建特征重要性分析
# ---------------------------------------------------------------------------
def analyze_builtin_importance(model: XGBClassifier, feature_cols: list[str]) -> pd.DataFrame:
    importance = model.feature_importances_
    df = pd.DataFrame({
        "feature": feature_cols,
        "importance": importance,
    }).sort_values("importance", ascending=False).reset_index(drop=True)
    df["rank"] = range(1, len(df) + 1)
    df["cumulative_pct"] = df["importance"].cumsum() / df["importance"].sum() * 100
    return df


# ---------------------------------------------------------------------------
# SHAP 分析
# ---------------------------------------------------------------------------
def analyze_shap(
    model: XGBClassifier,
    X_sample: np.ndarray,
    feature_cols: list[str],
) -> shap.Explanation:
    explainer = shap.TreeExplainer(model)
    shap_values = explainer(X_sample)
    return shap_values


# ---------------------------------------------------------------------------
# 可视化
# ---------------------------------------------------------------------------
def plot_feature_importance(imp_df: pd.DataFrame, top_n: int = 20) -> Path:
    fig, ax = plt.subplots(figsize=(10, 8))
    top = imp_df.head(top_n).iloc[::-1]
    bars = ax.barh(top["feature"], top["importance"])

    for bar, pct in zip(bars, top["cumulative_pct"].values[::-1]):
        ax.text(bar.get_width(), bar.get_y() + bar.get_height() / 2,
                f" {pct:.0f}%", va="center", fontsize=8, color="gray")

    ax.set_xlabel("Feature Importance (gain)")
    ax.set_title(f"XGBoost 特征重要性 Top {top_n}")
    plt.tight_layout()
    path = OUTPUT_DIR / "feature_importance.png"
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"特征重要性图已保存: {path}")
    return path


def plot_shap_summary(shap_values: shap.Explanation, feature_cols: list[str]) -> Path:
    """绘制三类标签的 SHAP 汇总图。"""
    class_names = ["hold (0)", "buy (1)", "sell (2)"]

    fig, axes = plt.subplots(1, 3, figsize=(24, 8))
    for i, (ax, name) in enumerate(zip(axes, class_names)):
        plt.sca(ax)
        shap.summary_plot(
            shap_values[:, :, i],
            feature_names=feature_cols,
            show=False,
            max_display=15,
            plot_size=None,
        )
        ax.set_title(f"SHAP - {name}")

    plt.tight_layout()
    path = OUTPUT_DIR / "shap_summary.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"SHAP 汇总图已保存: {path}")
    return path


def plot_shap_bar(shap_values: shap.Explanation, feature_cols: list[str]) -> Path:
    """绘制全局 SHAP bar 图（所有类别的平均绝对 SHAP 值）。"""
    # 多分类 SHAP values 形状: (n_samples, n_features, n_classes)
    # 手动计算跨所有类别的平均绝对 SHAP 值
    mean_abs = np.abs(shap_values.values).mean(axis=(0, 2))
    order = np.argsort(mean_abs)
    top_n = min(20, len(feature_cols))
    top_idx = order[-top_n:]

    fig, ax = plt.subplots(figsize=(10, 8))
    ax.barh(
        [feature_cols[i] for i in top_idx],
        mean_abs[top_idx],
    )
    ax.set_xlabel("|mean SHAP value|")
    ax.set_title("SHAP 全局特征重要性 (跨所有类别)")
    plt.tight_layout()
    path = OUTPUT_DIR / "shap_bar.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"SHAP bar 图已保存: {path}")
    return path


# ---------------------------------------------------------------------------
# 输出优化建议
# ---------------------------------------------------------------------------
def generate_optimization_report(
    imp_df: pd.DataFrame,
    shap_values: shap.Explanation,
    feature_cols: list[str],
) -> str:
    lines = []
    lines.append("=" * 70)
    lines.append("  ML 特征分析报告 — 策略优化建议")
    lines.append("=" * 70)

    # 1. 特征重要性排名
    lines.append("\n[1] 特征重要性排名 (XGBoost gain)")
    lines.append("-" * 50)
    for _, row in imp_df.head(15).iterrows():
        lines.append(f"  #{int(row['rank']):2d}  {row['feature']:<28s}  "
                      f"importance={row['importance']:.4f}  累计={row['cumulative_pct']:.0f}%")

    # 2. 前 80% 贡献的特征
    top80 = imp_df[imp_df["cumulative_pct"] <= 80]
    lines.append(f"\n  前 {len(top80)} 个特征贡献了 80% 的重要性")

    # 3. SHAP 对比 buy vs sell
    shap_vals = shap_values.values
    # shap_vals 形状: (n_samples, n_features, n_classes)
    mean_abs_buy = np.abs(shap_vals[:, :, 1]).mean(axis=0)
    mean_abs_sell = np.abs(shap_vals[:, :, 2]).mean(axis=0)

    buy_rank = pd.DataFrame({"feature": feature_cols, "shap": mean_abs_buy}).sort_values("shap", ascending=False)
    sell_rank = pd.DataFrame({"feature": feature_cols, "shap": mean_abs_sell}).sort_values("shap", ascending=False)

    lines.append("\n[2] 对买入信号最重要的特征 (SHAP)")
    lines.append("-" * 50)
    for i, (_, row) in enumerate(buy_rank.head(10).iterrows(), 1):
        lines.append(f"  #{i:2d}  {row['feature']:<28s}  |SHAP|={row['shap']:.6f}")

    lines.append("\n[3] 对卖出信号最重要的特征 (SHAP)")
    lines.append("-" * 50)
    for i, (_, row) in enumerate(sell_rank.head(10).iterrows(), 1):
        lines.append(f"  #{i:2d}  {row['feature']:<28s}  |SHAP|={row['shap']:.6f}")

    # 4. 优化建议
    lines.append("\n[4] 策略优化建议")
    lines.append("-" * 50)

    top_buy = buy_rank.head(5)["feature"].tolist()
    top_sell = sell_rank.head(5)["feature"].tolist()

    macd_features = [f for f in top_buy + top_sell if f.startswith(("dif", "dea", "histogram"))]
    kdj_features = [f for f in top_buy + top_sell if f.startswith("kdj")]
    rsi_features = [f for f in top_buy + top_sell if f == "rsi"]
    volume_features = [f for f in top_buy + top_sell if "volume" in f]
    time_features = [f for f in top_buy + top_sell if "minutes" in f]

    if macd_features:
        lines.append(f"  - MACD 相关特征 {macd_features} 在 top-5 中出现，"
                      "当前策略的 MACD 权重设置合理")
    if kdj_features:
        lines.append(f"  - KDJ 相关特征 {kdj_features} 对信号有显著贡献，"
                      "建议在 technical_signal 累积中增加 KDJ 极端值的权重")
    if rsi_features:
        lines.append(f"  - RSI 对信号有贡献，建议增加 RSI 超买超卖的信号权重")
    if volume_features:
        lines.append(f"  - 成交量特征 {volume_features} 有信号价值，"
                      "建议将成交量比率纳入 technical_signal 评分")
    if time_features:
        lines.append(f"  - 时间特征 {time_features} 说明日内不同时段信号强度不同，"
                      "建议对开盘/尾盘时段使用不同的信号阈值")

    low_importance = imp_df.tail(5)["feature"].tolist()
    lines.append(f"  - 低重要性特征（可考虑移除）: {low_importance}")

    report = "\n".join(lines)
    return report


# ---------------------------------------------------------------------------
# 主入口
# ---------------------------------------------------------------------------
def main() -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)

    # 加载模型
    model, meta = load_xgb_model()
    feature_cols = meta["feature_columns"]
    scaler = meta["scaler"]

    # 构建特征
    df_raw = pd.read_parquet(FEATURE_DATA)
    features = build_features(df_raw)
    X = features[feature_cols].values
    X_scaled = scaler.transform(X)

    # 1. 内建特征重要性
    imp_df = analyze_builtin_importance(model, feature_cols)
    plot_feature_importance(imp_df)

    # 2. SHAP 分析（取样以加速）
    sample_size = min(2000, len(X_scaled))
    rng = np.random.RandomState(42)
    sample_idx = rng.choice(len(X_scaled), sample_size, replace=False)
    sample_idx.sort()
    X_sample = X_scaled[sample_idx]

    print(f"SHAP 分析中（{sample_size} 样本）...")
    shap_values = analyze_shap(model, X_sample, feature_cols)

    plot_shap_bar(shap_values, feature_cols)
    plot_shap_summary(shap_values, feature_cols)

    # 3. 生成报告
    report = generate_optimization_report(imp_df, shap_values, feature_cols)
    print(report)

    report_path = OUTPUT_DIR / "feature_analysis_report.txt"
    report_path.write_text(report, encoding="utf-8")
    print(f"\n分析报告已保存: {report_path}")


if __name__ == "__main__":
    main()
