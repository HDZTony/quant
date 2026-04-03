#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
特征工程模块：从 1 分钟 Bar 数据构建 ML 特征矩阵与标注标签。

与现有策略保持参数一致：
- MACD: fast=12, slow=26, signal=9
- KDJ: n=9, k_period=3, d_period=3
- RSI: period=6
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class FeatureConfig:
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9
    rsi_period: int = 6
    kdj_n: int = 9
    kdj_k_smooth: int = 3
    kdj_d_smooth: int = 3
    return_windows: tuple[int, ...] = (5, 10, 20)
    volume_windows: tuple[int, ...] = (5, 10, 20)
    label_forward_minutes: int = 5
    label_threshold: float = 0.002


DEFAULT_CONFIG = FeatureConfig()


# ---------------------------------------------------------------------------
# 技术指标计算（纯 pandas，无外部依赖）
# ---------------------------------------------------------------------------
def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def compute_macd(
    close: pd.Series, fast: int, slow: int, signal: int
) -> pd.DataFrame:
    ema_fast = _ema(close, fast)
    ema_slow = _ema(close, slow)
    dif = ema_fast - ema_slow
    dea = _ema(dif, signal)
    histogram = (dif - dea) * 2
    return pd.DataFrame({"dif": dif, "dea": dea, "histogram": histogram})


def compute_rsi(close: pd.Series, period: int) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - 100 / (1 + rs)


def compute_kdj(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    n: int,
    k_smooth: int,
    d_smooth: int,
) -> pd.DataFrame:
    lowest_low = low.rolling(n, min_periods=1).min()
    highest_high = high.rolling(n, min_periods=1).max()
    denom = highest_high - lowest_low
    rsv = ((close - lowest_low) / denom.replace(0, np.nan)) * 100
    k = rsv.ewm(com=k_smooth - 1, adjust=False).mean()
    d = k.ewm(com=d_smooth - 1, adjust=False).mean()
    j = 3 * k - 2 * d
    return pd.DataFrame({"k": k, "d": d, "j": j})


# ---------------------------------------------------------------------------
# 极值点检测（简单的前后点比较方法，与策略一致）
# ---------------------------------------------------------------------------
def _detect_extremes(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    """返回两个布尔 Series：is_peak, is_trough。"""
    shifted_prev = series.shift(1)
    shifted_next = series.shift(-1)
    is_peak = (series > shifted_prev) & (series > shifted_next)
    is_trough = (series < shifted_prev) & (series < shifted_next)
    return is_peak, is_trough


def _minutes_since_last_true(mask: pd.Series) -> pd.Series:
    """计算距离上一个 True 的行数（分钟数）。"""
    idx = mask.values
    result = np.full(len(idx), np.nan)
    last_pos = -1
    for i in range(len(idx)):
        if idx[i]:
            last_pos = i
        if last_pos >= 0:
            result[i] = i - last_pos
    return pd.Series(result, index=mask.index)


# ---------------------------------------------------------------------------
# 主入口：构建特征矩阵 + 标签
# ---------------------------------------------------------------------------
def build_features(
    df: pd.DataFrame, config: FeatureConfig = DEFAULT_CONFIG
) -> pd.DataFrame:
    """
    从原始 1 分钟 OHLCV DataFrame 构建完整的特征矩阵。

    Parameters
    ----------
    df : DataFrame
        必须包含 datetime, open, high, low, close, volume 列。
    config : FeatureConfig

    Returns
    -------
    DataFrame
        包含所有特征列 + label 列，已去除 NaN 预热行。
    """
    df = df.copy().sort_values("datetime").reset_index(drop=True)

    close = df["close"]
    high = df["high"]
    low = df["low"]
    volume = df["volume"]
    open_ = df["open"]

    features = pd.DataFrame(index=df.index)
    features["datetime"] = df["datetime"]

    # ---- MACD ----
    macd = compute_macd(close, config.macd_fast, config.macd_slow, config.macd_signal)
    features["dif"] = macd["dif"]
    features["dea"] = macd["dea"]
    features["histogram"] = macd["histogram"]
    features["dif_diff1"] = macd["dif"].diff()
    features["dea_diff1"] = macd["dea"].diff()
    features["histogram_diff1"] = macd["histogram"].diff()
    features["histogram_diff2"] = features["histogram_diff1"].diff()

    # 金叉 / 死叉标记
    prev_dif = macd["dif"].shift(1)
    prev_dea = macd["dea"].shift(1)
    features["golden_cross"] = ((prev_dif <= prev_dea) & (macd["dif"] > macd["dea"])).astype(int)
    features["death_cross"] = ((prev_dif >= prev_dea) & (macd["dif"] < macd["dea"])).astype(int)

    # ---- RSI ----
    features["rsi"] = compute_rsi(close, config.rsi_period)

    # ---- KDJ ----
    kdj = compute_kdj(high, low, close, config.kdj_n, config.kdj_k_smooth, config.kdj_d_smooth)
    features["kdj_k"] = kdj["k"]
    features["kdj_d"] = kdj["d"]
    features["kdj_j"] = kdj["j"]
    features["kd_diff"] = kdj["k"] - kdj["d"]
    features["j_overbought"] = (kdj["j"] > 100).astype(int)
    features["j_oversold"] = (kdj["j"] < 0).astype(int)

    # ---- 价格特征 ----
    features["bar_return"] = (close - open_) / open_
    features["bar_range"] = (high - low) / close
    features["upper_shadow"] = (high - close.combine(open_, max)) / close
    features["lower_shadow"] = (close.combine(open_, min) - low) / close

    # 近 N 分钟收益率
    for w in config.return_windows:
        features[f"return_{w}m"] = close.pct_change(w)

    # 当日累计涨跌幅（以每天第一根 bar 的 open 为基准）
    dates = df["datetime"].dt.date
    daily_open = df.groupby(dates)["open"].transform("first")
    features["intraday_return"] = (close - daily_open) / daily_open

    # ---- 成交量特征 ----
    for w in config.volume_windows:
        avg_vol = volume.rolling(w, min_periods=1).mean()
        features[f"volume_ratio_{w}m"] = volume / avg_vol.replace(0, np.nan)
    features["volume_diff1"] = volume.pct_change()

    # ---- 极值点特征 ----
    is_price_peak, is_price_trough = _detect_extremes(close)
    is_dif_peak, is_dif_trough = _detect_extremes(macd["dif"])
    features["min_since_price_peak"] = _minutes_since_last_true(is_price_peak)
    features["min_since_price_trough"] = _minutes_since_last_true(is_price_trough)
    features["min_since_dif_peak"] = _minutes_since_last_true(is_dif_peak)
    features["min_since_dif_trough"] = _minutes_since_last_true(is_dif_trough)

    # ---- 时间特征 ----
    minutes_of_day = df["datetime"].dt.hour * 60 + df["datetime"].dt.minute
    trading_start = 9 * 60 + 30
    trading_end = 15 * 60
    features["minutes_from_open"] = minutes_of_day - trading_start
    features["minutes_to_close"] = trading_end - minutes_of_day

    # ---- 标签：未来 N 分钟收益率 ----
    fwd = config.label_forward_minutes
    future_close = close.shift(-fwd)
    future_return = (future_close - close) / close
    features["future_return"] = future_return

    threshold = config.label_threshold
    labels = pd.Series(0, index=df.index, dtype=int)
    labels[future_return > threshold] = 1   # 买入机会
    labels[future_return < -threshold] = -1  # 卖出机会
    features["label"] = labels

    # ---- 清理 ----
    feature_cols = [c for c in features.columns if c not in ("datetime", "label", "future_return")]
    warmup = max(config.macd_slow, config.kdj_n, max(config.return_windows)) + 5
    features = features.iloc[warmup:].copy()
    features = features[features["future_return"].notna()].copy()

    return features


def get_feature_columns(features: pd.DataFrame) -> list[str]:
    """返回纯特征列名（不含 datetime / label / future_return）。"""
    return [c for c in features.columns if c not in ("datetime", "label", "future_return")]


# ---------------------------------------------------------------------------
# CLI 入口：快速验证特征矩阵
# ---------------------------------------------------------------------------
def main() -> None:
    data_path = Path("data/159506_1min.parquet")
    if not data_path.exists():
        raise FileNotFoundError(f"数据文件不存在: {data_path}")

    df = pd.read_parquet(data_path)
    print(f"原始数据: {df.shape[0]} 行, 日期范围 {df['datetime'].min()} ~ {df['datetime'].max()}")

    features = build_features(df)
    feature_cols = get_feature_columns(features)

    print(f"\n特征矩阵: {features.shape[0]} 行 x {len(feature_cols)} 特征列")
    print(f"标签分布:\n{features['label'].value_counts().sort_index()}")
    print(f"\n特征列表 ({len(feature_cols)}):")
    for col in feature_cols:
        print(f"  {col}")

    nan_counts = features[feature_cols].isna().sum()
    has_nan = nan_counts[nan_counts > 0]
    if len(has_nan) > 0:
        print(f"\n存在 NaN 的特征列:")
        for col, cnt in has_nan.items():
            print(f"  {col}: {cnt} NaN ({cnt / len(features) * 100:.1f}%)")
    else:
        print("\n所有特征列无 NaN")

    output_path = Path("data/159506_features.parquet")
    features.to_parquet(output_path, index=False)
    print(f"\n特征矩阵已保存至 {output_path}")


if __name__ == "__main__":
    main()
