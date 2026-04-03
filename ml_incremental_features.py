#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
增量特征构建器：与 ml_feature_builder.py 的 build_features() 计算结果一致，
但逐条 bar 更新，适用于 Nautilus Strategy 的 on_bar() 回调。

用法::

    builder = IncrementalFeatureBuilder()
    for bar in bars:
        row = builder.update(bar.open, bar.high, bar.low, bar.close,
                             bar.volume, bar_datetime)
        if row is not None:
            X = scaler.transform([list(row.values())])
            probs = model.predict_proba(X)
"""

from __future__ import annotations

from collections import deque
from math import nan

from ml_feature_builder import FeatureConfig, DEFAULT_CONFIG


class _EMA:
    """在线 EMA 计算器（adjust=False，与 pandas ewm 一致）。"""

    __slots__ = ("_alpha", "_value", "_ready")

    def __init__(self, span: int) -> None:
        self._alpha = 2.0 / (span + 1)
        self._value = nan
        self._ready = False

    @property
    def value(self) -> float:
        return self._value

    def update(self, x: float) -> float:
        if not self._ready:
            self._value = x
            self._ready = True
        else:
            self._value = self._alpha * x + (1 - self._alpha) * self._value
        return self._value


class _RSI:
    """在线 RSI（Wilder's smoothing，与 ml_feature_builder.compute_rsi 一致）。"""

    __slots__ = ("_period", "_avg_gain", "_avg_loss", "_prev_close", "_count")

    def __init__(self, period: int) -> None:
        self._period = period
        self._avg_gain = 0.0
        self._avg_loss = 0.0
        self._prev_close: float | None = None
        self._count = 0

    def update(self, close: float) -> float | None:
        if self._prev_close is None:
            self._prev_close = close
            return None
        delta = close - self._prev_close
        self._prev_close = close
        gain = max(delta, 0.0)
        loss = max(-delta, 0.0)
        self._count += 1
        alpha = 1.0 / self._period
        self._avg_gain = alpha * gain + (1 - alpha) * self._avg_gain
        self._avg_loss = alpha * loss + (1 - alpha) * self._avg_loss
        if self._count < self._period:
            return None
        if self._avg_loss == 0:
            return 100.0
        rs = self._avg_gain / self._avg_loss
        return 100 - 100 / (1 + rs)


class IncrementalFeatureBuilder:
    """
    逐条 bar 构建与 ``build_features()`` 一致的 34 维特征向量。

    调用 ``update()`` 返回 ``dict[str, float]`` 或 ``None``（预热期）。
    dict 的 key 顺序与 ``get_feature_columns()`` 返回的列名完全对齐。
    """

    def __init__(self, config: FeatureConfig = DEFAULT_CONFIG) -> None:
        self.cfg = config
        self._bar_count = 0
        max_window = max(config.macd_slow, config.kdj_n, max(config.return_windows))
        self._warmup = max_window + 5
        buf = max_window + 50

        # MACD
        self._ema_fast = _EMA(config.macd_fast)
        self._ema_slow = _EMA(config.macd_slow)
        self._ema_signal = _EMA(config.macd_signal)

        # RSI
        self._rsi = _RSI(config.rsi_period)

        # KDJ rolling window
        self._kdj_highs: deque[float] = deque(maxlen=config.kdj_n)
        self._kdj_lows: deque[float] = deque(maxlen=config.kdj_n)
        self._kdj_k = _EMA(config.kdj_k_smooth)  # com = smooth-1 → span = 2*com+1 NOT exact; see note
        self._kdj_d = _EMA(config.kdj_d_smooth)
        # KDJ uses ewm(com=k_smooth-1) which is span=2*(k_smooth-1)+1.
        # For k_smooth=3: com=2 → span=5.  Re-init with correct span.
        self._kdj_k = _EMA(2 * (config.kdj_k_smooth - 1) + 1)
        self._kdj_d = _EMA(2 * (config.kdj_d_smooth - 1) + 1)

        # Price / DIF history for rolling features
        self._closes: deque[float] = deque(maxlen=buf)
        self._opens: deque[float] = deque(maxlen=buf)
        self._highs: deque[float] = deque(maxlen=buf)
        self._lows: deque[float] = deque(maxlen=buf)
        self._volumes: deque[float] = deque(maxlen=buf)
        self._difs: deque[float] = deque(maxlen=buf)
        self._deas: deque[float] = deque(maxlen=buf)
        self._histograms: deque[float] = deque(maxlen=buf)

        # Extreme tracking
        self._last_price_peak_idx = -1
        self._last_price_trough_idx = -1
        self._last_dif_peak_idx = -1
        self._last_dif_trough_idx = -1

        # Intraday open (reset per day)
        self._today_open: float | None = None
        self._today_date = None

        # Previous bar's MACD values (for golden/death cross)
        self._prev_dif: float | None = None
        self._prev_dea: float | None = None

    def update(
        self,
        open_: float,
        high: float,
        low: float,
        close: float,
        volume: float,
        dt,  # datetime-like with .date(), .hour, .minute
    ) -> dict[str, float] | None:
        """
        追加一条 bar，返回 34 维特征 dict；预热期返回 None。

        ``dt`` 需要有 ``.date()``, ``.hour``, ``.minute`` 属性。
        """
        self._bar_count += 1
        idx = self._bar_count - 1

        # --- Append raw data ---
        self._closes.append(close)
        self._opens.append(open_)
        self._highs.append(high)
        self._lows.append(low)
        self._volumes.append(volume)

        # --- Intraday open ---
        bar_date = dt.date() if hasattr(dt, "date") and callable(dt.date) else dt
        if self._today_date != bar_date:
            self._today_date = bar_date
            self._today_open = open_

        # --- MACD ---
        ef = self._ema_fast.update(close)
        es = self._ema_slow.update(close)
        dif = ef - es
        dea = self._ema_signal.update(dif)
        histogram = (dif - dea) * 2

        self._difs.append(dif)
        self._deas.append(dea)
        self._histograms.append(histogram)

        # --- RSI ---
        rsi_val = self._rsi.update(close)

        # --- KDJ ---
        self._kdj_highs.append(high)
        self._kdj_lows.append(low)
        lowest = min(self._kdj_lows)
        highest = max(self._kdj_highs)
        denom = highest - lowest
        rsv = ((close - lowest) / denom * 100) if denom != 0 else 50.0
        k_val = self._kdj_k.update(rsv)
        d_val = self._kdj_d.update(k_val)
        j_val = 3 * k_val - 2 * d_val

        # --- Extreme detection (needs >=3 points) ---
        n = len(self._closes)
        if n >= 3:
            prev_c, cur_c, _ = self._closes[-3], self._closes[-2], self._closes[-1]
            # check the *previous* point (index n-2) as a local extreme
            check_c = self._closes[-2]
            if check_c > prev_c and check_c > self._closes[-1]:
                self._last_price_peak_idx = idx - 1
            if check_c < prev_c and check_c < self._closes[-1]:
                self._last_price_trough_idx = idx - 1

        if len(self._difs) >= 3:
            prev_d, cur_d = self._difs[-3], self._difs[-2]
            if cur_d > prev_d and cur_d > self._difs[-1]:
                self._last_dif_peak_idx = idx - 1
            if cur_d < prev_d and cur_d < self._difs[-1]:
                self._last_dif_trough_idx = idx - 1

        # --- Warmup check ---
        if self._bar_count < self._warmup or rsi_val is None:
            self._prev_dif = dif
            self._prev_dea = dea
            return None

        # --- Build feature dict ---
        f: dict[str, float] = {}

        # MACD
        f["dif"] = dif
        f["dea"] = dea
        f["histogram"] = histogram
        f["dif_diff1"] = dif - self._difs[-2] if len(self._difs) >= 2 else 0.0
        f["dea_diff1"] = dea - self._deas[-2] if len(self._deas) >= 2 else 0.0
        prev_hist = self._histograms[-2] if len(self._histograms) >= 2 else histogram
        hist_diff1 = histogram - prev_hist
        f["histogram_diff1"] = hist_diff1
        prev_prev_hist = self._histograms[-3] if len(self._histograms) >= 3 else prev_hist
        f["histogram_diff2"] = hist_diff1 - (prev_hist - prev_prev_hist)

        # Golden / Death cross
        golden = 0
        death = 0
        if self._prev_dif is not None and self._prev_dea is not None:
            if self._prev_dif <= self._prev_dea and dif > dea:
                golden = 1
            if self._prev_dif >= self._prev_dea and dif < dea:
                death = 1
        f["golden_cross"] = golden
        f["death_cross"] = death
        self._prev_dif = dif
        self._prev_dea = dea

        # RSI
        f["rsi"] = rsi_val

        # KDJ
        f["kdj_k"] = k_val
        f["kdj_d"] = d_val
        f["kdj_j"] = j_val
        f["kd_diff"] = k_val - d_val
        f["j_overbought"] = 1 if j_val > 100 else 0
        f["j_oversold"] = 1 if j_val < 0 else 0

        # Price features
        f["bar_return"] = (close - open_) / open_ if open_ != 0 else 0.0
        f["bar_range"] = (high - low) / close if close != 0 else 0.0
        body_top = max(close, open_)
        body_bot = min(close, open_)
        f["upper_shadow"] = (high - body_top) / close if close != 0 else 0.0
        f["lower_shadow"] = (body_bot - low) / close if close != 0 else 0.0

        # N-minute returns
        for w in self.cfg.return_windows:
            if len(self._closes) > w and self._closes[-(w + 1)] != 0:
                f[f"return_{w}m"] = (close - self._closes[-(w + 1)]) / self._closes[-(w + 1)]
            else:
                f[f"return_{w}m"] = 0.0

        # Intraday return
        if self._today_open and self._today_open != 0:
            f["intraday_return"] = (close - self._today_open) / self._today_open
        else:
            f["intraday_return"] = 0.0

        # Volume ratios
        for w in self.cfg.volume_windows:
            if len(self._volumes) >= w:
                recent = list(self._volumes)[-w:]
                avg = sum(recent) / len(recent)
                f[f"volume_ratio_{w}m"] = volume / avg if avg > 0 else 1.0
            else:
                f[f"volume_ratio_{w}m"] = 1.0
        if len(self._volumes) >= 2 and self._volumes[-2] != 0:
            f["volume_diff1"] = (volume - self._volumes[-2]) / self._volumes[-2]
        else:
            f["volume_diff1"] = 0.0

        # Extreme point minutes
        f["min_since_price_peak"] = float(idx - self._last_price_peak_idx) if self._last_price_peak_idx >= 0 else 0.0
        f["min_since_price_trough"] = float(idx - self._last_price_trough_idx) if self._last_price_trough_idx >= 0 else 0.0
        f["min_since_dif_peak"] = float(idx - self._last_dif_peak_idx) if self._last_dif_peak_idx >= 0 else 0.0
        f["min_since_dif_trough"] = float(idx - self._last_dif_trough_idx) if self._last_dif_trough_idx >= 0 else 0.0

        # Time features
        minutes_of_day = dt.hour * 60 + dt.minute
        f["minutes_from_open"] = float(minutes_of_day - (9 * 60 + 30))
        f["minutes_to_close"] = float(15 * 60 - minutes_of_day)

        return f
