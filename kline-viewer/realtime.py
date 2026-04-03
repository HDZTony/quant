"""
Bar 聚合器 + 指标计算。

职责：
  1. 启动时从 pytdx 预加载当天已有 1 分钟 bar
  2. 接收 tick 采集器通过 Redis 发来的 OHLCV bar
  3. 计算 MACD / RSI / KDJ 指标
  4. 提供 get_today_kline() 供 REST / WebSocket 使用
"""

from __future__ import annotations

import logging
import time
from datetime import date

import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Bar 聚合器
# ---------------------------------------------------------------------------

UP_COLOR = "rgba(239,83,80,0.5)"
DOWN_COLOR = "rgba(38,166,154,0.5)"

TDX_HOSTS = [
    ("218.75.126.9", 7709),
    ("115.238.56.198", 7709),
    ("124.160.88.183", 7709),
    ("60.12.136.250", 7709),
    ("218.108.98.244", 7709),
    ("218.108.47.69", 7709),
    ("180.153.39.51", 7709),
    ("119.147.212.81", 7709),
    ("14.17.75.71", 7709),
]
TDX_MARKET = 0
TDX_CAT_1MIN = 8
TDX_BATCH_SIZE = 800


class BarAggregator:
    """管理当天 1 分钟 bar 并计算指标。

    启动时从 pytdx 预加载当天已有 bar；运行中接收 tick 采集器
    通过 Redis 发来的新 bar，保证指标计算有足够历史上下文。
    """

    def __init__(self, stock_code: str = "159506") -> None:
        self.stock_code = stock_code
        self.bars: list[dict] = []
        self._cur: dict | None = None
        self._cur_minute: int = -1

        self._closes: list[float] = []
        self._highs: list[float] = []
        self._lows: list[float] = []

        self._last_pytdx_refresh: float = 0.0
        self._has_live_ticks = False
        self._preload_today()

    def _preload_today(self) -> None:
        """从 pytdx 预加载当天已有的 1 分钟 bar。"""
        try:
            from pytdx.hq import TdxHq_API
        except ImportError:
            logger.warning("pytdx 未安装，跳过预加载")
            return

        api = TdxHq_API()
        connected = False
        for host, port in TDX_HOSTS:
            try:
                if api.connect(host, port):
                    connected = True
                    break
            except Exception:
                continue

        if not connected:
            logger.warning("pytdx 服务器连接失败，跳过预加载")
            return

        try:
            data = api.get_security_bars(TDX_CAT_1MIN, TDX_MARKET, self.stock_code, 0, TDX_BATCH_SIZE)
            if not data:
                return
            df = api.to_df(data)
            df["datetime"] = pd.to_datetime(df["datetime"])

            today = date.today()
            df = df[df["datetime"].dt.date == today].copy()
            if df.empty:
                logger.info("pytdx 无当天数据，跳过预加载")
                return

            df.sort_values("datetime", inplace=True)
            df["time"] = df["datetime"].astype("int64") // 10**9

            for _, row in df.iterrows():
                t = int(row["time"])
                o, h, l, c = float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"])
                v = float(row["vol"]) if "vol" in row.index else float(row.get("volume", 0))

                bar = {"time": t, "open": o, "high": h, "low": l, "close": c, "volume": v}
                self.bars.append(bar)
                self._closes.append(c)
                self._highs.append(h)
                self._lows.append(l)
                self._cur_minute = t

            self._last_pytdx_refresh = time.monotonic()
            logger.info("pytdx 预加载 %d 根当天 bar（%s）", len(df), today)
        except Exception as e:
            logger.warning("pytdx 预加载失败: %s", e)
        finally:
            try:
                api.disconnect()
            except Exception:
                pass

    def refresh_if_stale(self) -> None:
        """如果没有实时 tick 且 pytdx 数据超过 60 秒，重新拉一次。"""
        if self._has_live_ticks:
            return
        now = time.monotonic()
        if now - self._last_pytdx_refresh < 60:
            return
        self._reload_pytdx()

    def _reload_pytdx(self) -> None:
        """重新从 pytdx 拉取今天的数据，替换已有 bar。"""
        old_count = len(self.bars)
        self.bars.clear()
        self._closes.clear()
        self._highs.clear()
        self._lows.clear()
        self._cur = None
        self._cur_minute = -1
        self._preload_today()
        logger.info("pytdx 刷新: %d → %d 根 bar", old_count, len(self.bars))

    def get_today_kline(self) -> dict:
        """返回当天全部 bar（含指标），格式与 KlineDataStore 一致。"""
        self.refresh_if_stale()
        all_bars = list(self.bars)
        if self._cur is not None:
            all_bars.append({**self._cur})

        if not all_bars:
            return {"candles": [], "volume": [], "macd": [], "rsi": [], "kdj": [], "signals": []}

        closes = [b["close"] for b in all_bars]
        highs = [b["high"] for b in all_bars]
        lows = [b["low"] for b in all_bars]

        difs, deas, hists = _macd_series(closes)
        rsis = _rsi_series(closes)
        ks, ds, js = _kdj_series(highs, lows, closes)

        candles, volumes, macd_data, rsi_data, kdj_data = [], [], [], [], []

        for i, bar in enumerate(all_bars):
            t = bar["time"]
            o, h, l, c = bar["open"], bar["high"], bar["low"], bar["close"]
            v = bar["volume"]
            color = UP_COLOR if c >= o else DOWN_COLOR

            candles.append({"time": t, "open": o, "high": h, "low": l, "close": c})
            volumes.append({"time": t, "value": v, "color": color})
            macd_data.append({"time": t, "dif": difs[i], "dea": deas[i], "histogram": hists[i]})
            rsi_data.append({"time": t, "value": rsis[i]})
            kdj_data.append({"time": t, "k": ks[i], "d": ds[i], "j": js[i]})

        return {
            "candles": candles,
            "volume": volumes,
            "macd": macd_data,
            "rsi": rsi_data,
            "kdj": kdj_data,
            "signals": [],
        }

    def accept_bar(self, bar: dict) -> dict | None:
        """接收 tick 采集器通过 Redis 发来的 OHLCV bar，附加指标后返回。

        如果 bar 的 time 与当前 minute 相同，更新当前 bar（partial）；
        如果是新的 minute，关闭上一根并开始新的。
        """
        t = bar.get("time")
        if t is None:
            return None

        o = bar.get("open", 0.0)
        h = bar.get("high", 0.0)
        l = bar.get("low", 0.0)
        c = bar.get("close", 0.0)
        v = bar.get("volume", 0.0)

        self._has_live_ticks = True

        if t != self._cur_minute:
            self._close_bar()
            self._cur = {"time": t, "open": o, "high": h, "low": l, "close": c, "volume": v}
            self._cur_minute = t
        elif self._cur:
            self._cur["high"] = max(self._cur["high"], h)
            self._cur["low"] = min(self._cur["low"], l)
            self._cur["close"] = c
            self._cur["volume"] = v

        return self._attach_indicators({**self._cur}) if self._cur else None

    def current_bar_snapshot(self) -> dict | None:
        """返回当前正在聚合的 bar（含指标），用于实时更新。"""
        if self._cur is None:
            return None
        return self._attach_indicators({**self._cur})

    def _close_bar(self) -> dict | None:
        if self._cur is None:
            return None
        bar = {**self._cur}
        self._closes.append(bar["close"])
        self._highs.append(bar["high"])
        self._lows.append(bar["low"])
        self.bars.append(bar)
        return self._attach_indicators(bar)

    def _attach_indicators(self, bar: dict) -> dict:
        closes = self._closes + [bar["close"]]
        highs = self._highs + [bar["high"]]
        lows = self._lows + [bar["low"]]
        n = len(closes)

        dif, dea, hist = _macd(closes)
        bar["macd_dif"] = dif
        bar["macd_dea"] = dea
        bar["macd_histogram"] = hist

        bar["rsi"] = _rsi(closes) if n >= 2 else None

        k, d, j = _kdj(highs, lows, closes)
        bar["kdj_k"] = k
        bar["kdj_d"] = d
        bar["kdj_j"] = j

        return bar



# ---------------------------------------------------------------------------
# 指标计算（纯 list 输入，返回最新一个值）
# ---------------------------------------------------------------------------

def _ema(data: list[float], period: int) -> float:
    if not data:
        return 0.0
    alpha = 2.0 / (period + 1)
    val = data[0]
    for v in data[1:]:
        val = alpha * v + (1 - alpha) * val
    return val


def _macd(closes: list[float], fast: int = 12, slow: int = 26, sig: int = 9):
    if len(closes) < 2:
        return None, None, None
    ema_f = _ema(closes, fast)
    ema_s = _ema(closes, slow)
    dif = ema_f - ema_s
    difs = []
    ef, es = closes[0], closes[0]
    af, als_ = 2.0 / (fast + 1), 2.0 / (slow + 1)
    for c in closes:
        ef = af * c + (1 - af) * ef
        es = als_ * c + (1 - als_) * es
        difs.append(ef - es)
    dea = _ema(difs, sig)
    return round(difs[-1], 6), round(dea, 6), round(difs[-1] - dea, 6)


def _rsi(closes: list[float], period: int = 6) -> float | None:
    if len(closes) < period + 1:
        return None
    alpha = 1.0 / period
    gains, losses = 0.0, 0.0
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        if i == 1:
            gains = max(d, 0)
            losses = max(-d, 0)
        else:
            gains = alpha * max(d, 0) + (1 - alpha) * gains
            losses = alpha * max(-d, 0) + (1 - alpha) * losses
    if losses == 0:
        return 100.0
    return round(100 - 100 / (1 + gains / losses), 2)


def _kdj(highs: list[float], lows: list[float], closes: list[float], n: int = 9):
    if len(closes) < 2:
        return None, None, None
    k, d = 50.0, 50.0
    for i in range(len(closes)):
        window = slice(max(0, i - n + 1), i + 1)
        hh = max(highs[window])
        ll = min(lows[window])
        rsv = ((closes[i] - ll) / (hh - ll) * 100) if hh != ll else 50.0
        k = (2.0 / 3) * k + (1.0 / 3) * rsv
        d = (2.0 / 3) * d + (1.0 / 3) * k
    j = 3 * k - 2 * d
    return round(k, 2), round(d, 2), round(j, 2)


# ---------------------------------------------------------------------------
# 指标序列计算（用于 get_today_kline 一次性输出全部 bar）
# ---------------------------------------------------------------------------

def _macd_series(closes: list[float], fast: int = 12, slow: int = 26, sig: int = 9):
    n = len(closes)
    difs: list[float | None] = []
    deas: list[float | None] = []
    hists: list[float | None] = []
    if n < 2:
        return [None] * n, [None] * n, [None] * n

    af, als_ = 2.0 / (fast + 1), 2.0 / (slow + 1)
    as_ = 2.0 / (sig + 1)
    ef = es = closes[0]
    dea = 0.0
    for i, c in enumerate(closes):
        ef = af * c + (1 - af) * ef
        es = als_ * c + (1 - als_) * es
        dif = ef - es
        dea = as_ * dif + (1 - as_) * dea
        difs.append(round(dif, 6))
        deas.append(round(dea, 6))
        hists.append(round(dif - dea, 6))
    return difs, deas, hists


def _rsi_series(closes: list[float], period: int = 6):
    n = len(closes)
    out: list[float | None] = [None] * n
    if n < 2:
        return out
    alpha = 1.0 / period
    gains = losses = 0.0
    for i in range(1, n):
        d = closes[i] - closes[i - 1]
        if i == 1:
            gains = max(d, 0)
            losses = max(-d, 0)
        else:
            gains = alpha * max(d, 0) + (1 - alpha) * gains
            losses = alpha * max(-d, 0) + (1 - alpha) * losses
        out[i] = round(100 - 100 / (1 + gains / losses), 2) if losses != 0 else 100.0
    return out


def _kdj_series(highs: list[float], lows: list[float], closes: list[float], n: int = 9):
    size = len(closes)
    ks: list[float | None] = []
    ds: list[float | None] = []
    js: list[float | None] = []
    if size < 2:
        return [None] * size, [None] * size, [None] * size
    k, d = 50.0, 50.0
    for i in range(size):
        window = slice(max(0, i - n + 1), i + 1)
        hh = max(highs[window])
        ll = min(lows[window])
        rsv = ((closes[i] - ll) / (hh - ll) * 100) if hh != ll else 50.0
        k = (2.0 / 3) * k + (1.0 / 3) * rsv
        d = (2.0 / 3) * d + (1.0 / 3) * k
        j = 3 * k - 2 * d
        ks.append(round(k, 2))
        ds.append(round(d, 2))
        js.append(round(j, 2))
    return ks, ds, js


