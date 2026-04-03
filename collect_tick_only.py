"""
独立 Tick 数据采集器 —— 仅采集，不运行策略。

连接 jvquant WebSocket，持续接收 159506 L1 行情并保存到 ParquetDataCatalog。
同时聚合为 1 分钟 bar 发布到 Redis，供 kline-viewer 实时展示。
与实盘交易完全独立运行，互不影响。

核心特性：
  - 交易时段感知：仅在 09:15-11:30 / 13:00-15:05 采集，中间休市等待
  - 断线自动重连：jvquant 非交易时段会主动断开，采集器会持续重试
  - Windows 兼容：不依赖 POSIX signal handler
  - 收盘后自动退出
  - 1 分钟 bar 实时发布到 Redis（etf:159506:bar）

用法：
    uv run python collect_tick_only.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import time as _time
from datetime import datetime, time as dt_time, timedelta, timezone

import pytz

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("data/collect_tick.log", mode="a", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
    force=True,
)
logger = logging.getLogger("tick_collector")

TZ = pytz.timezone("Asia/Shanghai")

MORNING_OPEN = dt_time(9, 15)
MORNING_CLOSE = dt_time(11, 30)
AFTERNOON_OPEN = dt_time(12, 55)
AFTERNOON_CLOSE = dt_time(15, 5)

MAX_RECONNECT_WAIT = 120
BASE_RECONNECT_WAIT = 5


def now_beijing() -> datetime:
    return datetime.now(TZ)


def is_trading_time(t: dt_time | None = None) -> bool:
    t = t or now_beijing().time()
    return (MORNING_OPEN <= t <= MORNING_CLOSE) or (AFTERNOON_OPEN <= t <= AFTERNOON_CLOSE)


def seconds_until_next_session() -> float:
    """距下一个交易时段的秒数。如果已在时段内返回 0，收盘后返回 -1。"""
    now = now_beijing()
    t = now.time()

    if is_trading_time(t):
        return 0.0

    if t < MORNING_OPEN:
        target = now.replace(hour=MORNING_OPEN.hour, minute=MORNING_OPEN.minute, second=0, microsecond=0)
        return (target - now).total_seconds()

    if MORNING_CLOSE < t < AFTERNOON_OPEN:
        target = now.replace(hour=AFTERNOON_OPEN.hour, minute=AFTERNOON_OPEN.minute, second=0, microsecond=0)
        return (target - now).total_seconds()

    return -1.0


# ---------------------------------------------------------------------------
# 轻量 1 分钟 Bar 聚合 + Redis 发布
# ---------------------------------------------------------------------------

_TZ_OFFSET = timedelta(hours=8)

_redis_client = None


def _get_redis():
    global _redis_client
    if _redis_client is None:
        try:
            import redis
            _redis_client = redis.Redis(host="localhost", port=6379)
            _redis_client.ping()
            logger.info("Redis 连接成功，bar 数据将实时发布")
        except Exception as e:
            logger.warning("Redis 不可用，bar 不会发布（tick 存储不受影响）: %s", e)
            _redis_client = None
    return _redis_client


class _BarAggregator:
    """极简 OHLCV 聚合器：L1 tick → 1 分钟 bar → Redis 发布。"""

    def __init__(self) -> None:
        self._cur: dict | None = None
        self._cur_minute: int = -1
        self._prev_cum_vol: float = 0.0
        self._snapshot_time: float = 0.0

    def on_quote(self, quote_data: dict) -> None:
        price = quote_data.get("price", 0.0)
        cum_vol = quote_data.get("volume", 0.0)
        ts_str = quote_data.get("timestamp", "")
        if not price or not ts_str:
            return

        minute_ts = self._parse_minute(ts_str)
        if minute_ts is None:
            return

        if minute_ts != self._cur_minute:
            if self._cur is not None:
                self._publish(self._cur)
            vol = cum_vol - self._prev_cum_vol if self._prev_cum_vol > 0 else 0.0
            self._cur = {
                "time": minute_ts,
                "open": price, "high": price, "low": price, "close": price,
                "volume": max(vol, 0.0),
            }
            self._cur_minute = minute_ts
            self._prev_cum_vol = cum_vol
            return

        if self._cur:
            self._cur["high"] = max(self._cur["high"], price)
            self._cur["low"] = min(self._cur["low"], price)
            self._cur["close"] = price
            self._cur["volume"] = max(cum_vol - self._prev_cum_vol, 0.0)

        now = _time.monotonic()
        if now - self._snapshot_time >= 3.0 and self._cur:
            self._publish({**self._cur, "_partial": True})
            self._snapshot_time = now

    @staticmethod
    def _parse_minute(ts_str: str) -> int | None:
        try:
            now = datetime.now(timezone(_TZ_OFFSET))
            if len(ts_str) <= 8:
                parts = ts_str.split(":")
                h, m = int(parts[0]), int(parts[1])
                dt = now.replace(hour=h, minute=m, second=0, microsecond=0)
            else:
                dt = datetime.strptime(ts_str[:16], "%Y-%m-%d %H:%M")
                dt = dt.replace(tzinfo=timezone(_TZ_OFFSET))
            return int(dt.timestamp())
        except Exception:
            return None

    @staticmethod
    def _publish(bar: dict) -> None:
        r = _get_redis()
        if r is None:
            return
        try:
            r.publish("etf:159506:bar", json.dumps(bar, ensure_ascii=False))
        except Exception:
            pass


_bar_agg = _BarAggregator()


async def create_adapter():
    """延迟导入并创建 adapter（避免模块级 NautilusTrader 导入）"""
    from etf_159506_adapter import ETF159506Adapter

    config = {
        "token": "d0c519adcd47d266f1c96750d4e80aa6",
        "stock_code": "159506",
    }
    adapter = ETF159506Adapter(config)
    adapter.ws_client.max_reconnect_attempts = 10
    adapter.ws_client.reconnect_interval = 10

    adapter.ws_client.subscribe_quotes("159506", _bar_agg.on_quote)

    return adapter


async def connect_with_retry(adapter) -> bool:
    """指数退避重连，直到成功或收盘。"""
    wait = BASE_RECONNECT_WAIT
    attempt = 0

    while True:
        if now_beijing().time() > AFTERNOON_CLOSE:
            logger.info("已过收盘时间，停止重连")
            return False

        wait_sec = seconds_until_next_session()
        if wait_sec > 0:
            logger.info("当前非交易时段，等待 %.0f 秒后重试", wait_sec)
            await asyncio.sleep(min(wait_sec + 5, wait_sec + 30))
            wait = BASE_RECONNECT_WAIT

        attempt += 1
        logger.info("第 %d 次尝试连接 jvquant …", attempt)

        try:
            success = await adapter.connect()
            if success:
                logger.info("jvquant 连接成功")
                return True
        except Exception as e:
            logger.warning("连接异常: %s", e)

        logger.warning("连接失败，%d 秒后重试", wait)
        await asyncio.sleep(wait)
        wait = min(wait * 2, MAX_RECONNECT_WAIT)


async def run_collector() -> None:
    wait_sec = seconds_until_next_session()
    if wait_sec < 0:
        logger.info("今日交易已结束（当前 %s），直接退出", now_beijing().strftime("%H:%M:%S"))
        return
    if wait_sec > 0:
        logger.info("距开盘还有 %.0f 秒，等待中 …", wait_sec)
        await asyncio.sleep(wait_sec)

    adapter = await create_adapter()

    try:
        while True:
            if now_beijing().time() > AFTERNOON_CLOSE:
                logger.info("收盘时间到，准备退出")
                break

            if not adapter.is_connected:
                ok = await connect_with_retry(adapter)
                if not ok:
                    break

            wait_sec = seconds_until_next_session()
            if wait_sec < 0:
                break

            if wait_sec > 0:
                logger.info("进入休市，断开连接并等待 %.0f 秒", wait_sec)
                await adapter.disconnect()
                await asyncio.sleep(wait_sec)
                continue

            await asyncio.sleep(30)

    finally:
        if adapter.is_connected:
            await adapter.disconnect()
        logger.info("采集器已安全退出，数据已落盘")


def main() -> None:
    logger.info(
        "========== Tick 采集器启动: %s ==========",
        now_beijing().strftime("%Y-%m-%d %H:%M:%S"),
    )
    try:
        asyncio.run(run_collector())
    except KeyboardInterrupt:
        logger.info("手动中断 (Ctrl+C)")
    except Exception:
        logger.exception("采集器异常退出")
    logger.info("========== Tick 采集器结束 ==========")


if __name__ == "__main__":
    main()
