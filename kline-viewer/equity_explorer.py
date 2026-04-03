"""
全球股票探索模块 —— 基于 OpenBB 获取美股/全球市场历史行情。

职责：
- 通过 OpenBB (yfinance provider) 获取任意股票的日线行情
- 本地 JSON 缓存（当天有效），减少重复请求
- 对外暴露 get_equity_price() 函数供 API 端点调用
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CACHE_DIR = PROJECT_ROOT / "data" / "equity_cache"
CACHE_TTL = timedelta(hours=12)

_obb_instance = None


def _get_obb():
    """延迟初始化 OpenBB（首次调用约 10-15 秒，后续瞬间）。"""
    global _obb_instance
    if _obb_instance is None:
        from openbb import obb
        _obb_instance = obb
    return _obb_instance


PERIOD_MAP = {
    "1m": 30,
    "3m": 90,
    "6m": 180,
    "1y": 365,
    "2y": 730,
    "5y": 1825,
    "max": 7300,
}


def _cache_path(symbol: str, period: str) -> Path:
    safe_symbol = symbol.upper().replace("/", "_").replace("\\", "_")
    return CACHE_DIR / f"{safe_symbol}_{period}.json"


def _is_cache_valid(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        updated_at = payload.get("updated_at")
        if not updated_at:
            return False
        updated_time = datetime.fromisoformat(updated_at)
        if updated_time.tzinfo is None:
            updated_time = updated_time.replace(tzinfo=UTC)
        return (datetime.now(UTC) - updated_time) < CACHE_TTL
    except Exception:
        return False


def _write_cache(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def get_equity_price(
    symbol: str,
    period: str = "1y",
) -> dict[str, Any]:
    """获取股票日线数据，返回 K 线 + 成交量格式。"""
    symbol = symbol.strip().upper()
    if not symbol:
        raise ValueError("股票代码不能为空")

    cache_file = _cache_path(symbol, period)
    if _is_cache_valid(cache_file):
        return json.loads(cache_file.read_text(encoding="utf-8"))

    days = PERIOD_MAP.get(period, 365)
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    try:
        obb = _get_obb()
        result = obb.equity.price.historical(
            symbol,
            provider="yfinance",
            start_date=start_date,
        )
        df = result.to_dataframe()
    except Exception as e:
        logger.error("OpenBB 获取 %s 失败: %s", symbol, e)
        raise RuntimeError(f"获取 {symbol} 数据失败: {e}") from e

    if df.empty:
        return {
            "symbol": symbol,
            "period": period,
            "candles": [],
            "volume": [],
            "updated_at": datetime.now(UTC).isoformat(),
        }

    df.index = pd.to_datetime(df.index)
    df.sort_index(inplace=True)

    candles: list[dict] = []
    volumes: list[dict] = []

    for ts, row in df.iterrows():
        dt = pd.Timestamp(ts)
        if dt.tzinfo is None:
            dt = dt.tz_localize("UTC")
        t = int(dt.timestamp())
        o = float(row["open"])
        h = float(row["high"])
        l = float(row["low"])  # noqa: E741
        c = float(row["close"])
        v = float(row.get("volume", 0))

        candles.append({"time": t, "open": round(o, 4), "high": round(h, 4), "low": round(l, 4), "close": round(c, 4)})
        color = "rgba(239,83,80,0.5)" if c >= o else "rgba(38,166,154,0.5)"
        volumes.append({"time": t, "value": v, "color": color})

    payload = {
        "symbol": symbol,
        "period": period,
        "candles": candles,
        "volume": volumes,
        "updated_at": datetime.now(UTC).isoformat(),
    }
    _write_cache(cache_file, payload)
    return payload
