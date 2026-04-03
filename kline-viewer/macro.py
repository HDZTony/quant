"""
FRED 宏观数据采集与缓存模块。

职责：
- 定义美股相关宏观指标元数据
- 从 FRED 拉取时间序列
- 本地 JSON 缓存（默认 24 小时）
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from fredapi import Fred

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CACHE_DIR = PROJECT_ROOT / "data" / "macro"
CACHE_TTL = timedelta(hours=24)
DEFAULT_START_DATE = "2000-01-01"
# 输出改为日历日频后 bump，使旧缓存失效
CACHE_SCHEMA_VERSION = 2


@dataclass(frozen=True)
class MacroIndicator:
    series_id: str
    name: str
    unit: str
    frequency: str
    description: str
    transform: str = "raw"  # raw | yoy_pct


INDICATORS: list[MacroIndicator] = [
    # 说明：ISM 制造业 PMI（原 FRED 系列 NAPM）已于 2016 年从 FRED 下架，无法拉取，故不收录。
    MacroIndicator("FEDFUNDS", "联邦基金利率", "%", "daily", "Federal Funds Effective Rate"),
    MacroIndicator("CPIAUCSL", "CPI同比", "%", "daily", "Consumer Price Index (All Urban Consumers)", "yoy_pct"),
    MacroIndicator("CPILFESL", "核心CPI同比", "%", "daily", "Core CPI (Less Food and Energy)", "yoy_pct"),
    MacroIndicator("A191RL1Q225SBEA", "GDP增长率", "%", "daily", "Real GDP Percent Change"),
    MacroIndicator("PAYEMS", "非农就业人数", "千人", "daily", "All Employees, Total Nonfarm"),
    MacroIndicator("UNRATE", "失业率", "%", "daily", "Unemployment Rate"),
    MacroIndicator("DGS10", "10年期国债收益率", "%", "daily", "10-Year Treasury Constant Maturity Rate"),
    MacroIndicator("VIXCLS", "VIX恐慌指数", "指数", "daily", "CBOE Volatility Index: VIX"),
    MacroIndicator("RSAFS", "零售销售", "百万美元", "daily", "Retail Sales: Total"),
    MacroIndicator("UMCSENT", "消费者信心指数", "指数", "daily", "University of Michigan: Consumer Sentiment"),
    MacroIndicator("ICSA", "初次申请失业金", "人数", "daily", "Initial Claims"),
]

_INDICATOR_MAP = {item.series_id: item for item in INDICATORS}


def _cache_path(series_id: str) -> Path:
    return CACHE_DIR / f"{series_id}.json"


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _get_fred_client() -> Fred:
    api_key = get_fred_api_key()
    if not api_key:
        raise RuntimeError("未配置 FRED_API_KEY 环境变量")
    return Fred(api_key=api_key)


def get_fred_api_key() -> str:
    """统一读取 FRED API Key：优先环境变量，其次项目根目录 .env。"""
    return os.getenv("FRED_API_KEY", "").strip() or _read_key_from_dotenv("FRED_API_KEY")


def _read_key_from_dotenv(name: str) -> str:
    """从项目根目录 .env 读取指定 key（无需额外依赖）。"""
    env_file = PROJECT_ROOT / ".env"
    if not env_file.exists():
        return ""

    try:
        for raw_line in env_file.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            if key.strip() != name:
                continue
            cleaned = value.strip().strip("'").strip('"')
            return cleaned
    except Exception:
        return ""

    return ""


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
        if payload.get("schema_version") != CACHE_SCHEMA_VERSION:
            return False
        return (datetime.now(UTC) - updated_time) < CACHE_TTL
    except Exception:
        return False


def _load_cache(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_cache(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _resample_to_daily(series: pd.Series) -> pd.Series:
    """将 FRED 各频率序列前向填充为日历日频，与前端统一时间轴。"""
    if series.empty:
        return series
    s = series.sort_index().copy()
    if not isinstance(s.index, pd.DatetimeIndex):
        s.index = pd.to_datetime(s.index)
    if s.index.tz is not None:
        s.index = s.index.tz_convert("UTC").tz_localize(None)
    s = s.groupby(s.index.normalize()).last()
    start = s.index.min()
    end = s.index.max()
    daily = pd.date_range(start, end, freq="D")
    out = s.reindex(daily).ffill()
    first = out.first_valid_index()
    if first is None:
        return out.iloc[0:0]
    return out.loc[first:].dropna()


def _series_to_points(series: pd.Series) -> list[dict[str, float | int]]:
    clean_series = series.dropna()
    points: list[dict[str, float | int]] = []
    for ts, val in clean_series.items():
        dt = pd.Timestamp(ts)
        if dt.tzinfo is None:
            dt = dt.tz_localize("UTC")
        points.append(
            {
                "time": int(dt.timestamp()),
                "value": round(float(val), 6),
            },
        )
    return points


def _fetch_indicator(indicator: MacroIndicator) -> dict[str, Any]:
    try:
        fred = _get_fred_client()
        raw_series = fred.get_series(indicator.series_id, observation_start=DEFAULT_START_DATE)
        if raw_series is None or len(raw_series) == 0:
            return {
                "meta": asdict(indicator),
                "data": [],
                "updated_at": _utc_now_iso(),
                "error": "该指标暂无可用数据",
                "schema_version": CACHE_SCHEMA_VERSION,
            }

        series = raw_series.sort_index()
        if indicator.transform == "yoy_pct":
            series = series.pct_change(12, fill_method=None) * 100

        series = _resample_to_daily(series)

        return {
            "meta": asdict(indicator),
            "data": _series_to_points(series),
            "updated_at": _utc_now_iso(),
            "error": None,
            "schema_version": CACHE_SCHEMA_VERSION,
        }
    except Exception as e:
        return {
            "meta": asdict(indicator),
            "data": [],
            "updated_at": _utc_now_iso(),
            "error": str(e),
            "schema_version": CACHE_SCHEMA_VERSION,
        }


def get_all_indicators() -> list[dict[str, str]]:
    """返回指标元数据列表。"""
    return [asdict(item) for item in INDICATORS]


def get_indicator(series_id: str, force_refresh: bool = False) -> dict[str, Any]:
    """返回单个指标数据（优先缓存）。"""
    indicator = _INDICATOR_MAP.get(series_id)
    if indicator is None:
        raise ValueError(f"不支持的 series_id: {series_id}")

    cache_file = _cache_path(series_id)
    if not force_refresh and _is_cache_valid(cache_file):
        return _load_cache(cache_file)

    payload = _fetch_indicator(indicator)
    _write_cache(cache_file, payload)
    return payload

