"""
FastAPI 后端 —— 为前端提供 K 线 + 指标 + 实时推送 + 回测 + 实盘管理。

端点:
    GET  /api/dates                交易日列表
    GET  /api/kline/today          当天全部 bar（pytdx + 采集器实时）
    GET  /api/kline/{date}         K 线 + MACD/RSI/KDJ 指标
    POST /api/backtest             运行回测并返回 K 线 + 信号
    WS   /ws/realtime              实时 bar 推送（Redis 订阅 tick 采集器）
    GET  /api/live-trading/status  实盘交易状态
    POST /api/live-trading/start   启动实盘 + 注册计划任务
    POST /api/live-trading/stop    停止实盘 + 取消计划任务

数据流：
    tick 采集器 → jvquant WS → 聚合 bar → Redis pub
    本服务 → Redis sub → 附加指标 → WS 推前端
"""

from __future__ import annotations

import asyncio
import json
import logging
import subprocess
import sys
from contextlib import asynccontextmanager
from pathlib import Path

import numpy as np
import pandas as pd
import psutil
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi import HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from equity_explorer import get_equity_price
from macro import get_all_indicators, get_fred_api_key, get_indicator
from ml_api import get_feature_importance, run_ml_backtest_for_date
from realtime import BarAggregator

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(name)s - %(message)s")
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
CSV_PATH = DATA_DIR / "159506_1min.csv"
PARQUET_PATH = DATA_DIR / "159506_1min.parquet"

DIST_DIR = Path(__file__).resolve().parent / "frontend" / "dist"

VOLUME_NOISE_THRESHOLD = 1.0
UP_COLOR = "rgba(239,83,80,0.5)"
DOWN_COLOR = "rgba(38,166,154,0.5)"


# ---------------------------------------------------------------------------
# 指标计算
# ---------------------------------------------------------------------------

def compute_ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def compute_macd(
    close: pd.Series,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    ema_fast = compute_ema(close, fast)
    ema_slow = compute_ema(close, slow)
    dif = ema_fast - ema_slow
    dea = compute_ema(dif, signal)
    histogram = dif - dea
    return dif, dea, histogram


def compute_rsi(close: pd.Series, period: int = 6) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - 100 / (1 + rs)


def compute_kdj(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    n: int = 9,
    k_period: int = 3,
    d_period: int = 3,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    lowest_low = low.rolling(window=n, min_periods=1).min()
    highest_high = high.rolling(window=n, min_periods=1).max()
    denom = highest_high - lowest_low
    rsv = ((close - lowest_low) / denom.replace(0, np.nan)) * 100
    rsv = rsv.fillna(50)
    k = rsv.ewm(com=k_period - 1, adjust=False).mean()
    d = k.ewm(com=d_period - 1, adjust=False).mean()
    j = 3 * k - 2 * d
    return k, d, j


# ---------------------------------------------------------------------------
# 数据加载
# ---------------------------------------------------------------------------

class KlineDataStore:
    """加载并缓存 K 线数据，提供按日查询 + 指标计算。"""

    def __init__(self) -> None:
        self._df: pd.DataFrame = pd.DataFrame()
        self._dates: list[str] = []

    def load(self) -> None:
        src = PARQUET_PATH if PARQUET_PATH.exists() else CSV_PATH
        if not src.exists():
            logger.warning("K 线数据文件不存在: %s", src)
            return

        if src.suffix == ".parquet":
            df = pd.read_parquet(src)
        else:
            df = pd.read_csv(src, parse_dates=["datetime"])

        df["datetime"] = pd.to_datetime(df["datetime"])
        df.loc[df["volume"] < VOLUME_NOISE_THRESHOLD, ["volume", "amount"]] = 0
        df["date_str"] = df["datetime"].dt.strftime("%Y-%m-%d")
        df["time"] = df["datetime"].astype("int64") // 10**9
        df.sort_values("datetime", inplace=True)

        self._df = df
        self._dates = sorted(df["date_str"].unique().tolist())
        logger.info("已加载 %d 条 K 线，覆盖 %d 个交易日", len(df), len(self._dates))

    @property
    def dates(self) -> list[str]:
        return self._dates

    def get_kline_with_indicators(self, date: str) -> dict:
        day = self._df[self._df["date_str"] == date].copy()
        if day.empty:
            return {"candles": [], "volume": [], "macd": [], "rsi": [], "kdj": [], "signals": []}

        close = day["close"]
        dif, dea, hist = compute_macd(close)
        rsi = compute_rsi(close)
        k, d, j = compute_kdj(day["high"], day["low"], close)

        candles: list[dict] = []
        volumes: list[dict] = []
        macd_data: list[dict] = []
        rsi_data: list[dict] = []
        kdj_data: list[dict] = []

        for idx, row in day.iterrows():
            t = int(row["time"])
            o, h, l, c = row["open"], row["high"], row["low"], row["close"]
            v = row["volume"]

            candles.append({"time": t, "open": o, "high": h, "low": l, "close": c})
            color = UP_COLOR if c >= o else DOWN_COLOR
            volumes.append({"time": t, "value": v, "color": color})

            dif_val = dif.at[idx]
            dea_val = dea.at[idx]
            hist_val = hist.at[idx]
            macd_data.append({
                "time": t,
                "dif": None if pd.isna(dif_val) else round(float(dif_val), 6),
                "dea": None if pd.isna(dea_val) else round(float(dea_val), 6),
                "histogram": None if pd.isna(hist_val) else round(float(hist_val), 6),
            })

            rsi_val = rsi.at[idx]
            rsi_data.append({
                "time": t,
                "value": None if pd.isna(rsi_val) else round(float(rsi_val), 2),
            })

            k_val, d_val, j_val = k.at[idx], d.at[idx], j.at[idx]
            kdj_data.append({
                "time": t,
                "k": None if pd.isna(k_val) else round(float(k_val), 2),
                "d": None if pd.isna(d_val) else round(float(d_val), 2),
                "j": None if pd.isna(j_val) else round(float(j_val), 2),
            })

        return {
            "candles": candles,
            "volume": volumes,
            "macd": macd_data,
            "rsi": rsi_data,
            "kdj": kdj_data,
            "signals": [],
        }


    def get_kline_range(self, end_date: str, days: int = 5) -> dict:
        """返回 end_date 及之前 days 个交易日的 K 线 + 指标。"""
        if end_date not in self._dates:
            return self.get_kline_with_indicators(end_date)

        end_idx = self._dates.index(end_date)
        start_idx = max(0, end_idx - days + 1)
        selected_dates = self._dates[start_idx : end_idx + 1]

        subset = self._df[self._df["date_str"].isin(selected_dates)].copy()
        if subset.empty:
            return {"candles": [], "volume": [], "macd": [], "rsi": [], "kdj": [], "signals": []}

        subset.sort_values("datetime", inplace=True)
        close = subset["close"]
        dif, dea, hist = compute_macd(close)
        rsi = compute_rsi(close)
        k, d, j = compute_kdj(subset["high"], subset["low"], close)

        candles, volumes, macd_data, rsi_data, kdj_data = [], [], [], [], []
        for idx, row in subset.iterrows():
            t = int(row["time"])
            o, h, l, c = row["open"], row["high"], row["low"], row["close"]
            v = row["volume"]
            color = UP_COLOR if c >= o else DOWN_COLOR

            candles.append({"time": t, "open": o, "high": h, "low": l, "close": c})
            volumes.append({"time": t, "value": v, "color": color})

            dif_val, dea_val, hist_val = dif.at[idx], dea.at[idx], hist.at[idx]
            macd_data.append({
                "time": t,
                "dif": None if pd.isna(dif_val) else round(float(dif_val), 6),
                "dea": None if pd.isna(dea_val) else round(float(dea_val), 6),
                "histogram": None if pd.isna(hist_val) else round(float(hist_val), 6),
            })
            rsi_val = rsi.at[idx]
            rsi_data.append({"time": t, "value": None if pd.isna(rsi_val) else round(float(rsi_val), 2)})

            k_val, d_val, j_val = k.at[idx], d.at[idx], j.at[idx]
            kdj_data.append({
                "time": t,
                "k": None if pd.isna(k_val) else round(float(k_val), 2),
                "d": None if pd.isna(d_val) else round(float(d_val), 2),
                "j": None if pd.isna(j_val) else round(float(j_val), 2),
            })

        return {
            "candles": candles, "volume": volumes,
            "macd": macd_data, "rsi": rsi_data, "kdj": kdj_data,
            "signals": [],
        }


store = KlineDataStore()


# ---------------------------------------------------------------------------
# 实时行情广播（Redis 订阅 tick 采集器发布的 bar + 交易信号）
# ---------------------------------------------------------------------------

STOCK_CODE = "159506"


class RealtimeBroadcaster:
    """
    从 Redis 接收 tick 采集器发布的 1 分钟 bar，附加指标后推送给前端。

    数据流：
      tick 采集器 → jvquant → bar → Redis pub → 本广播器 → 计算指标 → WS 推前端
    """

    def __init__(self) -> None:
        self._clients: set[WebSocket] = set()
        self._redis_task: asyncio.Task | None = None
        self.aggregator = BarAggregator(stock_code=STOCK_CODE)

    async def start(self) -> None:
        try:
            import redis.asyncio as aioredis
            r = aioredis.Redis(host="localhost", port=6379, decode_responses=True)
            await r.ping()
            pubsub = r.pubsub()
            await pubsub.subscribe("etf:159506:bar", "etf:159506:signal", "etf:159506:paper-signal")
            self._redis_task = asyncio.create_task(self._listen_redis(pubsub))
            logger.info("Redis 订阅已启动（bar + signal + paper-signal）")
        except Exception as e:
            logger.warning("Redis 连接失败，实时推送不可用（pytdx 轮询仍可用）: %s", e)

    async def _listen_redis(self, pubsub) -> None:
        try:
            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue
                channel = message.get("channel", "")
                data = message["data"]

                if channel == "etf:159506:bar":
                    enriched = self._enrich_bar(data)
                    if enriched:
                        await self._broadcast(enriched)
                elif channel == "etf:159506:paper-signal":
                    tagged = self._tag_signal_source(data, "paper")
                    await self._broadcast(tagged)
                else:
                    tagged = self._tag_signal_source(data, "live")
                    await self._broadcast(tagged)
        except asyncio.CancelledError:
            pass

    @staticmethod
    def _tag_signal_source(raw_json: str, source: str) -> str:
        try:
            payload = json.loads(raw_json)
            if isinstance(payload, dict):
                payload["source"] = source
                return json.dumps(payload, ensure_ascii=False)
        except Exception:
            pass
        return raw_json

    def _enrich_bar(self, raw_json: str) -> str | None:
        """解析 tick 采集器发来的 OHLCV bar，附加 MACD/RSI/KDJ 后序列化。"""
        try:
            bar = json.loads(raw_json)
            if not isinstance(bar, dict) or "time" not in bar:
                return None

            bar.pop("_partial", None)

            enriched = self.aggregator.accept_bar(bar)
            if enriched:
                return json.dumps(enriched, ensure_ascii=False)
            return None
        except Exception as e:
            logger.debug("enrich_bar 失败: %s", e)
            return None

    async def _broadcast(self, text: str) -> None:
        dead: list[WebSocket] = []
        for ws in self._clients:
            try:
                await ws.send_text(text)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self._clients.discard(ws)

    async def stop(self) -> None:
        if self._redis_task:
            self._redis_task.cancel()
            try:
                await self._redis_task
            except asyncio.CancelledError:
                pass

    def connect(self, ws: WebSocket) -> None:
        self._clients.add(ws)

    def disconnect(self, ws: WebSocket) -> None:
        self._clients.discard(ws)


broadcaster = RealtimeBroadcaster()


# ---------------------------------------------------------------------------
# FastAPI 应用
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    store.load()
    if not get_fred_api_key():
        logger.warning("未检测到 FRED_API_KEY，宏观指标接口将仅能读取本地缓存")
    await broadcaster.start()
    yield
    await broadcaster.stop()


app = FastAPI(title="159506 ETF K 线监控", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/dates")
def get_dates() -> list[str]:
    return store.dates


@app.get("/api/kline/today")
def get_kline_today(history_days: int = 0) -> dict:
    """返回当天全部 1 分钟 bar（pytdx 历史 + 采集器实时），含指标。

    history_days: 额外拼接之前 N 个交易日的历史数据。
    """
    today = broadcaster.aggregator.get_today_kline()
    if history_days <= 0 or not store.dates:
        return today

    prev_dates = store.dates[-history_days:]
    prev = store.get_kline_range(prev_dates[-1], days=len(prev_dates)) if prev_dates else None
    if not prev or not prev["candles"]:
        return today

    for key in ("candles", "volume", "macd", "rsi", "kdj"):
        today[key] = prev[key] + today.get(key, [])
    return today


@app.get("/api/kline/range")
def get_kline_range(end: str, days: int = 5) -> dict:
    """返回指定日期及之前 N 个交易日的 K 线 + 指标。"""
    return store.get_kline_range(end, days=days)


@app.get("/api/kline/{date}")
def get_kline(date: str) -> dict:
    return store.get_kline_with_indicators(date)


@app.get("/api/macro/indicators")
def get_macro_indicators() -> dict:
    return {"indicators": get_all_indicators()}


@app.get("/api/macro/data")
def get_macro_data() -> dict:
    try:
        indicators = get_all_indicators()
        series = [get_indicator(item["series_id"]) for item in indicators]
        return {"series": series}
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    except Exception as e:
        logger.error("加载宏观数据失败: %s", e)
        raise HTTPException(status_code=500, detail="宏观数据加载失败") from e


@app.get("/api/equity/price/{symbol}")
def equity_price(symbol: str, period: str = "1y") -> dict:
    try:
        return get_equity_price(symbol, period)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e
    except Exception as e:
        logger.error("获取股票数据失败 %s: %s", symbol, e)
        raise HTTPException(status_code=500, detail="股票数据获取失败") from e


@app.websocket("/ws/realtime")
async def ws_realtime(ws: WebSocket):
    await ws.accept()
    broadcaster.connect(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        broadcaster.disconnect(ws)


# ---------------------------------------------------------------------------
# 回测
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BACKTEST_SIGNAL_DIR = PROJECT_ROOT / "data" / "backtest_signals"


class BacktestRequest(BaseModel):
    date: str


def _run_backtest_subprocess(date_str: str) -> Path:
    """在子进程中执行回测脚本，将信号保存到 JSON。"""
    BACKTEST_SIGNAL_DIR.mkdir(parents=True, exist_ok=True)
    output_path = BACKTEST_SIGNAL_DIR / f"{date_str}.json"

    script = PROJECT_ROOT / "run_backtest_for_viewer.py"
    if not script.exists():
        raise FileNotFoundError(f"回测脚本不存在: {script}")

    result = subprocess.run(
        [sys.executable, str(script), date_str, str(output_path)],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        timeout=300,
    )
    if result.returncode != 0:
        err_tail = (result.stderr or result.stdout or "")[-4000:]
        logger.error("回测子进程失败:\n%s", err_tail)
        detail = err_tail.strip().splitlines()[-8:]
        hint = "\n".join(detail) if detail else "(无 stderr)"
        raise RuntimeError(f"回测失败 (exit {result.returncode}): {hint}")

    return output_path


@app.post("/api/backtest")
async def run_backtest(req: BacktestRequest) -> dict:
    kline = store.get_kline_with_indicators(req.date)
    meta: dict = {"ok": False, "error": None, "signal_count": 0}

    signal_file = BACKTEST_SIGNAL_DIR / f"{req.date}.json"
    try:
        loop = asyncio.get_running_loop()
        signal_file = await loop.run_in_executor(None, _run_backtest_subprocess, req.date)
        meta["ok"] = True
    except FileNotFoundError as e:
        logger.warning("回测脚本未找到: %s", e)
        meta["error"] = "未找到 run_backtest_for_viewer.py，请从项目根目录启动 kline-viewer"
        kline["backtest_meta"] = meta
        return kline
    except Exception as e:
        logger.error("回测执行失败: %s", e)
        meta["error"] = str(e)
        kline["backtest_meta"] = meta
        return kline

    if signal_file.exists():
        try:
            signals = json.loads(signal_file.read_text(encoding="utf-8"))
            kline["signals"] = signals
            meta["signal_count"] = len(signals)
        except Exception as e:
            logger.error("解析回测信号失败: %s", e)
            meta["error"] = f"解析信号 JSON 失败: {e}"
            meta["ok"] = False

    kline["backtest_meta"] = meta
    return kline


# ---------------------------------------------------------------------------
# ML 回测
# ---------------------------------------------------------------------------

class MLBacktestRequest(BaseModel):
    date: str = ""


@app.post("/api/ml-backtest")
async def run_ml_backtest(req: MLBacktestRequest) -> dict:
    """运行 ML 策略回测，返回权益曲线 + 指标 + 信号 + 特征重要性。"""
    kline = store.get_kline_with_indicators(req.date) if req.date else {}
    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(None, run_ml_backtest_for_date, req.date)
    if kline:
        result["kline"] = kline
        if result.get("signals"):
            kline["signals"] = result["signals"]
    return result


@app.get("/api/ml-features/importance")
def ml_feature_importance() -> list[dict]:
    """返回模型特征重要性排名。"""
    return get_feature_importance()


# ---------------------------------------------------------------------------
# ML 模拟交易 (paper trading)
# ---------------------------------------------------------------------------

PAPER_TRADING_SCRIPT = "ml_paper_trading.py"
PAPER_TASK_NAME = "ETF159506_Paper_Trading"
_paper_process: subprocess.Popen | None = None


def _find_paper_trading_pid() -> int | None:
    for proc in psutil.process_iter(["pid", "cmdline"]):
        try:
            cmdline = proc.info.get("cmdline") or []
            if any(PAPER_TRADING_SCRIPT in arg for arg in cmdline):
                return proc.info["pid"]
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return None


@app.get("/api/paper-trading/status")
def paper_trading_status() -> dict:
    pid = _find_paper_trading_pid()
    return {"running": pid is not None, "pid": pid}


@app.post("/api/paper-trading/start")
async def start_paper_trading() -> dict:
    global _paper_process
    pid = _find_paper_trading_pid()
    if pid:
        return {"ok": True, "message": "ML 模拟交易已在运行", "pid": pid}

    script = PROJECT_ROOT / PAPER_TRADING_SCRIPT
    if not script.exists():
        raise HTTPException(status_code=404, detail=f"模拟交易脚本不存在: {script}")

    _paper_process = subprocess.Popen(
        [sys.executable, str(script)],
        cwd=str(PROJECT_ROOT),
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    await asyncio.sleep(2)
    pid = _find_paper_trading_pid()
    return {
        "ok": pid is not None,
        "message": "ML 模拟交易已启动" if pid else "进程启动中，请稍后检查状态",
        "pid": pid,
    }


@app.post("/api/paper-trading/stop")
def stop_paper_trading() -> dict:
    pid = _find_paper_trading_pid()
    killed = False
    if pid:
        try:
            parent = psutil.Process(pid)
            for child in parent.children(recursive=True):
                child.kill()
            parent.kill()
            killed = True
        except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
            logger.warning("停止模拟交易进程失败: %s", e)
    return {
        "ok": True,
        "killed": killed,
        "message": "ML 模拟交易已停止" if killed else "模拟交易未在运行",
    }


# ---------------------------------------------------------------------------
# 实盘交易管理
# ---------------------------------------------------------------------------

PROJECT_ROOT_ABS = str(PROJECT_ROOT)
LIVE_TRADING_SCRIPT = "etf_159506_live_trading.py"
LIVE_TASK_NAME = "ETF159506_Live_Trading"
LIVE_BAT = PROJECT_ROOT / "start_live_trading.bat"

_live_process: subprocess.Popen | None = None


def _find_live_trading_pid() -> int | None:
    """查找正在运行的 etf_159506_live_trading.py 进程。"""
    for proc in psutil.process_iter(["pid", "cmdline"]):
        try:
            cmdline = proc.info.get("cmdline") or []
            if any(LIVE_TRADING_SCRIPT in arg for arg in cmdline):
                return proc.info["pid"]
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return None


def _is_task_registered() -> bool:
    """检查 Windows 计划任务是否存在。"""
    try:
        r = subprocess.run(
            ["schtasks", "/query", "/tn", LIVE_TASK_NAME],
            capture_output=True, timeout=5,
        )
        return r.returncode == 0
    except Exception:
        return False


def _register_task() -> bool:
    try:
        subprocess.run(
            ["schtasks", "/delete", "/tn", LIVE_TASK_NAME, "/f"],
            capture_output=True, timeout=5,
        )
        r = subprocess.run(
            [
                "schtasks", "/create",
                "/tn", LIVE_TASK_NAME,
                "/tr", str(LIVE_BAT),
                "/sc", "weekly",
                "/d", "MON,TUE,WED,THU,FRI",
                "/st", "09:20",
                "/rl", "HIGHEST",
                "/f",
            ],
            capture_output=True, text=True, timeout=10,
        )
        return r.returncode == 0
    except Exception as e:
        logger.error("注册计划任务失败: %s", e)
        return False


def _unregister_task() -> bool:
    try:
        r = subprocess.run(
            ["schtasks", "/delete", "/tn", LIVE_TASK_NAME, "/f"],
            capture_output=True, timeout=5,
        )
        return r.returncode == 0
    except Exception:
        return False


@app.get("/api/live-trading/status")
def live_trading_status() -> dict:
    pid = _find_live_trading_pid()
    return {
        "running": pid is not None,
        "pid": pid,
        "scheduled": _is_task_registered(),
    }


@app.post("/api/live-trading/start")
async def start_live_trading() -> dict:
    global _live_process

    pid = _find_live_trading_pid()
    if pid:
        return {"ok": True, "message": "实盘交易已在运行", "pid": pid}

    if not LIVE_BAT.exists():
        raise HTTPException(status_code=404, detail=f"启动脚本不存在: {LIVE_BAT}")

    _live_process = subprocess.Popen(
        [str(LIVE_BAT)],
        cwd=PROJECT_ROOT_ABS,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    await asyncio.sleep(2)
    pid = _find_live_trading_pid()

    _register_task()

    return {
        "ok": pid is not None,
        "message": "实盘交易已启动" if pid else "进程启动中，请稍后检查状态",
        "pid": pid,
        "scheduled": _is_task_registered(),
    }


@app.post("/api/live-trading/stop")
def stop_live_trading() -> dict:
    pid = _find_live_trading_pid()
    killed = False
    if pid:
        try:
            parent = psutil.Process(pid)
            for child in parent.children(recursive=True):
                child.kill()
            parent.kill()
            killed = True
        except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
            logger.warning("停止进程失败: %s", e)

    _unregister_task()

    return {
        "ok": True,
        "killed": killed,
        "scheduled": False,
        "message": "实盘交易已停止" if killed else "实盘交易未在运行",
    }


# SPA 静态文件：必须放在最后，作为 fallback
if DIST_DIR.exists():
    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        file_path = DIST_DIR / full_path
        if file_path.is_file():
            return FileResponse(file_path)
        return FileResponse(DIST_DIR / "index.html")
