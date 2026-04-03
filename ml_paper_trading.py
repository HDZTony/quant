#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
ML 模拟交易脚本 —— 接收实时行情，运行 ML 策略但不发出真实订单。

信号通过 Redis ``etf:159506:paper-signal`` 频道发布，
kline-viewer 的 WebSocket 广播器会订阅此频道并推送到前端。

用法::

    uv run ml_paper_trading.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
import sys
from pathlib import Path

import redis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("ml_paper_trading.log", mode="w", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
    force=True,
)
logger = logging.getLogger(__name__)


class MLPaperTradingSimulator:
    """
    轻量级 ML 模拟交易器。

    不依赖 Nautilus TradingNode（避免对 jvquant exec client 的依赖），
    而是直接订阅 Redis ``etf:159506:bar`` 频道，
    用 IncrementalFeatureBuilder + XGBoost 模型实时产生信号，
    再发布到 ``etf:159506:paper-signal``。
    """

    def __init__(self) -> None:
        from ml_incremental_features import IncrementalFeatureBuilder

        self._builder = IncrementalFeatureBuilder()
        self._model = None
        self._scaler = None
        self._feature_cols: list[str] = []
        self._label_map: dict[int, int] = {}

        self._position = 0
        self._entry_price = 0.0
        self._capital = 230_000.0
        self._trade_count = 0

        self._redis = redis.Redis(host="localhost", port=6379, decode_responses=True)
        self._running = False

    def _load_model(self) -> None:
        import joblib
        from xgboost import XGBClassifier

        model_path = Path("models/xgb_signal.json")
        meta_path = Path("models/xgb_signal_meta.joblib")
        if not model_path.exists() or not meta_path.exists():
            raise FileNotFoundError("ML 模型文件不存在，请先运行 ml_signal_trainer.py")

        self._model = XGBClassifier()
        self._model.load_model(str(model_path))

        meta = joblib.load(str(meta_path))
        self._scaler = meta["scaler"]
        self._feature_cols = meta["feature_columns"]
        self._label_map = meta.get("label_map", {0: -1, 1: 0, 2: 1})
        logger.info("ML 模型加载完成: %d 维特征", len(self._feature_cols))

    def _class_index_for_label(self, label: int) -> int | None:
        for class_idx, mapped_label in self._label_map.items():
            if mapped_label == label:
                return int(class_idx)
        return None

    def _process_bar(self, bar: dict) -> None:
        import numpy as np
        from datetime import datetime

        t = bar.get("time")
        o = bar.get("open")
        h = bar.get("high")
        l = bar.get("low")
        c = bar.get("close")
        v = bar.get("volume", 0)
        if t is None or c is None:
            return

        dt = datetime.fromtimestamp(t)
        features = self._builder.update(o, h, l, c, v, dt)
        if features is None:
            return

        ordered = [features.get(col, 0.0) for col in self._feature_cols]
        X = np.array([ordered])
        X_scaled = self._scaler.transform(X)
        probs = self._model.predict_proba(X_scaled)[0]

        buy_idx = self._class_index_for_label(1)
        sell_idx = self._class_index_for_label(-1)
        buy_prob = probs[buy_idx] if buy_idx is not None else 0.0
        sell_prob = probs[sell_idx] if sell_idx is not None else 0.0

        BUY_THRESHOLD = 0.45
        SELL_THRESHOLD = 0.45

        if buy_prob > BUY_THRESHOLD and self._position == 0:
            self._position = 10000
            self._entry_price = c
            self._trade_count += 1
            self._publish_signal(t, "BUY", c, buy_prob)
            logger.info("模拟买入 | 价格=%.3f | 概率=%.3f | 交易#%d", c, buy_prob, self._trade_count)

        elif sell_prob > SELL_THRESHOLD and self._position > 0:
            pnl = (c - self._entry_price) * self._position
            self._capital += pnl
            self._position = 0
            self._trade_count += 1
            self._publish_signal(t, "SELL", c, sell_prob)
            logger.info(
                "模拟卖出 | 价格=%.3f | 概率=%.3f | 盈亏=%.2f | 交易#%d",
                c, sell_prob, pnl, self._trade_count,
            )

    def _publish_signal(self, time_val: int, side: str, price: float, prob: float) -> None:
        payload = {
            "time": time_val,
            "side": side,
            "price": round(price, 4),
            "probability": round(prob, 4),
            "signal_type": "ML_PAPER",
            "source": "paper",
        }
        try:
            self._redis.publish("etf:159506:paper-signal", json.dumps(payload))
        except Exception as e:
            logger.warning("Redis 发布信号失败: %s", e)

    def run(self) -> None:
        self._load_model()
        self._running = True
        pubsub = self._redis.pubsub()
        pubsub.subscribe("etf:159506:bar")
        logger.info("ML 模拟交易已启动，订阅 etf:159506:bar")

        try:
            for message in pubsub.listen():
                if not self._running:
                    break
                if message["type"] != "message":
                    continue
                try:
                    bar = json.loads(message["data"])
                    if isinstance(bar, dict) and "time" in bar:
                        self._process_bar(bar)
                except json.JSONDecodeError:
                    continue
        except KeyboardInterrupt:
            pass
        finally:
            pubsub.unsubscribe()
            logger.info("ML 模拟交易已停止 | 总交易=%d", self._trade_count)

    def stop(self) -> None:
        self._running = False


def main() -> None:
    sim = MLPaperTradingSimulator()

    def handle_signal(signum, frame):
        logger.info("收到退出信号")
        sim.stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    sim.run()


if __name__ == "__main__":
    main()
