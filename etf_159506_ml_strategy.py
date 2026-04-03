#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
159506 港股通医疗 ETF ML 策略 —— Nautilus Trader 版本。

使用 XGBoost 模型在每根 1 分钟 bar 上实时预测买卖概率，
当概率超过阈值时提交市价单。

特征计算通过 ``IncrementalFeatureBuilder`` 逐条 bar 增量更新，
与批量版 ``ml_feature_builder.build_features()`` 保持数值一致。
"""

from __future__ import annotations

import json
import logging
from decimal import Decimal
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from xgboost import XGBClassifier

from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import InstrumentId, Venue
from nautilus_trader.model.objects import Quantity
from nautilus_trader.trading.strategy import Strategy

from ml_incremental_features import IncrementalFeatureBuilder

logger = logging.getLogger(__name__)


class ETF159506MLStrategyConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType
    venue: Venue
    trade_size: Decimal = Decimal("10000")
    model_path: str = "models/xgb_signal.json"
    meta_path: str = "models/xgb_signal_meta.joblib"
    buy_prob_threshold: float = 0.45
    sell_prob_threshold: float = 0.45


class ETF159506MLStrategy(Strategy):
    """
    基于 ML 模型的 ETF 159506 策略。

    每根 bar 到达时：
    1. IncrementalFeatureBuilder 计算 34 维特征
    2. scaler 标准化 → model.predict_proba
    3. 买入概率 > buy_threshold 且无持仓 → 买入
    4. 卖出概率 > sell_threshold 且有持仓 → 卖出
    """

    def __init__(self, config: ETF159506MLStrategyConfig) -> None:
        super().__init__(config=config)

        self._model: XGBClassifier | None = None
        self._scaler = None
        self._feature_cols: list[str] = []
        self._label_map: dict[int, int] = {}
        self._builder = IncrementalFeatureBuilder()

        if config.trade_size == 0:
            self._trade_size = None
        else:
            self._trade_size = Quantity.from_int(int(config.trade_size))

        self._instrument = None
        self._is_backtest = False

        # Redis (仅实时模式使用)
        self._redis = None

        # 交易信号收集 (供 kline-viewer 展示)
        self.trade_signals: list[dict] = []
        self._saved_trade_signals: list[dict] = []

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def on_start(self) -> None:
        clock_name = type(self.clock).__name__
        self._is_backtest = clock_name == "TestClock"
        self._log.info(f"ML 策略启动 | 模式={'回测' if self._is_backtest else '实时'}")

        self._instrument = self.cache.instrument(self.config.instrument_id)
        if self._instrument is None:
            raise RuntimeError(f"Instrument {self.config.instrument_id} 未找到")

        self._load_model()
        self.subscribe_bars(self.config.bar_type)

        if not self._is_backtest:
            self._init_redis()

    def on_stop(self) -> None:
        self._saved_trade_signals = list(self.trade_signals)
        self._log.info(f"策略停止，共保存 {len(self._saved_trade_signals)} 条交易信号")

    # ------------------------------------------------------------------
    # Model loading
    # ------------------------------------------------------------------
    def _load_model(self) -> None:
        model_path = Path(self.config.model_path)
        meta_path = Path(self.config.meta_path)

        if not model_path.exists():
            raise FileNotFoundError(f"模型文件不存在: {model_path}")
        if not meta_path.exists():
            raise FileNotFoundError(f"元数据文件不存在: {meta_path}")

        self._model = XGBClassifier()
        self._model.load_model(str(model_path))

        meta = joblib.load(str(meta_path))
        self._scaler = meta["scaler"]
        self._feature_cols = meta["feature_columns"]
        self._label_map = meta.get("label_map", {0: -1, 1: 0, 2: 1})

        self._log.info(
            f"模型加载完成 | 特征数={len(self._feature_cols)} | "
            f"标签映射={self._label_map}"
        )

    # ------------------------------------------------------------------
    # Core: on_bar
    # ------------------------------------------------------------------
    def on_bar(self, bar: Bar) -> None:
        utc_dt = pd.Timestamp(bar.ts_event, unit="ns", tz="UTC")
        beijing_dt = utc_dt.tz_convert("Asia/Shanghai")

        features = self._builder.update(
            open_=bar.open.as_double(),
            high=bar.high.as_double(),
            low=bar.low.as_double(),
            close=bar.close.as_double(),
            volume=bar.volume.as_double(),
            dt=beijing_dt,
        )
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

        has_position = self._has_open_position()
        close_price = bar.close.as_double()
        bar_time = int(beijing_dt.timestamp())

        if buy_prob > self.config.buy_prob_threshold and not has_position:
            self._submit_market_order(OrderSide.BUY, close_price)
            self._record_signal(bar_time, "BUY", close_price, buy_prob)

        elif sell_prob > self.config.sell_prob_threshold and has_position:
            self._submit_market_order(OrderSide.SELL, close_price)
            self._record_signal(bar_time, "SELL", close_price, sell_prob)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _class_index_for_label(self, label: int) -> int | None:
        """根据 label_map 找到模型 classes_ 中的索引。"""
        for class_idx, mapped_label in self._label_map.items():
            if mapped_label == label:
                return int(class_idx)
        return None

    def _has_open_position(self) -> bool:
        positions = self.cache.positions_open(
            instrument_id=self.config.instrument_id
        )
        return len(positions) > 0

    def _get_position_quantity(self) -> int:
        positions = self.cache.positions_open(
            instrument_id=self.config.instrument_id
        )
        if not positions:
            return 0
        return int(positions[0].quantity)

    def _submit_market_order(self, side: OrderSide, price: float) -> None:
        if self._trade_size is None:
            account = self.portfolio.account(self.config.venue)
            if account is None:
                self._log.warning("无法获取账户信息，跳过交易")
                return
            balance = float(account.balance_total(account.base_currency).as_double())
            if side == OrderSide.BUY:
                qty = int(balance / price // 100) * 100
                if qty <= 0:
                    return
                quantity = Quantity.from_int(qty)
            else:
                quantity = Quantity.from_int(self._get_position_quantity())
        else:
            quantity = self._trade_size

        order = self.order_factory.market(
            instrument_id=self.config.instrument_id,
            order_side=side,
            quantity=quantity,
        )
        self.submit_order(order)

    def _record_signal(self, time_val: int, side: str, price: float, prob: float) -> None:
        signal = {
            "time": time_val,
            "side": side,
            "price": round(price, 4),
            "probability": round(prob, 4),
            "signal_type": "ML_MODEL",
        }
        self.trade_signals.append(signal)

        if not self._is_backtest:
            self._publish_signal(signal)

    # ------------------------------------------------------------------
    # Redis (仅实时)
    # ------------------------------------------------------------------
    def _init_redis(self) -> None:
        try:
            import redis as _redis_mod
            self._redis = _redis_mod.Redis(host="localhost", port=6379, socket_timeout=1)
            self._redis.ping()
            self._log.info("Redis 连接成功 (ML 策略)")
        except Exception as e:
            self._redis = None
            self._log.warning(f"Redis 连接失败: {e}")

    def _publish_signal(self, signal: dict) -> None:
        if self._redis is None:
            return
        try:
            self._redis.publish("etf:159506:paper-signal", json.dumps(signal))
        except Exception:
            pass
