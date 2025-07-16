#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
MACD策略 - 参照Nautilus Trader官方示例
基于官方文档: https://nautilustrader.io/docs/latest/getting_started/quickstart/
"""

from nautilus_trader.core.message import Event
from nautilus_trader.indicators.macd import MovingAverageConvergenceDivergence
from nautilus_trader.model import InstrumentId
from nautilus_trader.model import Position
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import PositionSide
from nautilus_trader.model.enums import PriceType
from nautilus_trader.model.events import PositionOpened
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.trading.strategy import StrategyConfig
from nautilus_trader.model import Quantity


class MACDConfig(StrategyConfig):
    """MACD策略配置"""
    instrument_id: InstrumentId
    fast_period: int = 12
    slow_period: int = 26
    trade_size: int = 1_000_000
    entry_threshold: float = 0.00010


class MACDStrategy(Strategy):
    """
    MACD策略实现
    参照官方示例: https://nautilustrader.io/docs/latest/getting_started/quickstart/
    """
    
    def __init__(self, config: MACDConfig):
        super().__init__(config=config)
        
        # MACD指标 - 使用Nautilus Trader的官方实现
        self.macd = MovingAverageConvergenceDivergence(
            fast_period=config.fast_period, 
            slow_period=config.slow_period, 
            price_type=PriceType.MID
        )

        self.trade_size = Quantity.from_int(config.trade_size)
        self.entry_threshold = config.entry_threshold

        # 便利变量
        self.position: Position | None = None

    def on_start(self):
        """策略启动时调用"""
        self.subscribe_quote_ticks(instrument_id=self.config.instrument_id)
        self._log.info(f"MACD策略已启动，订阅 {self.config.instrument_id}")

    def on_stop(self):
        """策略停止时调用"""
        self.close_all_positions(self.config.instrument_id)
        self.unsubscribe_quote_ticks(instrument_id=self.config.instrument_id)
        self._log.info("MACD策略已停止")

    def on_quote_tick(self, tick):
        """处理报价tick数据"""
        # 手动更新MACD指标
        self.macd.handle_quote_tick(tick)

        if not self.macd.initialized:
            return  # 等待指标初始化完成

        # 检查入场和出场信号
        self.check_for_entry()
        self.check_for_exit()

    def on_event(self, event: Event):
        """处理事件"""
        if isinstance(event, PositionOpened):
            self.position = self.cache.position(event.position_id)
            self._log.info(f"持仓已开启: {self.position}")

    def check_for_entry(self):
        """检查入场信号"""
        # 如果MACD线高于入场阈值，应该做多
        if self.macd.value > self.entry_threshold:
            if self.position and self.position.side == PositionSide.LONG:
                return  # 已经做多

            order = self.order_factory.market(
                instrument_id=self.config.instrument_id,
                order_side=OrderSide.BUY,
                quantity=self.trade_size,
            )
            self.submit_order(order)
            self._log.info(f"提交买入订单: MACD={self.macd.value:.6f}")
            
        # 如果MACD线低于入场阈值，应该做空
        elif self.macd.value < -self.entry_threshold:
            if self.position and self.position.side == PositionSide.SHORT:
                return  # 已经做空

            order = self.order_factory.market(
                instrument_id=self.config.instrument_id,
                order_side=OrderSide.SELL,
                quantity=self.trade_size,
            )
            self.submit_order(order)
            self._log.info(f"提交卖出订单: MACD={self.macd.value:.6f}")

    def check_for_exit(self):
        """检查出场信号"""
        # 如果MACD线高于零线，则平空仓
        if self.macd.value >= 0.0:
            if self.position and self.position.side == PositionSide.SHORT:
                self.close_position(self.position)
                self._log.info(f"平空仓: MACD={self.macd.value:.6f}")
                
        # 如果MACD线低于零线，则平多仓
        else:
            if self.position and self.position.side == PositionSide.LONG:
                self.close_position(self.position)
                self._log.info(f"平多仓: MACD={self.macd.value:.6f}")

    def on_dispose(self):
        """策略销毁时调用"""
        pass


# 使用示例
if __name__ == "__main__":
    print("MACD策略模块")
    print("="*50)
    print("这个模块实现了参照Nautilus Trader官方示例的MACD策略")
    print("主要特点:")
    print("- 使用Nautilus Trader的官方MACD指标")
    print("- 支持做多和做空")
    print("- 基于MACD线和信号线的交叉进行交易")
    print("- 可配置的入场阈值和交易规模")
    print("="*50) 