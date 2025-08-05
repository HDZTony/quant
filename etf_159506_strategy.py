#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
159506 港股通医疗ETF富国策略实现
基于MACD金叉死叉的简单交易策略
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
from nautilus_trader.model.data import Bar
from collections import deque
import pandas as pd

from etf_159506_strategy_config import ETF159506Config


class ETF159506Strategy(Strategy):
    """
    159506 港股通医疗ETF富国交易策略
    基于MACD金叉死叉的简单交易策略
    """
    
    def __init__(self, config: ETF159506Config):
        super().__init__(config=config)
        
        # MACD指标 - 使用Nautilus Trader的官方实现
        self.macd = MovingAverageConvergenceDivergence(
            fast_period=config.fast_ema_period,
            slow_period=config.slow_ema_period,
            price_type=PriceType.MID
        )
        
        # 交易数量设置
        if config.trade_size == 0:
            self.trade_size = None
        else:
            self.trade_size = Quantity.from_int(int(config.trade_size))

        # 便利变量
        self.position: Position | None = None
        
        # 历史数据存储 - 用于检测金叉死叉
        self.macd_history = deque(maxlen=10)  # 存储MACD值（DIF）
        self.signal_history = deque(maxlen=10)  # 存储信号线值（DEA）
        self.histogram_history = deque(maxlen=10)  # 存储柱状图值
        
        # 策略参数
        self.stop_loss_pct = config.stop_loss_pct
        self.take_profit_pct = config.take_profit_pct if hasattr(config, 'take_profit_pct') else 0.05
        
        # 交易状态
        self.last_signal = None  # 记录上一次信号类型
        self.signal_confirmation_bars = 2  # 信号确认K线数

    def on_start(self):
        """策略启动时调用"""
        bar_type = self.config.bar_type
        self.subscribe_bars(bar_type)
        self._log.info(f"ETF159506 MACD金叉死叉策略已启动，订阅 {self.config.instrument_id} 的 {bar_type}")

    def on_stop(self):
        """策略停止时调用"""
        if self.position and self.position.quantity.as_double() > 0:
            self.close_all_positions(self.config.instrument_id)
        self.unsubscribe_bars(self.config.bar_type)
        self._log.info("ETF159506 MACD金叉死叉策略已停止")

    def on_bar(self, bar: Bar):
        """处理K线数据"""
        # 更新MACD指标
        self.macd.handle_bar(bar)
        
        if not self.macd.initialized:
            return

        # 更新历史数据
        self.update_history_data(bar)
        
        # 检查金叉死叉信号
        self.check_macd_signals(bar)
        
        # 检查止损止盈
        self.check_risk_management(bar)

    def on_event(self, event: Event):
        """处理事件"""
        if isinstance(event, PositionOpened):
            self.position = self.cache.position(event.position_id)
            self._log.info(f"持仓已开启: {self.position}")
    
    def update_history_data(self, bar: Bar):
        """更新历史数据"""
        # 获取MACD值（DIF）
        macd_value = self.macd.value  # 这是DIF值
        
        # 计算信号线（DEA）- 对DIF的EMA
        self.macd_history.append(macd_value)
        signal_value = self.calculate_signal_line()
        
        # 计算柱状图（MACD柱）
        histogram_value = macd_value - signal_value
        
        # 添加到历史数据
        self.signal_history.append(signal_value)
        self.histogram_history.append(histogram_value)
        
        # 记录当前指标值
        self._log.info(f"MACD(DIF): {macd_value:.6f}, Signal(DEA): {signal_value:.6f}, Histogram: {histogram_value:.6f}")
    
    def calculate_signal_line(self):
        """计算信号线（DEA）- 对DIF的9周期EMA"""
        if len(self.macd_history) < 9:
            return 0.0
        
        # 使用pandas的ewm计算EMA
        dif_series = pd.Series(list(self.macd_history))
        dea = dif_series.ewm(span=9, adjust=False).mean().iloc[-1]
        return dea
    
    def check_macd_signals(self, bar: Bar):
        """检查MACD金叉死叉信号"""
        if len(self.macd_history) < 3 or len(self.signal_history) < 3:
            return
        
        current_macd = self.macd_history[-1]
        previous_macd = self.macd_history[-2]
        current_signal = self.signal_history[-1]
        previous_signal = self.signal_history[-2]
        
        # 检测金叉：MACD线从下方向上穿越信号线
        golden_cross = (previous_macd < previous_signal and current_macd > current_signal)
        
        # 检测死叉：MACD线从上方向下穿越信号线
        death_cross = (previous_macd > previous_signal and current_macd < current_signal)
        
        # 记录信号
        if golden_cross:
            self._log.info(f"检测到金叉信号: MACD={current_macd:.6f}, Signal={current_signal:.6f}")
            self.last_signal = "golden_cross"
            self.execute_buy_signal(bar)
        
        elif death_cross:
            self._log.info(f"检测到死叉信号: MACD={current_macd:.6f}, Signal={current_signal:.6f}")
            self.last_signal = "death_cross"
            self.execute_sell_signal(bar)
    
    def execute_buy_signal(self, bar: Bar):
        """执行买入信号"""
        if self.position and self.position.quantity.as_double() > 0:
            self._log.info("已有持仓，跳过买入信号")
            return
        
        # 计算交易数量
        if self.trade_size is None:
            account = self.cache.account_for_venue(self.config.venue)
            available_balance = account.balance_total().as_double()
            current_price = bar.close.as_double()
            quantity = int(available_balance * 0.95 / current_price)  # 使用95%资金
            trade_quantity = Quantity.from_int(quantity)
        else:
            trade_quantity = self.trade_size

        order = self.order_factory.market(
            instrument_id=self.config.instrument_id,
            order_side=OrderSide.BUY,
            quantity=trade_quantity,
        )
        self.submit_order(order)
        self._log.info(f"金叉买入信号: 数量={trade_quantity}, 价格={bar.close.as_double():.4f}")
    
    def execute_sell_signal(self, bar: Bar):
        """执行卖出信号"""
        if not self.position or self.position.quantity.as_double() <= 0:
            self._log.info("没有持仓，跳过卖出信号")
            return
        
        self.close_position(self.position)
        self._log.info(f"死叉卖出信号: 价格={bar.close.as_double():.4f}")
    
    def check_risk_management(self, bar: Bar):
        """检查风险管理"""
        if not self.position or self.position.quantity.as_double() <= 0:
            return
        
        current_price = bar.close.as_double()
        entry_price = self.position.avg_px_open
        
        # 计算盈亏百分比
        pnl_pct = (current_price - entry_price) / entry_price
        
        # 止损检查
        if pnl_pct <= -self.stop_loss_pct:
            self.close_position(self.position)
            self._log.info(f"触发止损: 亏损{pnl_pct*100:.2f}%")
        
        # 止盈检查
        elif pnl_pct >= self.take_profit_pct:
            self.close_position(self.position)
            self._log.info(f"触发止盈: 盈利{pnl_pct*100:.2f}%")

    def on_dispose(self):
        """策略销毁时调用"""
        self._log.info("ETF159506 MACD金叉死叉策略已销毁") 