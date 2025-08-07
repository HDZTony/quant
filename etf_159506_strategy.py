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
        self.macd_history = deque(maxlen=1000)  # 存储MACD值（DIF）
        self.signal_history = deque(maxlen=1000)  # 存储信号线值（DEA）
        self.histogram_history = deque(maxlen=1000)  # 存储柱状图值
        
        # 策略参数
        self.stop_loss_pct = config.stop_loss_pct
        self.take_profit_pct = config.take_profit_pct if hasattr(config, 'take_profit_pct') else 0.05
        self.lookback_period = config.lookback_period  # 添加lookback_period参数
        
        # 交易状态
        self.last_signal = None  # 记录上一次信号类型
        self.signal_confirmation_bars = 3  # 信号确认K线数
        
        # 交易信号记录
        self.trade_signals = []
        
        # 满仓状态管理
        self.simulated_position_quantity = 0  # 模拟持仓数量
        self.is_full_position_mode = True  # 满仓模式标志

    def on_start(self):
        """策略启动时调用"""
        bar_type = self.config.bar_type
        self.subscribe_bars(bar_type)
        self._log.info(f"ETF159506 MACD金叉死叉策略已启动，订阅 {self.config.instrument_id} 的 {bar_type}")
        
        # 设置初始状态：默认满仓股票状态
        self._log.info("设置初始状态：默认满仓股票状态")
        self.initial_position_set = False  # 标记是否需要处理初始持仓

    def on_stop(self):
        """策略停止时调用"""
        if self.position and self.position.quantity.as_double() > 0:
            self.close_all_positions(self.config.instrument_id)
        self.unsubscribe_bars(self.config.bar_type)
        
        # 保存交易信号到策略实例变量中，供回测系统获取
        if hasattr(self, 'trade_signals') and self.trade_signals:
            # 将交易信号保存到策略实例中，这样回测系统可以获取到
            if not hasattr(self, '_saved_trade_signals'):
                self._saved_trade_signals = []
            self._saved_trade_signals.extend(self.trade_signals)
            self._log.info(f"策略停止时保存了 {len(self.trade_signals)} 个交易信号")
        
        self._log.info("ETF159506 MACD金叉死叉策略已停止")

    def on_bar(self, bar: Bar):
        """处理K线数据"""
        # 更新MACD指标
        self.macd.handle_bar(bar)
        
        # 添加调试信息
        self._log.info(f"处理K线: 时间={pd.to_datetime(bar.ts_event, unit='ns')}, 价格={bar.close.as_double():.4f}, MACD初始化状态={self.macd.initialized}")
        
        # 检查是否需要处理初始持仓（默认满仓股票状态）
        if not hasattr(self, 'initial_position_set') or not self.initial_position_set:
            self._log.info("处理初始满仓状态：记录满仓持仓...")
            self.execute_initial_position_setup(bar)
            self.initial_position_set = True
            return  # 初始设置后，跳过本次信号检测
        
        if not self.macd.initialized:
            self._log.info("MACD指标未初始化，跳过信号检测")
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
        # 添加调试信息
        self._log.info(f"检查MACD信号: macd_history长度={len(self.macd_history)}, signal_history长度={len(self.signal_history)}")
        
        # 使用lookback_period参数而不是硬编码的值
        if len(self.macd_history) < self.lookback_period or len(self.signal_history) < self.lookback_period:
            self._log.info(f"历史数据不足，跳过信号检测: macd_history={len(self.macd_history)}, signal_history={len(self.signal_history)}, 需要{self.lookback_period}根K线")
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
            
            # 检查当前持仓状态，决定是否记录买入信号
            # 在满仓模式下，优先使用模拟持仓状态
            has_position = (self.position and self.position.quantity.as_double() > 0) or self.simulated_position_quantity > 0
            
            if has_position:
                # 已有持仓，记录"持有"信号而不是"买入"信号
                current_quantity = self.position.quantity.as_double() if self.position else self.simulated_position_quantity
                hold_signal = {
                    'timestamp': pd.to_datetime(bar.ts_event, unit='ns'),
                    'price': bar.close.as_double(),
                    'side': 'HOLD',  # 改为HOLD表示持有
                    'quantity': current_quantity,
                    'order_id': 'signal_detected',
                    'signal_type': 'golden_cross_hold'
                }
                self.trade_signals.append(hold_signal)
                self._log.info(f"记录持有信号（金叉但已有持仓）: {hold_signal}")
            else:
                # 没有持仓，记录买入信号
                buy_signal = {
                    'timestamp': pd.to_datetime(bar.ts_event, unit='ns'),
                    'price': bar.close.as_double(),
                    'side': 'BUY',
                    'quantity': 0,  # 暂时设为0，执行时再更新
                    'order_id': 'signal_detected',
                    'signal_type': 'golden_cross'
                }
                self.trade_signals.append(buy_signal)
                self._log.info(f"记录买入信号时间: {buy_signal}")
            
            self.execute_buy_signal(bar)
        
        elif death_cross:
            self._log.info(f"检测到死叉信号: MACD={current_macd:.6f}, Signal={current_signal:.6f}")
            self.last_signal = "death_cross"
            
            # 检查当前持仓状态，决定是否记录卖出信号
            # 在满仓模式下，优先使用模拟持仓状态
            has_position = (self.position and self.position.quantity.as_double() > 0) or self.simulated_position_quantity > 0
            
            if has_position:
                # 有持仓，记录卖出信号
                current_quantity = self.position.quantity.as_double() if self.position else self.simulated_position_quantity
                sell_signal = {
                    'timestamp': pd.to_datetime(bar.ts_event, unit='ns'),
                    'price': bar.close.as_double(),
                    'side': 'SELL',
                    'quantity': current_quantity,
                    'order_id': 'signal_detected',
                    'signal_type': 'death_cross'
                }
                self.trade_signals.append(sell_signal)
                self._log.info(f"记录卖出信号时间: {sell_signal}")
            else:
                # 没有持仓，记录"观望"信号
                watch_signal = {
                    'timestamp': pd.to_datetime(bar.ts_event, unit='ns'),
                    'price': bar.close.as_double(),
                    'side': 'WATCH',  # 改为WATCH表示观望
                    'quantity': 0,
                    'order_id': 'signal_detected',
                    'signal_type': 'death_cross_watch'
                }
                self.trade_signals.append(watch_signal)
                self._log.info(f"记录观望信号（死叉但无持仓）: {watch_signal}")
            
            self.execute_sell_signal(bar)
    
    def execute_initial_position_setup(self, bar: Bar):
        """执行初始持仓设置（默认满仓股票状态）"""
        self._log.info("开始执行初始满仓状态设置...")
        
        # 模拟初始满仓状态：假设有230000元价值的股票
        initial_capital = 230000  # 初始资金
        current_price = bar.close.as_double()
        self.simulated_position_quantity = int(initial_capital / current_price)
        
        self._log.info(f"设置初始满仓状态: {self.simulated_position_quantity} 股 (价值约{initial_capital}元)")
        
        # 记录初始满仓状态（不创建实际订单，因为初始状态就是满仓）
        initial_position_signal = {
            'timestamp': pd.to_datetime(bar.ts_event, unit='ns'),
            'price': bar.close.as_double(),
            'side': 'HOLD',  # 改为HOLD表示持有状态
            'quantity': self.simulated_position_quantity,
            'order_id': 'initial_position_hold',
            'signal_type': 'initial_position_hold'
        }
        self.trade_signals.append(initial_position_signal)
        
        # 不创建实际的订单，因为初始状态就是满仓
        # 只是记录这个状态，等待后续的MACD信号来决定买卖
        self._log.info(f"记录初始满仓状态: {self.simulated_position_quantity} 股，等待MACD信号")
        self._log.info(f"记录初始持仓信号: {initial_position_signal}")
    
    def execute_buy_signal(self, bar: Bar):
        """执行买入信号"""
        # 由于默认满仓状态，买入信号通常被跳过
        # 只有在特殊情况下（如止损后）才可能执行买入
        has_position = (self.position and self.position.quantity.as_double() > 0) or self.simulated_position_quantity > 0
        if has_position:
            self._log.info("已有持仓（默认满仓状态），跳过买入信号")
            return
        
        # 计算交易数量
        if self.trade_size is None:
            account = self.cache.account_for_venue(self.config.venue)
            available_balance = account.balance_total().as_double()
            current_price = bar.close.as_double()
            
            # 检查可用余额
            if available_balance <= 0:
                self._log.info(f"可用余额不足: {available_balance:.2f} CNY，跳过买入信号")
                return
                
            quantity = int(available_balance / current_price)  # 使用100%资金满仓交易
            
            # 检查计算出的数量是否有效
            if quantity <= 0:
                self._log.info(f"计算出的交易数量无效: {quantity}，跳过买入信号")
                return
                
            trade_quantity = Quantity.from_int(quantity)
        else:
            trade_quantity = self.trade_size

        order = self.order_factory.market(
            instrument_id=self.config.instrument_id,
            order_side=OrderSide.BUY,
            quantity=trade_quantity,
        )
        self.submit_order(order)
        
        # 更新最近的买入信号记录（如果有的话）
        for signal in reversed(self.trade_signals):
            if signal.get('signal_type') == 'golden_cross' and signal.get('side') == 'BUY':
                signal['quantity'] = trade_quantity.as_double()
                signal['order_id'] = str(order.client_order_id)
                break
        
        self._log.info(f"金叉买入信号: 数量={trade_quantity}, 价格={bar.close.as_double():.4f}")
        self._log.info(f"更新买入信号记录: 数量={trade_quantity.as_double()}")
    
    def execute_sell_signal(self, bar: Bar):
        """执行卖出信号"""
        # 由于默认满仓状态，卖出信号应该能够正常执行
        has_position = (self.position and self.position.quantity.as_double() > 0) or self.simulated_position_quantity > 0
        if not has_position:
            self._log.info("没有持仓，跳过卖出信号")
            return
        
        # 更新最近的卖出信号记录（如果有的话）
        for signal in reversed(self.trade_signals):
            if signal.get('signal_type') == 'death_cross' and signal.get('side') == 'SELL':
                signal['quantity'] = self.position.quantity.as_double() if self.position else self.simulated_position_quantity
                signal['order_id'] = 'close_position'
                break
        
        # 更新模拟持仓状态
        if self.simulated_position_quantity > 0:
            self.simulated_position_quantity = 0
            self._log.info("卖出后清空模拟持仓状态")
        
        # 执行平仓操作
        if self.position:
            # 有实际持仓，执行平仓
            self.close_position(self.position)
            self._log.info(f"死叉卖出信号: 价格={bar.close.as_double():.4f}, 数量={self.position.quantity.as_double()}")
        else:
            # 只有模拟持仓，记录卖出但不执行实际订单（因为初始状态就是满仓）
            self._log.info(f"死叉卖出信号（模拟持仓）: 价格={bar.close.as_double():.4f}, 数量={self.simulated_position_quantity}")
            # 这里不执行实际订单，因为初始状态就是满仓，只是记录卖出信号
    
    def check_risk_management(self, bar: Bar):
        """检查风险管理"""
        # 由于默认满仓状态，风险管理对持仓很重要
        has_position = (self.position and self.position.quantity.as_double() > 0) or self.simulated_position_quantity > 0
        if not has_position:
            return
        
        # 只有在有实际持仓时才进行风险管理
        if not self.position:
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