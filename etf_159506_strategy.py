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
from nautilus_trader.model.events import PositionOpened, PositionChanged, PositionClosed, OrderFilled
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
        
        # 添加调试信息
        self._log.info(f"策略初始化: instrument_id={config.instrument_id}, bar_type={config.bar_type}")
        self._log.info(f"MACD参数: fast_period={config.fast_ema_period}, slow_period={config.slow_ema_period}")
        self._log.info(f"风险参数: stop_loss_pct={config.stop_loss_pct}, take_profit_pct={getattr(config, 'take_profit_pct', 0.05)}")
        self._log.info(f"其他参数: lookback_period={config.lookback_period}")
        self._log.info(f"背离检测参数: dea_trend_period={config.dea_trend_period}, divergence_threshold={config.divergence_threshold}")
        self._log.info(f"背离确认参数: confirmation_bars={config.confirmation_bars}, advance_trading_bars={config.advance_trading_bars}")
        
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
        
        # 背离检测相关历史数据存储
        self.price_history = deque(maxlen=1000)  # 存储收盘价历史
        self.price_timestamps = deque(maxlen=1000)  # 存储价格对应的时间戳
        self.macd_timestamps = deque(maxlen=1000)  # 存储MACD对应的时间戳
        self.divergence_lookback = 30  # 背离检测的回看周期
        self.divergence_confirmation_bars = 3  # 背离确认所需的K线数
        self.last_divergence_signal = None  # 记录上一次背离信号类型
        
        # 背离检测参数（从配置中获取）
        self.dea_trend_period = config.dea_trend_period
        self.divergence_threshold = config.divergence_threshold
        self.advance_trading_bars = config.advance_trading_bars
        self.confirmation_bars = config.confirmation_bars
        self.max_divergence_duration = config.max_divergence_duration
        
        # 背离检测改进：记录历史极值点
        self.price_peaks = deque(maxlen=config.max_extremes)  # 存储价格峰值点 (timestamp, price, dif_value)
        self.price_troughs = deque(maxlen=config.max_extremes)  # 存储价格谷值点 (timestamp, price, dif_value)
        self.dif_peaks = deque(maxlen=config.max_extremes)  # 存储DIF峰值点 (timestamp, dif_value, price_value)
        self.dif_troughs = deque(maxlen=config.max_extremes)  # 存储DIF谷值点 (timestamp, dif_value, price_value)
        self.divergence_lookback_peaks = 3  # 回看过去几个极值点来检测背离
        
        # 按时间排序的所有极值点列表（用于比较上一个极值点）
        self.all_extremes = []
        
        # 技术指标信号累积系统
        self.technical_signal = 0  # 技术指标信号累积值，+100买入，-100卖出
        self.buy_threshold = 100   # 买入信号阈值
        self.sell_threshold = -100 # 卖出信号阈值
        
        # 记录极值点检测参数
        self._log.info(f"极值点检测参数: 回看极值点数量={self.divergence_lookback_peaks}, 最大极值点数量={config.max_extremes}")
        self._log.info(f"DIF信号过滤: 阈值={abs(self.divergence_threshold):.6f} (过滤DIF绝对值小于此值的金叉死叉和背离信号)")
        self._log.info(f"技术指标信号系统: 买入信号+30, 卖出信号-30, 阈值±100, 执行后归零")
        self._log.info(f"极值点检测: 使用简单的前后点比较方法，无需时间窗口和曲率检测")
        
        # 策略参数
        self.stop_loss_pct = config.stop_loss_pct
        self.take_profit_pct = config.take_profit_pct if hasattr(config, 'take_profit_pct') else 0.99
        self.lookback_period = config.lookback_period
        
        # 交易状态
        self.last_signal = None  # 记录上一次信号类型
        self.signal_confirmation_bars = 3  # 信号确认K线数
        
        # 交易信号记录
        self.trade_signals = []
        

    def on_start(self):
        """策略启动时调用"""
        bar_type = self.config.bar_type
        self.subscribe_bars(bar_type)
        self._log.info(f"ETF159506 MACD金叉死叉策略已启动，订阅 {self.config.instrument_id} 的 {bar_type}")
        
        # 策略始终空仓开始
        self._log.info("策略配置为空仓开始，等待交易信号")
        
        # 检查初始持仓状态
        initial_position = self.get_current_position()
        if initial_position:
            self._log.info(f"检测到初始持仓: {initial_position.quantity.as_double()} 股")
        else:
            self._log.info("确认初始状态：无持仓")

    def on_stop(self):
        """策略停止时调用"""
        current_position = self.get_current_position()
        if current_position is not None:
            self.close_all_positions(self.config.instrument_id)
            self._log.info(f"策略停止时关闭持仓: {current_position.quantity.as_double()} 股")
        self.unsubscribe_bars(self.config.bar_type)
        
        # 保存交易信号到策略实例变量中，供回测系统获取
        if hasattr(self, 'trade_signals') and self.trade_signals:
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
        
        
        # 无论MACD是否初始化，都计算图表MACD值
        chart_macd = self.calculate_chart_macd(bar)
        
        # 根据MACD初始化状态选择数据源
        if not self.macd.initialized:
            self._log.info(f"MACD指标未初始化，当前数据点: {len(self.macd_history)}")
            self._log.info(f"使用图表MACD: DIF={chart_macd['macd']:.6f}, DEA={chart_macd['signal']:.6f}, Histogram={chart_macd['histogram']:.6f}")
            
            # 使用图表MACD值
            self.macd_history.append(chart_macd['macd'])
            self.signal_history.append(chart_macd['signal'])
            self.histogram_history.append(chart_macd['histogram'])
            
            # 更新价格历史数据
            self.price_history.append(bar.close.as_double())
            self.price_timestamps.append(bar.ts_event)
            self.macd_timestamps.append(bar.ts_event)
        else:
            self._log.info(f"MACD指标已初始化，使用官方指标值")
            
            # 使用官方MACD值更新历史数据
            self.update_history_data(bar)
        
        # 统一的极值点检测（背离检测会在deque增加时自动执行）
        self.detect_and_record_extremes(bar)
        
        # 统一的交易信号检测和风险管理
        self.check_macd_signals(bar)
        self.check_risk_management(bar)
        
        # 定期监控持仓状态（每10个K线记录一次）
        if len(self.macd_history) % 10 == 0:
            current_position = self.get_current_position()
            if current_position:
                self._log.info(f"持仓状态监控: {current_position.quantity.as_double()} 股, 成本: {current_position.avg_px_open:.4f}")
            else:
                self._log.info("持仓状态监控: 无持仓")

    def on_event(self, event: Event):
        """处理所有事件"""
        pass  # 通用事件处理，具体事件由专门方法处理
    
    def on_position_opened(self, event: PositionOpened) -> None:
        """持仓开启事件处理"""
        self.position = self.cache.position(event.position_id)
        self._log.info(f"持仓已开启: {self.position}")
    
    def on_position_changed(self, event: PositionChanged) -> None:
        """持仓变化事件处理"""
        self.position = self.cache.position(event.position_id)
        self._log.info(f"持仓已变化: {self.position}")
    
    def on_position_closed(self, event: PositionClosed) -> None:
        """持仓关闭事件处理"""
        self.position = None
        self._log.info("持仓已关闭")
    
    def on_order_filled(self, event: OrderFilled) -> None:
        """订单成交事件处理"""
        self._log.info(f"订单成交: {event.client_order_id}, 数量: {event.last_qty}, 价格: {event.last_px}")
        # 订单成交后，持仓状态会在下一个持仓事件中更新
    
    def setup_initial_position(self, bar: Bar):
        """设置初始持仓 - 已禁用，策略始终空仓开始"""
        self._log.info("初始持仓设置已禁用，策略始终空仓开始")
        return
    
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
        
        # 更新价格历史数据（用于背离检测）
        self.price_history.append(bar.close.as_double())
        self.price_timestamps.append(bar.ts_event)
        self.macd_timestamps.append(bar.ts_event)
                
        # 记录当前指标值
        self._log.info(f"DIF: {macd_value:.6f}, DEA: {signal_value:.6f}, MACD柱: {histogram_value:.6f}")
    
    def calculate_chart_macd(self, bar: Bar):
        """计算图表风格的MACD值，弥补前26分钟的空白"""
        # 获取当前价格
        current_price = bar.close.as_double()
        
        # 如果MACD指标已初始化，使用官方指标值
        if self.macd.initialized:
            return {
                'macd': self.macd.value,  # DIF
                'signal': self.calculate_signal_line(),  # DEA
                'histogram': self.macd.value - self.calculate_signal_line()  # MACD柱
            }
        
        # 如果MACD指标未初始化，使用图表方法计算
        # 收集所有历史价格数据
        if not hasattr(self, '_price_history'):
            self._price_history = []
        
        self._price_history.append(current_price)
        
        # 使用pandas的ewm计算，即使数据不足也能计算
        price_series = pd.Series(self._price_history)
        
        # 计算EMA12和EMA26
        ema12 = price_series.ewm(span=12, adjust=False).mean()
        ema26 = price_series.ewm(span=26, adjust=False).mean()
        
        # 计算DIF (MACD线)
        dif = ema12.iloc[-1] - ema26.iloc[-1]
        
        # 计算DEA (信号线) - 对DIF的9周期EMA
        if len(self._price_history) >= 2:
            # 如果有DIF历史，计算DEA
            if not hasattr(self, '_dif_history'):
                self._dif_history = []
            self._dif_history.append(dif)
            
            dif_series = pd.Series(self._dif_history)
            span = min(9, len(dif_series))
            dea = dif_series.ewm(span=span, adjust=False).mean().iloc[-1]
        else:
            # 数据不足时，DEA = DIF
            dea = dif
        
        # 计算MACD柱
        histogram = 2 * (dif - dea)
        
        # 记录图表MACD计算
        self._log.info(f"图表MACD计算: 价格={current_price:.4f}, DIF={dif:.6f}, DEA={dea:.6f}, MACD柱={histogram:.6f}")
        
        return {
            'macd': dif,      # DIF
            'signal': dea,    # DEA
            'histogram': histogram  # MACD柱
        }
    
    def calculate_signal_line(self):
        """计算信号线（DEA）- 对DIF的9周期EMA"""
        if len(self.macd_history) < 2:
            # 如果数据不足，返回当前MACD值作为信号线
            return self.macd_history[-1] if self.macd_history else 0.0
        
        # 使用pandas的ewm计算EMA，span=9是标准MACD参数
        dif_series = pd.Series(list(self.macd_history))
        # 如果数据不足9个，使用所有可用数据
        span = min(9, len(dif_series))
        dea = dif_series.ewm(span=span, adjust=False).mean().iloc[-1]
        return dea
    
    def check_macd_signals(self, bar: Bar):
        """检查MACD金叉死叉信号"""
        # 添加调试信息
        self._log.info(f"检查MACD信号: macd_history长度={len(self.macd_history)}, signal_history长度={len(self.signal_history)}")
        
        # 需要至少2个数据点来检测金叉死叉
        if len(self.macd_history) < 2 or len(self.signal_history) < 2:
            self._log.info(f"历史数据不足，跳过信号检测: macd_history={len(self.macd_history)}, signal_history={len(self.signal_history)}, 需要至少2个数据点")
            return
        
        current_macd = self.macd_history[-1]
        previous_macd = self.macd_history[-2]
        current_signal = self.signal_history[-1]
        previous_signal = self.signal_history[-2]
        
        # 检测金叉：MACD线从下方向上穿越信号线
        golden_cross = (previous_macd < previous_signal and current_macd > current_signal)
        
        # 检测死叉：MACD线从上方向下穿越信号线
        death_cross = (previous_macd > previous_signal and current_macd < current_signal)
        
        # 检查MACD值是否足够大（过滤小波动）
        macd_threshold = abs(self.divergence_threshold)
        current_macd_abs = abs(current_macd)
        
        # 记录信号
        if golden_cross:
            self._log.info(f"检测到金叉信号: MACD={current_macd:.6f}, Signal={current_signal:.6f}")
            
            # 检查MACD值是否足够大
            if current_macd_abs < macd_threshold:
                self._log.info(f"金叉信号被过滤: MACD绝对值{current_macd_abs:.6f} < 阈值{macd_threshold:.6f}")
                return
            
            self.last_signal = "golden_cross"
            
            # 累积买入信号
            self.technical_signal += 30
            self._log.info(f"金叉买入信号累积: 当前信号值={self.technical_signal}")
            
            # 检查当前持仓状态
            current_position = self.get_current_position()
            has_position = current_position is not None
            
            if has_position:
                # 已有持仓，记录"持有"信号
                current_quantity = current_position.quantity.as_double()
                hold_signal = {
                    'timestamp': pd.to_datetime(bar.ts_event, unit='ns'),
                    'price': bar.close.as_double(),
                    'side': 'HOLD',
                    'quantity': current_quantity,
                    'order_id': 'signal_detected',
                    'signal_type': 'golden_cross_hold',
                    'signal_value': self.technical_signal
                }
                self.trade_signals.append(hold_signal)
                self._log.info(f"记录持有信号（金叉但已有持仓）: {hold_signal}")
            else:
                # 没有持仓，记录买入信号
                buy_signal = {
                    'timestamp': pd.to_datetime(bar.ts_event, unit='ns'),
                    'price': bar.close.as_double(),
                    'side': 'BUY',
                    'quantity': 0,
                    'order_id': 'signal_detected',
                    'signal_type': 'golden_cross',
                    'signal_value': self.technical_signal
                }
                self.trade_signals.append(buy_signal)
                self._log.info(f"记录买入信号时间: {buy_signal}")
            
            # 检查是否达到买入阈值
            if self.technical_signal >= self.buy_threshold:
                self._log.info(f"金叉买入信号达到阈值{self.buy_threshold}，执行买入操作")
                self.execute_buy_signal(bar)
                self.technical_signal = 0  # 信号归零
        
        elif death_cross:
            self._log.info(f"检测到死叉信号: MACD={current_macd:.6f}, Signal={current_signal:.6f}")
            
            # 检查MACD值是否足够大
            if current_macd_abs < macd_threshold:
                self._log.info(f"死叉信号被过滤: MACD绝对值{current_macd_abs:.6f} < 阈值{macd_threshold:.6f}")
                return
            
            self.last_signal = "death_cross"
            
            # 累积卖出信号
            self.technical_signal -= 30
            self._log.info(f"死叉卖出信号累积: 当前信号值={self.technical_signal}")
            
            # 检查当前持仓状态
            current_position = self.get_current_position()
            has_position = current_position is not None
            
            if has_position:
                # 有持仓，记录卖出信号
                current_quantity = current_position.quantity.as_double()
                sell_signal = {
                    'timestamp': pd.to_datetime(bar.ts_event, unit='ns'),
                    'price': bar.close.as_double(),
                    'side': 'SELL',
                    'quantity': current_quantity,
                    'order_id': 'signal_detected',
                    'signal_type': 'death_cross',
                    'signal_value': self.technical_signal
                }
                self.trade_signals.append(sell_signal)
                self._log.info(f"记录卖出信号时间: {sell_signal}")
            else:
                # 没有持仓，记录"观望"信号
                watch_signal = {
                    'timestamp': pd.to_datetime(bar.ts_event, unit='ns'),
                    'price': bar.close.as_double(),
                    'side': 'WATCH',
                    'quantity': 0,
                    'order_id': 'signal_detected',
                    'signal_type': 'death_cross_watch',
                    'signal_value': self.technical_signal
                }
                self.trade_signals.append(watch_signal)
                self._log.info(f"记录观望信号（死叉但无持仓）: {watch_signal}")
            
            # 检查是否达到卖出阈值
            if self.technical_signal <= self.sell_threshold:
                self._log.info(f"死叉卖出信号达到阈值{self.sell_threshold}，执行卖出操作")
                self.execute_sell_signal(bar)
                self.technical_signal = 0  # 信号归零
    
    def check_divergence(self, bar: Bar):
        """检查DIF背离信号"""
        # 需要足够的极值点来检测背离
        if len(self.price_peaks) < 2 or len(self.price_troughs) < 2 or len(self.dif_peaks) < 2 or len(self.dif_troughs) < 2:
            return
        
        # 检测顶背离和底背离
        top_divergence = self.detect_top_divergence()
        bottom_divergence = self.detect_bottom_divergence()
        
        # 处理背离信号
        if top_divergence:
            self.handle_top_divergence(bar)
        elif bottom_divergence:
            self.handle_bottom_divergence(bar)
    
    def detect_top_divergence(self):
        """检测顶背离：在DIF新高点判断价格变化"""
        # 需要至少2个DIF峰值来检测背离
        if len(self.dif_peaks) < 2:
            return False
        
        # 获取最新的两个DIF峰值
        latest_dif_peak = self.dif_peaks[-1]
        previous_dif_peak = self.dif_peaks[-2]
        
        # 检查DIF是否创新高
        if latest_dif_peak[1] > previous_dif_peak[1]:
            # DIF创新高，检查同一时间点的价格变化
            # 使用峰值点存储的实际价格数据
            latest_price = latest_dif_peak[2]  # (timestamp, dif_value, price_value)
            previous_price = previous_dif_peak[2]
            
            # 顶背离：DIF创新高，但价格未创新高（走低或持平）
            if latest_price <= previous_price:
                # 过滤DIF值太小的背离信号
                dif_threshold = abs(self.divergence_threshold)
                if abs(latest_dif_peak[1]) >= dif_threshold:
                    self._log.info(f"顶背离检测: DIF创新高{latest_dif_peak[1]:.6f} vs {previous_dif_peak[1]:.6f}")
                    self._log.info(f"价格变化: {latest_price:.4f} vs {previous_price:.4f}")
                    self._log.info("检测到顶背离：DIF创新高但价格未创新高")
                    return True
        
        return False
    
    def detect_bottom_divergence(self):
        """检测底背离：在DIF新低点判断价格变化"""
        # 需要至少2个DIF谷值来检测背离
        if len(self.dif_troughs) < 2:
            return False
        
        # 获取最新的两个DIF谷值
        latest_dif_trough = self.dif_troughs[-1]
        previous_dif_trough = self.dif_troughs[-2]
        
        # 检查DIF是否创新低
        if latest_dif_trough[1] < previous_dif_trough[1]:
            # DIF创新低，检查同一时间点的价格变化
            # 使用峰值点存储的实际价格数据
            latest_price = latest_dif_trough[2]  # (timestamp, dif_value, price_value)
            previous_price = previous_dif_trough[2]
            
            # 底背离：DIF创新低，但价格未创新低（走高或持平）
            if latest_price >= previous_price:
                # 过滤DIF值太小的背离信号
                dif_threshold = abs(self.divergence_threshold)
                if abs(latest_dif_trough[1]) >= dif_threshold:
                    self._log.info(f"底背离检测: DIF创新低{latest_dif_trough[1]:.6f} vs {previous_dif_trough[1]:.6f}")
                    self._log.info(f"价格变化: {latest_price:.4f} vs {previous_price:.4f}")
                    self._log.info("检测到底背离：DIF创新低但价格未创新低")
                    return True
        
        return False
    
    # 移除不再使用的峰值和谷值查找方法
    
    # 移除复杂的曲率和斜率计算方法，使用简单的前后点比较
    
    # 移除不再使用的时间窗口极值检测方法
    
    def _is_relative_extreme(self, extreme_type, current_value, current_timestamp, min_extreme_distance=0.1):
        """检查当前值是否相对于上一个极值点是真正的极值
        
        规则：
        1. 如果新极值与上一个不同类型极值差异太小，则略过当前极值
        2. 如果新极值与上一个同类型极值，则保留绝对值更大的那个
        """
        # 如果没有历史数据，直接返回True
        if len(self.all_extremes) == 0:
            return True, 'keep'
        
        # 获取上一个极值点
        last_extreme = self.all_extremes[-1]
        last_extreme_type = last_extreme[2]  # 极值类型
        last_extreme_value = last_extreme[1]  # 极值数值
        
        # 如果新极值与上一个极值类型不同
        if extreme_type != last_extreme_type:
            # 计算与上一个极值的绝对差异
            diff_absolute = abs(current_value - last_extreme_value)
            
            # 如果差异太小，略过当前极值
            if diff_absolute < min_extreme_distance:
                return False, None
            
            # 差异足够大，保留新极值
            return True, 'keep'
        
        # 如果新极值与上一个极值类型相同
        else:
            # 计算与上一个极值的绝对差异
            diff_absolute = abs(current_value - last_extreme_value)
            
            # 如果差异太小，略过当前极值
            if diff_absolute < min_extreme_distance:
                return False, None
            
            # 根据极值类型决定保留策略
            if extreme_type == 'peak':
                # 峰值：保留更大的值
                if current_value > last_extreme_value:
                    return True, 'keep'  # 保留新的
                else:
                    return True, 'replace'  # 替换旧的
            else:  # extreme_type == 'trough'
                # 谷值：保留更小的值
                if current_value < last_extreme_value:
                    return True, 'keep'  # 保留新的
                else:
                    return True, 'replace'  # 替换旧的
        
        return False, None
    
    def _detect_extreme(self, data, current_index):
        """检测极值点（基于前后点比较的简单方法）"""
        if current_index < 1 or current_index >= len(data) - 1:
            return False, None
        
        # 使用简单的极值检测逻辑：比较当前点与前后点的值
        current_value = data[current_index]
        prev_value = data[current_index - 1]
        next_value = data[current_index + 1]
        
        # 检测峰值：当前值比前后值都大
        if current_value > prev_value and current_value > next_value:
            return True, 'peak'
        
        # 检测谷值：当前值比前后值都小
        elif current_value < prev_value and current_value < next_value:
            return True, 'trough'
        
        return False, None
    
    def detect_and_record_extremes(self, bar: Bar):
        """改进的极值点检测：基于简单的前后点比较和相对极值检测"""
        if len(self.price_history) < 3:
            return
        
        current_timestamp = bar.ts_event
        current_price = self.price_history[-1]
        current_macd = self.macd_history[-1]
        
        # 检测价格极值点
        price_extreme, price_type = self._detect_extreme(self.price_history, len(self.price_history) - 1)
        if price_extreme:
            # 使用新的相对极值检测逻辑
            is_extreme, action = self._is_relative_extreme(price_type, current_price, current_timestamp)
            
            if is_extreme:
                # 处理替换逻辑
                if action == 'replace':
                    # 替换上一个极值点
                    if price_type == 'peak' and len(self.price_peaks) > 0:
                        removed_peak = self.price_peaks.pop()
                        # 同时从all_extremes中移除
                        if len(self.all_extremes) > 0:
                            self.all_extremes.pop()
                        self._log.debug(f"替换价格峰值: 时间{removed_peak[0]}, 价格{removed_peak[1]:.4f} -> 时间{current_timestamp}, 价格{current_price:.4f}")
                    elif price_type == 'trough' and len(self.price_troughs) > 0:
                        removed_trough = self.price_troughs.pop()
                        # 同时从all_extremes中移除
                        if len(self.all_extremes) > 0:
                            self.all_extremes.pop()
                        self._log.debug(f"替换价格谷值: 时间{removed_trough[0]}, 价格{removed_trough[1]:.4f} -> 时间{current_timestamp}, 价格{current_price:.4f}")
                
                elif action == 'keep':
                    # 保留当前极值点，不需要替换
                    self._log.debug(f"保留价格{price_type}: 时间{current_timestamp}, 价格{current_price:.4f}")
                
                # 无论是replace还是keep，都需要添加新极值点
                if price_type == 'peak':
                    self.price_peaks.append((current_timestamp, current_price, current_macd))
                    # 同时更新all_extremes列表
                    self.all_extremes.append((current_timestamp, current_price, 'peak'))
                    self._log.debug(f"检测到新价格峰值: 时间{current_timestamp}, 价格{current_price:.4f}, DIF{current_macd:.6f}")
                else:  # price_type == 'trough'
                    self.price_troughs.append((current_timestamp, current_price, current_macd))
                    # 同时更新all_extremes列表
                    self.all_extremes.append((current_timestamp, current_price, 'trough'))
                    self._log.debug(f"检测到新价格谷值: 时间{current_timestamp}, 价格{current_price:.4f}, DIF{current_macd:.6f}")
                
                # 简化后不再需要清理极值点
            else:
                self._log.debug(f"略过价格{price_type}: 时间{current_timestamp}, 价格{current_price:.4f} (差异太小)")
        
        # 检测MACD极值点
        macd_extreme, macd_type = self._detect_extreme(self.macd_history, len(self.macd_history) - 1)
        if macd_extreme:
            # 使用新的相对极值检测逻辑
            is_extreme, action = self._is_relative_extreme(macd_type, current_macd, current_timestamp)
            
            if is_extreme:
                if action == 'replace':
                    # 替换上一个极值点
                    if macd_type == 'peak' and len(self.dif_peaks) > 0:
                        removed_peak = self.dif_peaks.pop()
                        # 同时从all_extremes中移除
                        if len(self.all_extremes) > 0:
                            self.all_extremes.pop()
                        self._log.debug(f"替换DIF峰值: 时间{removed_peak[0]}, DIF{removed_peak[1]:.6f} -> 时间{current_timestamp}, DIF{current_macd:.6f}")
                    elif macd_type == 'trough' and len(self.dif_troughs) > 0:
                        removed_trough = self.dif_troughs.pop()
                        # 同时从all_extremes中移除
                        if len(self.all_extremes) > 0:
                            self.all_extremes.pop()
                        self._log.debug(f"替换DIF谷值: 时间{removed_trough[0]}, DIF{removed_trough[1]:.6f} -> 时间{current_timestamp}, DIF{current_macd:.6f}")
                
                elif action == 'keep':
                    # 保留当前极值点，不需要替换
                    self._log.debug(f"保留DIF{macd_type}: 时间{current_timestamp}, DIF{current_macd:.6f}")
                
                # 添加新极值点
                if macd_type == 'peak':
                    self.dif_peaks.append((current_timestamp, current_macd, current_price))
                    # 同时更新all_extremes列表
                    self.all_extremes.append((current_timestamp, current_macd, 'peak'))
                    self._log.debug(f"检测到新DIF峰值: 时间{current_timestamp}, DIF{current_macd:.6f}, 价格{current_price:.4f}")
                else:  # macd_type == 'trough'
                    self.dif_troughs.append((current_timestamp, current_macd, current_price))
                    # 同时更新all_extremes列表
                    self.all_extremes.append((current_timestamp, current_macd, 'trough'))
                    self._log.debug(f"检测到新DIF谷值: 时间{current_timestamp}, DIF{current_macd:.6f}, 价格{current_price:.4f}")
                
                # 无论是replace还是keep，都需要添加新极值点
                # 简化后不再需要清理极值点
                self.check_divergence(bar)
            else:
                self._log.debug(f"略过DIF{macd_type}: 时间{current_timestamp}, DIF{current_macd:.6f} (差异太小)")
    
    def handle_top_divergence(self, bar: Bar):
        """处理顶背离信号"""
        self._log.info("检测到顶背离信号：DIF创新高但价格未创新高，看跌信号")
        self.last_divergence_signal = "top_divergence"
        
        # 累积卖出信号
        self.technical_signal -= 30
        self._log.info(f"顶背离信号累积: 当前信号值={self.technical_signal}")
        
        # 记录顶背离信号
        divergence_signal = {
            'timestamp': pd.to_datetime(bar.ts_event, unit='ns'),
            'price': bar.close.as_double(),
            'side': 'SELL',
            'quantity': 0,
            'order_id': 'divergence_signal',
            'signal_type': 'top_divergence',
            'divergence_type': 'bearish',
            'signal_value': self.technical_signal
        }
        self.trade_signals.append(divergence_signal)
        
        # 检查是否达到卖出阈值
        if self.technical_signal <= self.sell_threshold:
            self._log.info(f"顶背离卖出信号达到阈值{self.sell_threshold}，执行卖出操作")
            self.execute_divergence_sell_signal(bar)
            self.technical_signal = 0  # 信号归零
    
    def handle_bottom_divergence(self, bar: Bar):
        """处理底背离信号"""
        self._log.info("检测到底背离信号：DIF创新低但价格未创新低，看涨信号")
        self.last_divergence_signal = "bottom_divergence"
        
        # 累积买入信号
        self.technical_signal += 30
        self._log.info(f"底背离信号累积: 当前信号值={self.technical_signal}")
        
        # 记录底背离信号
        divergence_signal = {
            'timestamp': pd.to_datetime(bar.ts_event, unit='ns'),
            'price': bar.close.as_double(),
            'side': 'BUY',
            'quantity': 0,
            'order_id': 'divergence_signal',
            'signal_type': 'bottom_divergence',
            'divergence_type': 'bullish',
            'signal_value': self.technical_signal
        }
        self.trade_signals.append(divergence_signal)
        
        # 检查是否达到买入阈值
        if self.technical_signal >= self.buy_threshold:
            self._log.info(f"底背离买入信号达到阈值{self.buy_threshold}，执行买入操作")
            self.execute_divergence_buy_signal(bar)
            self.technical_signal = 0  # 信号归零
    
    def execute_buy_signal(self, bar: Bar):
        """执行买入信号"""
        # 检查是否已有持仓 - 使用可靠的持仓查询方法
        current_position = self.get_current_position()
        if current_position is not None:
            self._log.info(f"已有持仓: {current_position.quantity.as_double()} 股，跳过买入信号")
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
        
        # 更新最近的买入信号记录
        for signal in reversed(self.trade_signals):
            if signal.get('signal_type') == 'golden_cross' and signal.get('side') == 'BUY':
                signal['quantity'] = trade_quantity.as_double()
                signal['order_id'] = str(order.client_order_id)
                break
        
        self._log.info(f"金叉买入信号: 数量={trade_quantity}, 价格={bar.close.as_double():.4f}")
    
    def execute_sell_signal(self, bar: Bar):
        """执行卖出信号"""
        # 检查是否有持仓 - 使用可靠的持仓查询方法
        current_position = self.get_current_position()
        if current_position is None:
            self._log.info("没有持仓，跳过卖出信号")
            return
        
        # 更新最近的卖出信号记录
        for signal in reversed(self.trade_signals):
            if signal.get('signal_type') == 'death_cross' and signal.get('side') == 'SELL':
                signal['quantity'] = current_position.quantity.as_double()
                signal['order_id'] = 'close_position'
                break
        
        # 执行平仓操作
        self.close_position(current_position)
        self._log.info(f"死叉卖出信号: 价格={bar.close.as_double():.4f}, 数量={current_position.quantity.as_double()}")
    
    def execute_divergence_buy_signal(self, bar: Bar):
        """执行背离买入信号"""
        # 检查是否已有持仓
        current_position = self.get_current_position()
        if current_position is not None:
            self._log.info(f"已有持仓: {current_position.quantity.as_double()} 股，跳过背离买入信号")
            return
        
        # 计算交易数量
        if self.trade_size is None:
            account = self.cache.account_for_venue(self.config.venue)
            available_balance = account.balance_total().as_double()
            current_price = bar.close.as_double()
            
            if available_balance <= 0:
                self._log.info(f"可用余额不足: {available_balance:.2f} CNY，跳过背离买入信号")
                return
                
            quantity = int(available_balance / current_price)
            if quantity <= 0:
                self._log.info(f"计算出的交易数量无效: {quantity}，跳过背离买入信号")
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
        
        # 更新最近的背离买入信号记录
        for signal in reversed(self.trade_signals):
            if signal.get('signal_type') == 'bottom_divergence' and signal.get('side') == 'BUY':
                signal['quantity'] = trade_quantity.as_double()
                signal['order_id'] = str(order.client_order_id)
                break
        
        self._log.info(f"背离买入信号: 数量={trade_quantity}, 价格={bar.close.as_double():.4f}")
    
    def execute_divergence_sell_signal(self, bar: Bar):
        """执行背离卖出信号"""
        # 检查是否有持仓
        current_position = self.get_current_position()
        if current_position is None:
            self._log.info("没有持仓，跳过背离卖出信号")
            return
        
        # 更新最近的背离卖出信号记录
        for signal in reversed(self.trade_signals):
            if signal.get('signal_type') == 'top_divergence' and signal.get('side') == 'SELL':
                signal['quantity'] = current_position.quantity.as_double()
                signal['order_id'] = 'close_position'
                break
        
        # 执行平仓操作
        self.close_position(current_position)
        self._log.info(f"背离卖出信号: 价格={bar.close.as_double():.4f}, 数量={current_position.quantity.as_double()}")
    
    def check_risk_management(self, bar: Bar):
        """检查风险管理"""
        # 检查是否有持仓 - 使用可靠的持仓查询方法
        current_position = self.get_current_position()
        if current_position is None:
            return
        
        current_price = bar.close.as_double()
        entry_price = current_position.avg_px_open
        
        # 计算盈亏百分比
        pnl_pct = (current_price - entry_price) / entry_price
        
        # 止损检查
        if pnl_pct <= -self.stop_loss_pct:
            self.close_position(current_position)
            self._log.info(f"触发止损: 亏损{pnl_pct*100:.2f}%")
        
        # 止盈检查
        elif pnl_pct >= self.take_profit_pct:
            self.close_position(current_position)
            self._log.info(f"触发止盈: 盈利{pnl_pct*100:.2f}%")

    def get_current_position(self):
        """获取当前持仓状态 - 回测环境优化版本"""
        # 方法1：检查实例变量（最优先）
        if self.position and self.position.quantity.as_double() > 0:
            return self.position
        
        # 方法2：从缓存查询当前工具的持仓（回测推荐）
        try:
            position = self.cache.position_for_instrument(self.config.instrument_id)
            if position and position.quantity.as_double() > 0:
                # 更新实例变量
                self.position = position
                self._log.debug(f"从缓存恢复持仓状态: {position.quantity.as_double()} 股")
                return position
        except Exception as e:
            self._log.debug(f"从缓存查询指定工具持仓失败: {e}")
        
        # 方法3：从缓存查询所有持仓（备用方案）
        try:
            positions = self.cache.positions()
            if positions:
                for pos in positions:
                    if pos.instrument_id == self.config.instrument_id and pos.quantity.as_double() > 0:
                        self.position = pos
                        self._log.debug(f"从缓存恢复持仓状态: {pos.quantity.as_double()} 股")
                        return pos
        except Exception as e:
            self._log.debug(f"从缓存查询所有持仓失败: {e}")
        
        # 没有持仓
        self.position = None
        return None

    def on_dispose(self):
        """策略销毁时调用"""
        self._log.info("ETF159506 MACD金叉死叉策略已销毁") 