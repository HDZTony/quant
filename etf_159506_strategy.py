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
from datetime import datetime, time

from etf_159506_strategy_config import ETF159506Config


class CatalogRSIIndicator:
    """
    基于etf_159506_catalog_loader.py的RSI指标实现
    使用pandas rolling window计算，与catalog_loader保持一致
    """
    
    def __init__(self, period=6):
        self.period = period
        
        # 存储历史数据
        self.closes = deque(maxlen=period + 10)  # 存储收盘价历史
        
        # 当前RSI值
        self.value = 50.0
        
        # 初始化状态
        self.initialized = False
    
    def update_raw(self, close: float):
        """
        更新RSI指标值
        
        Parameters
        ----------
        close : float
            收盘价
        """
        # 添加新数据
        self.closes.append(close)
        
        # 检查是否足够数据计算RSI
        if len(self.closes) >= 2:  # 至少需要2个数据点计算差值
            # 转换为pandas Series进行计算
            close_series = pd.Series(list(self.closes))
            
            # 计算价格变化
            delta = close_series.diff()
            gain = delta.where(delta > 0, 0.0)
            loss = -delta.where(delta < 0, 0.0)
            
            # 计算平均收益和损失
            avg_gain = gain.rolling(window=self.period, min_periods=1).mean()
            avg_loss = loss.rolling(window=self.period, min_periods=1).mean()
            
            # 计算相对强弱
            rs = avg_gain / avg_loss
            
            # 计算RSI
            rsi = 100 - (100 / (1 + rs))
            
            # 获取最新值
            self.value = rsi.iloc[-1]
            
            # 标记已初始化
            if not self.initialized and len(self.closes) >= self.period:
                self.initialized = True
    
    def handle_bar(self, bar: Bar):
        """
        处理K线数据
        
        Parameters
        ----------
        bar : Bar
            K线数据
        """
        self.update_raw(bar.close.as_double())
    
    def reset(self):
        """重置指标状态"""
        self.closes.clear()
        self.value = 50.0
        self.initialized = False
    
    def __str__(self) -> str:
        return f"CatalogRSIIndicator({self.period})"
    
    def __repr__(self) -> str:
        return f"CatalogRSIIndicator({self.period})"
    
class CatalogKDJIndicator:
    """
    基于etf_159506_catalog_loader.py的KDJ指标实现
    使用pandas rolling window计算，与catalog_loader保持一致
    """
    def __init__(self, n=9, k_period=3, d_period=3):
        self.n = n
        self.k_period = k_period
        self.d_period = d_period
        
        # 存储历史数据
        self.highs = deque(maxlen=n)
        self.lows = deque(maxlen=n)
        self.closes = deque(maxlen=n)
        
        # 当前KDJ值
        self.value_k = 50.0
        self.value_d = 50.0
        self.value_j = 50.0
        
        # 初始化状态
        self.initialized = False
    
    def update_raw(self, high: float, low: float, close: float):
        """
        更新KDJ指标值
        
        Parameters
        ----------
        high : float
            最高价
        low : float
            最低价
        close : float
            收盘价
        """
        # 添加新数据
        self.highs.append(high)
        self.lows.append(low)
        self.closes.append(close)
        
        # 检查是否足够数据计算KDJ
        if len(self.closes) >= self.n:
            # 转换为pandas Series进行计算
            close_series = pd.Series(list(self.closes))
            high_series = pd.Series(list(self.highs))
            low_series = pd.Series(list(self.lows))
            
            # 计算N周期内的最低价和最高价
            low_list = close_series.rolling(window=self.n, min_periods=1).min()
            high_list = close_series.rolling(window=self.n, min_periods=1).max()
            
            # 计算RSV：RSV = (CLOSE - LLV(LOW, N)) / (HHV(HIGH, N) - LLV(LOW, N)) * 100
            rsv = (close_series - low_list) / (high_list - low_list) * 100
            
            # 计算K值：K = MA(RSV, k_period)
            k = rsv.rolling(window=self.k_period, min_periods=1).mean()
            
            # 计算D值：D = MA(K, d_period)
            d = k.rolling(window=self.d_period, min_periods=1).mean()
            
            # 计算J值：J = 3*K - 2*D
            j = 3 * k - 2 * d
            
            # 获取最新值
            self.value_k = k.iloc[-1]
            self.value_d = d.iloc[-1]
            self.value_j = j.iloc[-1]
            
            # 标记已初始化
            if not self.initialized:
                self.initialized = True
    
    def handle_bar(self, bar: Bar):
        """
        处理K线数据
        
        Parameters
        ----------
        bar : Bar
            K线数据
        """
        self.update_raw(
            bar.high.as_double(),
            bar.low.as_double(),
            bar.close.as_double(),
        )
    
    def reset(self):
        """重置指标状态"""
        self.highs.clear()
        self.lows.clear()
        self.closes.clear()
        
        self.value_k = 50.0
        self.value_d = 50.0
        self.value_j = 50.0
        
        self.initialized = False
    
    def __str__(self) -> str:
        return f"CatalogKDJIndicator({self.n}, {self.k_period}, {self.d_period})"
    
    def __repr__(self) -> str:
        return f"CatalogKDJIndicator({self.n}, {self.k_period}, {self.d_period})"


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
        
        # KDJ指标 - 使用自定义实现
        self.kdj = CatalogKDJIndicator(n=9, k_period=3, d_period=3)
        
        # RSI指标 - 使用自定义实现
        self.rsi = CatalogRSIIndicator(period=6)
        
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
        self.timestamps = deque(maxlen=1000)  # 存储K线时间戳
        self.divergence_lookback = 30  # 背离检测的回看周期
        self.divergence_confirmation_bars = 3  # 背离确认所需的K线数
        self.last_divergence_signal = None  # 记录上一次背离信号类型
        
        # 背离检测参数（从配置中获取）
        self.dea_trend_period = config.dea_trend_period
        self.divergence_threshold = config.divergence_threshold
        self.advance_trading_bars = config.advance_trading_bars
        self.confirmation_bars = config.confirmation_bars
        
        # 背离检测改进：记录历史极值点
        self.price_peaks = deque(maxlen=config.max_extremes)  # 存储价格峰值点 (timestamp, price, dif_value)
        self.price_troughs = deque(maxlen=config.max_extremes)  # 存储价格谷值点 (timestamp, price, dif_value)
        self.dif_peaks = deque(maxlen=config.max_extremes)  # 存储DIF峰值点 (timestamp, dif_value, price_value)
        self.dif_troughs = deque(maxlen=config.max_extremes)  # 存储DIF谷值点 (timestamp, dif_value, price_value)
        self.divergence_lookback_peaks = 3  # 回看过去几个极值点来检测背离
        
        # 分别存储价格和MACD的极值点历史（用于相对极值检测）
        self.price_extremes_history = []  # 存储价格极值点历史
        self.macd_extremes_history = []    # 存储MACD极值点历史
        
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
        
        # 技术指标信号记录（金叉、死叉、背离等）
        self.technical_signals = []
        
        # 定时买入功能
        self.scheduled_buy_time = time(14, 50)  # 2:50分买入（北京时间）
        self.last_scheduled_buy_date = None  # 记录上次定时买入的日期
        self._log.info(f"定时买入功能已启用: 每天北京时间 {self.scheduled_buy_time.strftime('%H:%M')} 执行买入")

    def on_start(self):
        """策略启动时调用"""
        bar_type = self.config.bar_type
        self.subscribe_bars(bar_type)
        self._log.info(f"ETF159506 MACD金叉死叉策略已启动，订阅 {self.config.instrument_id} 的 {bar_type}")
        
        # 策略始终空仓开始
        self._log.info("策略配置为空仓开始，等待交易信号")
        
        # # 检查初始持仓状态
        # initial_position = self.get_current_position()
        # if initial_position:
        #     self._log.info(f"检测到初始持仓: {initial_position.quantity.as_double()} 股")
        # else:
        #     self._log.info("确认初始状态：无持仓")

    def on_stop(self):
        """策略停止时调用"""
        # current_position = self.get_current_position()
        # if current_position is not None:
        #     self.close_all_positions(self.config.instrument_id)
        #     self._log.info(f"策略停止时关闭持仓: {current_position.quantity.as_double()} 股")
        self.unsubscribe_bars(self.config.bar_type)
        
        # 保存交易信号到策略实例变量中，供回测系统获取
        if hasattr(self, 'trade_signals') and self.trade_signals:
            if not hasattr(self, '_saved_trade_signals'):
                self._saved_trade_signals = []
            self._saved_trade_signals.extend(self.trade_signals)
            self._log.info(f"策略停止时保存了 {len(self.trade_signals)} 个交易信号")
        
        # 保存技术指标信号到策略实例变量中，供回测系统获取
        if hasattr(self, 'technical_signals') and self.technical_signals:
            if not hasattr(self, '_saved_technical_signals'):
                self._saved_technical_signals = []
            self._saved_technical_signals.extend(self.technical_signals)
            self._log.info(f"策略停止时保存了 {len(self.technical_signals)} 个技术指标信号")
        
        # 保存极值点数据到策略实例变量中，供回测系统获取
        if not hasattr(self, '_saved_extremes'):
            self._saved_extremes = {
                'price_peaks': list(self.price_peaks),
                'price_troughs': list(self.price_troughs),
                'dif_peaks': list(self.dif_peaks),
                'dif_troughs': list(self.dif_troughs),
                'price_extremes_history': self.price_extremes_history.copy(),
                'macd_extremes_history': self.macd_extremes_history.copy()
            }
            self._log.info(f"策略停止时保存了 {len(self.price_peaks)} 个价格峰值, {len(self.price_troughs)} 个价格谷值, {len(self.dif_peaks)} 个DIF峰值, {len(self.dif_troughs)} 个DIF谷值")
        
        self.print_extremes_history()
        
    def on_bar(self, bar: Bar):
        """处理K线数据"""
        # 更新MACD指标
        self.macd.handle_bar(bar)
        
        # 更新KDJ指标
        self.kdj.handle_bar(bar)
        
        # 更新RSI指标
        self.rsi.handle_bar(bar)
        
        # 添加调试信息
        # 转换为北京时间格式
        utc_time = pd.to_datetime(bar.ts_event, unit='ns')
        beijing_time = utc_time.tz_localize('UTC').tz_convert('Asia/Shanghai')
        beijing_time_str = beijing_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        self._log.info(f"处理K线: 时间={beijing_time_str}, 价格={bar.close.as_double():.4f}, MACD初始化状态={self.macd.initialized}")
        if self.kdj.initialized:
            self._log.info(f"KDJ状态: K={self.kdj.value_k:.2f}, D={self.kdj.value_d:.2f}, J={self.kdj.value_j:.2f}")
        if self.rsi.initialized:
            self._log.info(f"RSI状态: RSI={self.rsi.value:.2f}")
        
        # 检查定时买入信号
        self.check_scheduled_buy(bar)
        
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
            self.timestamps.append(bar.ts_event)
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
        # if len(self.macd_history) % 10 == 0:
        #     current_position = self.get_current_position()
        #     if current_position:
        #         self._log.info(f"持仓状态监控: {current_position.quantity.as_double()} 股, 成本: {current_position.avg_px_open:.4f}")
        #     else:
        #         self._log.info("持仓状态监控: 无持仓")

    def on_event(self, event: Event):
        """处理所有事件"""
        pass  # 通用事件处理，具体事件由专门方法处理
    
    def on_position_opened(self, event: PositionOpened) -> None:
        """持仓开启事件处理"""
        self.position = self.cache.position(event.position_id)
        if self.position:
            self._log.info(f"持仓已开启: {self.position.quantity.as_double()} 股, 方向: {self.position.side}")
        else:
            self._log.info("持仓开启事件：无法获取持仓信息")
    
    def on_position_changed(self, event: PositionChanged) -> None:
        """持仓变化事件处理"""
        self.position = self.cache.position(event.position_id)
        if self.position:
            self._log.info(f"持仓已变化: {self.position.quantity.as_double()} 股, 方向: {self.position.side}")
        else:
            self._log.info("持仓变化事件：无法获取持仓信息")
    
    def on_position_closed(self, event: PositionClosed) -> None:
        """持仓关闭事件处理"""
        self._log.info(f"持仓关闭事件触发: position_id={event.position_id}")
        self.position = None
        self._log.info("持仓已关闭，实例变量已清空")
    
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
        self.timestamps.append(bar.ts_event)
                
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
    
    def check_negative_positive_histogram(self, bar: Bar, lookback_minutes: int = 5):
        """
        循环计算前几个分钟的histogram值，检测histogram由正到负再到正的模式
        
        Parameters:
        -----------
        bar : Bar
            当前K线数据
        lookback_minutes : int
            回看分钟数，默认5分钟
            
        Returns:
        --------
        dict or None
            如果检测到模式，返回包含时间点信息的字典；否则返回None
        """
        # 需要足够的历史数据
        if len(self.macd_history) < lookback_minutes or len(self.signal_history) < lookback_minutes:
            self._log.info(f"历史数据不足，需要至少{lookback_minutes}个数据点")
            return 
        
        # # 检查DIF值是否由负转正
        if len(self.macd_history) >= 2:
            current_dif = self.macd_history[-1]
            previous_dif = self.macd_history[-2]
            if previous_dif < 0 and current_dif >= 0:
                self._log.info(f"DIF值由负转正: 前值={previous_dif:.6f}, 当前值={current_dif:.6f}")
            else:
                self._log.info(f"DIF值未由负转正: 前值={previous_dif:.6f}, 当前值={current_dif:.6f}")
                return 
        else:
            self._log.info("DIF历史数据不足，无法判断由负转正")
            return 
        
        # 计算每个时间点的histogram值（从当前到过去）
        negative_point = False
        positive_index = None
        min_trough_info = None
        
        for i, histogram in enumerate(reversed(self.histogram_history)):
            if histogram < 0:
                negative_point = True
            if histogram > 0 and negative_point:
                # 计算实际索引（从最新开始反向）
                actual_index = len(self.histogram_history) - 1 - i
                positive_index = actual_index
                self._log.info(f"找到histogram为正的时间点: 索引={actual_index}, histogram值={histogram:.6f}")
                
                # 获取对应的时间戳
                if actual_index < len(self.timestamps):
                    timestamp = self.timestamps[actual_index]
                    # 转换为北京时间
                    utc_time = pd.to_datetime(timestamp, unit='ns')
                    beijing_time = utc_time.tz_localize('UTC').tz_convert('Asia/Shanghai')
                    time_str = beijing_time.strftime('%H:%M:%S')
                    self._log.info(f"对应时间: {time_str}")
                    
                    # 在macd_extremes_history中找到从该时间戳到当前时间中最小的极小值
                    min_trough_value = None
                    min_trough_timestamp = None
                    
                    for extreme_timestamp, extreme_value, extreme_type in self.macd_extremes_history:
                        # 只考虑极小值（trough）
                        if extreme_type == 'trough' and extreme_timestamp >= timestamp:
                            if min_trough_value is None or extreme_value < min_trough_value:
                                min_trough_value = extreme_value
                                min_trough_timestamp = extreme_timestamp
                    
                    if min_trough_value is not None:
                        # 转换为北京时间
                        utc_trough_time = pd.to_datetime(min_trough_timestamp, unit='ns')
                        beijing_trough_time = utc_trough_time.tz_localize('UTC').tz_convert('Asia/Shanghai')
                        min_trough_time_str = beijing_trough_time.strftime('%H:%M:%S')
                        self._log.info(f"从{time_str}到当前时间中最小的极小值: {min_trough_value:.6f}, 时间: {min_trough_time_str}")
                        
                        # 保存最小极小值信息
                        min_trough_info = {
                            'value': min_trough_value,
                            'timestamp': min_trough_timestamp,
                            'time_str': min_trough_time_str,
                            'start_time': time_str,
                            'start_timestamp': timestamp
                        }
                        if min_trough_value < -0.004:
                            self.technical_signal += 200
                            self._log.info(f"极小值信号触发: min_trough_value={min_trough_value:.6f} < -0.004, 买入信号+100, 当前信号值={self.technical_signal}")
                            
                            # 记录极小值技术指标信号
                            technical_signal = {
                                'timestamp': pd.to_datetime(bar.ts_event, unit='ns'),
                                'price': bar.close.as_double(),
                                'signal_type': 'n2p',
                                'signal_value': self.technical_signal,
                                'min_trough_value': min_trough_value,
                                'min_trough_time': min_trough_time_str,
                                'rsi_value': self.rsi.value if self.rsi.initialized else None,
                                'kdj_k': self.kdj.value_k if self.kdj.initialized else None,
                                'kdj_d': self.kdj.value_d if self.kdj.initialized else None,
                                'kdj_j': self.kdj.value_j if self.kdj.initialized else None
                            }
                            self.technical_signals.append(technical_signal)
                            self._log.info(f"记录极小值技术信号: {technical_signal}")
                            
                            # 检查是否达到买入阈值
                            if self.technical_signal >= self.buy_threshold:
                                self._log.info(f"极小值买入信号达到阈值{self.buy_threshold}，执行买入操作")
                                self.execute_buy_signal(bar)
                                self.technical_signal = 0  # 信号归零
                            
                    else:
                        self._log.info(f"从{time_str}到当前时间中未找到极小值")
                
                break
        


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
        
        # 计算当前histogram值
        current_histogram = current_macd - current_signal
        
        # 检查histogram模式
        histogram_result = self.check_negative_positive_histogram(bar, lookback_minutes=5)
        
        
        # 检查DIF<0且前五个DIF都是单调递减的情况
        if current_macd < 0 and len(self.macd_history) >= 6:
            # 获取前5个DIF值（不包括当前值）
            last_five_dif = list(self.macd_history)[-6:-1]  # 前5个值
            current_dif = current_macd
            
            # 检查是否单调递减（从历史到当前，即从旧到新）
            is_monotonic_decreasing = True
            for i in range(len(last_five_dif) - 1):
                if last_five_dif[i] < last_five_dif[i + 1]:  # 如果从历史到当前不是递减（即后面的值比前面的大）
                    is_monotonic_decreasing = False
                    break
            
            # 检查前5分钟内是否有卖出操作（只检查最后一个卖出交易）
            has_sell_operation = False
            current_timestamp = pd.to_datetime(bar.ts_event, unit='ns')
            
            # 只检查最后一个交易是否为SELL
            if self.trade_signals:
                last_signal = self.trade_signals[-1]
                if last_signal.get('side') == 'SELL':
                    has_sell_operation = True
                    self._log.info(f"最后一个交易是SELL，阻止卖出操作")
                else:
                    self._log.info(f"最后一个交易是{last_signal.get('side')}，允许卖出操作")
            
            # 添加调试日志
            self._log.info(f"DIF单调递减检查: 前5个DIF值={last_five_dif}, 当前DIF={current_dif}, 是否单调递减={is_monotonic_decreasing}")
            self._log.info(f"卖出操作检查: 当前时间={current_timestamp}, 是否有卖出操作={has_sell_operation}")
            
            # 如果前5个DIF单调递减且当前DIF<0且最后一个交易不是SELL，执行全部卖出
            if is_monotonic_decreasing and not has_sell_operation:
                self._log.info(f"检测到DIF<0且前5个DIF单调递减且最后一个交易不是SELL，执行全部卖出")
                self._log.info(f"前5个DIF值: {last_five_dif}")
                self._log.info(f"当前DIF值: {current_dif}")
                self._log.info(f"前5个DIF期间是否有卖出操作: {has_sell_operation}")
                # 检查当前时间是否在2:50分之后，如果是则跳过卖出操作
                if self.is_after_scheduled_time(bar):
                    self._log.info(f"当前时间已过2:50分，跳过卖出信号执行")
                    return
                # 记录实际交易信号
                trade_signal = {
                    'timestamp': pd.to_datetime(bar.ts_event, unit='ns'),
                    'price': bar.close.as_double(),
                    'side': 'SELL',
                    'order_id': 'close_position',
                    'signal_type': 'executed_divergence_sell',
                    'signal_value': self.technical_signal
                }
                self.trade_signals.append(trade_signal)
                # 执行全部卖出
                self.execute_sell_signal(bar)
                
                return
        
        
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
            
            # 检查MACD值是否足够大（可选：注释掉以下代码来禁用过滤）
            if current_macd_abs < macd_threshold:
                self._log.info(f"金叉信号被过滤: MACD绝对值{current_macd_abs:.6f} < 阈值{macd_threshold:.6f}")
                return
            
            self.last_signal = "golden_cross"
            
            # 累积买入信号
            # 如果是第一个金叉技术信号，使用40000系数，否则使用20000
            is_first_golden_cross = not any(signal.get('signal_type') == 'golden_cross' for signal in self.technical_signals)
            signal_coefficient = 40000 if is_first_golden_cross else 20000
            self.technical_signal += signal_coefficient*current_macd_abs
            self._log.info(f"金叉买入信号累积: 系数={signal_coefficient}, MACD绝对值={current_macd_abs:.6f}, 当前信号值={self.technical_signal}")

            # 检查RSI条件
            if self.rsi.initialized:
                self.technical_signal += 50-self.rsi.value
                self._log.info(f"RSI条件满足：RSI={self.rsi.value:.2f} < 50，增强买入信号")
            else:
                rsi_status = f"{self.rsi.value:.2f}" if self.rsi.initialized else "未初始化"
                self._log.info(f"RSI条件不满足：RSI={rsi_status}")

            # 检查KDJ条件
            # 计算KDJ三个值的最大差值
            kdj_values = [self.kdj.value_k, self.kdj.value_d, self.kdj.value_j]
            kdj_max_diff = max(kdj_values) - min(kdj_values)
            
            # 检查KDJ三个值是否都小于20（超卖条件）
            kdj_oversold = all(val < 25 for val in kdj_values)
            
            self._log.info(f"KDJ分析: K={self.kdj.value_k:.2f}, D={self.kdj.value_d:.2f}, J={self.kdj.value_j:.2f}")
            self._log.info(f"KDJ最大差值={kdj_max_diff:.2f}, 超卖状态={kdj_oversold}")
            
            # 如果KDJ三个值最大差值小于10且都小于20，增强信号
            if kdj_max_diff < 20 and kdj_oversold:
                self.technical_signal += 40-kdj_max_diff
                self._log.info("KDJ条件满足：最大差值<20且超卖，增强买入信号")
            else:
                self._log.info("KDJ条件不满足，使用标准信号")
            
            # 记录技术指标信号（金叉）
            technical_signal = {
                'timestamp': pd.to_datetime(bar.ts_event, unit='ns'),
                'price': bar.close.as_double(),
                'signal_type': 'golden_cross',
                'signal_value': self.technical_signal,
                'macd_value': current_macd,
                'signal_value_macd': current_signal,
                'histogram': current_histogram,
                'rsi_value': self.rsi.value if self.rsi.initialized else None,
                'kdj_k': self.kdj.value_k if self.kdj.initialized else None,
                'kdj_d': self.kdj.value_d if self.kdj.initialized else None,
                'kdj_j': self.kdj.value_j if self.kdj.initialized else None
            }
            self.technical_signals.append(technical_signal)
            self._log.info(f"记录金叉技术信号: {technical_signal}")
            
            # 检查是否达到买入阈值
            if self.technical_signal >= self.buy_threshold:
                self._log.info(f"金叉买入信号达到阈值{self.buy_threshold}，执行买入操作")
                self.execute_buy_signal(bar)
                self.technical_signal = 0  # 信号归零
        
        elif death_cross:
            self._log.info(f"检测到死叉信号: MACD={current_macd:.6f}, Signal={current_signal:.6f}")
            
            # 检查MACD值是否足够大（可选：注释掉以下代码来禁用过滤）
            if current_macd_abs < macd_threshold:
                self._log.info(f"死叉信号被过滤: MACD绝对值{current_macd_abs:.6f} < 阈值{macd_threshold:.6f}")
                return
            
            self.last_signal = "death_cross"
            
            # 累积卖出信号
            # 如果是第一个死叉技术信号，使用40000系数，否则使用20000
            is_first_death_cross = not any(signal.get('signal_type') == 'death_cross' for signal in self.technical_signals)
            signal_coefficient = 40000 if is_first_death_cross else 20000
            self.technical_signal -= signal_coefficient*current_macd_abs
            self._log.info(f"死叉卖出信号累积: 系数={signal_coefficient}, MACD绝对值={current_macd_abs:.6f}, 当前信号值={self.technical_signal}")
            
            # 检查RSI条件
            if self.rsi.initialized and self.rsi.value < 50:
                self.technical_signal -= self.rsi.value
                self._log.info(f"RSI条件满足：RSI={self.rsi.value:.2f} < 50，增强卖出信号")
            else:
                rsi_status = f"{self.rsi.value:.2f}" if self.rsi.initialized else "未初始化"
                self._log.info(f"RSI条件不满足：RSI={rsi_status}")
            # 检查KDJ条件
            # 计算KDJ三个值的最大差值
            kdj_values = [self.kdj.value_k, self.kdj.value_d, self.kdj.value_j]
            kdj_max_diff = max(kdj_values) - min(kdj_values)
            
            # 检查KDJ三个值是否都大于80（超买条件）
            kdj_oversold = all(val > 80 for val in kdj_values)
            
            self._log.info(f"KDJ分析: K={self.kdj.value_k:.2f}, D={self.kdj.value_d:.2f}, J={self.kdj.value_j:.2f}")
            self._log.info(f"KDJ最大差值={kdj_max_diff:.2f}, 超买状态={kdj_oversold}")
            
            # 如果KDJ三个值最大差值小于10且都大于80，增强信号
            if kdj_max_diff < 20 and kdj_oversold:
                self.technical_signal -= 30-kdj_max_diff
                self._log.info("KDJ条件满足：最大差值<20且超买，增强卖出信号")
            else:
                self._log.info("KDJ条件不满足，使用标准信号")
            # 记录技术指标信号（死叉）
            technical_signal = {
                'timestamp': pd.to_datetime(bar.ts_event, unit='ns'),
                'price': bar.close.as_double(),
                'signal_type': 'death_cross',
                'signal_value': self.technical_signal,
                'macd_value': current_macd,
                'signal_value_macd': current_signal,
                'histogram': current_histogram,
                'rsi_value': self.rsi.value if self.rsi.initialized else None,
                'kdj_k': self.kdj.value_k if self.kdj.initialized else None,
                'kdj_d': self.kdj.value_d if self.kdj.initialized else None,
                'kdj_j': self.kdj.value_j if self.kdj.initialized else None
            }
            self.technical_signals.append(technical_signal)
            self._log.info(f"记录死叉技术信号: {technical_signal}")
            
            # 检查是否达到卖出阈值
            if self.technical_signal <= self.sell_threshold:
                self._log.info(f"死叉卖出信号达到阈值{self.sell_threshold}，执行卖出操作")
                self.execute_sell_signal(bar)
                self.technical_signal = 0  # 信号归零
    
    def check_divergence(self, bar: Bar, action: str):
        """检查DIF背离信号"""
        # 需要足够的极值点来检测背离
        if len(self.price_peaks) < 2 or len(self.price_troughs) < 2 or len(self.dif_peaks) < 2 or len(self.dif_troughs) < 2:
            return
        
        # 检测顶背离和底背离
        top_divergence = self.detect_top_divergence()
        bottom_divergence = self.detect_bottom_divergence()
        
        # 处理背离信号
        if top_divergence:
            self.handle_top_divergence(bar, action)
        elif bottom_divergence:
            self.handle_bottom_divergence(bar, action)
    
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
    
    
    def _is_relative_extreme(self, extreme_type, prev_value, data_type, min_extreme_distance=None):
        """检查前值是否相对于上一个极值点是真正的极值
        
        规则：
        1. 如果新极值与上一个不同类型极值差异太小，则略过当前极值
        2. 如果新极值与上一个同类型极值，则保留绝对值更大的那个
        
        Args:
            extreme_type: 极值类型 ('peak' 或 'trough')
            prev_value: 前一个值
            data_type: 数据类型 ('price' 或 'macd')
            min_extreme_distance: 最小极值距离（如果为None，则根据数据类型自动设置）
        """
        # 根据数据类型自动设置最小极值距离
        if min_extreme_distance is None:
            if data_type == 'price':
                min_extreme_distance = 0.001  # 价格最小差异0.001元
            else:  # data_type == 'macd'
                min_extreme_distance = 0.0001  # DIF最小差异0.0001
        # 根据数据类型选择对应的历史记录
        if data_type == 'price':
            history = self.price_extremes_history
        else:  # data_type == 'macd'
            history = self.macd_extremes_history
        
        # 如果没有历史数据，直接返回True
        if len(history) == 0:
            return True, 'keep'
        
        # 获取上一个极值点
        last_extreme = history[-1]
        last_extreme_type = last_extreme[2]  # 极值类型
        last_extreme_value = last_extreme[1]  # 极值数值
        
        # 如果新极值与上一个极值类型不同
        if extreme_type != last_extreme_type:
            # 计算与上一个极值的绝对差异
            diff_absolute = abs(prev_value - last_extreme_value)
            
            # 如果差异太小，略过当前极值
            if diff_absolute < min_extreme_distance:
                return False, None
            
            # 差异足够大，保留新极值
            return True, 'keep'
        
        # 如果新极值与上一个极值类型相同
        else:
            # 计算与上一个极值的绝对差异
            diff_absolute = abs(prev_value - last_extreme_value)
            
            # 如果差异太小，略过当前极值
            if diff_absolute < min_extreme_distance:
                return False, None
            # 根据极值类型决定保留策略
            if extreme_type == 'peak':
                # 峰值：保留更大的值
                if prev_value > last_extreme_value:
                    return True, 'keep'  # 保留新的
                else:
                    return True, 'replace'  # 替换旧的
            else:  # extreme_type == 'trough'
                # 谷值：保留更小的值
                if prev_value < last_extreme_value:
                    return True, 'keep'  # 保留新的
                else:
                    return True, 'replace'  # 替换旧的
    
    def _detect_extreme(self, data, prev_index):
        """检测极值点（基于前后点比较的简单方法）"""
        # 修改边界条件：允许检测最后一个点，但需要确保有足够的数据进行比较
        if prev_index < 1:
            return False, None
        
        # 如果是最后一个点，暂时不检测（等待下一个点到来）
        if prev_index >= len(data) - 1:
            return False, None
        
        # 使用简单的极值检测逻辑：比较当前点与前后点的值
        prev_value = data[prev_index]
        prev_prev_value = data[prev_index - 1]
        current_value = data[prev_index + 1]

        # 添加调试信息
        self._log.debug(f"极值检测: 前值={prev_value:.6f}, 前前值={prev_prev_value:.6f}, 当前值={current_value:.6f}")
        
        # 检测峰值：当前值比前后值都大
        if prev_value > prev_prev_value and prev_value > current_value:
            self._log.info(f"检测到峰值: {prev_value:.6f} > {prev_prev_value:.6f} 且 {prev_value:.6f} > {current_value:.6f}")
            return True, 'peak'
        
        # 检测谷值：当前值比前后值都小
        elif prev_value < prev_prev_value and prev_value < current_value:
            self._log.info(f"检测到谷值: {prev_value:.6f} < {prev_prev_value:.6f} 且 {prev_value:.6f} < {current_value:.6f}")
            return True, 'trough'
        
        return False, None
    
    def detect_and_record_extremes(self, bar: Bar):
        """改进的极值点检测：每个新K线到来时检测上一个点的极值"""
        self._log.info(f"开始极值点检测: price_history长度={len(self.price_history)}, macd_history长度={len(self.macd_history)}")
        
        if len(self.price_history) < 3:
            self._log.info("价格历史数据不足3个点，跳过极值点检测")
            return
        
        current_timestamp = bar.ts_event
        current_price = self.price_history[-1]
        current_macd = self.macd_history[-1]
        
        # 检测上一个价格点的极值（延迟检测）
        if len(self.price_history) >= 2:
            prev_price_index = len(self.price_history) - 2  # 上一个价格点的索引
            prev_price_timestamp = self.timestamps[-2] if len(self.timestamps) >= 2 else current_timestamp
            prev_price = self.price_history[-2]
            prev_macd = self.macd_history[-2]
            price_extreme, price_type = self._detect_extreme(self.price_history, prev_price_index)
            if price_extreme:
                self._log.info(f"检测到价格极值点: 类型={price_type}, 价格={prev_price:.4f}")
                
                # 使用新的相对极值检测逻辑
                is_extreme, action = self._is_relative_extreme(price_type, prev_price, 'price')
                
                if is_extreme:
                    # 处理替换逻辑
                    if action == 'replace':
                        # 替换上一个极值点
                        if price_type == 'peak' and len(self.price_peaks) > 0:
                            removed_peak = self.price_peaks.pop()
                            # 同时从价格极值点历史中移除
                            if len(self.price_extremes_history) > 0:
                                self.price_extremes_history.pop()
                            self._log.debug(f"替换价格峰值: 时间{removed_peak[0]}, 价格{removed_peak[1]:.4f} -> 时间{prev_price_timestamp}, 价格{prev_price:.4f}")
                        elif price_type == 'trough' and len(self.price_troughs) > 0:
                            removed_trough = self.price_troughs.pop()
                            # 同时从价格极值点历史中移除
                            if len(self.price_extremes_history) > 0:
                                self.price_extremes_history.pop()
                            self._log.debug(f"替换价格谷值: 时间{removed_trough[0]}, 价格{removed_trough[1]:.4f} -> 时间{prev_price_timestamp}, 价格{prev_price:.4f}")
                    
                    elif action == 'keep':
                        # 保留当前极值点，不需要替换
                        self._log.debug(f"保留价格{price_type}: 时间{prev_price_timestamp}, 价格{prev_price:.4f}")
                    
                    # 无论是replace还是keep，都需要添加新极值点
                    if price_type == 'peak':
                        self.price_peaks.append((prev_price_timestamp, prev_price, prev_macd))
                        # 同时更新价格极值点历史
                        self.price_extremes_history.append((prev_price_timestamp, prev_price, 'peak'))
                        self._log.debug(f"检测到新价格峰值: 时间{prev_price_timestamp}, 价格{prev_price:.4f}, DIF{prev_macd:.6f}")
                    else:  # price_type == 'trough'
                        self.price_troughs.append((prev_price_timestamp, prev_price, prev_macd))
                        # 同时更新价格极值点历史
                        self.price_extremes_history.append((prev_price_timestamp, prev_price, 'trough'))
                        self._log.debug(f"检测到新价格谷值: 时间{prev_price_timestamp}, 价格{prev_price:.4f}, DIF{prev_macd:.6f}")
                    
                    # 简化后不再需要清理极值点
                else:
                    self._log.debug(f"略过价格{price_type}: 时间{prev_price_timestamp}, 价格{prev_price:.4f} (差异太小)")
            else:
                self._log.debug(f"未检测到价格极值点: 上一个价格={prev_price:.4f}")
        
        # 检测上一个MACD点的极值（延迟检测）
        if len(self.macd_history) >= 2:
            prev_macd_index = len(self.macd_history) - 2  # 上一个MACD点的索引
            prev_macd_timestamp = self.timestamps[-2] if len(self.timestamps) >= 2 else current_timestamp
            prev_macd = self.macd_history[-2]
            prev_price = self.price_history[-2]
            macd_extreme, macd_type = self._detect_extreme(self.macd_history, prev_macd_index)
            if macd_extreme:
                self._log.info(f"检测到DIF极值点: 类型={macd_type}, DIF={prev_macd:.6f}")
                
                # 使用新的相对极值检测逻辑
                is_extreme, action = self._is_relative_extreme(macd_type, prev_macd, 'macd')
            
                if is_extreme:
                    if action == 'replace':
                        # 替换上一个极值点
                        if macd_type == 'peak' and len(self.dif_peaks) > 0:
                            removed_peak = self.dif_peaks.pop()
                            # 同时从MACD极值点历史中移除
                            if len(self.macd_extremes_history) > 0:
                                self.macd_extremes_history.pop()
                            self._log.debug(f"替换DIF峰值: 时间{removed_peak[0]}, DIF{removed_peak[1]:.6f} -> 时间{prev_macd_timestamp}, DIF{prev_macd:.6f}")
                        elif macd_type == 'trough' and len(self.dif_troughs) > 0:
                            removed_trough = self.dif_troughs.pop()
                            # 同时从MACD极值点历史中移除
                            if len(self.macd_extremes_history) > 0:
                                self.macd_extremes_history.pop()
                            self._log.debug(f"替换DIF谷值: 时间{removed_trough[0]}, DIF{removed_trough[1]:.6f} -> 时间{prev_macd_timestamp}, DIF{prev_macd:.6f}")
                    
                    elif action == 'keep':
                        # 保留当前极值点，不需要替换
                        self._log.debug(f"保留DIF{macd_type}: 时间{prev_macd_timestamp}, DIF{prev_macd:.6f}")
                    
                    # 添加新极值点
                    if macd_type == 'peak':
                        self.dif_peaks.append((prev_macd_timestamp, prev_macd, prev_price))
                        # 同时更新MACD极值点历史
                        self.macd_extremes_history.append((prev_macd_timestamp, prev_macd, 'peak'))
                        self._log.debug(f"检测到新DIF峰值: 时间{prev_macd_timestamp}, DIF{prev_macd:.6f}, 价格{prev_price:.4f}")
                    else:  # macd_type == 'trough'
                        self.dif_troughs.append((prev_macd_timestamp, prev_macd, prev_price))
                        # 同时更新MACD极值点历史
                        self.macd_extremes_history.append((prev_macd_timestamp, prev_macd, 'trough'))
                        self._log.debug(f"检测到新DIF谷值: 时间{prev_macd_timestamp}, DIF{prev_macd:.6f}, 价格{prev_price:.4f}")
                    
                    self.check_divergence(bar, action)
                else:
                    self._log.debug(f"略过DIF{macd_type}: 时间{prev_macd_timestamp}, DIF{prev_macd:.6f} (差异太小)")
            else:
                self._log.debug(f"未检测到DIF极值点: 上一个DIF={prev_macd:.6f}")
    
    def handle_top_divergence(self, bar: Bar, action: str):
        """处理顶背离信号"""
        self._log.info("检测到顶背离信号：DIF创新高但价格未创新高，看跌信号")
        self.last_divergence_signal = "top_divergence"
        
        # 累积卖出信号
        if action == 'keep':
            self.technical_signal -= 30
        self._log.info(f"顶背离信号累积: 当前信号值={self.technical_signal}")
        
        # 记录顶背离技术信号
        divergence_signal = {
            'timestamp': pd.to_datetime(bar.ts_event, unit='ns'),
            'price': bar.close.as_double(),
            'signal_type': 'top_divergence',
            'divergence_type': 'bearish',
            'signal_value': self.technical_signal
        }
        self.technical_signals.append(divergence_signal)
        self._log.info(f"记录顶背离技术信号: {divergence_signal}")
        
        # 检查是否达到卖出阈值
        if self.technical_signal <= self.sell_threshold:
            self._log.info(f"顶背离卖出信号达到阈值{self.sell_threshold}，执行卖出操作")
            self.execute_divergence_sell_signal(bar)
            self.technical_signal = 0  # 信号归零
    
    def handle_bottom_divergence(self, bar: Bar, action: str):
        """处理底背离信号"""
        self._log.info("检测到底背离信号：DIF创新低但价格未创新低，看涨信号")
        self.last_divergence_signal = "bottom_divergence"
        
        # 累积买入信号
        if action == 'keep':
            self.technical_signal += 30
        self._log.info(f"底背离信号累积: 当前信号值={self.technical_signal}")
        
        # 记录底背离技术信号
        divergence_signal = {
            'timestamp': pd.to_datetime(bar.ts_event, unit='ns'),
            'price': bar.close.as_double(),
            'signal_type': 'bottom_divergence',
            'divergence_type': 'bullish',
            'signal_value': self.technical_signal
        }
        self.technical_signals.append(divergence_signal)
        self._log.info(f"记录底背离技术信号: {divergence_signal}")
        
        # 检查是否达到买入阈值
        if self.technical_signal >= self.buy_threshold:
            self._log.info(f"底背离买入信号达到阈值{self.buy_threshold}，执行买入操作")
            self.execute_divergence_buy_signal(bar)
            self.technical_signal = 0  # 信号归零
    
    def execute_buy_signal(self, bar: Bar):
        """执行买入信号"""
        # 检查是否已有持仓 - 使用可靠的持仓查询方法
        # current_position = self.get_current_position()
        # if current_position is not None:
        #     self._log.info(f"已有持仓: {current_position.quantity.as_double()} 股，跳过买入信号")
        #     return
        
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
        
        # 记录实际交易信号
        trade_signal = {
            'timestamp': pd.to_datetime(bar.ts_event, unit='ns'),
            'price': bar.close.as_double(),
            'side': 'BUY',
            'quantity': trade_quantity.as_double(),
            'order_id': str(order.client_order_id),
            'signal_type': 'executed_buy',
            'signal_value': self.technical_signal
        }
        self.trade_signals.append(trade_signal)
        
        self._log.info(f"执行买入交易: 数量={trade_quantity}, 价格={bar.close.as_double():.4f}")
    
    def execute_sell_signal(self, bar: Bar):
        """执行卖出信号"""
        # 检查当前时间是否在2:50分之后，如果是则跳过卖出操作
        if self.is_after_scheduled_time(bar):
            self._log.info(f"当前时间已过2:50分，跳过卖出信号执行")
            return
        
        # 检查是否有持仓 - 使用可靠的持仓查询方法
        # current_position = self.get_current_position()
        # if current_position is None:
        #     self._log.info("没有持仓，跳过卖出信号")
        #     return
        
        # 记录实际交易信号
        trade_signal = {
            'timestamp': pd.to_datetime(bar.ts_event, unit='ns'),
            'price': bar.close.as_double(),
            'side': 'SELL',
            'order_id': 'close_position',
            'signal_type': 'executed_sell',
            'signal_value': self.technical_signal
        }
        self.trade_signals.append(trade_signal)
        
        # 执行全部平仓操作
        self.close_all_positions(self.config.instrument_id, tags=["EXIT"])
        self._log.info(f"执行全部卖出交易: 价格={bar.close.as_double():.4f}")
    
    def execute_divergence_buy_signal(self, bar: Bar):
        """执行背离买入信号"""
        # 检查是否已有持仓
        # current_position = self.get_current_position()
        # if current_position is not None:
        #     self._log.info(f"已有持仓: {current_position.quantity.as_double()} 股，跳过背离买入信号")
        #     return
        
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
        
        # 记录实际交易信号
        trade_signal = {
            'timestamp': pd.to_datetime(bar.ts_event, unit='ns'),
            'price': bar.close.as_double(),
            'side': 'BUY',
            'quantity': trade_quantity.as_double(),
            'order_id': str(order.client_order_id),
            'signal_type': 'executed_divergence_buy',
            'signal_value': self.technical_signal
        }
        self.trade_signals.append(trade_signal)
        
        self._log.info(f"执行背离买入交易: 数量={trade_quantity}, 价格={bar.close.as_double():.4f}")
    
    def execute_divergence_sell_signal(self, bar: Bar):
        """执行背离卖出信号"""
        # 检查当前时间是否在2:50分之后，如果是则跳过卖出操作
        if self.is_after_scheduled_time(bar):
            self._log.info(f"当前时间已过2:50分，跳过背离卖出信号执行")
            return
        
        # 检查是否有持仓
        # current_position = self.get_current_position()
        # if current_position is None:
        #     self._log.info("没有持仓，跳过背离卖出信号")
        #     return
        
        # 记录实际交易信号
        trade_signal = {
            'timestamp': pd.to_datetime(bar.ts_event, unit='ns'),
            'price': bar.close.as_double(),
            'side': 'SELL',
            'order_id': 'close_position',
            'signal_type': 'executed_divergence_sell',
            'signal_value': self.technical_signal
        }
        self.trade_signals.append(trade_signal)
        
        # 执行全部平仓操作
        self.close_all_positions(self.config.instrument_id, tags=["EXIT"])
        self._log.info(f"执行背离全部卖出交易: 价格={bar.close.as_double():.4f}")
    
    def check_risk_management(self, bar: Bar):
        """检查风险管理"""
        # 检查当前时间是否在2:50分之后，如果是则跳过风险管理
        if self.is_after_scheduled_time(bar):
            self._log.info(f"当前时间已过2:50分，跳过风险管理检查")
            return
        
        # 检查是否有持仓 - 使用可靠的持仓查询方法
        # current_position = self.get_current_position()
        # if current_position is None:
        #     return
        
        # current_price = bar.close.as_double()
        # entry_price = current_position.avg_px_open
        
        # # 计算盈亏百分比
        # pnl_pct = (current_price - entry_price) / entry_price
        
        # # 止损检查
        # if pnl_pct <= -self.stop_loss_pct:
        #     self.close_position(current_position)
        #     self._log.info(f"触发止损: 亏损{pnl_pct*100:.2f}%")
        
        # # 止盈检查
        # elif pnl_pct >= self.take_profit_pct:
        #     self.close_position(current_position)
        #     self._log.info(f"触发止盈: 盈利{pnl_pct*100:.2f}%")

    def get_current_position(self):
        """获取当前持仓状态 - 回测环境优化版本"""
        self._log.info("开始查询当前持仓状态...")
        
        # 方法1：检查实例变量（最优先）
        if self.position and self.position.quantity.as_double() > 0:
            self._log.info(f"从实例变量获取持仓: {self.position.quantity.as_double()} 股")
            return self.position
        elif self.position:
            self._log.info(f"实例变量中的持仓数量为0或负数: {self.position.quantity.as_double()} 股")
            # 如果实例变量中的持仓为0或负数，清空它
            self.position = None
        
        # 方法2：从缓存查询当前工具的持仓（回测推荐）
        try:
            position = self.cache.position_for_instrument(self.config.instrument_id)
            if position and position.quantity.as_double() > 0:
                # 更新实例变量
                self.position = position
                self._log.info(f"从缓存查询指定工具持仓成功: {position.quantity.as_double()} 股")
                return position
            elif position:
                self._log.info(f"缓存中指定工具的持仓数量为0或负数: {position.quantity.as_double()} 股")
            else:
                self._log.info("缓存中未找到指定工具的持仓")
        except Exception as e:
            self._log.info(f"从缓存查询指定工具持仓失败: {e}")
        
        # 方法3：从缓存查询所有持仓（备用方案）
        try:
            positions = self.cache.positions()
            if positions:
                self._log.info(f"缓存中共有 {len(positions)} 个持仓")
                for pos in positions:
                    self._log.info(f"持仓详情: 工具={pos.instrument_id}, 数量={pos.quantity.as_double()}, 方向={pos.side}")
                    if pos.instrument_id == self.config.instrument_id and pos.quantity.as_double() > 0:
                        self.position = pos
                        self._log.info(f"从缓存恢复持仓状态: {pos.quantity.as_double()} 股")
                        return pos
            else:
                self._log.info("缓存中没有任何持仓")
        except Exception as e:
            self._log.info(f"从缓存查询所有持仓失败: {e}")
        
        # 没有持仓
        self._log.info("确认：当前没有任何持仓")
        self.position = None
        return None

    def on_dispose(self):
        """策略销毁时调用"""
        self._log.info("ETF159506 MACD金叉死叉策略已销毁")
    
    def print_extremes_history(self):
        """打印所有极值点历史内容"""
        self._log.info("=" * 80)
        self._log.info("价格极值点历史 (price_extremes_history) 详细内容:")
        self._log.info("=" * 80)
        if self.price_extremes_history:
            for i, (timestamp, price, extreme_type) in enumerate(self.price_extremes_history):
                time_str = pd.to_datetime(timestamp, unit='ns').strftime('%Y-%m-%d %H:%M:%S')
                self._log.info(f"  [{i+1:3d}] 时间: {time_str}, 价格: {price:.4f}, 类型: {extreme_type}")
        else:
            self._log.info("  无价格极值点历史记录")
        
        self._log.info("=" * 80)
        self._log.info("MACD极值点历史 (macd_extremes_history) 详细内容:")
        self._log.info("=" * 80)
        if self.macd_extremes_history:
            for i, (timestamp, macd_value, extreme_type) in enumerate(self.macd_extremes_history):
                time_str = pd.to_datetime(timestamp, unit='ns').strftime('%Y-%m-%d %H:%M:%S')
                self._log.info(f"  [{i+1:3d}] 时间: {time_str}, DIF值: {macd_value:.6f}, 类型: {extreme_type}")
        else:
            self._log.info("  无MACD极值点历史记录")
        
        self._log.info("=" * 80)
        
        # 同时打印当前deque中的极值点
        self._log.info("当前deque中的极值点:")
        self._log.info(f"  价格峰值 (price_peaks): {len(self.price_peaks)} 个")
        if self.price_peaks:
            for i, (timestamp, price, dif_value) in enumerate(self.price_peaks):
                time_str = pd.to_datetime(timestamp, unit='ns').strftime('%Y-%m-%d %H:%M:%S')
                self._log.info(f"    [{i+1:3d}] 时间: {time_str}, 价格: {price:.4f}, DIF: {dif_value:.6f}")
        
        self._log.info(f"  价格谷值 (price_troughs): {len(self.price_troughs)} 个")
        if self.price_troughs:
            for i, (timestamp, price, dif_value) in enumerate(self.price_troughs):
                time_str = pd.to_datetime(timestamp, unit='ns').strftime('%Y-%m-%d %H:%M:%S')
                self._log.info(f"    [{i+1:3d}] 时间: {time_str}, 价格: {price:.4f}, DIF: {dif_value:.6f}")
        
        self._log.info(f"  DIF峰值 (dif_peaks): {len(self.dif_peaks)} 个")
        if self.dif_peaks:
            for i, (timestamp, dif_value, price_value) in enumerate(self.dif_peaks):
                time_str = pd.to_datetime(timestamp, unit='ns').strftime('%Y-%m-%d %H:%M:%S')
                self._log.info(f"    [{i+1:3d}] 时间: {time_str}, DIF: {dif_value:.6f}, 价格: {price_value:.4f}")
        
        self._log.info(f"  DIF谷值 (dif_troughs): {len(self.dif_troughs)} 个")
        if self.dif_troughs:
            for i, (timestamp, dif_value, price_value) in enumerate(self.dif_troughs):
                time_str = pd.to_datetime(timestamp, unit='ns').strftime('%Y-%m-%d %H:%M:%S')
                self._log.info(f"    [{i+1:3d}] 时间: {time_str}, DIF: {dif_value:.6f}, 价格: {price_value:.4f}")
        
        self._log.info("=" * 80) 

    def check_scheduled_buy(self, bar: Bar):
        """检查定时买入信号"""
        # 获取当前K线的时间（UTC时间）
        current_time_utc = pd.to_datetime(bar.ts_event, unit='ns')
        
        # 转换为北京时间（UTC+8）
        current_time_beijing = current_time_utc.tz_localize('UTC').tz_convert('Asia/Shanghai')
        current_date = current_time_beijing.date()
        current_time_only = current_time_beijing.time()
        
        # 添加调试信息
        self._log.info(f"定时买入检查: UTC时间={current_time_utc.strftime('%Y-%m-%d %H:%M:%S')}, 北京时间={current_time_beijing.strftime('%Y-%m-%d %H:%M:%S')}")
        # 转换为北京时间格式显示
        beijing_time_str = current_time_beijing.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        self._log.info(f"目标买入时间: {self.scheduled_buy_time.strftime('%H:%M:%S')}, 当前北京时间: {beijing_time_str}")
        
        # 检查是否已经在该日期执行过定时买入
        if self.last_scheduled_buy_date == current_date:
            self._log.info(f"今天 {current_date} 已经执行过定时买入，跳过")
            return
        
        # 检查是否到达定时买入时间（2:50分）
        if current_time_only >= self.scheduled_buy_time:
            self._log.info(f"到达定时买入时间: {current_time_only.strftime('%H:%M:%S')}")
            
            # 检查是否已有持仓
            # current_position = self.get_current_position()
            # if current_position is not None:
            #     self._log.info(f"已有持仓: {current_position.quantity.as_double()} 股，跳过定时买入")
            #     self.last_scheduled_buy_date = current_date
            #     return
            
            # 执行定时买入
            self.execute_scheduled_buy(bar)
            
            # 记录已执行定时买入的日期
            self.last_scheduled_buy_date = current_date
        else:
            self._log.info(f"还未到达定时买入时间，当前时间: {current_time_only.strftime('%H:%M:%S')}, 目标时间: {self.scheduled_buy_time.strftime('%H:%M:%S')}")

    def execute_scheduled_buy(self, bar: Bar):
        """执行定时买入"""
        # 获取北京时间用于日志显示
        current_time_utc = pd.to_datetime(bar.ts_event, unit='ns')
        current_time_beijing = current_time_utc.tz_localize('UTC').tz_convert('Asia/Shanghai')
        
        self._log.info(f"执行定时买入: UTC时间={current_time_utc.strftime('%Y-%m-%d %H:%M:%S')}, 北京时间={current_time_beijing.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 计算交易数量
        if self.trade_size is None:
            account = self.cache.account_for_venue(self.config.venue)
            available_balance = account.balance_total().as_double()
            current_price = bar.close.as_double()
            
            # 检查可用余额
            if available_balance <= 0:
                self._log.info(f"可用余额不足: {available_balance:.2f} CNY，跳过定时买入")
                return
                
            quantity = int(available_balance / current_price)  # 使用100%资金满仓交易
            
            # 检查计算出的数量是否有效
            if quantity <= 0:
                self._log.info(f"计算出的交易数量无效: {quantity}，跳过定时买入")
                return
                
            trade_quantity = Quantity.from_int(quantity)
        else:
            trade_quantity = self.trade_size

        # 创建市价买入订单
        order = self.order_factory.market(
            instrument_id=self.config.instrument_id,
            order_side=OrderSide.BUY,
            quantity=trade_quantity,
        )
        self.submit_order(order)
        
        # 记录定时买入交易信号
        trade_signal = {
            'timestamp': pd.to_datetime(bar.ts_event, unit='ns'),
            'price': bar.close.as_double(),
            'side': 'BUY',
            'quantity': trade_quantity.as_double(),
            'order_id': str(order.client_order_id),
            'signal_type': 'scheduled_buy',
            'signal_value': 0  # 定时买入不依赖技术指标信号
        }
        self.trade_signals.append(trade_signal)
        
        self._log.info(f"定时买入订单已提交: 数量={trade_quantity}, 价格={bar.close.as_double():.4f}")
        
    def is_after_scheduled_time(self, bar: Bar) -> bool:
        """检查当前时间是否在定时买入时间（2:50分）之后"""
        # 获取当前K线的时间（UTC时间）
        current_time_utc = pd.to_datetime(bar.ts_event, unit='ns')
        
        # 转换为北京时间（UTC+8）
        current_time_beijing = current_time_utc.tz_localize('UTC').tz_convert('Asia/Shanghai')
        current_time_only = current_time_beijing.time()
        
        # 检查是否在2:50分之后
        return current_time_only >= self.scheduled_buy_time
