#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
159506 港股通医疗ETF富国策略实现
基于MACD金叉死叉的简单交易策略
"""

from nautilus_trader.core import Data
from nautilus_trader.core.message import Event
from nautilus_trader.indicators.trend import MovingAverageConvergenceDivergence
from nautilus_trader.indicators.momentum import RelativeStrengthIndex
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
from datetime import datetime, time, date, timedelta
import pytz
import warnings
from typing import List, Dict

from etf_159506_strategy_config import ETF159506Config
from etf_159506_strategy import CatalogKDJIndicator
import matplotlib
matplotlib.use('Agg')  # 非GUI后端，避免阻塞
import matplotlib.pyplot as plt
import mplfinance as mpf
import numpy as np

# 配置中文字体支持
plt.rcParams['font.sans-serif'] = ['Microsoft YaHei', 'SimHei', 'Arial Unicode MS']  # Windows中文字体
plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示问题

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
        )
        
        # KDJ指标 - 使用自定义实现
        self.kdj = CatalogKDJIndicator(n=9, k_period=3, d_period=3)
        
        # RSI指标 - 使用官方实现 (注意：官方RSI返回0-1，需要*100转为0-100)
        self.rsi = RelativeStrengthIndex(period=6)
        
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
        self.macd_extremes_history = []    # 存储MACD极值点历史(prev_macd_timestamp, prev_macd, 'trough'))
        
        # 技术指标信号累积系统
        self.technical_signal = 0  # 技术指标信号累积值，+100买入，-100卖出
        self.buy_threshold = 100   # 买入信号阈值
        self.sell_threshold = -100 # 卖出信号阈值
        self.macd_top_signal = False
        self.macd_bottom_signal = False
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
        
        # 每分钟成交量记录（输入为1分钟Bar，无需累加）
        self.minute_volume_data = deque(maxlen=1000)  # 存储每分钟成交量数据 [(minute_key, volume), ...]
        self._log.info("每分钟成交量记录功能已启用")
        
        # MACD极值点时间差属性
        self.time_diff_minutes_from_latest_extreme = None  # 距离最近极值点的分钟数

    def on_start(self):
        """策略启动时调用"""
        bar_type = self.config.bar_type
        
        # 请求历史数据用于初始化指标
        self._log.info(f"正在请求历史数据: {bar_type}")
        
        # Register the indicators for updating
        self.register_indicator_for_bars(bar_type, self.macd)
        self.register_indicator_for_bars(bar_type, self.kdj)
        self.register_indicator_for_bars(bar_type, self.rsi)

        
        
   
        
        # 请求历史数据 - 将通过on_historical_data()方法处理
        self._log.info(f"开始请求历史数据，bar_type: {bar_type}")
        
        # 使用正确的参数调用request_bars
        self._log.info(f"bar_type类型: {type(bar_type)}, 值: {bar_type}")
        # 获取当天的时间范围
        now = pd.Timestamp.now(tz="UTC")
        start_time = now.normalize()  # 当天 00:00:00
        self._log.info(f"请求历史数据时间范围（当天）: {start_time} ")
        
        try:
            request_id = self.request_bars(bar_type, start=start_time)
            self._log.info(f"历史数据请求已发送，request_id: {request_id}")
        except Exception as e:
            self._log.error(f"请求历史数据时发生错误: {e}")
            import traceback
            self._log.error(f"错误详情: {traceback.format_exc()}")
       

        # Subscribe to real-time data - will be processed by on_bar() handler
        # 订阅实时数据
        self.subscribe_bars(bar_type)
        self._log.info(f"ETF159506 MACD金叉死叉策略已启动，订阅 {self.config.instrument_id} 的 {bar_type}")
        

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
        
        # 保存每分钟成交量数据到策略实例变量中，供回测系统获取
        if not hasattr(self, '_saved_minute_volume_data'):
            self._saved_minute_volume_data = list(self.minute_volume_data)
            self._log.info(f"策略停止时保存了 {len(self.minute_volume_data)} 分钟的成交量数据")
        
        self.print_extremes_history()
        
        # 打印每分钟成交量汇总
        self.print_minute_volume_data()
    
    def on_historical_data(self, data):
        """处理历史数据"""
        from nautilus_trader.model.data import Bar
        if type(data) is Bar:
            self._log.info(f"🎯 on_historical_data被调用！数据类型: {type(data)}")
                # 处理单条历史K线数据
            self._log.info(f"📈 接收到历史K线数据: {data.ts_event}, 价格: {data.close}")
            self._process_bar(data)
            # 转换为北京时间用于日志
            utc_time = pd.to_datetime(data.ts_event, unit='ns')
            beijing_time = utc_time.tz_localize('UTC').tz_convert('Asia/Shanghai')
            beijing_time_str = beijing_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            
            self._log.debug(f"处理历史K线: 时间={beijing_time_str}, 价格={data.close.as_double():.4f}, "
                        f"MACD初始化状态={self.macd.initialized}, 历史数据长度={len(self.macd_history)}")
    
    def _process_bar(self, bar: Bar):
        """处理单条历史K线数据"""
        
        # 记录每分钟成交量
        self.record_minute_volume(bar)
        
        # 计算图表MACD值
        chart_macd = self.calculate_chart_macd(bar)
        
        # 根据MACD初始化状态选择数据源
        if not self.macd.initialized:
            # 使用图表MACD值
            self.macd_history.append(chart_macd['macd'])
            self.signal_history.append(chart_macd['signal'])
            self.histogram_history.append(chart_macd['histogram'])
            
            # 更新价格历史数据
            self.price_history.append(bar.close.as_double())
            self.timestamps.append(bar.ts_event)
        else:
            # 使用官方MACD值更新历史数据
            self.update_history_data(bar)
        
        # 检测极值点（背离检测会在deque增加时自动执行）
        self.detect_and_record_extremes(bar)
        self.check_time_diff_minutes_MACD(bar)
        
        
        
        # 在历史数据处理中不执行交易逻辑，只初始化指标
    
    def record_minute_volume(self, bar: Bar):
        """记录每分钟成交量数据（包含OHLC信息）
        
        注意：此方法接收1分钟Bar作为输入，Bar.volume已经是该分钟的总成交量，
        直接记录即可，无需累加。数据格式直接匹配图表需求，无需二次转换。
        """
        # 转换为北京时间并格式化为ISO8601格式（图表直接使用）
        utc_time = pd.to_datetime(bar.ts_event, unit='ns')
        beijing_time = utc_time.tz_localize('UTC').tz_convert('Asia/Shanghai')
        
        # 记录数据：格式直接匹配图表需求
        self.minute_volume_data.append({
            'timestamp': beijing_time.isoformat(),  # ISO8601格式，带时区
            'price': bar.close.as_double(),         # 收盘价（图表用'price'字段）
            'open': bar.open.as_double(),
            'high': bar.high.as_double(),
            'low': bar.low.as_double(),
            'volume': bar.volume.as_double(),
        })
        
        self._log.info(f"分钟数据记录: {beijing_time.strftime('%H:%M')} - 成交量: {bar.volume.as_double():,.0f}, 收盘价: {bar.close.as_double():.3f}")
    
    def get_minute_volume_summary(self):
        """获取每分钟成交量汇总数据
        
        返回格式: [{'minute_time': 'YYYY-MM-DD HH:MM', 'total_volume': float}, ...]
        """
        summary = []
        for data in self.minute_volume_data:
            # 解析ISO8601时间戳格式为简单格式
            timestamp = pd.to_datetime(data['timestamp'], format='ISO8601')
            minute_time = timestamp.strftime('%Y-%m-%d %H:%M')
            
            summary.append({
                'minute_time': minute_time,
                'total_volume': data['volume']
            })
        
        return summary
    
    def print_minute_volume_data(self, start_time=None, end_time=None):
        """打印每分钟成交量数据"""
        summary = self.get_minute_volume_summary()
        
        if not summary:
            self._log.info("没有每分钟成交量数据可显示")
            return
        
        # 如果指定了时间范围，进行筛选
        if start_time and end_time:
            filtered_summary = [s for s in summary if start_time <= s['minute_time'] <= end_time]
            self._log.info(f"筛选时间范围: {start_time} 到 {end_time}")
            self._log.info(f"筛选到的分钟数: {len(filtered_summary)}")
        else:
            filtered_summary = summary
            self._log.info(f"总分钟数: {len(filtered_summary)}")
        
        self._log.info("\n每分钟成交量统计:")
        self._log.info("=" * 50)
        self._log.info(f"{'时间':<20} {'成交量':<15}")
        self._log.info("-" * 50)
        
        for data in filtered_summary:
            self._log.info(f"{data['minute_time']:<20} {data['total_volume']:<15}")
    
    def calculate_volume_ratio(self, index: int, current_bar: Bar) -> float:
        """
        计算成交量对比：从index开始到前一分钟的每分钟平均成交量和当前成交量的比值
        
        Args:
            index: 开始时间索引
            current_bar: 当前K线数据
            
        Returns:
            成交量比值 (当前成交量 / 历史平均成交量)
        """
        try:
            # 获取当前时间戳
            current_timestamp = current_bar.ts_event
            
            # 获取当前成交量
            current_volume = current_bar.volume.as_double()
            
            # 如果没有历史成交量数据，返回1.0
            if not self.minute_volume_data:
                self._log.info(f"无历史成交量数据，成交量比值设为1.0")
                return 0
            
        
            
            # 筛选从index到当前时间前一分钟的数据
            filtered_volumes = []
            for i in range(index, len(self.minute_volume_data)):
                filtered_volumes.append(self.minute_volume_data[i])
                self._log.info(f"成交量对比计算: 历史成交量={self.minute_volume_data[i]:.2f}")
            

            
            # 计算历史平均成交量
            avg_historical_volume = sum(filtered_volumes) / len(filtered_volumes)
            
            # 计算成交量比值
            volume_ratio = current_volume / avg_historical_volume
            
            # 转换为北京时间用于日志
        
            
            current_utc_time = pd.to_datetime(current_timestamp, unit='ns')
            current_beijing_time = current_utc_time.tz_localize('UTC').tz_convert('Asia/Shanghai')
            current_time_str = current_beijing_time.strftime('%H:%M:%S')
            
            self._log.info(f"成交量对比计算: 开始索引={index}, 当前时间={current_time_str}, "
                          f"历史数据点数={len(filtered_volumes)}, 历史平均成交量={avg_historical_volume:.2f}, "
                          f"当前成交量={current_volume:.2f}, 成交量比值={volume_ratio:.4f}")
            
            return volume_ratio
            
        except Exception as e:
            self._log.error(f"计算成交量比值时发生错误: {e}")
            return 1.0
        
    def on_bar(self, bar: Bar):
        """处理K线数据"""
        # 记录每分钟成交量
        self._process_bar(bar)
        # Show latest bars
        last_bar = self.cache.bar(self.config.bar_type)
        previous_bar = self.cache.bar(self.config.bar_type, index=1)
        self._log.info(f"Current bar:  {bar}")
        self._log.info(f"Last bar:  {last_bar}")
        self._log.info(f"Previous bar: {previous_bar}")
        # 添加调试信息
        # 转换为北京时间格式
        utc_time = pd.to_datetime(last_bar.ts_event, unit='ns')
        beijing_time = utc_time.tz_localize('UTC').tz_convert('Asia/Shanghai')
        beijing_time_str = beijing_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        self._log.info(f"处理K线: 时间={beijing_time_str}, 价格={last_bar.close.as_double():.4f}, MACD初始化状态={self.macd.initialized}")
        if self.kdj.initialized:
            self._log.info(f"KDJ状态: K={self.kdj.value_k:.2f}, D={self.kdj.value_d:.2f}, J={self.kdj.value_j:.2f}")
        if self.rsi.initialized:
            self._log.info(f"RSI状态: RSI={self.rsi.value * 100:.2f}")
        
        # 检查定时买入信号
        self.check_scheduled_buy(last_bar)
        # 统一的交易信号检测和风险管理
        self.check_macd_signals(last_bar)
        self.check_macd_top_signals(last_bar)
        self.check_macd_bottom_signals(last_bar)
        self.check_risk_management(last_bar)
        self.technical_signal = 0
        # 定期监控持仓状态（每10个K线记录一次）
        # if len(self.macd_history) % 10 == 0:
        #     current_position = self.get_current_position()
        #     if current_position:
        #         self._log.info(f"持仓状态监控: {current_position.quantity.as_double()} 股, 成本: {current_position.avg_px_open:.4f}")
        #     else:
        #         self._log.info("持仓状态监控: 无持仓")
        
        # 每分钟更新图表（非阻塞方式）
        self.update_realtime_charts(last_bar)

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
                         # 计算成交量对比
                        volume_ratio = self.calculate_volume_ratio(actual_index, bar)
                        # 动态计算信号强度：min_trough_value越小，信号强度越大
                        # 使用指数函数：signal_strength = base_signal * (1 + abs(min_trough_value) / threshold)^power
                        base_signal = 10000  # 基础信号强度
                        power = 1.5  # 指数幂，控制增长曲线
                        
                        # 计算动态信号强度
                        signal_strength = (base_signal * abs(min_trough_value)) ** power * volume_ratio
                        
                        self.technical_signal += signal_strength
                        self._log.info(f"极小值信号触发: min_trough_value={min_trough_value:.6f}, 动态信号强度={signal_strength:.1f}, 当前信号值={self.technical_signal}")
                       
                        # 记录极小值技术指标信号
                        technical_signal = {
                            'timestamp': pd.to_datetime(bar.ts_event, unit='ns'),
                            'price': bar.close.as_double(),
                            'signal_type': 'n2p',
                            'signal_value': self.technical_signal,
                            'min_trough_value': min_trough_value,
                            'min_trough_time': min_trough_time_str,
                            'rsi_value': self.rsi.value * 100 if self.rsi.initialized else None,
                            'kdj_k': self.kdj.value_k if self.kdj.initialized else None,
                            'kdj_d': self.kdj.value_d if self.kdj.initialized else None,
                            'kdj_j': self.kdj.value_j if self.kdj.initialized else None,
                            'volume_ratio': volume_ratio
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
        

    def check_macd_rank(self, extreme_type='peak'):
        """
        检查MACD极值排名：返回当前MACD值在指定类型极值中的排名比例
        
        Args:
            extreme_type (str): 极值类型，'peak'表示极大值，'trough'表示极小值
        
        Returns:
            float: 排名比例 (0=最低, 1=最高)
        """
        if not self.macd_extremes_history:
            self._log.debug("MACD极值点历史为空，无法进行排序分析")
            return 1
        
        # 获取当前MACD值
        current_macd = self.macd_history[-1] if self.macd_history else 0
        
        # 根据极值类型筛选对应的极值点
        filtered_extremes = [extreme for extreme in self.macd_extremes_history if extreme[2] == extreme_type]
        
        if not filtered_extremes:
            self._log.debug(f"没有找到{extreme_type}类型的极值点")
            return 1
        
        # 提取筛选后极值点的MACD值进行排序
        macd_values = [extreme[1] for extreme in filtered_extremes]  # extreme[1]是MACD值
        
        # 对MACD值进行排序（从小到大）
        sorted_macd_values = sorted(macd_values)
        
        # 计算当前MACD值的排名百分比
        total_count = len(sorted_macd_values)
        if total_count == 0:
            return 1
        
        # 计算有多少个值小于当前值
        values_below_current = sum(1 for val in sorted_macd_values if val < current_macd)
        
        rank_ratio = values_below_current / total_count

        self._log.info(f"MACD{extreme_type}排序分析: 总{extreme_type}点数={total_count}, 当前MACD={current_macd:.6f}")
        self._log.info(f"排名比例={rank_ratio:.3f} (0=最低, 1=最高)")
        
        return rank_ratio
    
    def check_price_rank(self, extreme_type='peak'):
        """
        检查价格极值排名：返回当前价格在指定类型极值中的排名比例
        
        Args:
            extreme_type (str): 极值类型，'peak'表示极大值，'trough'表示极小值
        
        Returns:
            float: 排名比例 (0=最低, 1=最高)
        """
        if not self.price_extremes_history:
            self._log.debug("价格极值点历史为空，无法进行排序分析")
            return 1
        
        # 获取当前价格
        current_price = self.price_history[-1] if self.price_history else 0
        
        # 根据极值类型筛选对应的极值点
        filtered_extremes = [extreme for extreme in self.price_extremes_history if extreme[2] == extreme_type]
        
        if not filtered_extremes:
            self._log.debug(f"没有找到{extreme_type}类型的极值点")
            return 1
        
        # 提取筛选后极值点的价格值进行排序
        price_values = [extreme[1] for extreme in filtered_extremes]  # extreme[1]是价格值
        
        # 对价格值进行排序（从小到大）
        sorted_price_values = sorted(price_values)
        
        # 计算当前价格的排名百分比
        total_count = len(sorted_price_values)
        if total_count == 0:
            return 1
        
        # 计算有多少个值小于当前值
        values_below_current = sum(1 for val in sorted_price_values if val < current_price)
        
        rank_ratio = values_below_current / total_count

        self._log.info(f"价格{extreme_type}排序分析: 总{extreme_type}点数={total_count}, 当前价格={current_price:.4f}")
        self._log.info(f"排名比例={rank_ratio:.3f} (0=最低, 1=最高)")
        
        return rank_ratio
    
    def check_macd_top_signals(self, bar: Bar):
        rank_ratio = self.check_macd_rank('peak')  # 比较极大值
        price_rank_ratio = self.check_price_rank('peak')
        if rank_ratio > 0.9 and price_rank_ratio > 0.9 and self.latest_extreme_type == 'peak':
            # 如果排名比例大于0.9，表示当前MACD值排在前10%（排名很好），计算成交量比值
            if self.time_diff_minutes_from_latest_extreme is not None:
                # 根据时间差计算索引
                # time_diff_minutes_from_latest_extreme 表示距离最近极值点过了多少分钟
                # 我们需要找到对应的成交量数据索引
                
                # 计算从最近极值点开始的索引
                # 假设每分钟一个数据点，时间差就是索引差
                time_diff_minutes = int(self.time_diff_minutes_from_latest_extreme)
                
                # 计算起始索引（从最近极值点开始）
                start_index = max(0, len(self.minute_volume_data) - time_diff_minutes - 1)
                
                self._log.info(f"MACD排名分析: 排名比例={rank_ratio:.3f} > 0.9")
                self._log.info(f"时间差分析: 距离最近极值点={self.time_diff_minutes_from_latest_extreme:.2f}分钟")
                self._log.info(f"成交量分析: 起始索引={start_index}, 总数据点数={len(self.minute_volume_data)}")
                
                # 计算成交量比值
                volume_ratio = self.calculate_volume_ratio(start_index, bar)
                # 避免除零错误，当rank_ratio为0时使用一个很小的值
                safe_rank_ratio = max(rank_ratio, 0.01)  # 避免除零
                self.technical_signal -= 10/volume_ratio+safe_rank_ratio*30
                self._log.info(f"成交量比值: {volume_ratio:.4f}")
                self._log.info(f"技术信号: {self.technical_signal:.2f}")
                 # 检查RSI条件
                if self.rsi.initialized:
                    rsi_value = self.rsi.value * 100
                    self.technical_signal -= rsi_value - 60
                    self._log.info(f"RSI条件满足：RSI={rsi_value:.2f} < 50，增强买入信号")
                    self._log.info(f"RSI技术信号: {self.technical_signal:.2f}")

                # 检查KDJ条件
                # 计算KDJ三个值的最大差值
                kdj_values = [self.kdj.value_k, self.kdj.value_d, self.kdj.value_j]
                kdj_max_diff = max(kdj_values) - min(kdj_values)
                
                
                self._log.info(f"KDJ分析: K={self.kdj.value_k:.2f}, D={self.kdj.value_d:.2f}, J={self.kdj.value_j:.2f}")
                
                # 如果KDJ三个值最大差值小于10且都小于20，增强信号
                if kdj_max_diff < 20:
                    self.technical_signal -= 40-kdj_max_diff
                    self._log.info("KDJ条件满足：最大差值<20且超卖，增强买入信号")
                else:
                    self._log.info("KDJ条件不满足，使用标准信号")
                self._log.info(f"KDJ技术信号: {self.technical_signal:.2f}")
        
                
                # 检查是否达到卖出阈值
                if self.technical_signal <= self.sell_threshold:
                    self._log.info(f"卖出信号达到阈值{self.sell_threshold}，执行卖出操作")
                    self.execute_sell_signal(bar, signal_type='macd_top_signals')
                    self.technical_signal = 0  # 信号归零
                    self.macd_top_signal = True
                    self.macd_bottom_signal = False
            else:
                self._log.info(f"MACD排名分析: 排名比例={rank_ratio:.3f} > 0.9, 但无时间差数据, 最近极值点类型={self.latest_extreme_type}")
        else:
            self._log.info(f"MACD排名分析: 排名比例={rank_ratio:.3f} >= 0.2, 无需计算成交量比值")
    def check_macd_bottom_signals(self, bar: Bar):
        rank_ratio = self.check_macd_rank('trough')  # 比较极小值
        if self.macd_top_signal and self.latest_extreme_type == 'trough':
            # 如果排名比例大于0.9，表示当前MACD值排在前10%（排名很好），计算成交量比值
            if self.time_diff_minutes_from_latest_extreme is not None:
                # 根据时间差计算索引
                # time_diff_minutes_from_latest_extreme 表示距离最近极值点过了多少分钟
                # 我们需要找到对应的成交量数据索引
                
                # 计算从最近极值点开始的索引
                # 假设每分钟一个数据点，时间差就是索引差
                time_diff_minutes = int(self.time_diff_minutes_from_latest_extreme)
                
                # 计算起始索引（从最近极值点开始）
                start_index = max(0, len(self.minute_volume_data) - time_diff_minutes - 1)
                
                self._log.info(f"MACD排名分析: 排名比例={rank_ratio:.3f} > 0.9")
                self._log.info(f"时间差分析: 距离最近极值点={self.time_diff_minutes_from_latest_extreme:.2f}分钟")
                self._log.info(f"成交量分析: 起始索引={start_index}, 总数据点数={len(self.minute_volume_data)}")
                
                # 计算成交量比值
                volume_ratio = self.calculate_volume_ratio(start_index, bar)
                # 避免除零错误，当rank_ratio为0时使用一个很小的值
                safe_rank_ratio = max(rank_ratio, 0.01)  # 避免除零
                self.technical_signal += 10/volume_ratio+safe_rank_ratio*30
                self._log.info(f"成交量比值: {volume_ratio:.4f}")
                self._log.info(f"技术信号: {self.technical_signal:.2f}")
                 # 检查RSI条件
                if self.rsi.initialized:
                    rsi_value = self.rsi.value * 100
                    self.technical_signal += 80 - rsi_value
                    self._log.info(f"RSI条件满足：RSI={rsi_value:.2f} < 50，增强买入信号")
                    self._log.info(f"RSI技术信号: {self.technical_signal:.2f}")

                # 检查KDJ条件
                # 计算KDJ三个值的最大差值
                kdj_values = [self.kdj.value_k, self.kdj.value_d, self.kdj.value_j]
                kdj_max_diff = max(kdj_values) - min(kdj_values)
                
                
                self._log.info(f"KDJ分析: K={self.kdj.value_k:.2f}, D={self.kdj.value_d:.2f}, J={self.kdj.value_j:.2f}")
                
                # 如果KDJ三个值最大差值小于10且都小于20，增强信号
                self.technical_signal += 60-self.kdj.value_k
                
                self._log.info(f"KDJ技术信号: {self.technical_signal:.2f}")
        
                
                # 检查是否达到买入阈值
                if self.technical_signal >= self.buy_threshold:
                    self._log.info(f"买入信号达到阈值{self.buy_threshold}，执行买入操作")
                    self.execute_buy_signal(bar, signal_type='macd_bottom_signals')
                    self.technical_signal = 0  # 信号归零
                    self.macd_top_signal = False
                    self.macd_bottom_signal = True
            else:
                self._log.info(f"MACD排名分析: 排名比例={rank_ratio:.3f} > 0.9, 但无时间差数据, 最近极值点类型={self.latest_extreme_type}")
        else:
            self._log.info(f"MACD排名分析: 排名比例={rank_ratio:.3f}, 无需计算成交量比值, 最近极值点类型={self.latest_extreme_type} 或 macd_top_signal={self.macd_top_signal}")

    def check_macd_signals(self, bar: Bar):
        """检查MACD金叉死叉信号"""
        # ====== 首先检查交易时间（午休时间不生成信号） ======
        bar_time_utc = pd.to_datetime(bar.ts_event, unit='ns')
        bar_time_beijing = bar_time_utc.tz_localize('UTC').tz_convert('Asia/Shanghai')
        current_time = bar_time_beijing.time()
        
        # 午休时间：11:30-13:00
        if time(11, 30) <= current_time < time(13, 0):
            self._log.debug(f"当前时间 {current_time} 在午休时间，跳过MACD信号检测")
            return
        
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
        if current_macd < 0 and len(self.macd_history) >= 6 and current_histogram < 0 and not self.macd_bottom_signal:
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
                # 计算最近三个MACD极值点的DIF值及其最大差值
                if len(self.macd_extremes_history) >= 3:
                    last_three_extremes = self.macd_extremes_history[-3:]
                    dif_values = [extreme[1] for extreme in last_three_extremes]
                    max_dif = max(dif_values)
                    min_dif = min(dif_values)
                    max_dif_diff = max_dif - min_dif
                    self._log.info(f"最近三个MACD极值点DIF值: {dif_values}, 最大差值: {max_dif_diff:.6f}")
                    if max_dif_diff < 0.0005:
                        self._log.info(f"当前DIF和最近三个MACD极值点DIF的最大差值小于0.0005，跳过卖出操作")
                        return
                else:
                    self._log.info("MACD极值点历史不足3个，无法计算最大DIF差值")
                # 检查当前时间是否在2:50分之后，如果是则跳过卖出操作
                if self.is_after_scheduled_time(bar):
                    self._log.info(f"当前时间已过2:50分，跳过卖出信号执行")
                    return
                # 执行全部卖出
                self.execute_sell_signal(bar ,signal_type='DIF<0且前五个DIF都是单调递减')
                
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
                rsi_value = self.rsi.value * 100
                self.technical_signal += 50 - rsi_value
                self._log.info(f"RSI条件满足：RSI={rsi_value:.2f} < 50，增强买入信号")
            else:
                rsi_status = f"{self.rsi.value * 100:.2f}" if self.rsi.initialized else "未初始化"
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
                'rsi_value': self.rsi.value * 100 if self.rsi.initialized else None,
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
            if self.rsi.initialized:
                rsi_value = self.rsi.value * 100
                if rsi_value < 50:
                    self.technical_signal -= rsi_value
                    self._log.info(f"RSI条件满足：RSI={rsi_value:.2f} < 50，增强卖出信号")
                else:
                    self._log.info(f"RSI条件不满足：RSI={rsi_value:.2f}")
            else:
                self._log.info(f"RSI条件不满足：RSI=未初始化")
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
                'rsi_value': self.rsi.value * 100 if self.rsi.initialized else None,
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
        # ====== 首先检查交易时间（午休时间不检测极值点） ======
        bar_time_utc = pd.to_datetime(bar.ts_event, unit='ns')
        bar_time_beijing = bar_time_utc.tz_localize('UTC').tz_convert('Asia/Shanghai')
        current_time = bar_time_beijing.time()
        
        # 午休时间：11:30-13:00
        if time(11, 30) <= current_time < time(13, 0):
            self._log.debug(f"当前时间 {current_time} 在午休时间，跳过极值点检测")
            return
        
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
    def check_time_diff_minutes_MACD(self, bar: Bar):
        """检查当前时间距离最近的极值点过了几分钟，并将结果存储为属性"""
        if not self.macd_extremes_history:
            self._log.debug("MACD极值点历史为空，无法计算时间差")
            self.time_diff_minutes_from_latest_extreme = None
            self.latest_extreme_type = None
            return
        
        # 获取当前时间戳（纳秒）
        current_timestamp = bar.ts_event
        
        # 获取最近的极值点
        latest_extreme = self.macd_extremes_history[-1]
        latest_extreme_timestamp = latest_extreme[0]
        latest_extreme_value = latest_extreme[1]
        latest_extreme_type = latest_extreme[2]
        
        # 计算时间差（纳秒）
        time_diff_ns = current_timestamp - latest_extreme_timestamp
        
        # 转换为分钟
        time_diff_minutes = time_diff_ns / (60 * 1_000_000_000)  # 纳秒转分钟
        
        # 存储为属性
        self.time_diff_minutes_from_latest_extreme = time_diff_minutes
        self.latest_extreme_type = latest_extreme_type
        
        # 转换为北京时间用于日志显示
        current_time_utc = pd.to_datetime(current_timestamp, unit='ns')
        current_time_beijing = current_time_utc.tz_localize('UTC').tz_convert('Asia/Shanghai')
        
        latest_time_utc = pd.to_datetime(latest_extreme_timestamp, unit='ns')
        latest_time_beijing = latest_time_utc.tz_localize('UTC').tz_convert('Asia/Shanghai')
        
        self._log.info(f"距离最近极值点时间差: {time_diff_minutes:.2f}分钟")
        self._log.info(f"当前时间: {current_time_beijing.strftime('%Y-%m-%d %H:%M:%S')}")
        self._log.info(f"最近极值点时间: {latest_time_beijing.strftime('%Y-%m-%d %H:%M:%S')}")
        self._log.info(f"最近极值点: DIF={latest_extreme_value:.6f}, 类型={latest_extreme_type}")
        
    def handle_top_divergence(self, bar: Bar, action: str):
        """处理顶背离信号"""
        # ====== 首先检查交易时间（午休时间不生成信号） ======
        bar_time_utc = pd.to_datetime(bar.ts_event, unit='ns')
        bar_time_beijing = bar_time_utc.tz_localize('UTC').tz_convert('Asia/Shanghai')
        current_time = bar_time_beijing.time()
        
        # 午休时间：11:30-13:00
        if time(11, 30) <= current_time < time(13, 0):
            self._log.debug(f"当前时间 {current_time} 在午休时间，跳过顶背离信号处理")
            return
        
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
        # ====== 首先检查交易时间（午休时间不生成信号） ======
        bar_time_utc = pd.to_datetime(bar.ts_event, unit='ns')
        bar_time_beijing = bar_time_utc.tz_localize('UTC').tz_convert('Asia/Shanghai')
        current_time = bar_time_beijing.time()
        
        # 午休时间：11:30-13:00
        if time(11, 30) <= current_time < time(13, 0):
            self._log.debug(f"当前时间 {current_time} 在午休时间，跳过底背离信号处理")
            return
        
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
    
    def execute_buy_signal(self, bar: Bar, signal_type: str = 'executed_buy'):
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
            'signal_type': signal_type,
            'signal_value': self.technical_signal
        }
        self.trade_signals.append(trade_signal)
        
        self._log.info(f"执行买入交易: 数量={trade_quantity}, 价格={bar.close.as_double():.4f}")
    
    def execute_sell_signal(self, bar: Bar, signal_type: str = 'executed_sell'):
        """执行卖出信号 - 显式创建SELL订单"""
        # 检查当前时间是否在2:50分之后，如果是则跳过卖出操作
        if self.is_after_scheduled_time(bar):
            self._log.info(f"当前时间已过2:50分，跳过卖出信号执行")
            return
        
        # 显式检查持仓（Explicit is better than implicit）
        positions = self.cache.positions_open(
            instrument_id=self.config.instrument_id,
            strategy_id=self.id
        )
        
        if not positions:
            self._log.info("没有持仓，跳过卖出信号")
            return
        
        # 获取当前持仓
        position = positions[0]
        
        # 验证持仓方向（只处理多头持仓的卖出）
        if position.side != PositionSide.LONG:
            self._log.warning(f"持仓方向不是LONG而是{position.side}，跳过卖出")
            return
        
        # 获取持仓数量
        quantity = position.quantity
        
        self._log.info(f"准备卖出: 持仓ID={position.id}, 数量={quantity}, 价格={bar.close.as_double():.4f}")
        
        # 显式创建市价SELL订单（Explicit is better than implicit）
        order = self.order_factory.market(
            instrument_id=self.config.instrument_id,
            order_side=OrderSide.SELL,  # 显式指定SELL方向
            quantity=quantity,
            reduce_only=True,  # 只减仓，不开新仓
            tags=["EXIT", signal_type]
        )
        
        # 提交订单
        self.submit_order(order, position_id=position.id)
        
        # 记录实际交易信号
        trade_signal = {
            'timestamp': pd.to_datetime(bar.ts_event, unit='ns'),
            'price': bar.close.as_double(),
            'side': 'SELL',
            'quantity': quantity.as_double(),
            'order_id': str(order.client_order_id),
            'signal_type': signal_type,
            'signal_value': self.technical_signal
        }
        self.trade_signals.append(trade_signal)
        
        self._log.info(f"执行显式SELL订单: 数量={quantity}, 价格={bar.close.as_double():.4f}, 订单ID={order.client_order_id}")
    
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
        """执行背离卖出信号 - 显式创建SELL订单"""
        # 检查当前时间是否在2:50分之后，如果是则跳过卖出操作
        if self.is_after_scheduled_time(bar):
            self._log.info(f"当前时间已过2:50分，跳过背离卖出信号执行")
            return
        
        # 显式检查持仓（Explicit is better than implicit）
        positions = self.cache.positions_open(
            instrument_id=self.config.instrument_id,
            strategy_id=self.id
        )
        
        if not positions:
            self._log.info("没有持仓，跳过背离卖出信号")
            return
        
        # 获取当前持仓
        position = positions[0]
        
        # 验证持仓方向
        if position.side != PositionSide.LONG:
            self._log.warning(f"持仓方向不是LONG而是{position.side}，跳过背离卖出")
            return
        
        # 获取持仓数量
        quantity = position.quantity
        
        self._log.info(f"准备背离卖出: 持仓ID={position.id}, 数量={quantity}, 价格={bar.close.as_double():.4f}")
        
        # 显式创建市价SELL订单
        order = self.order_factory.market(
            instrument_id=self.config.instrument_id,
            order_side=OrderSide.SELL,  # 显式指定SELL方向
            quantity=quantity,
            reduce_only=True,
            tags=["EXIT", "divergence_sell"]
        )
        
        # 提交订单
        self.submit_order(order, position_id=position.id)
        
        # 记录实际交易信号
        trade_signal = {
            'timestamp': pd.to_datetime(bar.ts_event, unit='ns'),
            'price': bar.close.as_double(),
            'side': 'SELL',
            'quantity': quantity.as_double(),
            'order_id': str(order.client_order_id),
            'signal_type': 'executed_divergence_sell',
            'signal_value': self.technical_signal
        }
        self.trade_signals.append(trade_signal)
        
        self._log.info(f"执行背离显式SELL订单: 数量={quantity}, 价格={bar.close.as_double():.4f}, 订单ID={order.client_order_id}")
    
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

        """检查定时买入信号"""
    def check_scheduled_buy(self, bar: Bar):
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
            if account is None:
                self._log.info("测试模式下无账户信息，使用固定交易数量")
                trade_quantity = Quantity.from_int(100)  # 测试模式下使用固定数量
            else:
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
    
    def update_realtime_charts(self, bar: Bar):
        """每分钟更新实时图表（非阻塞方式，数据来源于cache）"""
        try:
            # 抑制字体警告（显式静默已知的无害警告）
            warnings.filterwarnings('ignore', category=UserWarning, message='.*Glyph.*missing from font.*')
            
            # 获取当前时间（北京时间）
            current_time_utc = pd.to_datetime(bar.ts_event, unit='ns')
            current_time_beijing = current_time_utc.tz_localize('UTC').tz_convert('Asia/Shanghai')
            target_date = current_time_beijing.date()
            
            # 使用固定文件名，每次覆盖旧图片
            # 1. 生成K线图（包含买卖点）
            kline_filename = "etf_159506_realtime_kline.png"
            self.create_realtime_kline_chart(
                save_path=kline_filename,
                target_date=target_date,
                trade_signals=self.trade_signals,
                technical_signals=self.technical_signals,
            )
            self._log.info(f"实时K线图已更新: {kline_filename} (时间: {current_time_beijing.strftime('%H:%M:%S')})")
            
            # 2. 生成买卖点分析图表
            trade_points_filename = "etf_159506_realtime_trade_points.png"
            self.create_trade_points_chart(
                save_path=trade_points_filename,
                target_date=target_date,
                trade_signals=self.trade_signals
            )
            self._log.info(f"买卖点分析图表已更新: {trade_points_filename}")
            
            # 3. 生成极值点分析图表
            extremes_filename = "etf_159506_realtime_extremes.png"
            extremes_data = {
                'price_peaks': list(self.price_peaks),
                'price_troughs': list(self.price_troughs),
                'dif_peaks': list(self.dif_peaks),
                'dif_troughs': list(self.dif_troughs),
                'price_extremes_history': self.price_extremes_history.copy(),
                'macd_extremes_history': self.macd_extremes_history.copy()
            }
            self.create_extremes_chart(
                save_path=extremes_filename,
                target_date=target_date,
                extremes_data=extremes_data
            )
            self._log.info(f"极值点分析图表已更新: {extremes_filename}")
            
        except Exception as e:
            self._log.error(f"更新实时图表失败: {e}")
    
    def get_kline_data_from_cache(self, target_date=None):
        """直接从minute_volume_data获取K线数据（零转换，直接返回）"""
        try:
            if not self.minute_volume_data:
                self._log.warning("minute_volume_data中没有数据")
                return []
            
            # 如果不需要过滤日期，直接返回全部数据
            if not target_date:
                return list(self.minute_volume_data)
            
            # 需要过滤日期时，只返回匹配的数据
            kline_data = []
            for data in self.minute_volume_data:
                # 解析timestamp字段（ISO8601格式）
                timestamp = pd.to_datetime(data['timestamp'], format='ISO8601')
                
                # 过滤指定日期
                if timestamp.date() == target_date:
                    kline_data.append(data)
            
            return kline_data
            
        except Exception as e:
            self._log.error(f"从minute_volume_data获取K线数据失败: {e}")
            import traceback
            self._log.error(f"详细错误: {traceback.format_exc()}")
            return []
    
    def create_extremes_chart(self, save_path: str = None, target_date: date = None, extremes_data: Dict = None):
        """创建专门的极值点图表"""
        try:
            # 获取数据（从minute_volume_data，内存访问）
            kline_data = self.get_kline_data_from_cache()
            
            if not kline_data:
                self._log.warning("没有数据可绘制")
                return
            
            # 转换为DataFrame
            df = pd.DataFrame(kline_data)
            df['timestamp'] = pd.to_datetime(df['timestamp'], format='ISO8601')
            
            # 检查是否需要时区转换
            if df['timestamp'].dt.tz is None:
                # 假设是UTC时间，转换为北京时间
                import pytz
                utc_tz = pytz.UTC
                beijing_tz = pytz.timezone('Asia/Shanghai')
                
                # 添加UTC时区信息
                df['timestamp'] = df['timestamp'].dt.tz_localize(utc_tz)
                # 转换为北京时间
                df['timestamp'] = df['timestamp'].dt.tz_convert(beijing_tz)
                self._log.info("已将UTC时间转换为北京时间")
            
            df = df.sort_values('timestamp')
            
            # 检查数据时间范围
            start_time = df['timestamp'].min()
            end_time = df['timestamp'].max()
            data_date = start_time.date()
            self._log.info(f"数据时间范围: {start_time} 到 {end_time}")
            self._log.info(f"数据条数: {len(df)}")
            
            # 确定图表标题
            if target_date:
                chart_title = f'159506 ETF {target_date} 极值点分析 (北京时间)'
            else:
                chart_title = f'159506 ETF {data_date} 极值点分析 (北京时间)'
            
            # 过滤交易时间内的数据，正确处理午休时间
            from datetime import time as datetime_time
            morning_start = datetime_time(9, 30)
            morning_end = datetime_time(11, 30)
            afternoon_start = datetime_time(13, 0)
            afternoon_end = datetime_time(15, 0)
            
            # 分别获取上午和下午的数据
            morning_data = df[
                (df['timestamp'].dt.time >= morning_start) & 
                (df['timestamp'].dt.time <= morning_end)
            ]
            
            afternoon_data = df[
                (df['timestamp'].dt.time >= afternoon_start) & 
                (df['timestamp'].dt.time <= afternoon_end)
            ]
            
            # 合并上午和下午数据
            trading_data = pd.concat([morning_data, afternoon_data])
            
            if len(trading_data) == 0:
                self._log.warning("没有交易时间内的数据可绘制")
                return
            
            self._log.info(f"上午数据: {len(morning_data)} 条")
            self._log.info(f"下午数据: {len(afternoon_data)} 条")
            self._log.info(f"总交易数据: {len(trading_data)} 条")
            
            # 使用交易数据的时间作为索引
            trading_data = trading_data.set_index('timestamp')
            
            # 创建时间映射，处理午休时间
            from datetime import timedelta
            
            # 先过滤掉午休时间的数据（11:30-13:00），然后再创建映射
            filtered_data = trading_data[
                (trading_data.index.time < datetime_time(11, 30)) | 
                (trading_data.index.time > datetime_time(13, 0))
            ].copy()
            
            # 创建映射后的时间索引（此时长度一定匹配）
            mapped_times = []
            for timestamp in filtered_data.index:
                current_time = timestamp.time()
                if current_time < datetime_time(11, 30):
                    # 上午时间保持不变
                    mapped_time = timestamp
                else:  # current_time > datetime_time(13, 0)
                    # 下午时间减去1.5小时（午休时间），保持图表连续
                    mapped_time = timestamp - timedelta(hours=1, minutes=30)
                mapped_times.append(mapped_time)
            
            # 创建映射后的DataFrame
            mapped_df = filtered_data.copy()
            mapped_df.index = mapped_times
            mapped_df = mapped_df.sort_index()
            
            self._log.info(f"映射后数据时间范围: {mapped_df.index.min()} 到 {mapped_df.index.max()}")
            self._log.info(f"映射后数据条数: {len(mapped_df)}")
            
            # 创建图表
            fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(16, 12), height_ratios=[3, 1, 2])
            fig.suptitle(chart_title, fontsize=16, fontweight='bold')
            
            # ====== ax1主图（价格走势 + 极值点） ======
            # 绘制价格走势（使用更清晰的线条，设置较高的zorder确保在极值点之上）
            ax1.plot(mapped_df.index, mapped_df['price'], linewidth=1.0, color='darkblue', alpha=1.0, label='价格走势', zorder=20)
            
            # 添加极值点标记
            if extremes_data and len(extremes_data) > 0:
                self._log.info("开始处理极值点数据...")
                
                # 处理价格极值点
                if 'price_extremes_history' in extremes_data and extremes_data['price_extremes_history']:
                    price_extremes = extremes_data['price_extremes_history']
                    self._log.info(f"处理 {len(price_extremes)} 个价格极值点")
                    
                    for extreme in price_extremes:
                        try:
                            # 极值点格式: (timestamp, price, 'peak'/'trough')
                            extreme_timestamp = pd.to_datetime(extreme[0], unit='ns')
                            # 确保时间戳有时区信息，与mapped_df.index保持一致
                            if extreme_timestamp.tz is None:
                                extreme_timestamp = extreme_timestamp.tz_localize('UTC').tz_convert('Asia/Shanghai')
                            extreme_price = extreme[1]
                            extreme_type = extreme[2]  # 'peak' 或 'trough'
                            
                            # 应用时间映射
                            current_time = extreme_timestamp.time()
                            if current_time < datetime_time(11, 30):
                                mapped_extreme_time = extreme_timestamp
                            elif current_time > datetime_time(13, 0):
                                mapped_extreme_time = extreme_timestamp - timedelta(hours=1, minutes=30)
                            else:
                                continue
                            
                            # 检查时间范围
                            if mapped_extreme_time < mapped_df.index.min():
                                mapped_extreme_time = mapped_df.index.min()
                            elif mapped_extreme_time > mapped_df.index.max():
                                mapped_extreme_time = mapped_df.index.max()
                            
                            # 根据极值类型绘制不同的标记
                            if extreme_type == 'peak':
                                # 绘制价格峰值点（紫色菱形）
                                ax1.scatter(mapped_extreme_time, extreme_price, 
                                           color='purple', marker='D', s=100, label='', zorder=25, alpha=0.8)
                                
                                # 添加峰值标注
                                ax1.annotate(f'峰值\n{extreme_price:.5f}', 
                                           xy=(mapped_extreme_time, extreme_price),
                                           xytext=(5, 15), textcoords='offset points',
                                           fontsize=8, color='purple', weight='bold',
                                           bbox=dict(boxstyle='round,pad=0.2', facecolor='purple', alpha=0.1))
                            else:  # extreme_type == 'trough'
                                # 绘制价格谷值点（棕色菱形）
                                ax1.scatter(mapped_extreme_time, extreme_price, 
                                           color='brown', marker='D', s=100, label='', zorder=25, alpha=0.8)
                                
                                # 添加谷值标注
                                ax1.annotate(f'谷值\n{extreme_price:.5f}', 
                                           xy=(mapped_extreme_time, extreme_price),
                                           xytext=(5, -20), textcoords='offset points',
                                           fontsize=8, color='brown', weight='bold',
                                           bbox=dict(boxstyle='round,pad=0.2', facecolor='brown', alpha=0.1))
                            
                        except Exception as e:
                            self._log.warning(f"处理价格极值点失败: {e}")
                
                self._log.info("极值点处理完成")
            else:
                self._log.info("没有极值点数据")
            
            # 设置主图属性
            ax1.set_title('价格走势与极值点', fontsize=14)
            ax1.set_ylabel('价格', fontsize=12)
            ax1.grid(True, alpha=0.3)
            ax1.legend(loc='upper left')
            
            # 设置x轴格式
            ax1.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
            ax1.xaxis.set_major_locator(plt.matplotlib.dates.MinuteLocator(interval=10))
            plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)
            
            # ====== ax2成交量（按分钟聚合） ======
            # 计算每分钟成交量
            minute_volume_stats, _ = self.calculate_minute_volume()
            
            if not minute_volume_stats.empty:
                # 创建时间映射，与主图保持一致
                minute_volume_filtered = minute_volume_stats[minute_volume_stats['minute_time'].dt.time < datetime_time(11, 30)]
                minute_volume_afternoon = minute_volume_stats[minute_volume_stats['minute_time'].dt.time > datetime_time(13, 0)]
                
                # 合并上午和下午数据
                minute_volume_trading = pd.concat([minute_volume_filtered, minute_volume_afternoon])
                
                if len(minute_volume_trading) > 0:
                    # 应用时间映射
                    minute_volume_mapped = minute_volume_trading.copy()
                    minute_volume_mapped['mapped_time'] = minute_volume_mapped['minute_time'].apply(
                        lambda x: x if x.time() < datetime_time(11, 30) else x - timedelta(hours=1, minutes=30)
                    )
                    
                    # 计算涨跌颜色（基于开盘价和收盘价）
                    colors = np.where(
                        minute_volume_mapped['收盘价'] > minute_volume_mapped['开盘价'], 
                        'red', 
                        np.where(minute_volume_mapped['收盘价'] < minute_volume_mapped['开盘价'], 'green', 'gray')
                    )
                    
                    # 计算一分钟在时间轴上的宽度
                    if len(minute_volume_mapped) > 1:
                        # 计算相邻时间点的平均间隔
                        time_diffs = minute_volume_mapped['mapped_time'].diff().dropna()
                        avg_time_diff = time_diffs.mean()
                        bar_width = avg_time_diff.total_seconds() / 86400  # 转换为天为单位
                    else:
                        bar_width = 1/1440  # 默认一分钟的宽度（1/1440天）
                    
                    # 绘制每分钟成交量柱状图
                    ax2.bar(minute_volume_mapped['mapped_time'], minute_volume_mapped['总成交量'], 
                           alpha=0.7, color=colors, width=bar_width, label='每分钟成交量')
                    
                    self._log.info(f"绘制了 {len(minute_volume_mapped)} 分钟的成交量数据")
                else:
                    self._log.warning("没有交易时间内的分钟成交量数据")
            else:
                self._log.warning("无法计算分钟成交量数据")
            
            ax2.set_title('成交量', fontsize=12)
            ax2.set_ylabel('成交量', fontsize=10)
            ax2.grid(True, alpha=0.3)
            ax2.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
            ax2.xaxis.set_major_locator(plt.matplotlib.dates.MinuteLocator(interval=10))
            plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
            
            # ====== ax3 MACD副图 ======
            # 生成1分钟K线收盘价序列，用于技术指标
            minute_close = mapped_df['price'].resample('1min').last().dropna()
            minute_index = minute_close.index

            # 计算MACD
            ema12 = minute_close.ewm(span=12, adjust=False).mean()
            ema26 = minute_close.ewm(span=26, adjust=False).mean()
            dif = ema12 - ema26  # DIF
            dea = dif.ewm(span=9, adjust=False).mean()  # DEA
            macd_hist = 2 * (dif - dea) # MACD柱子
            
            # 绘制MACD
            macd_colors = np.where(macd_hist > 0, 'red', np.where(macd_hist < 0, 'green', 'gray'))
            ax3.bar(minute_index, macd_hist, color=macd_colors, width=0.0005, alpha=0.7, label='MACD柱')
            ax3.plot(minute_index, dif, color='orange', label='DIF线')
            ax3.plot(minute_index, dea, color='deepskyblue', label='DEA线')
            
            # 添加DIF极值点
            if extremes_data and len(extremes_data) > 0:
                # 处理MACD极值点
                if 'macd_extremes_history' in extremes_data and extremes_data['macd_extremes_history']:
                    macd_extremes = extremes_data['macd_extremes_history']
                    self._log.info(f"处理 {len(macd_extremes)} 个MACD极值点")
                    
                    for extreme in macd_extremes:
                        try:
                            # 极值点格式: (timestamp, dif_value, 'peak'/'trough')
                            extreme_timestamp = pd.to_datetime(extreme[0], unit='ns')
                            # 确保时间戳有时区信息，与minute_index保持一致
                            if extreme_timestamp.tz is None:
                                extreme_timestamp = extreme_timestamp.tz_localize('UTC').tz_convert('Asia/Shanghai')
                            extreme_dif = extreme[1]
                            extreme_type = extreme[2]  # 'peak' 或 'trough'
                            
                            # 应用时间映射
                            current_time = extreme_timestamp.time()
                            if current_time < datetime_time(11, 30):
                                mapped_extreme_time = extreme_timestamp
                            elif current_time > datetime_time(13, 0):
                                mapped_extreme_time = extreme_timestamp - timedelta(hours=1, minutes=30)
                            else:
                                continue
                            
                            # 检查时间范围
                            if mapped_extreme_time < minute_index.min():
                                mapped_extreme_time = minute_index.min()
                            elif mapped_extreme_time > minute_index.max():
                                mapped_extreme_time = minute_index.max()
                            
                            # 根据极值类型绘制不同的标记
                            if extreme_type == 'peak':
                                # 绘制DIF峰值点（红色三角形）
                                ax3.scatter(mapped_extreme_time, extreme_dif, 
                                           color='red', marker='^', s=80, label='', zorder=25, alpha=0.8)
                                # 添加峰值标注
                                ax3.annotate(f'峰值\n{extreme_dif:.8f}', 
                                           xy=(mapped_extreme_time, extreme_dif),
                                           xytext=(5, 15), textcoords='offset points',
                                           fontsize=8, color='red', weight='bold',
                                           bbox=dict(boxstyle='round,pad=0.2', facecolor='red', alpha=0.1))
                            else:  # extreme_type == 'trough'
                                # 绘制DIF谷值点（绿色三角形）
                                ax3.scatter(mapped_extreme_time, extreme_dif, 
                                           color='green', marker='v', s=80, label='', zorder=25, alpha=0.8)
                                # 添加谷值标注
                                ax3.annotate(f'谷值\n{extreme_dif:.8f}', 
                                           xy=(mapped_extreme_time, extreme_dif),
                                           xytext=(5, -20), textcoords='offset points',
                                           fontsize=8, color='green', weight='bold',
                                           bbox=dict(boxstyle='round,pad=0.2', facecolor='green', alpha=0.1))
                            
                        except Exception as e:
                            self._log.warning(f"处理MACD极值点失败: {e}")
                    
                    self._log.info("MACD极值点处理完成")
                else:
                    self._log.info("没有MACD极值点数据")
            
            ax3.set_title('MACD指标与DIF极值点', fontsize=12)
            ax3.set_ylabel('MACD', fontsize=10)
            ax3.set_xlabel('时间 (北京时间)', fontsize=12)
            ax3.grid(True, alpha=0.3)
            ax3.legend(loc='upper left')
            ax3.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
            ax3.xaxis.set_major_locator(plt.matplotlib.dates.MinuteLocator(interval=10))
            plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45)
            
            # 调整布局
            plt.tight_layout()
            
            # 保存图片
            if save_path:
                plt.savefig(save_path, dpi=300, bbox_inches='tight')
                self._log.info(f"极值点图表已保存到: {save_path}")
            
            # 显示图表（非阻塞模式）
            plt.show(block=False)
            
            self._log.info("极值点图表生成完成")
            
        except Exception as e:
            self._log.error(f"创建极值点图表失败: {e}")
            import traceback
            self._log.error(f"详细错误: {traceback.format_exc()}")

    def create_trade_points_chart(self, save_path: str = None, target_date: date = None, trade_signals: List[Dict] = None):
        """创建专门的买卖点图表"""
        try:
            # 获取数据（从minute_volume_data，内存访问）
            kline_data = self.get_kline_data_from_cache()
            
            if not kline_data:
                self._log.warning("没有数据可绘制")
                return
            
            # 转换为DataFrame
            df = pd.DataFrame(kline_data)
            df['timestamp'] = pd.to_datetime(df['timestamp'], format='ISO8601')
            
            # 检查是否需要时区转换
            if df['timestamp'].dt.tz is None:
                # 假设是UTC时间，转换为北京时间
                import pytz
                utc_tz = pytz.UTC
                beijing_tz = pytz.timezone('Asia/Shanghai')
                
                # 添加UTC时区信息
                df['timestamp'] = df['timestamp'].dt.tz_localize(utc_tz)
                # 转换为北京时间
                df['timestamp'] = df['timestamp'].dt.tz_convert(beijing_tz)
                self._log.info("已将UTC时间转换为北京时间")
            
            df = df.sort_values('timestamp')
            
            # 检查数据时间范围
            start_time = df['timestamp'].min()
            end_time = df['timestamp'].max()
            data_date = start_time.date()
            self._log.info(f"数据时间范围: {start_time} 到 {end_time}")
            self._log.info(f"数据条数: {len(df)}")
            
            # 确定图表标题
            if target_date:
                chart_title = f'159506 ETF {target_date} 买卖点分析 (北京时间)'
            else:
                chart_title = f'159506 ETF {data_date} 买卖点分析 (北京时间)'
            
            # 过滤交易时间内的数据，正确处理午休时间
            from datetime import time as datetime_time
            morning_start = datetime_time(9, 30)
            morning_end = datetime_time(11, 30)
            afternoon_start = datetime_time(13, 0)
            afternoon_end = datetime_time(15, 0)
            
            # 分别获取上午和下午的数据
            morning_data = df[
                (df['timestamp'].dt.time >= morning_start) & 
                (df['timestamp'].dt.time <= morning_end)
            ]
            
            afternoon_data = df[
                (df['timestamp'].dt.time >= afternoon_start) & 
                (df['timestamp'].dt.time <= afternoon_end)
            ]
            
            # 合并上午和下午数据
            trading_data = pd.concat([morning_data, afternoon_data])
            
            if len(trading_data) == 0:
                self._log.warning("没有交易时间内的数据可绘制")
                return
            
            self._log.info(f"上午数据: {len(morning_data)} 条")
            self._log.info(f"下午数据: {len(afternoon_data)} 条")
            self._log.info(f"总交易数据: {len(trading_data)} 条")
            
            # 使用交易数据的时间作为索引
            trading_data = trading_data.set_index('timestamp')
            
            # 确保索引为升序、唯一、无NaN
            complete_df = trading_data.copy()
            complete_df = complete_df.sort_index()
            complete_df = complete_df[~complete_df.index.duplicated(keep='first')]
            complete_df = complete_df[complete_df.index.notnull()]

            # 创建三联图，专门用于买卖点分析
            fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(20, 24), height_ratios=[2, 1, 1])

            # 创建时间轴映射，保持时间连续性但保留原始时间信息
            def create_time_mapping(df):
                """创建时间轴映射，保持图表连续性"""
                new_times = []
                time_mapping = {}
                original_time_mapping = {}  # 保存原始时间到映射时间的对应关系
                
                for idx in df.index:
                    current_time = idx.time()
                    
                    if current_time < datetime_time(11, 30):
                        # 上午时间保持不变
                        new_time = idx
                    elif current_time > datetime_time(13, 0):
                        # 下午时间减去1.5小时（午休时间），保持图表连续
                        new_time = idx - timedelta(hours=1, minutes=30)
                    else:
                        # 午休时间的数据跳过
                        continue
                    
                    new_times.append(new_time)
                    time_mapping[idx] = new_time
                    original_time_mapping[new_time] = idx  # 反向映射
                
                return pd.DatetimeIndex(new_times), time_mapping, original_time_mapping
            
            # 创建时间映射
            new_index, time_mapping, original_time_mapping = create_time_mapping(complete_df)
            
            # 重新索引数据
            mapped_df = complete_df[complete_df.index.isin(time_mapping.keys())].copy()
            mapped_df.index = [time_mapping[idx] for idx in mapped_df.index]
            
            # 设置x轴范围
            x_min = new_index.min()
            x_max = new_index.max()
            
            # 为每个子图设置相同的x轴范围
            for ax in [ax1, ax2, ax3]:
                ax.set_xlim(x_min, x_max)

            # ====== ax1主图（价格走势 + 买卖点） ======
            # 绘制价格走势（使用较细的线条）
            ax1.plot(mapped_df.index, mapped_df['price'], linewidth=0.8, color='lightgray', alpha=0.6, label='价格走势')
            
            # 标记关键价格点
            valid_prices = mapped_df['price'].dropna()
            if len(valid_prices) > 0:
                # 开盘价（第一个有效价格）
                open_price = valid_prices.iloc[0]
                open_time = valid_prices.index[0]
                ax1.scatter(open_time, open_price, color='black', s=80, marker='o', label='', zorder=5)
                
                # 当前价（最后一个有效价格）
                current_price = valid_prices.iloc[-1]
                current_time = valid_prices.index[-1]
                ax1.scatter(current_time, current_price, color='black', s=80, marker='o', label='', zorder=5)
                
                # 最高价
                high_price = valid_prices.max()
                high_time = valid_prices.idxmax()
                ax1.scatter(high_time, high_price, color='orange', s=60, marker='^', label='最高', zorder=5)
                
                # 最低价
                low_price = valid_prices.min()
                low_time = valid_prices.idxmin()
                ax1.scatter(low_time, low_price, color='purple', s=60, marker='v', label='最低', zorder=5)
                
                # 添加价格信息
                price_info = f'开盘: {open_price:.3f} ({open_time.strftime("%H:%M")})\n'
                price_info += f'当前: {current_price:.3f} ({current_time.strftime("%H:%M")})\n'
                price_info += f'最高: {high_price:.3f} ({high_time.strftime("%H:%M")})\n'
                price_info += f'最低: {low_price:.3f} ({low_time.strftime("%H:%M")})'
                
                ax1.text(0.02, 0.98, price_info, 
                       transform=ax1.transAxes, verticalalignment='top', 
                       bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
            
            ax1.set_title(chart_title, fontsize=16, fontweight='bold')
            ax1.set_ylabel('价格', fontsize=12)
            ax1.grid(True, alpha=0.3)
            
            # 添加买卖点标记
            if trade_signals and len(trade_signals) > 0:
                buy_signals = []
                sell_signals = []
                hold_signals = []
                watch_signals = []
                
                # 添加调试日志
                self._log.info(f"开始处理 {len(trade_signals)} 个交易信号...")
                self._log.info(f"图表时间范围: {mapped_df.index.min()} 到 {mapped_df.index.max()}")
                
                for i, signal in enumerate(trade_signals):
                    self._log.info(f"处理第 {i+1} 个信号: {signal}")
                    
                    # 转换时间戳为pandas datetime
                    if isinstance(signal['timestamp'], str):
                        signal_time = pd.to_datetime(signal['timestamp'])
                    else:
                        signal_time = signal['timestamp']
                    
                    # 确保时间戳有时区信息，与图表数据保持一致
                    if not isinstance(signal_time, pd.Timestamp):
                        signal_time = pd.Timestamp(signal_time)
                    
                    if signal_time.tz is None:
                        # 如果没有时区信息，假设是UTC时间，转换为北京时间
                        import pytz
                        utc_tz = pytz.UTC
                        beijing_tz = pytz.timezone('Asia/Shanghai')
                        signal_time = signal_time.tz_localize(utc_tz).tz_convert(beijing_tz)
                    
                    self._log.info(f"信号 {i+1} 原始时间: {signal_time}")
                    
                    # 应用相同的时间映射，保持图表连续性
                    current_time = signal_time.time()
                    if current_time < datetime_time(11, 30):
                        # 上午时间保持不变
                        mapped_signal_time = signal_time
                        self._log.info(f"信号 {i+1} 上午时间，映射后时间: {mapped_signal_time}")
                    elif current_time > datetime_time(13, 0):
                        # 下午时间减去1.5小时（午休时间），保持图表连续
                        mapped_signal_time = signal_time - timedelta(hours=1, minutes=30)
                        self._log.info(f"信号 {i+1} 下午时间，映射后时间: {mapped_signal_time}")
                    else:
                        # 午休时间的信号跳过
                        self._log.warning(f"信号 {i+1} 在午休时间 {current_time}，跳过")
                        continue
                    
                    # 检查映射后的时间是否在图表范围内，如果超出范围则调整到最近的有效时间
                    if mapped_signal_time < mapped_df.index.min():
                        self._log.warning(f"信号 {i+1} 映射后时间 {mapped_signal_time} 早于图表开始时间，调整到 {mapped_df.index.min()}")
                        mapped_signal_time = mapped_df.index.min()
                    elif mapped_signal_time > mapped_df.index.max():
                        self._log.warning(f"信号 {i+1} 映射后时间 {mapped_signal_time} 晚于图表结束时间，调整到 {mapped_df.index.max()}")
                        mapped_signal_time = mapped_df.index.max()
                    
                    self._log.info(f"信号 {i+1} 最终映射时间: {mapped_signal_time}")
                    
                    # 所有信号都添加到对应列表（经过时间调整后）
                    if signal['side'] == 'BUY':
                        buy_signals.append({
                            'timestamp': mapped_signal_time,
                            'price': signal['price'],
                            'original_time': signal_time,  # 保存原始时间
                            'signal_type': signal.get('signal_type', 'unknown')
                        })
                    elif signal['side'] == 'SELL':
                        sell_signals.append({
                            'timestamp': mapped_signal_time,
                            'price': signal['price'],
                            'original_time': signal_time,  # 保存原始时间
                            'signal_type': signal.get('signal_type', 'unknown')
                        })
                    elif signal['side'] == 'HOLD':
                        hold_signals.append({
                            'timestamp': mapped_signal_time,
                            'price': signal['price'],
                            'original_time': signal_time,  # 保存原始时间
                            'signal_type': signal.get('signal_type', 'unknown')
                        })
                    elif signal['side'] == 'WATCH':
                        watch_signals.append({
                            'timestamp': mapped_signal_time,
                            'price': signal['price'],
                            'original_time': signal_time,  # 保存原始时间
                            'signal_type': signal.get('signal_type', 'unknown')
                        })
                
                self._log.info(f"信号处理完成: 买入={len(buy_signals)}, 卖出={len(sell_signals)}, 持有={len(hold_signals)}, 观望={len(watch_signals)}")
                
                # 绘制买入点（红色三角形向上，更大更显眼）
                if buy_signals:
                    buy_df = pd.DataFrame(buy_signals)
                    ax1.scatter(buy_df['timestamp'], buy_df['price'], 
                               color='red', marker='^', s=200, label='买入信号', zorder=15, alpha=0.9)
                    # 添加买入点标注
                    for _, row in buy_df.iterrows():
                        # 显示原始时间（北京时间）
                        original_time_str = row['original_time'].strftime('%H:%M')
                        signal_type = row.get('signal_type', 'unknown')
                        ax1.annotate(f'买入\n{row["price"]:.3f}\n{original_time_str}\n{signal_type}', 
                                   xy=(row['timestamp'], row['price']),
                                   xytext=(15, 15), textcoords='offset points',
                                   fontsize=10, color='red', weight='bold',
                                   bbox=dict(boxstyle='round,pad=0.5', facecolor='red', alpha=0.3))
                
                # 绘制卖出点（绿色三角形向下，更大更显眼）
                if sell_signals:
                    sell_df = pd.DataFrame(sell_signals)
                    ax1.scatter(sell_df['timestamp'], sell_df['price'], 
                               color='green', marker='v', s=200, label='卖出信号', zorder=15, alpha=0.9)
                    # 添加卖出点标注
                    for _, row in sell_df.iterrows():
                        # 显示原始时间（北京时间）
                        original_time_str = row['original_time'].strftime('%H:%M')
                        signal_type = row.get('signal_type', 'unknown')
                        ax1.annotate(f'卖出\n{row["price"]:.3f}\n{original_time_str}\n{signal_type}', 
                                   xy=(row['timestamp'], row['price']),
                                   xytext=(15, -25), textcoords='offset points',
                                   fontsize=10, color='green', weight='bold',
                                   bbox=dict(boxstyle='round,pad=0.5', facecolor='green', alpha=0.3))
                
                # 绘制持有点（蓝色圆点）
                if hold_signals:
                    hold_df = pd.DataFrame(hold_signals)
                    ax1.scatter(hold_df['timestamp'], hold_df['price'], 
                               color='blue', marker='o', s=120, label='持有信号', zorder=15, alpha=0.8)
                    # 添加持有点标注
                    for _, row in hold_df.iterrows():
                        original_time_str = row['original_time'].strftime('%H:%M')
                        signal_type = row.get('signal_type', 'unknown')
                        ax1.annotate(f'持有\n{row["price"]:.3f}\n{original_time_str}\n{signal_type}', 
                                   xy=(row['timestamp'], row['price']),
                                   xytext=(15, 0), textcoords='offset points',
                                   fontsize=9, color='blue', weight='bold',
                                   bbox=dict(boxstyle='round,pad=0.4', facecolor='blue', alpha=0.2))
                
                # 绘制观望点（黄色方块）
                if watch_signals:
                    watch_df = pd.DataFrame(watch_signals)
                    ax1.scatter(watch_df['timestamp'], watch_df['price'], 
                               color='orange', marker='s', s=120, label='观望信号', zorder=15, alpha=0.8)
                    # 添加观望点标注
                    for _, row in watch_df.iterrows():
                        original_time_str = row['original_time'].strftime('%H:%M')
                        signal_type = row.get('signal_type', 'unknown')
                        ax1.annotate(f'观望\n{row["price"]:.3f}\n{original_time_str}\n{signal_type}', 
                                   xy=(row['timestamp'], row['price']),
                                   xytext=(15, 0), textcoords='offset points',
                                   fontsize=9, color='orange', weight='bold',
                                   bbox=dict(boxstyle='round,pad=0.4', facecolor='orange', alpha=0.2))
                
                self._log.info(f"添加了 {len(buy_signals)} 个买入点、{len(sell_signals)} 个卖出点、{len(hold_signals)} 个持有点、{len(watch_signals)} 个观望点")
            else:
                self._log.info("没有交易信号数据")
            
            # 添加信号类型说明
            # signal_info = "信号说明:\n"
            # signal_info += "▲ 买入信号: 金叉出现，无持仓时买入\n"
            # signal_info += "▼ 卖出信号: 死叉出现，有持仓时卖出\n"
            # signal_info += "● 持有信号: 金叉出现，已有持仓时持有\n"
            # signal_info += "■ 观望信号: 死叉出现，无持仓时观望"
            
            # ax1.text(0.02, 0.85, signal_info, 
            #        transform=ax1.transAxes, verticalalignment='top', 
            #        bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.8),
            #        fontsize=10)
            
            # ax1.legend(loc='upper right')
            
            # 设置x轴格式 - 显示北京时间
            ax1.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
            ax1.xaxis.set_major_locator(plt.matplotlib.dates.MinuteLocator(interval=10))  # 每10分钟一个刻度
            plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)
            
            # 设置y轴格式 - 价格三位小数
            import matplotlib.ticker as mticker
            ax1.yaxis.set_major_formatter(mticker.FormatStrFormatter('%.3f'))

            # 添加x轴标签（明确标注北京时间）
            ax1.set_xlabel('时间 (北京时间)', fontsize=13)
            ax1.annotate('所有横轴时间均为北京时间', xy=(1, 0), xycoords='axes fraction', fontsize=11, color='gray', ha='right', va='top')
            
            # ====== ax2成交量（按分钟聚合） ======
            # 计算每分钟成交量
            minute_volume_stats, _ = self.calculate_minute_volume()
            
            if not minute_volume_stats.empty:
                # 创建时间映射，与主图保持一致
                minute_volume_filtered = minute_volume_stats[minute_volume_stats['minute_time'].dt.time < datetime_time(11, 30)]
                minute_volume_afternoon = minute_volume_stats[minute_volume_stats['minute_time'].dt.time > datetime_time(13, 0)]
                
                # 合并上午和下午数据
                minute_volume_trading = pd.concat([minute_volume_filtered, minute_volume_afternoon])
                
                if len(minute_volume_trading) > 0:
                    # 应用时间映射
                    minute_volume_mapped = minute_volume_trading.copy()
                    minute_volume_mapped['mapped_time'] = minute_volume_mapped['minute_time'].apply(
                        lambda x: x if x.time() < datetime_time(11, 30) else x - timedelta(hours=1, minutes=30)
                    )
                    
                    # 计算涨跌颜色（基于开盘价和收盘价）
                    colors = np.where(
                        minute_volume_mapped['收盘价'] > minute_volume_mapped['开盘价'], 
                        'red', 
                        np.where(minute_volume_mapped['收盘价'] < minute_volume_mapped['开盘价'], 'green', 'gray')
                    )
                    
                    # 计算一分钟在时间轴上的宽度
                    if len(minute_volume_mapped) > 1:
                        # 计算相邻时间点的平均间隔
                        time_diffs = minute_volume_mapped['mapped_time'].diff().dropna()
                        avg_time_diff = time_diffs.mean()
                        bar_width = avg_time_diff.total_seconds() / 86400  # 转换为天为单位
                    else:
                        bar_width = 1/1440  # 默认一分钟的宽度（1/1440天）
                    
                    # 绘制每分钟成交量柱状图
                    ax2.bar(minute_volume_mapped['mapped_time'], minute_volume_mapped['总成交量'], 
                           alpha=0.7, color=colors, width=bar_width, label='每分钟成交量')
                    
                    self._log.info(f"绘制了 {len(minute_volume_mapped)} 分钟的成交量数据")
                else:
                    self._log.warning("没有交易时间内的分钟成交量数据")
            else:
                self._log.warning("无法计算分钟成交量数据")
                ax2.set_title('成交量', fontsize=12)
                ax2.set_ylabel('成交量', fontsize=10)
                ax2.grid(True, alpha=0.3)
                
                # 设置x轴格式
                ax2.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
                ax2.xaxis.set_major_locator(plt.matplotlib.dates.MinuteLocator(interval=10))
                plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
            
            # ====== ax3 MACD指标 ======
            # 生成1分钟K线收盘价序列，用于技术指标
            minute_close = mapped_df['price'].resample('1min').last().dropna()
            minute_index = minute_close.index
            
            if len(minute_close) > 0:
                ema12 = minute_close.ewm(span=12, adjust=False).mean()
                ema26 = minute_close.ewm(span=26, adjust=False).mean()
                dif = ema12 - ema26  # DIF
                dea = dif.ewm(span=9, adjust=False).mean()  # DEA
                macd_hist = 2 * (dif - dea) # MACD柱子
                macd_colors = np.where(macd_hist > 0, 'red', np.where(macd_hist < 0, 'green', 'gray'))
                
                ax3.bar(minute_index, macd_hist, color=macd_colors, width=0.0005, alpha=0.7, label='MACD柱')
                ax3.plot(minute_index, dif, color='orange', linewidth=1.5, label='DIF线')
                ax3.plot(minute_index, dea, color='deepskyblue', linewidth=1.5, label='DEA线')
                ax3.set_title('MACD指标 (12,26,9)', fontsize=12)
                ax3.set_ylabel('MACD', fontsize=10)
                ax3.set_xlabel('时间 (北京时间)', fontsize=13)
                ax3.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
                ax3.xaxis.set_major_locator(plt.matplotlib.dates.MinuteLocator(interval=10))
                plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45)
                ax3.legend(loc='upper right')
                ax3.grid(True, alpha=0.3)
            
            # 调整布局
            plt.tight_layout()
            
            # 保存图片
            if save_path:
                plt.savefig(save_path, dpi=300, bbox_inches='tight')
                self._log.info(f"买卖点图表已保存到: {save_path}")
            
            # 显示图表
            plt.show(block=False)            
            self._log.info("买卖点图表生成完成")
            
        except Exception as e:
            self._log.error(f"生成买卖点图表失败: {e}")
            import traceback
            self._log.error(f"详细错误: {traceback.format_exc()}")

    def create_realtime_kline_chart(self, save_path: str = None, target_date: date = None, trade_signals: List[Dict] = None, technical_signals: List[Dict] = None):
        """绘制价格走势图"""
        try:
            # 获取数据（从minute_volume_data，内存访问）
            kline_data = self.get_kline_data_from_cache()
            
            if not kline_data:
                self._log.warning("没有数据可绘制")
                return
            
            # 转换为DataFrame
            df = pd.DataFrame(kline_data)
            df['timestamp'] = pd.to_datetime(df['timestamp'], format='ISO8601')
            
            # 检查是否需要时区转换
            if df['timestamp'].dt.tz is None:
                # 假设是UTC时间，转换为北京时间
                import pytz
                utc_tz = pytz.UTC
                beijing_tz = pytz.timezone('Asia/Shanghai')
                
                # 添加UTC时区信息
                df['timestamp'] = df['timestamp'].dt.tz_localize(utc_tz)
                # 转换为北京时间
                df['timestamp'] = df['timestamp'].dt.tz_convert(beijing_tz)
                self._log.info("已将UTC时间转换为北京时间")
            
            df = df.sort_values('timestamp')
            
            # 检查数据时间范围
            start_time = df['timestamp'].min()
            end_time = df['timestamp'].max()
            data_date = start_time.date()
            self._log.info(f"数据时间范围: {start_time} 到 {end_time}")
            self._log.info(f"数据条数: {len(df)}")
            
            # 确定图表标题
            if target_date:
                chart_title = f'159506 ETF {target_date} 价格走势 (北京时间)'
            else:
                chart_title = f'159506 ETF {data_date} 价格走势 (北京时间)'
            
            # 过滤交易时间内的数据，正确处理午休时间
            from datetime import time as datetime_time
            morning_start = datetime_time(9, 30)
            morning_end = datetime_time(11, 30)
            afternoon_start = datetime_time(13, 0)
            afternoon_end = datetime_time(15, 0)
            
            # 分别获取上午和下午的数据
            morning_data = df[
                (df['timestamp'].dt.time >= morning_start) & 
                (df['timestamp'].dt.time <= morning_end)
            ]
            
            afternoon_data = df[
                (df['timestamp'].dt.time >= afternoon_start) & 
                (df['timestamp'].dt.time <= afternoon_end)
            ]
            
            # 合并上午和下午数据
            trading_data = pd.concat([morning_data, afternoon_data])
            
            if len(trading_data) == 0:
                self._log.warning("没有交易时间内的数据可绘制")
                return
            
            self._log.info(f"上午数据: {len(morning_data)} 条")
            self._log.info(f"下午数据: {len(afternoon_data)} 条")
            self._log.info(f"总交易数据: {len(trading_data)} 条")
            
            # 使用交易数据的时间作为索引
            trading_data = trading_data.set_index('timestamp')
            
            # 确保索引为升序、唯一、无NaN
            complete_df = trading_data.copy()
            complete_df = complete_df.sort_index()
            complete_df = complete_df[~complete_df.index.duplicated(keep='first')]
            complete_df = complete_df[complete_df.index.notnull()]

            # 创建五联图，图片高度更大
            fig, (ax1, ax2, ax3, ax4, ax5) = plt.subplots(5, 1, figsize=(20, 40), height_ratios=[3, 1, 1, 1, 1])

            # 创建时间轴映射，保持时间连续性但保留原始时间信息
            def create_time_mapping(df):
                """创建时间轴映射，保持图表连续性"""
                new_times = []
                time_mapping = {}
                original_time_mapping = {}  # 保存原始时间到映射时间的对应关系
                
                for idx in df.index:
                    current_time = idx.time()
                    
                    if current_time < datetime_time(11, 30):
                        # 上午时间保持不变
                        new_time = idx
                    elif current_time > datetime_time(13, 0):
                        # 下午时间减去1.5小时（午休时间），保持图表连续
                        new_time = idx - timedelta(hours=1, minutes=30)
                    else:
                        # 午休时间的数据跳过
                        continue
                    
                    new_times.append(new_time)
                    time_mapping[idx] = new_time
                    original_time_mapping[new_time] = idx  # 反向映射
                
                return pd.DatetimeIndex(new_times), time_mapping, original_time_mapping
            
            # 创建时间映射
            new_index, time_mapping, original_time_mapping = create_time_mapping(complete_df)
            
            # 重新索引数据
            mapped_df = complete_df[complete_df.index.isin(time_mapping.keys())].copy()
            mapped_df.index = [time_mapping[idx] for idx in mapped_df.index]
            
            # 设置x轴范围
            x_min = new_index.min()
            x_max = new_index.max()
            
            # 为每个子图设置相同的x轴范围
            for ax in [ax1, ax2, ax3, ax4, ax5]:
                ax.set_xlim(x_min, x_max)
            


            # ====== ax1主图（价格走势） ======
            # 绘制价格走势
            ax1.plot(mapped_df.index, mapped_df['price'], linewidth=1, color='blue', alpha=0.8, label='成交价')
            
            # 标记关键价格点
            valid_prices = mapped_df['price'].dropna()
            if len(valid_prices) > 0:
                # 开盘价（第一个有效价格）
                open_price = valid_prices.iloc[0]
                open_time = valid_prices.index[0]
                ax1.scatter(open_time, open_price, color='green', s=100, marker='o', label='开盘')
                
                # 当前价（最后一个有效价格）
                current_price = valid_prices.iloc[-1]
                current_time = valid_prices.index[-1]
                ax1.scatter(current_time, current_price, color='red', s=100, marker='o', label='当前')
                
                # 最高价
                high_price = valid_prices.max()
                high_time = valid_prices.idxmax()
                ax1.scatter(high_time, high_price, color='orange', s=80, marker='^', label='最高')
                
                # 最低价
                low_price = valid_prices.min()
                low_time = valid_prices.idxmin()
                ax1.scatter(low_time, low_price, color='purple', s=80, marker='v', label='最低')
                
                # 添加价格信息 - 显示北京时间，精确到三位小数
                price_info = f'开盘: {open_price:.3f} ({open_time.strftime("%H:%M")})\n'
                price_info += f'当前: {current_price:.3f} ({current_time.strftime("%H:%M")})\n'
                price_info += f'最高: {high_price:.3f} ({high_time.strftime("%H:%M")})\n'
                price_info += f'最低: {low_price:.3f} ({low_time.strftime("%H:%M")})'
                
                ax1.text(0.02, 0.98, price_info, 
                       transform=ax1.transAxes, verticalalignment='top', 
                       bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
            
            ax1.set_title(chart_title)
            ax1.set_ylabel('价格')
            ax1.grid(True, alpha=0.3)
            
            # 添加信号类型说明
            signal_info = "信号说明:\n"
            signal_info += "▲ 买入信号: 金叉出现，无持仓时买入\n"
            signal_info += "▼ 卖出信号: 死叉出现，有持仓时卖出\n"
            signal_info += "● 持有信号: 金叉出现，已有持仓时持有\n"
            signal_info += "■ 观望信号: 死叉出现，无持仓时观望\n"
            
            ax1.text(0.02, 0.85, signal_info, 
                   transform=ax1.transAxes, verticalalignment='top', 
                   bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.8),
                   fontsize=9)
            
            ax1.legend(loc='upper right')
            
            # 设置x轴格式 - 显示北京时间
            ax1.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
            ax1.xaxis.set_major_locator(plt.matplotlib.dates.MinuteLocator(interval=10))  # 每10分钟一个刻度
            plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)
            
            # 设置y轴格式 - 价格三位小数
            import matplotlib.ticker as mticker
            ax1.yaxis.set_major_formatter(mticker.FormatStrFormatter('%.3f'))

            # 添加x轴标签（明确标注北京时间）
            ax1.set_xlabel('时间 (北京时间)', fontsize=13)
            ax1.annotate('所有横轴时间均为北京时间', xy=(1, 0), xycoords='axes fraction', fontsize=11, color='gray', ha='right', va='top')
            
            # ====== 添加买卖点标记 ======
            # 初始化信号列表
            buy_signals = []
            sell_signals = []
            hold_signals = []
            watch_signals = []
            
            if trade_signals and len(trade_signals) > 0:
                
                # 添加调试日志
                self._log.info(f"开始处理 {len(trade_signals)} 个交易信号...")
                self._log.info(f"图表时间范围: {mapped_df.index.min()} 到 {mapped_df.index.max()}")
                
                for i, signal in enumerate(trade_signals):
                    self._log.info(f"处理第 {i+1} 个信号: {signal}")
                    
                    # 转换时间戳为pandas datetime
                    if isinstance(signal['timestamp'], str):
                        signal_time = pd.to_datetime(signal['timestamp'])
                    else:
                        signal_time = signal['timestamp']
                    
                    # 确保时间戳有时区信息，与图表数据保持一致
                    if not isinstance(signal_time, pd.Timestamp):
                        signal_time = pd.Timestamp(signal_time)
                    
                    if signal_time.tz is None:
                        # 如果没有时区信息，假设是UTC时间，转换为北京时间
                        import pytz
                        utc_tz = pytz.UTC
                        beijing_tz = pytz.timezone('Asia/Shanghai')
                        signal_time = signal_time.tz_localize(utc_tz).tz_convert(beijing_tz)
                    
                    self._log.info(f"信号 {i+1} 原始时间: {signal_time}")
                    
                    # 应用相同的时间映射，保持图表连续性
                    current_time = signal_time.time()
                    if current_time < datetime_time(11, 30):
                        # 上午时间保持不变
                        mapped_signal_time = signal_time
                        self._log.info(f"信号 {i+1} 上午时间，映射后时间: {mapped_signal_time}")
                    elif current_time > datetime_time(13, 0):
                        # 下午时间减去1.5小时（午休时间），保持图表连续
                        mapped_signal_time = signal_time - timedelta(hours=1, minutes=30)
                        self._log.info(f"信号 {i+1} 下午时间，映射后时间: {mapped_signal_time}")
                    else:
                        # 午休时间的信号跳过
                        self._log.warning(f"信号 {i+1} 在午休时间 {current_time}，跳过")
                        continue
                    
                    # 检查映射后的时间是否在图表范围内，如果超出范围则调整到最近的有效时间
                    if mapped_signal_time < mapped_df.index.min():
                        self._log.warning(f"信号 {i+1} 映射后时间 {mapped_signal_time} 早于图表开始时间，调整到 {mapped_df.index.min()}")
                        mapped_signal_time = mapped_df.index.min()
                    elif mapped_signal_time > mapped_df.index.max():
                        self._log.warning(f"信号 {i+1} 映射后时间 {mapped_signal_time} 晚于图表结束时间，调整到 {mapped_df.index.max()}")
                        mapped_signal_time = mapped_df.index.max()
                    
                    self._log.info(f"信号 {i+1} 最终映射时间: {mapped_signal_time}")
                    
                    # 所有信号都添加到对应列表（经过时间调整后）
                    if signal['side'] == 'BUY':
                        buy_signals.append({
                            'timestamp': mapped_signal_time,
                            'price': signal['price'],
                            'original_time': signal_time,  # 保存原始时间
                            'signal_type': signal.get('signal_type', 'unknown')
                        })
                    elif signal['side'] == 'SELL':
                        sell_signals.append({
                            'timestamp': mapped_signal_time,
                            'price': signal['price'],
                            'original_time': signal_time,  # 保存原始时间
                            'signal_type': signal.get('signal_type', 'unknown')
                        })
                    elif signal['side'] == 'HOLD':
                        hold_signals.append({
                            'timestamp': mapped_signal_time,
                            'price': signal['price'],
                            'original_time': signal_time,  # 保存原始时间
                            'signal_type': signal.get('signal_type', 'unknown')
                        })
                    elif signal['side'] == 'WATCH':
                        watch_signals.append({
                            'timestamp': mapped_signal_time,
                            'price': signal['price'],
                            'original_time': signal_time,  # 保存原始时间
                            'signal_type': signal.get('signal_type', 'unknown')
                        })
                
                self._log.info(f"信号处理完成: 买入={len(buy_signals)}, 卖出={len(sell_signals)}, 持有={len(hold_signals)}, 观望={len(watch_signals)}")
                self._log.info(f"总共 {len(buy_signals) + len(sell_signals) + len(hold_signals) + len(watch_signals)} 个信号被添加到图表")
            
            # ====== 添加技术指标信号标记 ======
            technical_markers = []
            if technical_signals and len(technical_signals) > 0:
                self._log.info(f"开始处理 {len(technical_signals)} 个技术指标信号...")
                
                for i, signal in enumerate(technical_signals):
                    self._log.info(f"处理第 {i+1} 个技术信号: {signal}")
                    
                    # 转换时间戳为pandas datetime
                    if isinstance(signal['timestamp'], str):
                        signal_time = pd.to_datetime(signal['timestamp'])
                    else:
                        signal_time = signal['timestamp']
                    
                    # 确保时间戳有时区信息，与图表数据保持一致
                    if not isinstance(signal_time, pd.Timestamp):
                        signal_time = pd.Timestamp(signal_time)
                    
                    if signal_time.tz is None:
                        # 如果没有时区信息，假设是UTC时间，转换为北京时间
                        import pytz
                        utc_tz = pytz.UTC
                        beijing_tz = pytz.timezone('Asia/Shanghai')
                        signal_time = signal_time.tz_localize(utc_tz).tz_convert(beijing_tz)
                    
                    # 应用相同的时间映射，保持图表连续性
                    current_time = signal_time.time()
                    if current_time < datetime_time(11, 30):
                        # 上午时间保持不变
                        mapped_signal_time = signal_time
                    elif current_time > datetime_time(13, 0):
                        # 下午时间减去1.5小时（午休时间），保持图表连续
                        mapped_signal_time = signal_time - timedelta(hours=1, minutes=30)
                    else:
                        # 午休时间的信号跳过
                        self._log.warning(f"技术信号 {i+1} 在午休时间 {current_time}，跳过")
                        continue
                    
                    # 检查映射后的时间是否在图表范围内
                    if mapped_signal_time < mapped_df.index.min():
                        mapped_signal_time = mapped_df.index.min()
                    elif mapped_signal_time > mapped_df.index.max():
                        mapped_signal_time = mapped_df.index.max()
                    
                    # 根据信号类型设置不同的标记样式
                    signal_type = signal.get('signal_type', 'unknown')
                    if signal_type == 'golden_cross':
                        marker_style = '^'  # 上三角
                        marker_color = 'green'
                        marker_size = 100
                    elif signal_type == 'death_cross':
                        marker_style = 'v'  # 下三角
                        marker_color = 'red'
                        marker_size = 100
                    elif signal_type == 'top_divergence':
                        marker_style = 's'  # 正方形
                        marker_color = 'orange'
                        marker_size = 80
                    elif signal_type == 'bottom_divergence':
                        marker_style = 's'  # 正方形
                        marker_color = 'purple'
                        marker_size = 80
                    else:
                        marker_style = 'o'  # 圆形
                        marker_color = 'gray'
                        marker_size = 60
                    
                    technical_markers.append({
                        'timestamp': mapped_signal_time,
                        'price': signal['price'],
                        'signal_type': signal_type,
                        'marker_style': marker_style,
                        'marker_color': marker_color,
                        'marker_size': marker_size,
                        'signal_value': signal.get('signal_value', 0)
                    })
                
                self._log.info(f"技术信号处理完成: {len(technical_markers)} 个技术信号被添加到图表")
                
                # 绘制买入点（红色三角形向上）
                if buy_signals:
                    buy_df = pd.DataFrame(buy_signals)
                    ax1.scatter(buy_df['timestamp'], buy_df['price'], 
                               color='red', marker='^', s=150, label='买入信号', zorder=10, alpha=0.8)
                    # 添加买入点标注
                    for _, row in buy_df.iterrows():
                        # 显示原始时间（北京时间）
                        original_time_str = row['original_time'].strftime('%H:%M')
                        ax1.annotate(f'买入\n{row["price"]:.3f}\n{original_time_str}', 
                                   xy=(row['timestamp'], row['price']),
                                   xytext=(10, 10), textcoords='offset points',
                                   fontsize=8, color='red', weight='bold',
                                   bbox=dict(boxstyle='round,pad=0.3', facecolor='red', alpha=0.2))
                
                # 绘制卖出点（绿色三角形向下）
                if sell_signals:
                    sell_df = pd.DataFrame(sell_signals)
                    ax1.scatter(sell_df['timestamp'], sell_df['price'], 
                               color='green', marker='v', s=150, label='卖出信号', zorder=10, alpha=0.8)
                    # 添加卖出点标注
                    for _, row in sell_df.iterrows():
                        # 显示原始时间（北京时间）
                        original_time_str = row['original_time'].strftime('%H:%M')
                        ax1.annotate(f'卖出\n{row["price"]:.3f}\n{original_time_str}', 
                                   xy=(row['timestamp'], row['price']),
                                   xytext=(10, -20), textcoords='offset points',
                                   fontsize=8, color='green', weight='bold',
                                   bbox=dict(boxstyle='round,pad=0.3', facecolor='green', alpha=0.2))
                
                # 绘制持有点（蓝色圆点）
                if hold_signals:
                    hold_df = pd.DataFrame(hold_signals)
                    ax1.scatter(hold_df['timestamp'], hold_df['price'], 
                               color='blue', marker='o', s=100, label='持有信号', zorder=10, alpha=0.8)
                    # 添加持有点标注
                    for _, row in hold_df.iterrows():
                        original_time_str = row['original_time'].strftime('%H:%M')
                        ax1.annotate(f'持有\n{row["price"]:.3f}\n{original_time_str}', 
                                   xy=(row['timestamp'], row['price']),
                                   xytext=(10, 0), textcoords='offset points',
                                   fontsize=8, color='blue', weight='bold',
                                   bbox=dict(boxstyle='round,pad=0.3', facecolor='blue', alpha=0.2))
                
                # 绘制观望点（黄色方块）
                if watch_signals:
                    watch_df = pd.DataFrame(watch_signals)
                    ax1.scatter(watch_df['timestamp'], watch_df['price'], 
                               color='orange', marker='s', s=100, label='观望信号', zorder=10, alpha=0.8)
                    # 添加观望点标注
                    for _, row in watch_df.iterrows():
                        original_time_str = row['original_time'].strftime('%H:%M')
                        ax1.annotate(f'观望\n{row["price"]:.3f}\n{original_time_str}', 
                                   xy=(row['timestamp'], row['price']),
                                   xytext=(10, 0), textcoords='offset points',
                                   fontsize=8, color='orange', weight='bold',
                                   bbox=dict(boxstyle='round,pad=0.3', facecolor='orange', alpha=0.2))
                
                self._log.info(f"添加了 {len(buy_signals)} 个买入点、{len(sell_signals)} 个卖出点、{len(hold_signals)} 个持有点、{len(watch_signals)} 个观望点")
            else:
                self._log.info("没有交易信号数据")
            
            # ====== 绘制技术指标信号标记 ======
            if technical_markers:
                technical_df = pd.DataFrame(technical_markers)
                for _, row in technical_df.iterrows():
                    ax1.scatter(row['timestamp'], row['price'], 
                               color=row['marker_color'], marker=row['marker_style'], 
                               s=row['marker_size'], alpha=0.7, zorder=5)
                    
                    # 添加技术信号标注
                    signal_type = row['signal_type']
                    if signal_type == 'golden_cross':
                        label = '金叉'
                        color = 'green'
                    elif signal_type == 'death_cross':
                        label = '死叉'
                        color = 'red'
                    elif signal_type == 'top_divergence':
                        label = '顶背离'
                        color = 'orange'
                    elif signal_type == 'bottom_divergence':
                        label = '底背离'
                        color = 'purple'
                    else:
                        label = signal_type
                        color = 'gray'
                    
                    ax1.annotate(f'{label}\n{row["price"]:.3f}', 
                               xy=(row['timestamp'], row['price']),
                               xytext=(5, 5), textcoords='offset points',
                               fontsize=7, color=color, weight='bold',
                               bbox=dict(boxstyle='round,pad=0.2', facecolor=color, alpha=0.1))
                
                self._log.info(f"添加了 {len(technical_markers)} 个技术指标信号标记")
            else:
                self._log.info("没有技术指标信号数据")
            
           
            # ====== ax2成交量（按分钟聚合） ======
            # 计算每分钟成交量
            minute_volume_stats, _ = self.calculate_minute_volume()
            
            # ====== DEBUG: 打印成交量数据 ======
            self._log.info(f"[DEBUG K线图] 成交量数据检查:")
            if not minute_volume_stats.empty:
                self._log.info(f"  ✓ 数据条数: {len(minute_volume_stats)}")
                self._log.info(f"  ✓ 总成交量: {minute_volume_stats['总成交量'].sum():,.0f}")
                self._log.info(f"  ✓ 平均成交量: {minute_volume_stats['总成交量'].mean():,.2f}")
                self._log.info(f"  ✓ 最大成交量: {minute_volume_stats['总成交量'].max():,.0f}")
                self._log.info(f"  ✓ 最小成交量: {minute_volume_stats['总成交量'].min():,.0f}")
                self._log.info(f"  ✓ 成交量>0: {(minute_volume_stats['总成交量'] > 0).sum()}/{len(minute_volume_stats)}")
                self._log.info(f"  ✓ 时间范围: {minute_volume_stats['minute_time'].min()} 到 {minute_volume_stats['minute_time'].max()}")
                self._log.info(f"  ✓ 前3条:")
                for idx, row in minute_volume_stats.head(3).iterrows():
                    self._log.info(f"     {row['minute_time']} | 量:{row['总成交量']:>12,.0f} | O:{row['开盘价']:.3f} C:{row['收盘价']:.3f}")
                self._log.info(f"  ✓ 后3条:")
                for idx, row in minute_volume_stats.tail(3).iterrows():
                    self._log.info(f"     {row['minute_time']} | 量:{row['总成交量']:>12,.0f} | O:{row['开盘价']:.3f} C:{row['收盘价']:.3f}")
            else:
                self._log.error(f"  ✗ minute_volume_stats 为空！")
            # ====== END DEBUG ======
            
            if not minute_volume_stats.empty:
                # 创建时间映射，与主图保持一致
                minute_volume_filtered = minute_volume_stats[minute_volume_stats['minute_time'].dt.time < datetime_time(11, 30)]
                minute_volume_afternoon = minute_volume_stats[minute_volume_stats['minute_time'].dt.time > datetime_time(13, 0)]
                
                # 合并上午和下午数据
                minute_volume_trading = pd.concat([minute_volume_filtered, minute_volume_afternoon])
                
                if len(minute_volume_trading) > 0:
                    # 应用时间映射
                    minute_volume_mapped = minute_volume_trading.copy()
                    minute_volume_mapped['mapped_time'] = minute_volume_mapped['minute_time'].apply(
                        lambda x: x if x.time() < datetime_time(11, 30) else x - timedelta(hours=1, minutes=30)
                    )
                    
                    # 计算涨跌颜色（基于开盘价和收盘价）
                    colors = np.where(
                        minute_volume_mapped['收盘价'] > minute_volume_mapped['开盘价'], 
                        'red', 
                        np.where(minute_volume_mapped['收盘价'] < minute_volume_mapped['开盘价'], 'green', 'gray')
                    )
                    
                    # 计算一分钟在时间轴上的宽度
                    if len(minute_volume_mapped) > 1:
                        # 计算相邻时间点的平均间隔
                        time_diffs = minute_volume_mapped['mapped_time'].diff().dropna()
                        avg_time_diff = time_diffs.mean()
                        bar_width = avg_time_diff.total_seconds() / 86400  # 转换为天为单位
                    else:
                        bar_width = 1/1440  # 默认一分钟的宽度（1/1440天）
                    
                    # 绘制每分钟成交量柱状图
                    ax2.bar(minute_volume_mapped['mapped_time'], minute_volume_mapped['总成交量'], 
                           alpha=0.7, color=colors, width=bar_width, label='每分钟成交量')
                    
                    self._log.info(f"绘制了 {len(minute_volume_mapped)} 分钟的成交量数据")
                else:
                    self._log.warning("没有交易时间内的分钟成交量数据")
            else:
                self._log.warning("无法计算分钟成交量数据")
            
            if target_date:
                ax2.set_title(f'每分钟成交量 {target_date} (北京时间)')
            else:
                ax2.set_title(f'每分钟成交量 {data_date} (北京时间)')
            ax2.set_ylabel('成交量')
            ax2.set_xlabel('时间 (北京时间)', fontsize=13)
            ax2.annotate('每个柱子代表一分钟的总成交量', xy=(1, 0), xycoords='axes fraction', fontsize=11, color='gray', ha='right', va='top')
            ax2.grid(True, alpha=0.3)
            
            # 设置x轴格式 - 显示北京时间
            ax2.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
            ax2.xaxis.set_major_locator(plt.matplotlib.dates.MinuteLocator(interval=10))  # 每10分钟一个刻度
            plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
            
            # ====== 生成1分钟K线收盘价序列，用于技术指标 ======
            # 使用映射后的数据重新采样，上午和下午数据直接连接
            minute_close = mapped_df['price'].resample('1min').last().dropna()
            minute_index = minute_close.index

            # ====== ax3 MACD副图（用1分钟K线收盘价） ======
            ema12 = minute_close.ewm(span=12, adjust=False).mean()
            ema26 = minute_close.ewm(span=26, adjust=False).mean()
            dif = ema12 - ema26  # DIF
            dea = dif.ewm(span=9, adjust=False).mean()  # DEA
            macd_hist = 2 * (dif - dea) # MACD柱子
            macd_colors = np.where(macd_hist > 0, 'red', np.where(macd_hist < 0, 'green', 'gray'))
            ax3.bar(minute_index, macd_hist, color=macd_colors, width=0.0005, alpha=0.7, label='MACD柱')
            ax3.plot(minute_index, dif, color='orange', label='DIF线')      # DIF橙色
            ax3.plot(minute_index, dea, color='deepskyblue', label='DEA线') # DEA天蓝色
            if target_date:
                ax3.set_title(f'MACD指标 {target_date} (12,26,9)')
            else:
                ax3.set_title(f'MACD指标 {data_date} (12,26,9)')
            ax3.set_ylabel('MACD')
            ax3.set_xlabel('时间 (北京时间)', fontsize=13)
            ax3.annotate('所有横轴时间均为北京时间', xy=(1, 0), xycoords='axes fraction', fontsize=11, color='gray', ha='right', va='top')
            ax3.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
            ax3.xaxis.set_major_locator(plt.matplotlib.dates.MinuteLocator(interval=10))
            plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45)
            ax3.legend(loc='upper right')
            ax3.grid(True, alpha=0.3)

            # ====== 计算RSI(6), RSI(12), RSI(24)（用1分钟K线收盘价） ======
            def calc_rsi(series, period):
                delta = series.diff()
                gain = delta.where(delta > 0, 0.0)
                loss = -delta.where(delta < 0, 0.0)
                avg_gain = gain.rolling(window=period, min_periods=period).mean()
                avg_loss = loss.rolling(window=period, min_periods=period).mean()
                rs = avg_gain / avg_loss
                rsi = 100 - (100 / (1 + rs))
                return rsi
            
            # 先计算RSI(1)，用于填充其他RSI的初始值
            def calc_rsi1(series):
                """计算RSI(1)，第一个值为0"""
                delta = series.diff()
                gain = delta.where(delta > 0, 0.0)
                loss = -delta.where(delta < 0, 0.0)
                # RSI(1)使用当前值，不需要移动平均
                rs = gain / loss
                rsi = 100 - (100 / (1 + rs))
                # 第一个值设为0，其他值保持不变
                rsi.iloc[0] = 0
                return rsi
            
            # 计算基础RSI值
            rsi1 = calc_rsi1(minute_close)
            rsi6_raw = calc_rsi(minute_close, 6)
            rsi12_raw = calc_rsi(minute_close, 12)
            rsi24_raw = calc_rsi(minute_close, 24)
            
            # 使用级联填充逻辑，在数据不足时用更短周期的RSI填充
            # RSI(6)在数据不足时用RSI(1)填充
            rsi6 = rsi6_raw.copy()
            for i in range(len(rsi6)):
                if pd.isna(rsi6.iloc[i]):
                    rsi6.iloc[i] = rsi1.iloc[i]
            
            # RSI(12)在数据不足时用RSI(6)填充
            rsi12 = rsi12_raw.copy()
            for i in range(len(rsi12)):
                if pd.isna(rsi12.iloc[i]):
                    rsi12.iloc[i] = rsi6.iloc[i]
            
            # RSI(24)在数据不足时用RSI(12)填充
            rsi24 = rsi24_raw.copy()
            for i in range(len(rsi24)):
                if pd.isna(rsi24.iloc[i]):
                    rsi24.iloc[i] = rsi12.iloc[i]

            # ====== ax4 RSI副图（6,12,24三线，用1分钟K线收盘价） ======
            ax4.plot(minute_index, rsi6, color='orange', label='RSI(6)')         # 橙色
            ax4.plot(minute_index, rsi12, color='deepskyblue', label='RSI(12)')  # 天蓝色
            ax4.plot(minute_index, rsi24, color='purple', label='RSI(24)')       # 紫色
            ax4.axhline(70, color='red', linestyle='--', linewidth=1, label='超买70')
            ax4.axhline(30, color='green', linestyle='--', linewidth=1, label='超卖30')
            if target_date:
                ax4.set_title(f'RSI指标 {target_date} (6,12,24)')
            else:
                ax4.set_title(f'RSI指标 {data_date} (6,12,24)')
            ax4.set_ylabel('RSI')
            ax4.set_xlabel('时间 (北京时间)', fontsize=13)
            ax4.annotate('所有横轴时间均为北京时间', xy=(1, 0), xycoords='axes fraction', fontsize=11, color='gray', ha='right', va='top')
            ax4.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
            ax4.xaxis.set_major_locator(plt.matplotlib.dates.MinuteLocator(interval=10))
            plt.setp(ax4.xaxis.get_majorticklabels(), rotation=45)
            ax4.set_ylim(0, 100)
            ax4.legend(loc='upper right')
            ax4.grid(True, alpha=0.3)

            # ====== 计算KDJ(9,3,3)（用1分钟K线收盘价） ======
            def calc_kdj(close, n=9, k_period=3, d_period=3):
                # 计算N1周期内的最低价和最高价
                low_list = close.rolling(window=n, min_periods=1).min()
                high_list = close.rolling(window=n, min_periods=1).max()
                
                # 计算RSV：RSV = (CLOSE - LLV(LOW, N1)) / (HHV(HIGH, N1) - LLV(LOW, N1)) * 100
                rsv = (close - low_list) / (high_list - low_list) * 100
                
                # 计算K值：K = MA(RSV, N2) 其中 N2 = 3
                k = rsv.rolling(window=k_period, min_periods=1).mean()
                
                # 计算D值：D = MA(K, N3) 其中 N3 = 3
                d = k.rolling(window=d_period, min_periods=1).mean()
                
                # 计算J值：J = 3*K - 2*D
                j = 3 * k - 2 * d
                
                return k, d, j
            kdj_k, kdj_d, kdj_j = calc_kdj(minute_close, n=9, k_period=3, d_period=3)

            # ====== ax5 KDJ副图（用1分钟K线收盘价） ======
            ax5.plot(minute_index, kdj_k, color='orange', label='K')         # 橙色
            ax5.plot(minute_index, kdj_d, color='deepskyblue', label='D')    # 天蓝色
            ax5.plot(minute_index, kdj_j, color='purple', label='J')         # 紫色
            if target_date:
                ax5.set_title(f'KDJ指标 {target_date} (9,3,3)')
            else:
                ax5.set_title(f'KDJ指标 {data_date} (9,3,3)')
            ax5.set_ylabel('KDJ')
            ax5.set_xlabel('时间 (北京时间)', fontsize=13)
            ax5.annotate('所有横轴时间均为北京时间', xy=(1, 0), xycoords='axes fraction', fontsize=11, color='gray', ha='right', va='top')
            ax5.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
            ax5.xaxis.set_major_locator(plt.matplotlib.dates.MinuteLocator(interval=10))
            plt.setp(ax5.xaxis.get_majorticklabels(), rotation=45)
            ax5.legend(loc='upper right')
            ax5.grid(True, alpha=0.3)

            # 统一x轴格式化，防止内容错乱
            for ax in [ax1, ax2, ax3, ax4, ax5]:
                ax.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
                # 根据数据时间跨度调整刻度间隔
                time_span = x_max - x_min
                if time_span.total_seconds() < 3600:  # 小于1小时
                    interval = 5  # 每5分钟
                elif time_span.total_seconds() < 7200:  # 小于2小时
                    interval = 10  # 每10分钟
                else:
                    interval = 15  # 每15分钟
                ax.xaxis.set_major_locator(plt.matplotlib.dates.MinuteLocator(interval=interval))
                plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

            plt.tight_layout()
            
            # 只生成一张图，且只show一次
            if save_path:
                plt.savefig(save_path, dpi=300, bbox_inches='tight')
                self._log.info(f"价格走势图已保存到: {save_path}")
            
            # 显示图表（非阻塞模式）
            plt.show(block=False)
            
        except Exception as e:
            self._log.error(f"绘制价格走势图失败: {e}")
            import traceback
            self._log.error(f"详细错误: {traceback.format_exc()}")

    def get_minute_volume_for_chart(self, target_date: date = None) -> pd.DataFrame:
        """
        直接从已记录的minute_volume_data获取成交量数据供图表使用（零转换版本）
        无需访问cache，直接使用内存中已记录的OHLC+Volume数据
        
        Args:
            target_date: 目标日期，如果为None则使用所有数据
        
        Returns:
            DataFrame: 包含列['minute_time', '总成交量', '开盘价', '收盘价', '最低价', '最高价']
        """
        try:
            if not self.minute_volume_data:
                self._log.warning("没有已记录的分钟成交量数据")
                return pd.DataFrame()
            
            # 直接从minute_volume_data转换为DataFrame（字段已匹配）
            volume_list = []
            for data in self.minute_volume_data:
                # 解析ISO8601时间戳
                timestamp = pd.to_datetime(data['timestamp'], format='ISO8601')
                
                # 如果指定了目标日期，进行过滤
                if target_date and timestamp.date() != target_date:
                    continue
                
                # 构建记录（字段名直接匹配）
                volume_list.append({
                    'minute_time': timestamp,
                    '总成交量': data['volume'],
                    '开盘价': data['open'],
                    '收盘价': data['price'],  # 已改为'price'字段
                    '最低价': data['low'],
                    '最高价': data['high'],
                })
            
            if not volume_list:
                self._log.warning(f"目标日期 {target_date} 没有成交量数据")
                return pd.DataFrame()
            
            minute_stats = pd.DataFrame(volume_list)
            
            self._log.info(f"从已记录数据获取了 {len(minute_stats)} 分钟的成交量（内存访问，无需读取cache）")
            self._log.info(f"成交量统计 - 总计: {minute_stats['总成交量'].sum():.0f}, 平均: {minute_stats['总成交量'].mean():.2f}, 最大: {minute_stats['总成交量'].max():.0f}")
            
            return minute_stats
            
        except Exception as e:
            self._log.error(f"获取分钟成交量数据失败: {e}")
            import traceback
            self._log.error(f"详细错误: {traceback.format_exc()}")
            return pd.DataFrame()
    
    def calculate_minute_volume(self, target_date: date = None) -> tuple:
        """
        兼容旧代码的方法（保留用于向后兼容）
        实际上调用 get_minute_volume_for_chart()
        
        Args:
            target_date: 目标日期，如果为None则使用今日/最近交易日
        
        Returns:
            tuple: (minute_stats, df) 包含每分钟成交量统计的数据和原始数据
        """
        minute_stats = self.get_minute_volume_for_chart(target_date)
        # 返回tuple以保持兼容性
        return minute_stats, pd.DataFrame()