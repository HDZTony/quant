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
from nautilus_trader.indicators.averages import ExponentialMovingAverage
from nautilus_trader.model import InstrumentId
from nautilus_trader.model import Position
from nautilus_trader.model.enums import OrderSide, OrderType
from nautilus_trader.model.enums import PositionSide
from nautilus_trader.model.enums import PriceType
from nautilus_trader.model.events import PositionOpened, PositionChanged, PositionClosed, OrderFilled
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.trading.strategy import StrategyConfig
from nautilus_trader.model import Quantity
from nautilus_trader.model.data import Bar
from nautilus_trader.model.currencies import CNY
from nautilus_trader.model.objects import Price
from collections import deque
import pandas as pd
from datetime import datetime, time, date, timedelta
import pytz
import warnings
from typing import List, Dict
import threading

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
        
        # MACD信号线（DEA）- 对DIF的9期EMA（增量计算）
        self.macd_signal = ExponentialMovingAverage(9)
        
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
        self.dif_history = deque(maxlen=10000)  # 存储MACD值（DIF）
        self.signal_history = deque(maxlen=10000)  # 存储信号线值（DEA）
        self.histogram_history = deque(maxlen=10000)  # 存储柱状图值
        
        # 背离检测相关历史数据存储
        self.price_history = deque(maxlen=10000)  #存储收盘价历史
        self.timestamps = deque(maxlen=10000)  # 存储K线时间戳
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
        self.technical_signal_steps = []  # 记录每分钟的计算步骤
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
        
        # 技术信号累积值历史记录（每分钟记录一次）
        self.technical_signal_history = []
        
        # 定时买入功能
        self.scheduled_buy_time = time(14, 50)  # 2:50分买入（北京时间）
        self.last_scheduled_buy_date = None  # 记录上次定时买入的日期
        self._log.info(f"定时买入功能已启用: 每天北京时间 {self.scheduled_buy_time.strftime('%H:%M')} 执行买入")
        
        # 每分钟成交量记录（输入为1分钟Bar，无需累加）
        self.minute_volume_data = deque(maxlen=1000)  # 存储每分钟成交量数据 [(minute_key, volume), ...]
        
        # 异步图表更新相关
        self._log.info("每分钟成交量记录功能已启用")
        
        # MACD极值点时间差属性
        self.time_diff_minutes_from_latest_extreme = None  # 距离最近极值点的分钟数
        
        # 死叉卖出后的买入监控
        self.first_death_cross_triggered = False  # 标记第一个死叉信号已触发
        self.first_death_cross_sold = False  # 标记第一个死叉已实际卖出
        self.monitor_histogram_shrink = False  # 是否开始监控柱状图缩小
        self._log.info("死叉卖出后买入监控已初始化: MACD柱连续2分钟缩小将增加+300技术信号")

    def _check_existing_positions(self):
        """
        检查策略启动时的现有持仓
        
        这个方法非常重要！因为：
        1. 策略可能重启，之前的持仓还在
        2. 可能有手动交易产生的持仓
        3. 需要知道当前状态才能正确交易
        """
        from nautilus_trader.core.nautilus_pyo3 import LogColor
        
        self._log.info("=" * 80, color=LogColor.CYAN)
        self._log.info("🔍 检查策略启动时的现有持仓", color=LogColor.CYAN)
        self._log.info("=" * 80, color=LogColor.CYAN)
        
        # ✅ 只用 instrument_id 过滤，不用 strategy_id
        # 原因：需要看到所有相关持仓，无论是哪个策略创建的
        open_positions = self.cache.positions_open(
            instrument_id=self.config.instrument_id
        )
        
        if not open_positions:
            self._log.info("✓ 没有现有持仓，策略可以正常开始交易", color=LogColor.GREEN)
            self._log.info("=" * 80, color=LogColor.CYAN)
            return
        
        # 如果有持仓，详细记录
        self._log.warning(
            f"⚠️ 发现 {len(open_positions)} 个现有持仓！",
            color=LogColor.YELLOW
        )
        
        for i, position in enumerate(open_positions, 1):
            self._log.info(
                f"持仓 #{i}: "
                f"标的={position.instrument_id} | "
                f"方向={position.side} | "
                f"数量={position.quantity} | "
                f"成本价={position.avg_px_open} | "
                f"策略ID={position.strategy_id}",
                color=LogColor.YELLOW
            )
        
        self._log.info("=" * 80, color=LogColor.CYAN)
    
    def on_start(self):
        """策略启动时调用"""
        bar_type = self.config.bar_type
        
        # ✅ 初始化 instrument 对象（用于价格创建）
        self.instrument = self.cache.instrument(self.config.instrument_id)
        if self.instrument is None:
            self._log.error(f"无法从缓存获取 instrument: {self.config.instrument_id}")
            raise RuntimeError(f"Instrument {self.config.instrument_id} not found in cache")
        self._log.info(f"已获取 instrument: {self.instrument.id}, 价格精度: {self.instrument.price_precision}")
        
        # ✅ 检查现有持仓
        self._check_existing_positions()
        
        # 📝 官方做法：不在 on_start 中撤销对账认领的订单
        # 说明：external_order_claims 配置认领的订单会由策略管理
        # 撤单由交易逻辑处理（每次买卖前都会撤销未成交委托）
        # 参考：nautilus_trader/test_kit/strategies/tester_exec.py
        
        # 请求历史数据用于初始化指标
        self._log.info(f"正在请求历史数据: {bar_type}")
        
        # 注册指标自动更新（框架会自动将 bar 数据推送给指标）
        self.register_indicator_for_bars(bar_type, self.macd)  # MACD（DIF）
        self.register_indicator_for_bars(bar_type, self.kdj)   # KDJ
        self.register_indicator_for_bars(bar_type, self.rsi)   # RSI
        
        # 注意：self.macd_signal（DEA）无法注册，因为它需要 DIF 值作为输入
        # DEA = EMA(DIF, 9)，将在 on_bar 中手动更新

        
        
   
        
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
        
        # ✅ 订阅订单成交事件 - 必须订阅才能接收 OrderFilled 事件
        self.subscribe_order_fills(self.config.instrument_id)
        self._log.info(f"已订阅订单成交事件: {self.config.instrument_id}")
        
        self._log.info(f"ETF159506 MACD金叉死叉策略已启动，订阅 {self.config.instrument_id} 的 {bar_type}")
        

    def _round_to_lot_size(self, quantity: int, lot_size: int = 100) -> int:
        """
        将订单数量规整到交易所要求的最小交易单位（手数）
        
        Pythonic原则：
        - Explicit: 显式的lot_size参数
        - Simple: 简单的向下取整逻辑
        - Practical: 避免订单被交易所拒绝
        
        Parameters
        ----------
        quantity : int
            原始订单数量（股）
        lot_size : int, optional
            最小交易单位（股/手），默认100股
            - 深交所ETF: 100股/手
            - 上交所ETF: 100股/手
        
        Returns
        -------
        int
            规整后的数量（保证是lot_size的整数倍）
        
        Examples
        --------
        >>> _round_to_lot_size(148460, 100)
        148400  # 向下取整到100的倍数
        >>> _round_to_lot_size(99, 100)
        0  # 不足1手，返回0
        """
        if quantity < lot_size:
            self._log.warning(f"⚠️  订单数量 {quantity} 小于最小交易单位 {lot_size}，无法交易")
            return 0
        
        # 向下取整到lot_size的整数倍
        rounded_quantity = (quantity // lot_size) * lot_size
        
        if rounded_quantity != quantity:
            self._log.info(f"📐 订单数量规整: {quantity} → {rounded_quantity} (手数: {rounded_quantity // lot_size})")
        
        return rounded_quantity
    
    def on_stop(self):
        """策略停止时调用"""

        # 取消订阅实时数据和事件
        self.unsubscribe_bars(self.config.bar_type)
        
        # ✅ 取消订阅订单成交事件 - 策略停止时清理订阅
        self.unsubscribe_order_fills(self.config.instrument_id)
        self._log.info(f"已取消订阅订单成交事件: {self.config.instrument_id}")
        
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
            # self._log.info(f"策略停止时保存了 {len(self.minute_volume_data)} 分钟的成交量数据")
        
        self.print_extremes_history()
        
        # 打印每分钟成交量汇总
        self.print_minute_volume_data()
    
    def _process_technical_signals(self, bar: Bar, beijing_time_str: str, is_historical: bool = False):
        """处理技术信号计算和显示
        
        Args:
            bar: K线数据
            beijing_time_str: 北京时间字符串
            is_historical: 是否为历史数据（历史数据不执行交易）
        
        Returns:
            float: 当前技术信号值
        """
        # 检查定时买入信号
        self.check_scheduled_buy(bar)
        # 统一的交易信号检测
        self.check_macd_signals(bar)
        self.check_macd_top_signals(bar)
        self.check_macd_bottom_signals(bar)

        # 🆕 检测死叉后MACD柱连续缩小（买入信号）
        self.check_histogram_shrink_for_rebuy()

        # 保存当前信号值
        current_technical_signal = self.technical_signal

        # ========== 输出本分钟 technical_signal 的详细计算过程 ==========
        data_type = "历史数据" if is_historical else ""
        self._log.info("=" * 80)
        self._log.info(f"【{data_type} {beijing_time_str}】technical_signal 计算过程{'汇总' if not is_historical else ''}")
        self._log.info("=" * 80)

        # 显示计算步骤
        if self.technical_signal_steps:
            self._log.info("计算步骤明细:")
            cumulative_value = 0.0
            for i, step in enumerate(self.technical_signal_steps, 1):
                cumulative_value += step['delta']
                self._log.info(f"  步骤{i}: {step['description']}")
                self._log.info(f"         变化值: {step['delta']:+.2f}, 累积值: {cumulative_value:.2f}")
        else:
            self._log.info("本分钟无信号计算步骤")

        self._log.info("-" * 80)
        self._log.info(f"最终信号值: {current_technical_signal:.2f}")
        self._log.info(f"买入阈值: {self.buy_threshold}, 卖出阈值: {self.sell_threshold}")

        # 显示当前MACD相关指标（仅实时数据）
        if not is_historical:
            if len(self.dif_history) > 0 and len(self.signal_history) > 0:
                current_dif = self.dif_history[-1]
                current_signal = self.signal_history[-1]
                current_histogram = (current_dif - current_signal) * 2
                self._log.info(f"MACD指标: DIF={current_dif:.6f}, DEA={current_signal:.6f}, 柱状图={current_histogram:.6f}")

            # 显示RSI
            if self.rsi.initialized:
                rsi_value = self.rsi.value * 100
                self._log.info(f"RSI指标: {rsi_value:.2f}")
            else:
                self._log.info("RSI指标: 未初始化")

            # 显示KDJ
            if self.kdj.initialized:
                kdj_values = [self.kdj.value_k, self.kdj.value_d, self.kdj.value_j]
                kdj_max_diff = max(kdj_values) - min(kdj_values)
                self._log.info(f"KDJ指标: K={self.kdj.value_k:.2f}, D={self.kdj.value_d:.2f}, J={self.kdj.value_j:.2f}, 最大差值={kdj_max_diff:.2f}")
            else:
                self._log.info("KDJ指标: 未初始化")

            # 显示极值点信息
            if hasattr(self, 'latest_extreme_type') and self.latest_extreme_type:
                self._log.info(f"最近极值点类型: {self.latest_extreme_type}")
                if self.time_diff_minutes_from_latest_extreme is not None:
                    self._log.info(f"距离最近极值点: {self.time_diff_minutes_from_latest_extreme:.2f}分钟")

            # 显示本分钟触发的信号类型
            if current_technical_signal > 0:
                self._log.info("信号方向: 【买入】")
            elif current_technical_signal < 0:
                self._log.info("信号方向: 【卖出】")
            else:
                self._log.info("信号方向: 【无信号】")

        self._log.info("=" * 80)
        # ========== 计算过程汇总结束 ==========

        return current_technical_signal
    
    def on_historical_data(self, data):
        """处理历史数据"""
        from nautilus_trader.model.data import Bar
        if type(data) is Bar:
            self._log.info(f"🎯 on_historical_data被调用！数据类型: {type(data)}")
            # 处理单条历史K线数据
            self._log.info(f"📈 接收到历史K线数据: {data.ts_event}, 价格: {data.close}")
            
            # 清空本分钟的计算步骤记录
            self.technical_signal_steps = []
            self.last_bar = data
            self._process_bar(data)
            
            # 转换为北京时间用于日志
            utc_time = pd.to_datetime(data.ts_event, unit='ns')
            beijing_time = utc_time.tz_localize('UTC').tz_convert('Asia/Shanghai')
            beijing_time_str = beijing_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            
            self._log.debug(f"处理历史K线: 时间={beijing_time_str}, 价格={data.close.as_double():.4f}, "
                        f"MACD初始化状态={self.macd.initialized}, 历史数据长度={len(self.dif_history)}")
            
            # 如果MACD已初始化，执行信号计算（但不执行交易）
            if self.macd.initialized:
                # 调用统一的信号处理方法
                self._process_technical_signals(data, beijing_time_str, is_historical=True)
                
                # 历史数据处理：信号归零但不执行交易
                self.technical_signal = 0
            # self.update_realtime_charts(self.last_bar)

    def _process_bar(self, bar: Bar):
        """处理单条历史K线数据"""
        
        # 记录每分钟成交量
        self.record_minute_volume(bar)
        
        # 计算图表MACD值
        chart_macd = self.calculate_chart_macd(bar)
        
        # 根据MACD初始化状态选择数据源
        if not self.macd.initialized:
            # 使用图表MACD值
            self.dif_history.append(chart_macd['macd'])
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
                return 1.0
            
        
            
            # 筛选从index到当前时间前一分钟的数据（不包括当前分钟）
            filtered_volumes = []
            # 排除最后一个数据（当前分钟），只计算历史数据
            end_index = len(self.minute_volume_data) - 1 if len(self.minute_volume_data) > 0 else 0
            for i in range(index, end_index):
                volume_value = self.minute_volume_data[i]['volume']
                filtered_volumes.append(volume_value)
                self._log.info(f"成交量对比计算: 历史成交量={volume_value:.2f}")
            
            # 如果没有历史数据，返回1.0
            if len(filtered_volumes) == 0:
                self._log.info(f"无历史成交量数据用于对比，成交量比值设为1.0")
                return 1.0
            
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
        # 注意：self.macd, self.kdj, self.rsi 已通过注册自动更新

        # 🆕 每分钟开始先检查并自动处理未成交订单（按当前价格跟价重挂）
        try:
            self._refresh_open_orders(bar)
        except Exception as e:
            self._log.warning(f"自动刷新未成交订单失败（忽略并继续处理K线）: {e}")
        
        # 手动更新级联指标：MACD信号线（DEA）
        # 原因：self.macd_signal 需要 DIF 值作为输入，无法直接注册到 bar
        if self.macd.initialized:
            self.macd_signal.update_raw(self.macd.value)
        
        # 清空本分钟的计算步骤记录
        self.technical_signal_steps = []
        
        # 记录每分钟成交量
        self._process_bar(bar)
        # Show latest bars
        last_bar = self.cache.bar(self.config.bar_type)
        previous_bar = self.cache.bar(self.config.bar_type, index=1)
        self._log.info(f"Current bar:  {bar}")
        self._log.info(f"Last bar:  {last_bar}")
        self._log.info(f"Previous bar: {previous_bar}")
        self.last_bar = last_bar
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
        
        # 调用统一的信号处理方法（包含MACD柱缩小检测）
        current_technical_signal = self._process_technical_signals(last_bar, beijing_time_str, is_historical=False)
        
        # ========== 统一判断买入/卖出阈值 ==========
        if current_technical_signal >= self.buy_threshold:
            self._log.info(f"【买入信号达到阈值】信号值={current_technical_signal:.2f} >= 买入阈值={self.buy_threshold}")
            self.execute_buy_signal(last_bar)
        elif current_technical_signal <= self.sell_threshold:
            self._log.info(f"【卖出信号达到阈值】信号值={current_technical_signal:.2f} <= 卖出阈值={self.sell_threshold}")
            self.execute_sell_signal(last_bar)
        else:
            self._log.info(f"【未达到交易阈值】信号值={current_technical_signal:.2f}, 需要达到 ≥{self.buy_threshold}(买入) 或 ≤{self.sell_threshold}(卖出)")
        # ========== 买入/卖出判断结束 ==========
        
        # 记录每分钟的技术信号累积值
        signal_record = {
            'timestamp': beijing_time.isoformat(),
            'signal_value': current_technical_signal
        }
        self.technical_signal_history.append(signal_record)
        self._log.info(f"记录技术信号历史: 时间={beijing_time_str}, 信号值={current_technical_signal:.2f}")
        
        # ✅ 更新实时图表（同步执行，保证与其他图一致）
        self.update_realtime_charts(last_bar)
        self.technical_signal = 0
    
    def _refresh_open_orders(self, bar: Bar) -> None:
        """在每分钟开始时自动处理未成交订单：按当前价格撤单重下。
        
        逻辑：
        1. 只处理当前标的 instrument_id 的限价单。
        2. 使用当前 bar 的收盘价作为基准价（买单+tick，卖单-tick）。
        3. 如果新价和旧价差距很小，则不改价，避免频繁撤单。
        4. 撤销旧订单，再以相同数量、方向、标签重下新单（增加 'AUTO_REFRESH' 标记）。
        """
        instrument_id = self.config.instrument_id
        
        # 从 Portfolio 获取当前标的的所有未成交订单
        try:
            open_orders = self.portfolio.open_orders(instrument_id)
        except Exception as e:
            self._log.warning(f"获取未成交订单失败，跳过自动改价: {e}")
            return
        
        if not open_orders:
            # self._log.info("本分钟无未成交订单，无需自动改价")
            return
        
        last_price = bar.close.as_double()
        # 价格调整步长：固定使用 0.001，与核心买入/卖出逻辑保持一致
        tick_value = 0.001
        
        self._log.info(f"本分钟检测到 {len(open_orders)} 个未成交订单，开始自动改价，当前价={last_price:.3f}")
        
        for order in open_orders:
            try:
                # 只处理限价单
                if order.order_type is not OrderType.LIMIT:
                    continue
                
                old_price = order.price.as_double()
                side = order.side
                qty = order.quantity
                
                # 计算新的跟价：在当前价格基础上 ±0.001
                if side is OrderSide.BUY:
                    new_price_value = last_price + tick_value
                else:
                    new_price_value = last_price - tick_value
                
                # 如果新价和旧价差别太小，跳过
                if abs(new_price_value - old_price) < tick_value / 2:
                    continue
                
                new_price = self.instrument.make_price(new_price_value)
                
                self._log.info(
                    f"自动跟价: 取消旧订单 {order.client_order_id}, "
                    f"{'BUY' if side is OrderSide.BUY else 'SELL'} {int(qty)} @ {old_price:.3f} -> {new_price_value:.3f}"
                )
                
                # 1）取消旧订单
                self.cancel_order(order.client_order_id)
                
                # 2）以同样数量、方向、标签重下一个新单，加上 AUTO_REFRESH 标记
                new_tags = list(order.tags) if order.tags else []
                if "AUTO_REFRESH" not in new_tags:
                    new_tags.append("AUTO_REFRESH")
                
                new_order = self.order_factory.limit(
                    instrument_id=instrument_id,
                    order_side=side,
                    quantity=qty,
                    price=new_price,
                    reduce_only=order.reduce_only,
                    tags=new_tags,
                )
                self.submit_order(new_order)
            
            except Exception as e:
                self._log.warning(f"自动改价处理订单 {getattr(order, 'client_order_id', None)} 失败: {e}")
    def on_event(self, event: Event):
        """处理所有事件"""
        pass  # 通用事件处理，具体事件由专门方法处理
    
    def on_order_filled(self, event: OrderFilled) -> None:
        """订单成交事件处理"""
        self._log.info(f"订单成交: {event.client_order_id}, 数量: {event.last_qty}, 价格: {event.last_px}")
        # 订单成交后，持仓状态会在下一个持仓事件中更新
    
    def update_history_data(self, bar: Bar):
        """更新历史数据 - 记录MACD指标到历史队列（用于回溯分析）
        
        注意：self.macd_signal 已在 on_bar 中自动更新，这里只负责记录历史数据
        """
        # 获取DIF值（已由注册机制自动更新）
        dif = self.macd.value
        
        # 【重要】始终更新价格历史数据（极值点检测需要）
        self.price_history.append(bar.close.as_double())
        self.timestamps.append(bar.ts_event)
        
        # 【重要】始终更新DIF历史数据（金叉死叉检测需要）
        self.dif_history.append(dif)
        
        # 只有在信号线初始化后才记录完整MACD指标（DEA和柱状图）
        if self.macd_signal.initialized:
            dea = self.macd_signal.value
            histogram = (dif - dea) * 2
            
            # 添加到历史数据队列（用于回溯分析）
            self.signal_history.append(dea)
            self.histogram_history.append(histogram)
            
            # 记录当前指标值
            self._log.info(
                f"MACD指标: DIF={dif:.6f}, DEA={dea:.6f}, "
                f"柱状图={histogram:.6f}"
            )
        else:
            # 初始化阶段：计算临时DEA（与calculate_chart_macd相同的逻辑）
            # 这样可以让金叉死叉检测在信号线初始化前就能工作
            if len(self.dif_history) >= 2:
                # 计算临时DEA（使用pandas ewm，与calculate_chart_macd一致）
                dif_series = pd.Series(list(self.dif_history))
                span = min(9, len(dif_series))
                temp_dea = dif_series.ewm(span=span, adjust=False).mean().iloc[-1]
                temp_histogram = (dif - temp_dea) * 2
                
                # 添加临时数据（供金叉死叉检测使用）
                self.signal_history.append(temp_dea)
                self.histogram_history.append(temp_histogram)
                
                self._log.debug(
                    f"MACD信号线初始化中... DIF={dif:.6f}, 临时DEA={temp_dea:.6f}, "
                    f"需要{9 - self.macd_signal.count}个数据点"
                )
            else:
                # 数据不足，DEA = DIF
                self.signal_history.append(dif)
                self.histogram_history.append(0.0)
                self._log.debug(
                    f"MACD信号线初始化中... DIF={dif:.6f}, 数据不足(DEA=DIF), "
                    f"需要{9 - self.macd_signal.count}个数据点"
                )
    
    def calculate_chart_macd(self, bar: Bar):
        """计算图表风格的MACD值，弥补前26分钟的空白"""
        # 获取当前价格
        current_price = bar.close.as_double()
        
        # 如果MACD指标已初始化，使用官方指标值
        if self.macd.initialized and self.macd_signal.initialized:
            dif = self.macd.value
            dea = self.macd_signal.value
            return {
                'macd': dif,  # DIF
                'signal': dea,  # DEA
                'histogram': (dif - dea) * 2  # MACD柱
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
        histogram = (dif - dea) * 2
        
        # 记录图表MACD计算
        self._log.info(f"图表MACD计算: 价格={current_price:.4f}, DIF={dif:.6f}, DEA={dea:.6f}, MACD柱={histogram:.6f}")
        
        return {
            'macd': dif,      # DIF
            'signal': dea,    # DEA
            'histogram': histogram  # MACD柱
        }
    
    def check_negative_positive_histogram(self, bar: Bar):
        """
        循环计算前几个分钟的histogram值，检测histogram由正到负再到正的模式
        
        Parameters:
        -----------
        bar : Bar
            当前K线数据
        Returns:
        --------
        dict or None
            如果检测到模式，返回包含时间点信息的字典；否则返回None
        """
        # # 检查DIF值是否由负转正
        if len(self.dif_history) >= 2:
            current_dif = self.dif_history[-1]
            previous_dif = self.dif_history[-2]
            if previous_dif < 0 and current_dif >= 0:
                self._log.info(f"DIF值由负转正: 前值={previous_dif:.6f}, 当前值={current_dif:.6f}")
            else:
                self._log.info(f"DIF值未由负转正: 前值={previous_dif:.6f}, 当前值={current_dif:.6f}")
                return 
        else:
            self._log.info("DIF历史数据不足，无法判断由负转正")
            return 
        
        # 从当前时间点往前查找三个histogram交替点（从正到负或从负到正）
        # 从第三个交替点到当前时间点，找出这段区间的最大值和最小值（保存为绝对值）
        max_histogram_abs = 0.0  # 从第三个交替点到当前时间点的最大histogram值（保存绝对值）
        min_histogram_abs = 0.0  # 从第三个交替点到当前时间点的最小histogram值（保存绝对值）
        
        if len(self.histogram_history) < 2:
            self._log.info("histogram历史数据不足，无法计算")
            return
        
        current_index = len(self.histogram_history) - 1
        
        # 往前查找三个交替点（从正到负或从负到正）
        # 交替点定义：histogram[i-1] 和 histogram[i] 符号不同
        alternating_points = []  # 存储交替点的索引
        
        for i in range(current_index, 0, -1):  # 从当前点往前查找
            if i >= len(self.histogram_history) or i < 1:
                continue
            current_hist = self.histogram_history[i]
            previous_hist = self.histogram_history[i - 1]
            
            # 检查是否是交替点（符号变化）
            if (current_hist > 0 and previous_hist < 0) or (current_hist < 0 and previous_hist > 0):
                alternating_points.append(i)
                if len(alternating_points) >= 3:
                    break
        
        # 如果找到了至少3个交替点，从第三个交替点到当前时间点找出最大值和最小值
        if len(alternating_points) >= 3:
            third_alternating_index = alternating_points[2]  # 第三个交替点（最旧的那个）
            
            self._log.info(f"找到3个交替点: {alternating_points}, 第三个交替点索引={third_alternating_index}")
            
            # 从第三个交替点到当前时间点，找出最大值和最小值
            max_value = None
            min_value = None
            
            for i in range(third_alternating_index, current_index + 1):
                hist_value = self.histogram_history[i]
                if max_value is None or hist_value > max_value:
                    max_value = hist_value
                if min_value is None or hist_value < min_value:
                    min_value = hist_value
            
            # 保存为绝对值
            if max_value is not None:
                max_histogram_abs = abs(max_value)
            if min_value is not None:
                min_histogram_abs = abs(min_value)
            
            self._log.info(f"从第三个交替点到当前时间点: 最大值={max_value:.6f}, 最小值={min_value:.6f}, "
                          f"最大值绝对值={max_histogram_abs:.6f}, 最小值绝对值={min_histogram_abs:.6f}")
        
        # 如果找到了值，计算信号强度
        if max_histogram_abs > 0 or min_histogram_abs > 0:
            # 获取当前时间戳
            if current_index < len(self.timestamps):
                
                
                # 记录histogram值信息（已经是绝对值）
                if max_histogram_abs > 0:
                    self._log.info(f"从第三个交替点到当前时间点的最大histogram绝对值: {max_histogram_abs:.6f}")
                if min_histogram_abs > 0:
                    self._log.info(f"从第三个交替点到当前时间点的最小histogram绝对值: {min_histogram_abs:.6f}")
                
                # 计算成交量对比：使用前5分钟的成交量数据
                # 计算前5分钟的平均成交量（不包括当前分钟）
                if len(self.minute_volume_data) >= 5:
                    # 从倒数第6个数据开始到倒数第1个数据（不包括当前）
                    volume_start_index = len(self.minute_volume_data) - 6
                    volume_ratio = self.calculate_volume_ratio(volume_start_index, bar)
                else:
                    # 如果数据不足5个，使用所有可用数据
                    volume_ratio = self.calculate_volume_ratio(0, bar)
                
                # 计算动态信号强度（放弃min_trough_value，只使用histogram值）
                base_signal = 60000  # 基础信号强度
                
                # 新的信号强度公式：只使用histogram值和成交量
                signal_strength = base_signal * (max_histogram_abs + min_histogram_abs) * volume_ratio
                
                self.technical_signal += signal_strength
                
                # 构建描述信息
                desc_parts = [f'histogram信号(成交量比值={volume_ratio:.4f})']
                if max_histogram_abs > 0:
                    desc_parts.append(f'从第三个交替点到当前时间点最大histogram绝对值={max_histogram_abs:.6f}')
                if min_histogram_abs > 0:
                    desc_parts.append(f'从第三个交替点到当前时间点最小histogram绝对值={min_histogram_abs:.6f}')
                
                self.technical_signal_steps.append({
                    'description': ', '.join(desc_parts),
                    'delta': signal_strength
                })
                
                log_msg = (f"histogram信号触发: 动态信号强度={signal_strength:.1f}, 当前信号值={self.technical_signal}")
                if max_histogram_abs > 0:
                    log_msg += f", 从第三个交替点到当前时间点最大histogram绝对值={max_histogram_abs:.6f}"
                if min_histogram_abs > 0:
                    log_msg += f", 从第三个交替点到当前时间点最小histogram绝对值={min_histogram_abs:.6f}"
                self._log.info(log_msg)
               
                # 记录技术指标信号
                technical_signal = {
                    'timestamp': pd.to_datetime(bar.ts_event, unit='ns'),
                    'price': bar.close.as_double(),
                    'signal_type': 'n2p',
                    'signal_value': self.technical_signal,
                    'max_histogram_abs': max_histogram_abs,  # 从第三个交替点到当前时间点的最大histogram值（保存绝对值）
                    'min_histogram_abs': min_histogram_abs,  # 从第三个交替点到当前时间点的最小histogram值（保存绝对值）
                    'rsi_value': self.rsi.value * 100 if self.rsi.initialized else None,
                    'kdj_k': self.kdj.value_k if self.kdj.initialized else None,
                    'kdj_d': self.kdj.value_d if self.kdj.initialized else None,
                    'kdj_j': self.kdj.value_j if self.kdj.initialized else None,
                    'volume_ratio': volume_ratio
                }
                
                self.technical_signals.append(technical_signal)
                self._log.info(f"记录histogram技术信号: {technical_signal}")
        else:
            self._log.info("未找到三个交替点或未找到有效的histogram值，直接增加技术信号300")
            self.technical_signal += 300
            self.technical_signal_steps.append({
                'description': '未找到三个交替点或未找到有效的histogram值',
                'delta': 300
            })
        

    def check_macd_rank(self, extreme_type):
        """
        检查MACD极值排名：返回当前MACD值在指定类型极值中的排名比例
        
        Args:
            extreme_type (str): 极值类型，'peak'表示极大值，'trough'表示极小值
        
        Returns:
            float: 排名比例 (0=最低, 1=最高)
        """
        if not self.macd_extremes_history:
            self._log.debug("MACD极值点历史为空，无法进行排序分析")
            return 0.01
        
        # 获取当前MACD值
        current_dif = self.dif_history[-1] if self.dif_history else 0
        
        # 根据极值类型筛选对应的极值点
        filtered_extremes = [extreme for extreme in self.macd_extremes_history if extreme[2] == extreme_type]
        
        if not filtered_extremes:
            self._log.debug(f"没有找到{extreme_type}类型的极值点")
            return 0.01
        
        # 提取筛选后极值点的MACD值进行排序
        macd_values = [extreme[1] for extreme in filtered_extremes]  # extreme[1]是MACD值
        
        # 对MACD值进行排序（从小到大）
        sorted_macd_values = sorted(macd_values)
        
        # 计算当前MACD值的排名百分比
        total_count = len(sorted_macd_values)
        if total_count == 0:
            return 0.01
        
        # 计算有多少个值小于当前值
        values_below_current = sum(1 for val in sorted_macd_values if val < current_dif)
        
        # 根据极值类型调整排名逻辑
        if extreme_type == 'trough':
            # 极小值：MACD值越小，排名越高（越接近1）
            rank_ratio = 1 - (values_below_current / total_count)
            self._log.info(f"MACD极小值排序分析: 总极小值点数={total_count}, 当前MACD={current_dif:.6f}")
            self._log.info(f"排名比例={rank_ratio:.3f} (值越小排名越高，1=最小值)")
        else:  # extreme_type == 'peak'
            # 极大值：MACD值越大，排名越高（越接近1）
            rank_ratio = values_below_current / total_count
            self._log.info(f"MACD极大值排序分析: 总极大值点数={total_count}, 当前MACD={current_dif:.6f}")
            self._log.info(f"排名比例={rank_ratio:.3f} (值越大排名越高，1=最大值)")
        
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
        
        # 根据极值类型调整排名逻辑
        if extreme_type == 'trough':
            # 极小值：价格越低，排名越高（越接近1）
            rank_ratio = 1 - (values_below_current / total_count)
            self._log.info(f"价格极小值排序分析: 总极小值点数={total_count}, 当前价格={current_price:.4f}")
            self._log.info(f"排名比例={rank_ratio:.3f} (价格越低排名越高，1=最低价)")
        else:  # extreme_type == 'peak'
            # 极大值：价格越高，排名越高（越接近1）
            rank_ratio = values_below_current / total_count
            self._log.info(f"价格极大值排序分析: 总极大值点数={total_count}, 当前价格={current_price:.4f}")
            self._log.info(f"排名比例={rank_ratio:.3f} (价格越高排名越高，1=最高价)")
        
        return rank_ratio
    
    def check_macd_top_signals(self, bar: Bar):
        rank_ratio = self.check_macd_rank('peak')  # 比较极大值
        price_rank_ratio = self.check_price_rank('peak')
        self._log.info(f"【顶部信号检查】MACD排名={rank_ratio:.3f}, 价格排名={price_rank_ratio:.3f}, 最近极值点类型={self.latest_extreme_type}, 是否有时间差={self.time_diff_minutes_from_latest_extreme is not None}")
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
                # rank_ratio 已经通过 > 0.9 条件判断，直接使用
                # 避免除零错误：当volume_ratio为0时使用最大贡献值
                if volume_ratio > 0:
                    top_signal_contribution = -(10/volume_ratio+rank_ratio*30)
                else:
                    # 成交量为0时，给予较大的顶部信号贡献（相当于volume_ratio极小的情况）
                    top_signal_contribution = -(100 + rank_ratio*30)
                self.technical_signal += top_signal_contribution
                self.technical_signal_steps.append({
                    'description': f'顶部信号(成交量比值={volume_ratio:.4f}, 排名比例={rank_ratio:.3f})',
                    'delta': top_signal_contribution
                })
                self._log.info(f"成交量比值: {volume_ratio:.4f}")
                self._log.info(f"技术信号: {self.technical_signal:.2f}")
                 # 检查RSI条件
                if self.rsi.initialized:
                    rsi_value = self.rsi.value * 100
                    rsi_contribution = -(rsi_value - 60)
                    self.technical_signal += rsi_contribution
                    self.technical_signal_steps.append({
                        'description': f'顶部信号RSI调整(RSI={rsi_value:.2f}, 贡献=-(RSI-60))',
                        'delta': rsi_contribution
                    })
                    self._log.info(f"RSI条件满足：RSI={rsi_value:.2f} < 50，增强买入信号")
                    self._log.info(f"RSI技术信号: {self.technical_signal:.2f}")

                # 检查KDJ条件
                # 计算KDJ三个值的最大差值
                kdj_values = [self.kdj.value_k, self.kdj.value_d, self.kdj.value_j]
                kdj_max_diff = max(kdj_values) - min(kdj_values)
                
                
                self._log.info(f"KDJ分析: K={self.kdj.value_k:.2f}, D={self.kdj.value_d:.2f}, J={self.kdj.value_j:.2f}")
                
                # 如果KDJ三个值最大差值小于10且都小于20，增强信号
                if kdj_max_diff < 20:
                    kdj_contribution = -(40-kdj_max_diff)
                    self.technical_signal += kdj_contribution
                    self.technical_signal_steps.append({
                        'description': f'顶部信号KDJ调整(最大差值={kdj_max_diff:.2f}, 贡献=-(40-差值))',
                        'delta': kdj_contribution
                    })
                    self._log.info("KDJ条件满足：最大差值<20且超卖，增强买入信号")
                else:
                    self._log.info("KDJ条件不满足，使用标准信号")
                self._log.info(f"KDJ技术信号: {self.technical_signal:.2f}")
            else:
                self._log.warning(f"✗ 顶部信号条件满足但无时间差数据")
        else:
            self._log.debug(f"✗ 顶部信号条件不满足")
    
    def check_macd_bottom_signals(self, bar: Bar):
        rank_ratio = self.check_macd_rank('trough')  # 比较极小值
        self._log.info(f"【底部信号检查】排名比例={rank_ratio:.3f}, 最近极值点类型={self.latest_extreme_type}, 是否有时间差={self.time_diff_minutes_from_latest_extreme is not None}")
        if rank_ratio > 0.9 and self.latest_extreme_type == 'trough':
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
                # rank_ratio 已经通过 > 0.9 条件判断，直接使用
                # 避免除零错误：当volume_ratio为0时使用最大贡献值
                if volume_ratio > 0:
                    bottom_signal_contribution = 10/volume_ratio+rank_ratio*30
                else:
                    # 成交量为0时，给予较大的底部信号贡献（相当于volume_ratio极小的情况）
                    bottom_signal_contribution = 100 + rank_ratio*30
                self.technical_signal += bottom_signal_contribution
                self.technical_signal_steps.append({
                    'description': f'底部信号(成交量比值={volume_ratio:.4f}, 排名比例={rank_ratio:.3f})',
                    'delta': bottom_signal_contribution
                })
                self._log.info(f"成交量比值: {volume_ratio:.4f}")
                self._log.info(f"技术信号: {self.technical_signal:.2f}")
                 # 检查RSI条件
                if self.rsi.initialized:
                    rsi_value = self.rsi.value * 100
                    rsi_contribution = 60 - rsi_value
                    self.technical_signal += rsi_contribution
                    self.technical_signal_steps.append({
                        'description': f'底部信号RSI调整(RSI={rsi_value:.2f}, 贡献=60-RSI)',
                        'delta': rsi_contribution
                    })
                    self._log.info(f"RSI条件满足：RSI={rsi_value:.2f} < 50，增强买入信号")
                    self._log.info(f"RSI技术信号: {self.technical_signal:.2f}")

                # 检查KDJ条件
                # 计算KDJ三个值的最大差值
                kdj_values = [self.kdj.value_k, self.kdj.value_d, self.kdj.value_j]
                kdj_max_diff = max(kdj_values) - min(kdj_values)
                
                
                self._log.info(f"KDJ分析: K={self.kdj.value_k:.2f}, D={self.kdj.value_d:.2f}, J={self.kdj.value_j:.2f}")
                
                kdj_contribution = 60-self.kdj.value_k - kdj_max_diff
                self.technical_signal += kdj_contribution
                self.technical_signal_steps.append({
                    'description': f'底部信号KDJ调整(K={self.kdj.value_k:.2f}, 贡献=60-K-差值)',
                    'delta': kdj_contribution
                })
                
                self._log.info(f"KDJ技术信号: {self.technical_signal:.2f}")
            else:
                self._log.warning(f"✗ 底部信号条件满足但无时间差数据")
        else:
            self._log.debug(f"✗ 底部信号条件不满足")

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
        self._log.info(f"检查MACD信号: macd_history长度={len(self.dif_history)}, signal_history长度={len(self.signal_history)}")
        
        # 需要至少2个数据点来检测金叉死叉
        if len(self.dif_history) < 2 or len(self.signal_history) < 2:
            self._log.info(f"历史数据不足，跳过信号检测: macd_history={len(self.dif_history)}, signal_history={len(self.signal_history)}, 需要至少2个数据点")
            return
        
        current_dif = self.dif_history[-1]
        previous_dif = self.dif_history[-2]
        current_signal = self.signal_history[-1]
        previous_signal = self.signal_history[-2]
        
        # 计算当前histogram值
        current_histogram = current_dif - current_signal
        
        # 检查histogram模式
        histogram_result = self.check_negative_positive_histogram(bar)
        
        if len(self.dif_history) < 5 and current_dif < 0:
            #如果开盘前5分钟，dif就小于0，则直接卖出
            dif_decreasing_contribution = -300
            self.technical_signal += dif_decreasing_contribution
            description = f'DIF<0且开盘前5分钟(固定贡献=-300)'
            self.technical_signal_steps.append({
                'description': description,
                'delta': dif_decreasing_contribution
            })
            self._log.info(f"如果开盘前5分钟，dif就小于0，则直接卖出")
            return
        elif previous_dif > 0 and current_dif < 0 and current_histogram < 0:
            # 获取前N个DIF值（不包括当前值），最多5个，有几个算几个
            dif_count = min(len(self.dif_history) - 1, 5)  # 不包括当前值，最多5个
            if dif_count >= 1:
                # 从历史中取前N个值（不包括当前值）
                last_dif_values = list(self.dif_history)[-(dif_count + 1):-1]  # 前N个值
                
                # 检查是否单调递减（从历史到当前，即从旧到新）
                is_monotonic_decreasing = True
                if len(last_dif_values) >= 2:  # 至少需要2个值才能判断单调性
                    for i in range(len(last_dif_values) - 1):
                        if last_dif_values[i] < last_dif_values[i + 1]:  # 如果从历史到当前不是递减（即后面的值比前面的大）
                            is_monotonic_decreasing = False
                            break
                else:
                    # 只有1个值，数据不足，算作递减
                    is_monotonic_decreasing = True
            else:
                # 没有足够的前值，数据不足，算作递减
                is_monotonic_decreasing = True
                last_dif_values = []
            
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
            
            # 计算过去5个histogram的平均值（有几个算几个）
            avg_histogram = 0
            last_histogram_values = []
            histogram_count = 0
            if len(self.histogram_history) > 0:
                # 取最多5个，有几个算几个
                histogram_count = min(len(self.histogram_history), 5)
                last_histogram_values = list(self.histogram_history)[-histogram_count:]
                avg_histogram = sum(last_histogram_values) / len(last_histogram_values)
            else:
                self._log.info(f"Histogram历史数据为空，无法计算平均值")
            
            # 添加调试日志
            if dif_count >= 1 and last_dif_values:
                self._log.info(f"DIF单调递减检查: 前{dif_count}个DIF值={last_dif_values}, 当前DIF={current_dif}, 是否单调递减={is_monotonic_decreasing}")
            else:
                self._log.info(f"DIF单调递减检查: 历史数据不足，无法判断单调性，当前DIF={current_dif}")
            if last_histogram_values:
                self._log.info(f"Histogram平均值检查: 过去{histogram_count}个histogram值={last_histogram_values}, 平均值={avg_histogram:.6f}")
            self._log.info(f"卖出操作检查: 当前时间={current_timestamp}, 是否有卖出操作={has_sell_operation}")
            
            # 如果(前N个DIF单调递减 或 过去N个histogram平均值<-0.002)且当前DIF<0且最后一个交易不是SELL，添加卖出信号
            condition_met = is_monotonic_decreasing or avg_histogram < -0.002
            if condition_met and not has_sell_operation:
                condition_desc = []
                if is_monotonic_decreasing:
                    condition_desc.append(f"前{dif_count}个DIF单调递减")
                if avg_histogram < -0.002:
                    condition_desc.append(f"过去{histogram_count}个histogram平均值<-0.002(实际={avg_histogram:.6f})")
                self._log.info(f"检测到DIF<0且({'或'.join(condition_desc)})且最后一个交易不是SELL")
                if last_dif_values:
                    self._log.info(f"前{dif_count}个DIF值: {last_dif_values}")
                self._log.info(f"当前DIF值: {current_dif}")
                if last_histogram_values:
                    self._log.info(f"过去{histogram_count}个histogram值: {last_histogram_values}, 平均值: {avg_histogram:.6f}")
                self._log.info(f"前5个DIF期间是否有卖出操作: {has_sell_operation}")
                # 计算最近三个MACD极值点的DIF值及其最大差值
                if len(self.macd_extremes_history) >= 3:
                    last_three_extremes = self.macd_extremes_history[-3:]
                    dif_values = [extreme[1] for extreme in last_three_extremes]
                    max_dif = max(dif_values)
                    min_dif = min(dif_values)
                    max_dif_diff = max_dif - min_dif
                    self._log.info(f"最近三个MACD极值点DIF值: {dif_values}, 最大差值: {max_dif_diff:.6f}")
                    if max_dif_diff < 0.0002:
                        self._log.info(f"当前DIF和最近三个MACD极值点DIF的最大差值小于0.0002，跳过卖出信号")
                        return
                else:
                    self._log.info("MACD极值点历史不足3个，无法计算最大DIF差值")
                # 检查当前时间是否在2:50分之后，如果是则跳过卖出信号
                if self.is_after_scheduled_time(bar):
                    self._log.info(f"当前时间已过2:50分，跳过卖出信号")
                    return
                # 不再直接卖出，而是给 technical_signal 减 300
                dif_decreasing_contribution = -300
                self.technical_signal += dif_decreasing_contribution
                description = f'DIF<0且({"或".join(condition_desc)})(固定贡献=-300)'
                self.technical_signal_steps.append({
                    'description': description,
                    'delta': dif_decreasing_contribution
                })
                self._log.info(f"DIF单调递减卖出信号：减少信号值 {dif_decreasing_contribution}，当前信号值={self.technical_signal:.2f}")
                
                return
        
        
        # 检测金叉：MACD线从下方向上穿越信号线
        golden_cross = (previous_dif < previous_signal and current_dif > current_signal)
        
        # 检测死叉：MACD线从上方向下穿越信号线
        death_cross = (previous_dif > previous_signal and current_dif < current_signal)
        
        # 检查MACD值是否足够大（过滤小波动）
        macd_threshold = abs(self.divergence_threshold)
        current_dif_abs = abs(current_dif)
        
        # 记录信号
        if golden_cross:
            self._log.info(f"检测到金叉信号: MACD={current_dif:.6f}, Signal={current_signal:.6f}")
            
            # 检查MACD值是否足够大（可选：注释掉以下代码来禁用过滤）
            if current_dif_abs < macd_threshold:
                self._log.info(f"金叉信号被过滤: MACD绝对值{current_dif_abs:.6f} < 阈值{macd_threshold:.6f}")
                return
            
            self.last_signal = "golden_cross"
            
            # 累积买入信号
            # 如果是第一个金叉技术信号，使用40000系数，否则使用20000
            is_first_golden_cross = not any(signal.get('signal_type') == 'golden_cross' for signal in self.technical_signals)
            signal_coefficient = 40000 if is_first_golden_cross else 20000
            macd_contribution = signal_coefficient*current_dif_abs
            self.technical_signal += macd_contribution
            self.technical_signal_steps.append({
                'description': f'金叉信号(系数={signal_coefficient}, MACD绝对值={current_dif_abs:.6f})',
                'delta': macd_contribution
            })
            self._log.info(f"金叉买入信号累积: 系数={signal_coefficient}, MACD绝对值={current_dif_abs:.6f}, 当前信号值={self.technical_signal}")

            # 检查RSI条件
            if self.rsi.initialized:
                rsi_value = self.rsi.value * 100
                rsi_contribution = 50 - rsi_value
                self.technical_signal += rsi_contribution
                self.technical_signal_steps.append({
                    'description': f'RSI调整(RSI={rsi_value:.2f}, 贡献=50-RSI)',
                    'delta': rsi_contribution
                })
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
                kdj_contribution = 40-kdj_max_diff
                self.technical_signal += kdj_contribution
                self.technical_signal_steps.append({
                    'description': f'KDJ调整(最大差值={kdj_max_diff:.2f}, 超卖, 贡献=40-差值)',
                    'delta': kdj_contribution
                })
                self._log.info("KDJ条件满足：最大差值<20且超卖，增强买入信号")
            else:
                self._log.info("KDJ条件不满足，使用标准信号")
            
            # 记录技术指标信号（金叉）
            technical_signal = {
                'timestamp': pd.to_datetime(bar.ts_event, unit='ns'),
                'price': bar.close.as_double(),
                'signal_type': 'golden_cross',
                'signal_value': self.technical_signal,
                'macd_value': current_dif,
                'signal_value_macd': current_signal,
                'histogram': current_histogram,
                'rsi_value': self.rsi.value * 100 if self.rsi.initialized else None,
                'kdj_k': self.kdj.value_k if self.kdj.initialized else None,
                'kdj_d': self.kdj.value_d if self.kdj.initialized else None,
                'kdj_j': self.kdj.value_j if self.kdj.initialized else None
            }
            self.technical_signals.append(technical_signal)
            self._log.info(f"记录金叉技术信号: {technical_signal}")
        
        elif death_cross:
            self._log.info(f"检测到死叉信号: MACD={current_dif:.6f}, Signal={current_signal:.6f}")
            
            # 检查MACD值是否足够大（可选：注释掉以下代码来禁用过滤）
            if current_dif_abs < macd_threshold:
                self._log.info(f"死叉信号被过滤: MACD绝对值{current_dif_abs:.6f} < 阈值{macd_threshold:.6f}")
                return
            
            self.last_signal = "death_cross"
            
            # 累积卖出信号
            # 如果是第一个死叉技术信号，使用40000系数，否则使用20000
            is_first_death_cross = not any(signal.get('signal_type') == 'death_cross' for signal in self.technical_signals)
            signal_coefficient = 40000 if is_first_death_cross else 20000
            
            # 🆕 标记第一个死叉信号已触发（待确认卖出）
            if is_first_death_cross:
                self.first_death_cross_triggered = True
                self._log.info("✅ 第一个死叉信号已触发（待在on_bar中确认卖出）")
            else:
                if self.monitor_histogram_shrink:
                    self.monitor_histogram_shrink = False
                    self._log.info("检测到非首个死叉，关闭MACD柱缩小监控，避免重复买入")
                self.first_death_cross_triggered = False
            macd_contribution = -signal_coefficient*current_dif_abs
            self.technical_signal += macd_contribution
            self.technical_signal_steps.append({
                'description': f'死叉信号(系数={signal_coefficient}, MACD绝对值={current_dif_abs:.6f})',
                'delta': macd_contribution
            })
            self._log.info(f"死叉卖出信号累积: 系数={signal_coefficient}, MACD绝对值={current_dif_abs:.6f}, 当前信号值={self.technical_signal}")
            
            # 检查RSI条件
            if self.rsi.initialized:
                rsi_value = self.rsi.value * 100
                if rsi_value < 50:
                    rsi_contribution = -rsi_value
                    self.technical_signal += rsi_contribution
                    self.technical_signal_steps.append({
                        'description': f'RSI调整(RSI={rsi_value:.2f} < 50, 贡献=-RSI)',
                        'delta': rsi_contribution
                    })
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
                kdj_contribution = -(30-kdj_max_diff)
                self.technical_signal += kdj_contribution
                self.technical_signal_steps.append({
                    'description': f'KDJ调整(最大差值={kdj_max_diff:.2f}, 超买, 贡献=-(30-差值))',
                    'delta': kdj_contribution
                })
                self._log.info("KDJ条件满足：最大差值<20且超买，增强卖出信号")
            else:
                self._log.info("KDJ条件不满足，使用标准信号")
            # 记录技术指标信号（死叉）
            technical_signal = {
                'timestamp': pd.to_datetime(bar.ts_event, unit='ns'),
                'price': bar.close.as_double(),
                'signal_type': 'death_cross',
                'signal_value': self.technical_signal,
                'macd_value': current_dif,
                'signal_value_macd': current_signal,
                'histogram': current_histogram,
                'rsi_value': self.rsi.value * 100 if self.rsi.initialized else None,
                'kdj_k': self.kdj.value_k if self.kdj.initialized else None,
                'kdj_d': self.kdj.value_d if self.kdj.initialized else None,
                'kdj_j': self.kdj.value_j if self.kdj.initialized else None
            }
            self.technical_signals.append(technical_signal)
            self._log.info(f"记录死叉技术信号: {technical_signal}")
    
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
        
        self._log.info(f"开始极值点检测: price_history长度={len(self.price_history)}, macd_history长度={len(self.dif_history)}")
        
        if len(self.price_history) < 3:
            self._log.info("价格历史数据不足3个点，跳过极值点检测")
            return
        
        current_timestamp = bar.ts_event
        current_price = self.price_history[-1]
        current_dif = self.dif_history[-1]
        
        # 检测上一个价格点的极值（延迟检测）
        if len(self.price_history) >= 2:
            prev_price_index = len(self.price_history) - 2  # 上一个价格点的索引
            prev_price_timestamp = self.timestamps[-2] if len(self.timestamps) >= 2 else current_timestamp
            prev_price = self.price_history[-2]
            prev_macd = self.dif_history[-2]
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
        if len(self.dif_history) >= 2:
            prev_macd_index = len(self.dif_history) - 2  # 上一个MACD点的索引
            prev_macd_timestamp = self.timestamps[-2] if len(self.timestamps) >= 2 else current_timestamp
            prev_macd = self.dif_history[-2]
            prev_price = self.price_history[-2]
            macd_extreme, macd_type = self._detect_extreme(self.dif_history, prev_macd_index)
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
            divergence_contribution = -30
            self.technical_signal += divergence_contribution
            self.technical_signal_steps.append({
                'description': f'顶背离信号(DIF创新高但价格未创新高, 固定贡献=-30)',
                'delta': divergence_contribution
            })
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
            divergence_contribution = 30
            self.technical_signal += divergence_contribution
            self.technical_signal_steps.append({
                'description': f'底背离信号(DIF创新低但价格未创新低, 固定贡献=+30)',
                'delta': divergence_contribution
            })
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
    
    def _execute_buy_order(
        self, 
        bar: Bar, 
        signal_type: str,
        signal_value: int = 0,
        log_detailed_balance: bool = False,
        log_time_info: bool = False
    ) -> bool:
        """核心买入逻辑（内部方法）
        
        将买入逻辑统一到一个方法中，避免代码重复。
        
        Args:
            bar: K线数据
            signal_type: 信号类型（'executed_buy'/'scheduled_buy'等）
            signal_value: 技术指标信号值
            log_detailed_balance: 是否记录详细的账户余额信息（总余额、冻结余额等）
            log_time_info: 是否记录UTC和北京时间信息
            
        Returns:
            bool: True表示订单成功提交，False表示执行失败
        """
        # ✅ 撤销所有未成交的委托（官方方法）
        # 说明：external_order_claims 配置会在对账时认领外部订单到缓存
        # 然后使用官方 cancel_all_orders 方法即可撤销
        # 参考：https://nautilustrader.io/docs/latest/concepts/live/#execution-reconciliation
        self._log.info("检查并撤销未成交委托...")
        self.cancel_all_orders(self.config.instrument_id)
        
        # 可选的时间信息日志
        if log_time_info:
            current_time_utc = pd.to_datetime(bar.ts_event, unit='ns')
            current_time_beijing = current_time_utc.tz_localize('UTC').tz_convert('Asia/Shanghai')
            self._log.info(
                f"执行买入: UTC时间={current_time_utc.strftime('%Y-%m-%d %H:%M:%S')}, "
                f"北京时间={current_time_beijing.strftime('%Y-%m-%d %H:%M:%S')}"
            )
        
        # 计算交易数量
        if self.trade_size is None:
            # 从 instrument_id 提取 venue（官方推荐方式）
            venue = self.config.instrument_id.venue
            self._log.info(f"正在获取账户信息，venue={venue}")
            
            # 按venue获取账户
            account = self.cache.account_for_venue(venue)
            if account is None:
                self._log.error(f"无法获取venue={venue}的账户信息，买入失败")
                
                # 调试信息：列出所有账户
                all_accounts = self.cache.accounts()
                if all_accounts:
                    self._log.error(f"缓存中有 {len(all_accounts)} 个账户:")
                    for acc in all_accounts:
                        self._log.error(f"  账户: {acc.id}, venue={acc.id.get_issuer()}")
                else:
                    self._log.error("缓存中没有任何账户信息")
                
                return False
            
            self._log.info(f"成功获取账户: {account.id}")
            
            # 获取可用余额
            free_balance = account.balance_free(CNY)
            if free_balance is None:
                self._log.error("无法获取CNY可用余额，买入失败")
                return False
            
            available_balance = free_balance.as_double()
            
            # 计算订单价格（加价提高成交概率）
            buy_price_value = bar.close.as_double() + 0.001
            buy_price = self.instrument.make_price(buy_price_value)
            
            # 详细或简洁的余额信息日志
            if log_detailed_balance:
                total_balance = account.balance_total(CNY)
                locked_balance = account.balance_locked(CNY)
                self._log.info(
                    f"账户余额详情: 总余额={total_balance.as_double():.2f}, "
                    f"可用余额={available_balance:.2f}, "
                    f"冻结余额={locked_balance.as_double() if locked_balance else 0:.2f}"
                )
            else:
                self._log.info(
                    f"买入信号 - 可用余额: {available_balance:.2f} CNY, "
                    f"收盘价: {bar.close.as_double():.4f}, "
                    f"订单价格: {buy_price_value:.4f}"
                )
            
            # 检查可用余额
            if available_balance <= 0:
                self._log.warning(f"可用余额不足: {available_balance:.2f} CNY，无法买入")
                return False
            
            # 用订单价格计算数量，避免超出可用余额
            # 留0.1%的安全边际，防止手续费等导致余额不足
            raw_quantity = int(available_balance * 0.999 / buy_price_value)
            
            # 规整到100股的整数倍（交易所要求）
            quantity = self._round_to_lot_size(raw_quantity, lot_size=100)
            
            # 可选的详细计算日志
            if log_detailed_balance:
                self._log.info(
                    f"交易计算: 可用余额={available_balance:.2f}, "
                    f"收盘价={bar.close.as_double():.4f}, "
                    f"订单价格={buy_price_value:.4f}, "
                    f"原始数量={raw_quantity}, 规整后数量={quantity}, "
                    f"预计花费={quantity * buy_price_value:.2f}"
                )
            
            # 检查规整后的数量是否有效
            if quantity <= 0:
                self._log.warning(
                    f"⚠️  规整后交易数量无效: {quantity}（原始: {raw_quantity}），无法买入"
                )
                return False
                
            trade_quantity = Quantity.from_int(quantity)
        else:
            trade_quantity = self.trade_size
            # ✅ 固定数量模式下仍需计算价格
            buy_price = self.instrument.make_price(bar.close.as_double() + 0.001)
        
        # 创建并提交订单
        order = self.order_factory.limit(
            instrument_id=self.config.instrument_id,
            order_side=OrderSide.BUY,
            quantity=trade_quantity,
            price=buy_price,
        )
        self.submit_order(order)
        
        # 记录交易信号
        trade_signal = {
            'timestamp': pd.to_datetime(bar.ts_event, unit='ns'),
            'price': buy_price.as_double(),
            'side': 'BUY',
            'quantity': trade_quantity.as_double(),
            'order_id': str(order.client_order_id),
            'signal_type': signal_type,
            'signal_value': signal_value
        }
        self.trade_signals.append(trade_signal)
        
        self._log.info(
            f"买入订单已提交: 数量={trade_quantity}, "
            f"价格={buy_price.as_double():.4f}, 类型={signal_type}"
        )
        return True
    
    def execute_buy_signal(self, bar: Bar, signal_type: str = 'executed_buy'):
        """执行买入信号
        
        基于技术指标信号执行买入操作。
        """
        # 🆕 如果正在监控MACD柱缩小，买入后重置监控状态
        if self.monitor_histogram_shrink:
            self._reset_histogram_monitor()
            self._log.info("买入信号执行，重置MACD柱监控状态")
        
        self._execute_buy_order(
            bar=bar,
            signal_type=signal_type,
            signal_value=self.technical_signal,
            log_detailed_balance=False,
            log_time_info=False
        )
    
    def _execute_sell_order(
        self,
        bar: Bar,
        signal_type: str,
        signal_value: int = 0,
        filter_by_strategy_id: bool = False
    ) -> bool:
        """核心卖出逻辑（内部方法）
        
        将卖出逻辑统一到一个方法中，避免代码重复。
        
        Args:
            bar: K线数据
            signal_type: 信号类型（'executed_sell'/'executed_divergence_sell'等）
            signal_value: 技术指标信号值
            filter_by_strategy_id: 是否按strategy_id过滤持仓
                False: 查询所有持仓（用于策略重启场景）
                True: 只查询当前策略的持仓
        
        Returns:
            bool: True表示订单成功提交，False表示执行失败
        """
        # ✅ 撤销所有未成交的委托（官方方法）
        # 说明：external_order_claims 配置会在对账时认领外部订单到缓存
        # 然后使用官方 cancel_all_orders 方法即可撤销
        # 参考：https://nautilustrader.io/docs/latest/concepts/live/#execution-reconciliation
        self._log.info("检查并撤销未成交委托...")
        self.cancel_all_orders(self.config.instrument_id)
        
        # 检查当前时间是否在2:50分之后，如果是则跳过卖出操作
        if self.is_after_scheduled_time(bar):
            self._log.info("当前时间已过2:50分，跳过卖出信号执行")
            return False
        
        # 查询持仓（根据参数决定是否过滤strategy_id）
        if filter_by_strategy_id:
            positions = self.cache.positions_open(
                instrument_id=self.config.instrument_id,
                strategy_id=self.id
            )
        else:
            # 不用 strategy_id 过滤 - 处理策略重启后的持仓
            positions = self.cache.positions_open(
                instrument_id=self.config.instrument_id
            )
        
        if not positions:
            self._log.info("没有持仓，跳过卖出信号")
            return False
        
        # 记录持仓信息（仅非strategy_id过滤时记录详细信息）
        if not filter_by_strategy_id:
            self._log.info(f"找到 {len(positions)} 个持仓:")
            for pos in positions:
                self._log.info(
                    f"  - 方向={pos.side}, 数量={pos.quantity}, "
                    f"成本价={pos.avg_px_open}, 策略ID={pos.strategy_id}"
                )
        
        any_submitted = False
        
        for position in positions:
            if position.side != PositionSide.LONG:
                self._log.info(f"跳过非多头持仓: {position.id} side={position.side}")
                continue
            
            quantity = position.quantity
            if quantity.as_double() <= 0:
                self._log.info(f"跳过数量<=0的持仓: {position.id}")
                continue
            
            self._log.info(
                f"准备卖出: 持仓ID={position.id}, 数量={quantity}, "
                f"价格={bar.close.as_double():.4f}"
            )
            
            sell_price = self.instrument.make_price(bar.close.as_double() - 0.001)
            order = self.order_factory.limit(
                instrument_id=self.config.instrument_id,
                order_side=OrderSide.SELL,
                quantity=quantity,
                price=sell_price,
                reduce_only=True,
                tags=["EXIT", signal_type]
            )
            
            self.submit_order(order, position_id=position.id)
            
            trade_signal = {
                'timestamp': pd.to_datetime(bar.ts_event, unit='ns'),
                'price': sell_price.as_double(),
                'side': 'SELL',
                'quantity': quantity.as_double(),
                'order_id': str(order.client_order_id),
                'signal_type': signal_type,
                'signal_value': signal_value
            }
            self.trade_signals.append(trade_signal)
            
            self._log.info(
                f"执行SELL订单: 数量={quantity}, 价格={sell_price.as_double():.4f}, "
                f"订单ID={order.client_order_id}, 类型={signal_type}"
            )
            any_submitted = True
        
        if not any_submitted:
            self._log.info("未提交任何卖单（没有多头持仓或数量为0）")
            return False
        
        return True
    
    def execute_sell_signal(self, bar: Bar, signal_type: str = 'executed_sell'):
        """执行卖出信号
        
        基于技术指标信号执行卖出操作。
        不按strategy_id过滤持仓，以便处理策略重启后的持仓。
        """
        # 🆕 检测第一个死叉实际卖出
        if self.first_death_cross_triggered and not self.first_death_cross_sold:
            self.first_death_cross_sold = True
            self.monitor_histogram_shrink = True
            self._log.info("✅ 确认第一个死叉信号触发实际卖出，激活MACD柱缩小监控")
        
        self._execute_sell_order(
            bar=bar,
            signal_type=signal_type,
            signal_value=self.technical_signal,
            filter_by_strategy_id=False  # 不按strategy_id过滤，处理策略重启场景
        )
    
    def execute_divergence_buy_signal(self, bar: Bar):
        """执行背离买入信号
        
        基于MACD背离检测执行买入操作。
        """
        self._execute_buy_order(
            bar=bar,
            signal_type='executed_divergence_buy',
            signal_value=self.technical_signal,
            log_detailed_balance=False,
            log_time_info=False
        )
    
    def execute_divergence_sell_signal(self, bar: Bar):
        """执行背离卖出信号
        
        基于MACD背离检测执行卖出操作。
        """
        self._execute_sell_order(
            bar=bar,
            signal_type='executed_divergence_sell',
            signal_value=self.technical_signal,
            filter_by_strategy_id=True  # 按strategy_id过滤持仓
        )
    
    def check_histogram_shrink_for_rebuy(self):
        """检测MACD柱连续两分钟缩小（往前判断2分钟）
        
        判断逻辑：|histogram[n-2]| > |histogram[n-1]| > |histogram[n]|
        满足条件时增加+300技术信号值
        
        Parameters:
        -----------
        
        """
        if not self.first_death_cross_triggered:
            return

        if not self.monitor_histogram_shrink:
            return  # 未激活监控，直接返回
        
        # 需要至少3个历史值才能判断（n-2, n-1, n）
        if len(self.histogram_history) < 3:
            self._log.info(
                f"收集MACD柱值 [{len(self.histogram_history)}/3]"
            )
            return
        
        # 直接从历史记录中取最后3个值
        hist_n_minus_2 = abs(self.histogram_history[-3])
        hist_n_minus_1 = abs(self.histogram_history[-2])
        hist_n = abs(self.histogram_history[-1])
        
        # 检测连续缩小：|histogram[n-2]| > |histogram[n-1]| > |histogram[n]|
        is_shrinking = (hist_n_minus_2 > hist_n_minus_1) and (hist_n_minus_1 > hist_n)
        
        if is_shrinking:
            self._log.info(
                f"🎯 检测到MACD柱连续2分钟缩小: "
                f"{hist_n_minus_2:.6f} > {hist_n_minus_1:.6f} > {hist_n:.6f}"
            )
            self._trigger_rebuy_signal_after_death_cross()
            self._reset_histogram_monitor()
        else:
            self._log.debug(
                f"MACD柱未连续缩小: {hist_n_minus_2:.6f}, {hist_n_minus_1:.6f}, {hist_n:.6f}"
            )
    
    def _trigger_rebuy_signal_after_death_cross(self):
        """死叉卖出后，MACD柱连续缩小触发的买入信号（+300技术信号值）"""
        rebuy_contribution = 300  # 增加300技术信号值，而非直接买入
        self.technical_signal += rebuy_contribution
        
        self.technical_signal_steps.append({
            'description': '死叉后MACD柱连续2分钟缩小买入信号(+300)',
            'delta': rebuy_contribution
        })
        
        self._log.info(
            f"🔔 触发死叉后买入信号：MACD柱连续2分钟缩小，"
            f"增加技术信号+300，当前信号值={self.technical_signal}"
        )
    
    def _reset_histogram_monitor(self):
        """重置柱状图监控状态"""
        self.first_death_cross_triggered = False
        self.first_death_cross_sold = False
        self.monitor_histogram_shrink = False
        self._log.info("重置MACD柱监控状态")

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
        # self._log.info(f"定时买入检查: UTC时间={current_time_utc.strftime('%Y-%m-%d %H:%M:%S')}, 北京时间={current_time_beijing.strftime('%Y-%m-%d %H:%M:%S')}")
        # 转换为北京时间格式显示
        beijing_time_str = current_time_beijing.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        # self._log.info(f"目标买入时间: {self.scheduled_buy_time.strftime('%H:%M:%S')}, 当前北京时间: {beijing_time_str}")
        
        # 检查是否已经在该日期执行过定时买入
        if self.last_scheduled_buy_date == current_date:
            self._log.info(f"今天 {current_date} 已经执行过定时买入，跳过")
            return
        
        # 检查是否到达定时买入时间（2:50分）
        if current_time_only >= self.scheduled_buy_time:
            self._log.info(f"到达定时买入时间: {current_time_only.strftime('%H:%M:%S')}")
            # 不再直接买入，而是给 technical_signal 增加 300
            scheduled_buy_contribution = 300
            self.technical_signal += scheduled_buy_contribution
            self.technical_signal_steps.append({
                'description': f'2:50定时买入信号(固定贡献=300)',
                'delta': scheduled_buy_contribution
            })
            # 标记今天已经添加过定时买入信号
            self.last_scheduled_buy_date = current_date
            self._log.info(f"2:50定时买入：增加信号值 +{scheduled_buy_contribution}，当前信号值={self.technical_signal:.2f}")
            self._log.info(f"已标记日期: {current_date}，今天不会再次添加定时买入信号")
        else:
            self._log.info(f"还未到达定时买入时间，当前时间: {current_time_only.strftime('%H:%M:%S')}, 目标时间: {self.scheduled_buy_time.strftime('%H:%M:%S')}")
        
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
        """每分钟更新实时图表（同步方法，由异步包装调用，从technical_signal_history读取信号值）"""
        try:
            # 抑制字体警告（显式静默已知的无害警告）
            warnings.filterwarnings('ignore', category=UserWarning, message='.*Glyph.*missing from font.*')
            
            # 从technical_signal_history读取最新的技术信号值
            technical_signal = 0.0
            if self.technical_signal_history and len(self.technical_signal_history) > 0:
                # 获取最后一个记录的信号值
                technical_signal = self.technical_signal_history[-1].get('signal_value', 0.0)
                self._log.info(f"从technical_signal_history读取最新信号值: {technical_signal:.2f}")
            else:
                self._log.warning("technical_signal_history为空，使用默认信号值0")
            
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
                technical_signal_value=technical_signal,
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
            # self._log.info(f"数据时间范围: {start_time} 到 {end_time}")
            # self._log.info(f"数据条数: {len(df)}")
            
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
            
            # self._log.info(f"上午数据: {len(morning_data)} 条")
            # self._log.info(f"下午数据: {len(afternoon_data)} 条")
            # self._log.info(f"总交易数据: {len(trading_data)} 条")
            
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
            
            # 固定X轴范围为完整交易日，确保时间轴覆盖全天
            if len(mapped_df) > 0:
                base_date = mapped_df.index[0].date()
                tz_info = mapped_df.index[0].tzinfo
            else:
                base_date = data_date
                import pytz
                tz_info = pytz.timezone('Asia/Shanghai')

            x_min = pd.Timestamp(year=base_date.year, month=base_date.month, day=base_date.day,
                                 hour=9, minute=30, second=0, tz=tz_info)
            x_max = pd.Timestamp(year=base_date.year, month=base_date.month, day=base_date.day,
                                 hour=13, minute=30, second=0, tz=tz_info)

            self._log.info(f"映射后数据时间范围: {mapped_df.index.min()} 到 {mapped_df.index.max()}")
            
            # 创建图表
            fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(16, 12), height_ratios=[3, 1, 2])
            fig.suptitle(chart_title, fontsize=16, fontweight='bold')

            for axis in (ax1, ax2, ax3):
                axis.set_xlim(x_min, x_max)
            
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
            ax1.grid(True, alpha=0.3, which='major')
            ax1.grid(True, alpha=0.1, which='minor', linestyle=':', linewidth=0.5)
            ax1.legend(loc='upper left')

            # 设置x轴格式
            ax1.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
            ax1.xaxis.set_major_locator(plt.matplotlib.dates.MinuteLocator(interval=10))
            ax1.xaxis.set_minor_locator(plt.matplotlib.dates.MinuteLocator(interval=1))
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
                    
                    # self._log.info(f"绘制了 {len(minute_volume_mapped)} 分钟的成交量数据")
                else:
                    self._log.warning("没有交易时间内的分钟成交量数据")
            else:
                self._log.warning("无法计算分钟成交量数据")

            ax2.set_title('成交量', fontsize=12)
            ax2.set_ylabel('成交量', fontsize=10)
            ax2.grid(True, alpha=0.3, which='major')
            ax2.grid(True, alpha=0.1, which='minor', linestyle=':', linewidth=0.5)
            ax2.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
            ax2.xaxis.set_major_locator(plt.matplotlib.dates.MinuteLocator(interval=10))
            ax2.xaxis.set_minor_locator(plt.matplotlib.dates.MinuteLocator(interval=1))
            plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
            
            # ====== ax3 MACD副图（用1分钟K线收盘价） ======
            minute_close = mapped_df['price'].resample('1min').last().dropna()
            minute_index = minute_close.index

            ema12 = minute_close.ewm(span=12, adjust=False).mean()
            ema26 = minute_close.ewm(span=26, adjust=False).mean()
            dif = ema12 - ema26  # DIF
            dea = dif.ewm(span=9, adjust=False).mean()  # DEA
            macd_hist = 2 * (dif - dea) # MACD柱子
            macd_colors = np.where(macd_hist > 0, 'red', np.where(macd_hist < 0, 'green', 'gray'))
            ax3.bar(minute_index, macd_hist, color=macd_colors, width=0.0005, alpha=0.7, label='MACD柱')
            ax3.plot(minute_index, dif, color='orange', label='DIF线')      # DIF橙色
            ax3.plot(minute_index, dea, color='deepskyblue', label='DEA线') # DEA天蓝色
            
            # 添加DIF极值点（极值图特有）
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
            
            if target_date:
                ax3.set_title(f'MACD指标 {target_date} (12,26,9)')
            else:
                ax3.set_title(f'MACD指标 {data_date} (12,26,9)')
            ax3.set_ylabel('MACD')
            ax3.set_xlabel('时间 (北京时间)', fontsize=13)
            ax3.annotate('所有横轴时间均为北京时间', xy=(1, 0), xycoords='axes fraction', fontsize=11, color='gray', ha='right', va='top')
            ax3.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
            ax3.xaxis.set_major_locator(plt.matplotlib.dates.MinuteLocator(interval=10))
            ax3.xaxis.set_minor_locator(plt.matplotlib.dates.MinuteLocator(interval=1))
            plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45)
            ax3.legend(loc='upper right')
            ax3.grid(True, alpha=0.3, which='major')
            ax3.grid(True, alpha=0.1, which='minor', linestyle=':', linewidth=0.5)
            
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
            # self._log.info(f"数据时间范围: {start_time} 到 {end_time}")
            # self._log.info(f"数据条数: {len(df)}")
            
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
            
            # 固定X轴范围为完整交易日，并确保时区匹配
            if len(mapped_df) > 0:
                base_date = mapped_df.index[0].date()
                tz_info = mapped_df.index[0].tzinfo
            else:
                base_date = data_date
                import pytz
                tz_info = pytz.timezone('Asia/Shanghai')

            x_min = pd.Timestamp(year=base_date.year, month=base_date.month, day=base_date.day,
                                 hour=9, minute=30, second=0, tz=tz_info)
            x_max = pd.Timestamp(year=base_date.year, month=base_date.month, day=base_date.day,
                                 hour=13, minute=30, second=0, tz=tz_info)

            for axis in (ax1, ax2, ax3):
                axis.set_xlim(x_min, x_max)

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
            ax1.grid(True, alpha=0.3, which='major')
            ax1.grid(True, alpha=0.1, which='minor', linestyle=':', linewidth=0.5)

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
                    # self._log.info(f"处理第 {i+1} 个信号: {signal}")
                    
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
                    
                    # self._log.info(f"绘制了 {len(minute_volume_mapped)} 分钟的成交量数据")
                else:
                    self._log.warning("没有交易时间内的分钟成交量数据")
            else:
                self._log.warning("无法计算分钟成交量数据")

            ax2.set_title('成交量', fontsize=12)
            ax2.set_ylabel('成交量', fontsize=10)
            ax2.grid(True, alpha=0.3, which='major')
            ax2.grid(True, alpha=0.1, which='minor', linestyle=':', linewidth=0.5)
            ax2.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
            ax2.xaxis.set_major_locator(plt.matplotlib.dates.MinuteLocator(interval=10))
            ax2.xaxis.set_minor_locator(plt.matplotlib.dates.MinuteLocator(interval=1))
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
                ax3.xaxis.set_minor_locator(plt.matplotlib.dates.MinuteLocator(interval=1))
                plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45)
                ax3.legend(loc='upper right')
                ax3.grid(True, alpha=0.3, which='major')
                ax3.grid(True, alpha=0.1, which='minor', linestyle=':', linewidth=0.5)

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

    def create_realtime_kline_chart(self, save_path: str = None, target_date: date = None, trade_signals: List[Dict] = None, technical_signals: List[Dict] = None, technical_signal_value: float = 0):
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
            # self._log.info(f"数据时间范围: {start_time} 到 {end_time}")
            # self._log.info(f"数据条数: {len(df)}")
            
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

            # 创建六联图，图片高度更大
            fig, (ax1, ax2, ax3, ax4, ax5, ax6) = plt.subplots(6, 1, figsize=(20, 48), height_ratios=[3, 1, 1, 1, 1, 1])

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
            
            # 数据验证和调试信息
            self._log.info(f"[K线图] complete_df列: {list(complete_df.columns)}")
            self._log.info(f"[K线图] complete_df行数: {len(complete_df)}")
            self._log.info(f"[K线图] time_mapping键数量: {len(time_mapping)}")
            self._log.info(f"[K线图] mapped_df列: {list(mapped_df.columns)}")
            self._log.info(f"[K线图] mapped_df行数: {len(mapped_df)}")
            
            # 检查mapped_df是否有price列
            if 'price' not in mapped_df.columns:
                self._log.error(f"[K线图] mapped_df缺少'price'列，可用列: {list(mapped_df.columns)}")
                # 尝试使用close列
                if 'close' in mapped_df.columns:
                    mapped_df['price'] = mapped_df['close']
                    self._log.info("[K线图] 使用'close'列作为'price'")
                else:
                    self._log.error("[K线图] mapped_df既没有'price'也没有'close'列，无法绘制")
                    return
            
            if len(mapped_df) == 0:
                self._log.error("[K线图] mapped_df为空，无法绘制")
                return
            
            # 固定X轴范围为完整交易日（不随数据量变化）
            # 获取基准日期和时区（从数据中取第一个有效时间）
            if len(mapped_df) > 0:
                base_date = mapped_df.index[0].date()
                # 获取数据的时区信息
                tz_info = mapped_df.index[0].tzinfo
            else:
                base_date = data_date
                # 如果没有数据，使用北京时区
                import pytz
                tz_info = pytz.timezone('Asia/Shanghai')
            
            # 设置固定的交易时间范围（必须带时区信息，与数据时间匹配）
            # 上午：9:30-11:30（映射后保持不变）
            # 下午：13:00-15:00（映射后为11:30-13:30，因为减去了1.5小时）
            x_min = pd.Timestamp(year=base_date.year, month=base_date.month, day=base_date.day, 
                                hour=9, minute=30, second=0, tz=tz_info)
            x_max = pd.Timestamp(year=base_date.year, month=base_date.month, day=base_date.day, 
                                hour=13, minute=30, second=0, tz=tz_info)  # 映射后的下午15:00
            
            # 验证price数据
            price_data = mapped_df['price'].dropna()
            self._log.info(f"[K线图] price数据条数: {len(price_data)}")
            if len(price_data) > 0:
                self._log.info(f"[K线图] price数据时间范围: {price_data.index.min()} 到 {price_data.index.max()}")
                # 确保x轴范围包含数据范围
                data_min = price_data.index.min()
                data_max = price_data.index.max()
                if data_min < x_min:
                    x_min = data_min - pd.Timedelta(minutes=5)  # 留出一些边距
                    self._log.info(f"[K线图] 调整x_min以包含数据: {x_min}")
                if data_max > x_max:
                    x_max = data_max + pd.Timedelta(minutes=5)  # 留出一些边距
                    self._log.info(f"[K线图] 调整x_max以包含数据: {x_max}")
            self._log.info(f"[K线图] 最终x轴范围: {x_min} 到 {x_max}")
            
            # 为每个子图设置相同的固定x轴范围
            for ax in [ax1, ax2, ax3, ax4, ax5, ax6]:
                ax.set_xlim(x_min, x_max)


            # ====== ax1主图（价格走势） ======
            # 绘制价格走势
            if len(price_data) > 0:
                if len(price_data) >= 2:
                    # 多个数据点时绘制线
                    ax1.plot(price_data.index, price_data.values, linewidth=1, color='blue', alpha=0.8, label='成交价', zorder=1)
                    self._log.info(f"[K线图] 已绘制价格线，数据点: {len(price_data)}")
                else:
                    # 单个数据点时使用scatter显示
                    ax1.scatter(price_data.index, price_data.values, s=50, color='blue', alpha=0.8, label='成交价', zorder=1)
                    self._log.info(f"[K线图] 已绘制价格点（单点），数据点: {len(price_data)}")
                
                # 如果有OHLC数据，绘制K线（蜡烛图）
                if all(col in mapped_df.columns for col in ['open', 'high', 'low', 'price']):
                    ohlc_data = mapped_df[['open', 'high', 'low', 'price']].dropna()
                    if len(ohlc_data) > 0:
                        # 绘制K线（简单的OHLC柱状图）
                        for idx, row in ohlc_data.iterrows():
                            open_val = row['open']
                            high_val = row['high']
                            low_val = row['low']
                            close_val = row['price']  # price就是close
                            
                            # 确定颜色：收盘价>开盘价为红色（涨），否则为绿色（跌）
                            color = 'red' if close_val >= open_val else 'green'
                            
                            # 绘制上下影线
                            ax1.plot([idx, idx], [low_val, high_val], color='black', linewidth=0.5, alpha=0.5, zorder=2)
                            
                            # 绘制实体（开盘价到收盘价）
                            body_height = abs(close_val - open_val)
                            body_bottom = min(open_val, close_val)
                            # 使用bar绘制实体，宽度很小
                            bar_width = pd.Timedelta(minutes=0.5).total_seconds() / 86400  # 转换为天为单位
                            ax1.bar(idx, body_height, bottom=body_bottom, width=bar_width, 
                                   color=color, alpha=0.7, edgecolor='black', linewidth=0.5, zorder=3)
                        
                        self._log.info(f"[K线图] 已绘制K线，数据点: {len(ohlc_data)}")
            else:
                self._log.warning("[K线图] price数据为空，无法绘制价格线")
            
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
            
            # 添加技术信号累积值显示
            # 根据信号值确定颜色和状态
            if technical_signal_value >= 100:
                signal_color = 'lightgreen'
                signal_status = '买入信号达标'
            elif technical_signal_value <= -100:
                signal_color = 'lightcoral'
                signal_status = '卖出信号达标'
            elif technical_signal_value > 0:
                signal_color = 'lightyellow'
                signal_status = '偏多信号'
            elif technical_signal_value < 0:
                signal_color = 'lightyellow'
                signal_status = '偏空信号'
            else:
                signal_color = 'lightgray'
                signal_status = '中性信号'
            
            technical_signal_info = f"技术信号累积值:\n"
            technical_signal_info += f"{technical_signal_value:.1f}\n"
            technical_signal_info += f"({signal_status})\n"
            technical_signal_info += f"买入阈值: {self.buy_threshold}\n"
            technical_signal_info += f"卖出阈值: {self.sell_threshold}"
            
            ax1.text(0.02, 0.65, technical_signal_info, 
                   transform=ax1.transAxes, verticalalignment='top', 
                   bbox=dict(boxstyle='round', facecolor=signal_color, alpha=0.8),
                   fontsize=9, fontweight='bold')
            
            ax1.legend(loc='upper right')
            
            # 设置x轴格式 - 显示北京时间
            ax1.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
            ax1.xaxis.set_major_locator(plt.matplotlib.dates.MinuteLocator(interval=10))  # 每10分钟一个刻度
            ax1.xaxis.set_minor_locator(plt.matplotlib.dates.MinuteLocator(interval=1))
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
                    # self._log.info(f"处理第 {i+1} 个信号: {signal}")
                    
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
                    # self._log.info(f"处理第 {i+1} 个技术信号: {signal}")
                    
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
            # self._log.info(f"[DEBUG K线图] 成交量数据检查:")
            # if not minute_volume_stats.empty:
            #     self._log.info(f"  ✓ 数据条数: {len(minute_volume_stats)}")
            #     self._log.info(f"  ✓ 总成交量: {minute_volume_stats['总成交量'].sum():,.0f}")
            #     self._log.info(f"  ✓ 平均成交量: {minute_volume_stats['总成交量'].mean():,.2f}")
            #     self._log.info(f"  ✓ 最大成交量: {minute_volume_stats['总成交量'].max():,.0f}")
            #     self._log.info(f"  ✓ 最小成交量: {minute_volume_stats['总成交量'].min():,.0f}")
            #     self._log.info(f"  ✓ 成交量>0: {(minute_volume_stats['总成交量'] > 0).sum()}/{len(minute_volume_stats)}")
            #     self._log.info(f"  ✓ 时间范围: {minute_volume_stats['minute_time'].min()} 到 {minute_volume_stats['minute_time'].max()}")
            #     self._log.info(f"  ✓ 前3条:")
            #     for idx, row in minute_volume_stats.head(3).iterrows():
            #         self._log.info(f"     {row['minute_time']} | 量:{row['总成交量']:>12,.0f} | O:{row['开盘价']:.3f} C:{row['收盘价']:.3f}")
            #     self._log.info(f"  ✓ 后3条:")
            #     for idx, row in minute_volume_stats.tail(3).iterrows():
            #         self._log.info(f"     {row['minute_time']} | 量:{row['总成交量']:>12,.0f} | O:{row['开盘价']:.3f} C:{row['收盘价']:.3f}")
            # else:
            #     self._log.error(f"  ✗ minute_volume_stats 为空！")
            # ====== END DEBUG ======
            
            if not minute_volume_stats.empty:
                # 创建时间映射，与主图保持一致
                minute_volume_filtered = minute_volume_stats[minute_volume_stats['minute_time'].dt.time < datetime_time(11, 30)]
                minute_volume_afternoon = minute_volume_stats[minute_volume_stats['minute_time'].dt.time > datetime_time(13, 0)]
                minute_volume_trading = pd.concat([minute_volume_filtered, minute_volume_afternoon])

                if len(minute_volume_trading) > 0:
                    minute_volume_mapped = minute_volume_trading.copy()
                    minute_volume_mapped['mapped_time'] = minute_volume_mapped['minute_time'].apply(
                        lambda x: x if x.time() < datetime_time(11, 30) else x - timedelta(hours=1, minutes=30)
                    )

                    colors = np.where(
                        minute_volume_mapped['收盘价'] > minute_volume_mapped['开盘价'],
                        'red',
                        np.where(minute_volume_mapped['收盘价'] < minute_volume_mapped['开盘价'], 'green', 'gray')
                    )

                    if len(minute_volume_mapped) > 1:
                        time_diffs = minute_volume_mapped['mapped_time'].diff().dropna()
                        avg_time_diff = time_diffs.mean()
                        bar_width = avg_time_diff.total_seconds() / 86400
                    else:
                        bar_width = 1 / 1440

                    ax2.bar(
                        minute_volume_mapped['mapped_time'],
                        minute_volume_mapped['总成交量'],
                        alpha=0.7,
                        color=colors,
                        width=bar_width,
                        label='每分钟成交量'
                    )
                else:
                    self._log.warning("没有交易时间内的分钟成交量数据")
            else:
                self._log.warning("无法计算分钟成交量数据")

            ax2.set_title('成交量', fontsize=12)
            ax2.set_ylabel('成交量', fontsize=10)
            ax2.grid(True, alpha=0.3, which='major')
            ax2.grid(True, alpha=0.1, which='minor', linestyle=':', linewidth=0.5)
            ax2.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
            ax2.xaxis.set_major_locator(plt.matplotlib.dates.MinuteLocator(interval=10))
            ax2.xaxis.set_minor_locator(plt.matplotlib.dates.MinuteLocator(interval=1))
            plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)

            # ====== ax3 MACD副图（用1分钟K线收盘价） ======
            minute_close = mapped_df['price'].resample('1min').last().dropna()
            minute_index = minute_close.index

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
            ax3.xaxis.set_minor_locator(plt.matplotlib.dates.MinuteLocator(interval=1))
            plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45)
            ax3.legend(loc='upper right')
            ax3.grid(True, alpha=0.3, which='major')
            ax3.grid(True, alpha=0.1, which='minor', linestyle=':', linewidth=0.5)

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
            ax4.xaxis.set_minor_locator(plt.matplotlib.dates.MinuteLocator(interval=1))
            plt.setp(ax4.xaxis.get_majorticklabels(), rotation=45)
            ax4.set_ylim(0, 100)
            ax4.legend(loc='upper right')
            ax4.grid(True, alpha=0.3, which='major')
            ax4.grid(True, alpha=0.1, which='minor', linestyle=':', linewidth=0.5)

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
            ax5.xaxis.set_minor_locator(plt.matplotlib.dates.MinuteLocator(interval=1))
            plt.setp(ax5.xaxis.get_majorticklabels(), rotation=45)
            ax5.legend(loc='upper right')
            ax5.grid(True, alpha=0.3, which='major')
            ax5.grid(True, alpha=0.1, which='minor', linestyle=':', linewidth=0.5)

            # ====== ax6 技术信号累积值副图 ======
            if self.technical_signal_history and len(self.technical_signal_history) > 0:
                # 将技术信号历史转换为DataFrame
                signal_df = pd.DataFrame(self.technical_signal_history)
                signal_df['timestamp'] = pd.to_datetime(signal_df['timestamp'])
                
                # 确保时区一致
                if signal_df['timestamp'].dt.tz is None:
                    import pytz
                    beijing_tz = pytz.timezone('Asia/Shanghai')
                    signal_df['timestamp'] = signal_df['timestamp'].dt.tz_localize(beijing_tz)
                
                # 应用时间映射（处理午休）
                signal_df['mapped_time'] = signal_df['timestamp'].apply(
                    lambda x: x if x.time() < datetime_time(11, 30) else x - timedelta(hours=1, minutes=30)
                )
                
                # 过滤掉午休时间
                signal_df = signal_df[
                    (signal_df['timestamp'].dt.time < datetime_time(11, 30)) | 
                    (signal_df['timestamp'].dt.time > datetime_time(13, 0))
                ]
                
                if len(signal_df) > 0:
                    # 绘制技术信号曲线
                    ax6.plot(signal_df['mapped_time'], signal_df['signal_value'], 
                            color='blue', linewidth=1.5, label='技术信号累积值')
                    
                    # 绘制买入和卖出阈值线
                    ax6.axhline(self.buy_threshold, color='red', linestyle='--', 
                               linewidth=1.5, label=f'买入阈值 ({self.buy_threshold})')
                    ax6.axhline(self.sell_threshold, color='green', linestyle='--', 
                               linewidth=1.5, label=f'卖出阈值 ({self.sell_threshold})')
                    ax6.axhline(0, color='gray', linestyle='-', linewidth=0.8, alpha=0.5)
                    
                    # 填充颜色区域
                    ax6.fill_between(signal_df['mapped_time'], 0, signal_df['signal_value'], 
                                    where=(signal_df['signal_value'] > 0), 
                                    color='red', alpha=0.2, label='多头区域')
                    ax6.fill_between(signal_df['mapped_time'], 0, signal_df['signal_value'], 
                                    where=(signal_df['signal_value'] < 0), 
                                    color='green', alpha=0.2, label='空头区域')
                    
                    # 显示当前信号值
                    current_signal = signal_df['signal_value'].iloc[-1]
                    current_time = signal_df['mapped_time'].iloc[-1]
                    ax6.scatter(current_time, current_signal, color='black', s=100, zorder=5)
                    
                    # 添加当前信号值标注
                    if current_signal >= self.buy_threshold:
                        signal_status = '买入信号达标'
                        status_color = 'red'
                    elif current_signal <= self.sell_threshold:
                        signal_status = '卖出信号达标'
                        status_color = 'green'
                    elif current_signal > 0:
                        signal_status = '偏多信号'
                        status_color = 'orange'
                    elif current_signal < 0:
                        signal_status = '偏空信号'
                        status_color = 'orange'
                    else:
                        signal_status = '中性信号'
                        status_color = 'gray'
                    
                    ax6.annotate(f'{current_signal:.1f}\n{signal_status}', 
                               xy=(current_time, current_signal),
                               xytext=(10, 10), textcoords='offset points',
                               fontsize=10, color=status_color, weight='bold',
                               bbox=dict(boxstyle='round,pad=0.5', facecolor=status_color, alpha=0.3))
                    
                    self._log.info(f"绘制了 {len(signal_df)} 个技术信号历史数据点")
            else:
                self._log.warning("没有技术信号历史数据")
            
            if target_date:
                ax6.set_title(f'技术信号累积值 {target_date} (北京时间)')
            else:
                ax6.set_title(f'技术信号累积值 {data_date} (北京时间)')
            ax6.set_ylabel('信号值')
            ax6.set_xlabel('时间 (北京时间)', fontsize=13)
            ax6.annotate('所有横轴时间均为北京时间', xy=(1, 0), xycoords='axes fraction', 
                        fontsize=11, color='gray', ha='right', va='top')
            ax6.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
            ax6.xaxis.set_major_locator(plt.matplotlib.dates.MinuteLocator(interval=10))  # 主刻度10分钟
            ax6.xaxis.set_minor_locator(plt.matplotlib.dates.MinuteLocator(interval=1))   # 次刻度1分钟
            plt.setp(ax6.xaxis.get_majorticklabels(), rotation=45)
            ax6.legend(loc='upper left')
            ax6.grid(True, alpha=0.3, which='major')  # 主网格线
            ax6.grid(True, alpha=0.1, which='minor', linestyle=':')  # 次网格线（1分钟）

            # 统一x轴格式化，主刻度10分钟，次刻度1分钟
            for ax in [ax1, ax2, ax3, ax4, ax5, ax6]:
                ax.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
                ax.xaxis.set_major_locator(plt.matplotlib.dates.MinuteLocator(interval=10))  # 主刻度10分钟
                ax.xaxis.set_minor_locator(plt.matplotlib.dates.MinuteLocator(interval=1))   # 次刻度1分钟
                plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
                # 同时设置主网格线和1分钟的细网格线
                ax.grid(True, alpha=0.3, which='major')  # 主网格线（10分钟）
                ax.grid(True, alpha=0.1, which='minor', linestyle=':', linewidth=0.5)  # 次网格线（1分钟）

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