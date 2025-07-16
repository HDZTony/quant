#!/usr/bin/env python3
# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2025 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------

"""
KDJ指标实现

KDJ指标是基于随机指标(Stochastics)发展而来的技术分析工具，
包含三条线：K线、D线和J线。

计算公式：
1. K线 = 2/3 × 前一日K值 + 1/3 × 今日RSV
2. D线 = 2/3 × 前一日D值 + 1/3 × 今日K值  
3. J线 = 3 × K值 - 2 × D值

其中RSV = (收盘价 - 最低价) / (最高价 - 最低价) × 100
"""

from collections import deque
from typing import Optional

from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.indicators.base.indicator import Indicator
from nautilus_trader.indicators.average.ema import ExponentialMovingAverage
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import PriceType


class KDJIndicator(Indicator):
    """
    KDJ指标实现
    
    KDJ指标是一个基于随机指标的技术分析工具，包含K、D、J三条线。
    
    Parameters
    ----------
    period : int
        KDJ计算周期，通常为9
    k_period : int
        K线平滑周期，通常为3
    d_period : int
        D线平滑周期，通常为3
        
    Raises
    ------
    ValueError
        If `period` is not positive (> 0).
    ValueError
        If `k_period` is not positive (> 0).
    ValueError
        If `d_period` is not positive (> 0).
    """

    def __init__(self, period: int = 9, k_period: int = 3, d_period: int = 3):
        PyCondition.positive_int(period, "period")
        PyCondition.positive_int(k_period, "k_period")
        PyCondition.positive_int(d_period, "d_period")
        super().__init__(params=[period, k_period, d_period])

        self.period = period
        self.k_period = k_period
        self.d_period = d_period
        
        # 存储最高价和最低价
        self._highs = deque(maxlen=period)
        self._lows = deque(maxlen=period)
        
        # K、D、J值
        self.value_k = 50.0  # 初始值通常设为50
        self.value_d = 50.0  # 初始值通常设为50
        self.value_j = 50.0  # 初始值通常设为50
        
        # 前一日值
        self._prev_k = 50.0
        self._prev_d = 50.0

    def handle_quote_tick(self, tick: QuoteTick):
        """
        KDJ指标通常基于K线数据计算，不支持tick数据
        """
        PyCondition.not_none(tick, "tick")
        # KDJ需要OHLC数据，tick数据不适用

    def handle_trade_tick(self, tick: TradeTick):
        """
        KDJ指标通常基于K线数据计算，不支持tick数据
        """
        PyCondition.not_none(tick, "tick")
        # KDJ需要OHLC数据，tick数据不适用

    def handle_bar(self, bar: Bar):
        """
        Update the indicator with the given bar.

        Parameters
        ----------
        bar : Bar
            The update bar to handle.
        """
        PyCondition.not_none(bar, "bar")
        
        self.update_raw(
            bar.high.as_double(),
            bar.low.as_double(),
            bar.close.as_double(),
        )

    def update_raw(self, high: float, low: float, close: float):
        """
        Update the indicator with the given raw values.

        Parameters
        ----------
        high : float
            The high price.
        low : float
            The low price.
        close : float
            The close price.
        """
        # 检查是否是第一个输入
        if not self.has_inputs:
            self._set_has_inputs(True)

        # 添加新的高低价
        self._highs.append(high)
        self._lows.append(low)

        # 初始化逻辑
        if not self.initialized:
            if len(self._highs) == self.period and len(self._lows) == self.period:
                self._set_initialized(True)

        # 计算RSV
        if len(self._highs) >= self.period:
            period_high = max(self._highs)
            period_low = min(self._lows)
            
            if period_high == period_low:
                rsv = 50.0  # 避免除零，设为中性值
            else:
                rsv = (close - period_low) / (period_high - period_low) * 100.0
            
            # 计算K值：K = 2/3 × 前一日K值 + 1/3 × 今日RSV
            self.value_k = (2.0 / 3.0) * self._prev_k + (1.0 / 3.0) * rsv
            
            # 计算D值：D = 2/3 × 前一日D值 + 1/3 × 今日K值
            self.value_d = (2.0 / 3.0) * self._prev_d + (1.0 / 3.0) * self.value_k
            
            # 计算J值：J = 3 × K值 - 2 × D值
            self.value_j = 3.0 * self.value_k - 2.0 * self.value_d
            
            # 更新前一日值
            self._prev_k = self.value_k
            self._prev_d = self.value_d

    def _reset(self):
        """重置指标状态"""
        self._highs.clear()
        self._lows.clear()
        
        self.value_k = 50.0
        self.value_d = 50.0
        self.value_j = 50.0
        
        self._prev_k = 50.0
        self._prev_d = 50.0


class KDJIndicatorWithEMA(Indicator):
    """
    使用EMA平滑的KDJ指标实现
    
    这个版本使用指数移动平均来平滑K和D值，而不是简单的加权平均。
    
    Parameters
    ----------
    period : int
        KDJ计算周期，通常为9
    k_period : int
        K线平滑周期，通常为3
    d_period : int
        D线平滑周期，通常为3
    """

    def __init__(self, period: int = 9, k_period: int = 3, d_period: int = 3):
        PyCondition.positive_int(period, "period")
        PyCondition.positive_int(k_period, "k_period")
        PyCondition.positive_int(d_period, "d_period")
        super().__init__(params=[period, k_period, d_period])

        self.period = period
        
        # 使用EMA来平滑K和D值
        self.k_ema = ExponentialMovingAverage(period=k_period)
        self.d_ema = ExponentialMovingAverage(period=d_period)
        
        # 存储最高价和最低价
        self._highs = deque(maxlen=period)
        self._lows = deque(maxlen=period)
        
        # K、D、J值
        self.value_k = 50.0
        self.value_d = 50.0
        self.value_j = 50.0

    def handle_bar(self, bar: Bar):
        """Update the indicator with the given bar."""
        PyCondition.not_none(bar, "bar")
        
        self.update_raw(
            bar.high.as_double(),
            bar.low.as_double(),
            bar.close.as_double(),
        )

    def update_raw(self, high: float, low: float, close: float):
        """Update the indicator with the given raw values."""
        if not self.has_inputs:
            self._set_has_inputs(True)

        self._highs.append(high)
        self._lows.append(low)

        if not self.initialized:
            if len(self._highs) == self.period and len(self._lows) == self.period:
                self._set_initialized(True)

        if len(self._highs) >= self.period:
            period_high = max(self._highs)
            period_low = min(self._lows)
            
            if period_high == period_low:
                rsv = 50.0
            else:
                rsv = (close - period_low) / (period_high - period_low) * 100.0
            
            # 使用EMA平滑K和D值
            self.k_ema.update_raw(rsv)
            self.value_k = self.k_ema.value
            
            self.d_ema.update_raw(self.value_k)
            self.value_d = self.d_ema.value
            
            # 计算J值
            self.value_j = 3.0 * self.value_k - 2.0 * self.value_d

    def _reset(self):
        """重置指标状态"""
        self._highs.clear()
        self._lows.clear()
        
        self.k_ema.reset()
        self.d_ema.reset()
        
        self.value_k = 50.0
        self.value_d = 50.0
        self.value_j = 50.0


def test_kdj_indicator():
    """测试KDJ指标"""
    print("=" * 60)
    print("KDJ指标测试")
    print("=" * 60)
    
    # 创建KDJ指标
    kdj = KDJIndicator(period=9, k_period=3, d_period=3)
    
    # 模拟K线数据 (OHLC)
    test_data = [
        (100.0, 95.0, 98.0),   # (high, low, close)
        (102.0, 97.0, 101.0),
        (105.0, 100.0, 103.0),
        (108.0, 102.0, 106.0),
        (110.0, 105.0, 108.0),
        (112.0, 107.0, 110.0),
        (115.0, 110.0, 113.0),
        (118.0, 112.0, 116.0),
        (120.0, 115.0, 118.0),
        (122.0, 117.0, 120.0),
        (125.0, 120.0, 123.0),
        (128.0, 122.0, 126.0),
    ]
    
    print("\n测试数据:")
    print("序号\t最高价\t最低价\t收盘价\tK值\t\tD值\t\tJ值")
    print("-" * 70)
    
    for i, (high, low, close) in enumerate(test_data):
        kdj.update_raw(high, low, close)
        
        if kdj.initialized:
            print(f"{i+1}\t{high:.1f}\t{low:.1f}\t{close:.1f}\t{kdj.value_k:.2f}\t\t{kdj.value_d:.2f}\t\t{kdj.value_j:.2f}")
        else:
            print(f"{i+1}\t{high:.1f}\t{low:.1f}\t{close:.1f}\t--\t\t--\t\t--")
    
    print(f"\n最终KDJ值:")
    print(f"K值: {kdj.value_k:.2f}")
    print(f"D值: {kdj.value_d:.2f}")
    print(f"J值: {kdj.value_j:.2f}")
    
    # 测试EMA版本
    print("\n" + "=" * 60)
    print("KDJ指标（EMA平滑版本）测试")
    print("=" * 60)
    
    kdj_ema = KDJIndicatorWithEMA(period=9, k_period=3, d_period=3)
    
    print("\n测试数据:")
    print("序号\t最高价\t最低价\t收盘价\tK值\t\tD值\t\tJ值")
    print("-" * 70)
    
    for i, (high, low, close) in enumerate(test_data):
        kdj_ema.update_raw(high, low, close)
        
        if kdj_ema.initialized:
            print(f"{i+1}\t{high:.1f}\t{low:.1f}\t{close:.1f}\t{kdj_ema.value_k:.2f}\t\t{kdj_ema.value_d:.2f}\t\t{kdj_ema.value_j:.2f}")
        else:
            print(f"{i+1}\t{high:.1f}\t{low:.1f}\t{close:.1f}\t--\t\t--\t\t--")
    
    print(f"\n最终KDJ值（EMA版本）:")
    print(f"K值: {kdj_ema.value_k:.2f}")
    print(f"D值: {kdj_ema.value_d:.2f}")
    print(f"J值: {kdj_ema.value_j:.2f}")


if __name__ == "__main__":
    test_kdj_indicator() 