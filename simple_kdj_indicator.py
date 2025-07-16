#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
简化的KDJ指标实现
不依赖Nautilus Trader，用于测试和演示
"""

from collections import deque
from typing import Optional


class SimpleKDJIndicator:
    """
    简化的KDJ指标实现
    
    KDJ指标是一个基于随机指标的技术分析工具，包含K、D、J三条线。
    
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
        if period <= 0:
            raise ValueError("period must be positive")
        if k_period <= 0:
            raise ValueError("k_period must be positive")
        if d_period <= 0:
            raise ValueError("d_period must be positive")
        
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
        
        # 状态标志
        self.initialized = False
        self.has_inputs = False

    def update_raw(self, high: float, low: float, close: float):
        """
        更新指标值
        
        Parameters
        ----------
        high : float
            最高价
        low : float
            最低价
        close : float
            收盘价
        """
        # 检查是否是第一个输入
        if not self.has_inputs:
            self.has_inputs = True

        # 添加新的高低价
        self._highs.append(high)
        self._lows.append(low)

        # 初始化逻辑
        if not self.initialized:
            if len(self._highs) == self.period and len(self._lows) == self.period:
                self.initialized = True

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

    def reset(self):
        """重置指标状态"""
        self._highs.clear()
        self._lows.clear()
        
        self.value_k = 50.0
        self.value_d = 50.0
        self.value_j = 50.0
        
        self._prev_k = 50.0
        self._prev_d = 50.0
        
        self.initialized = False
        self.has_inputs = False

    def update(self, high: float, low: float, close: float):
        self.update_raw(high, low, close)


def test_simple_kdj():
    """测试简化的KDJ指标"""
    print("测试简化的KDJ指标")
    print("="*40)
    
    # 创建KDJ指标
    kdj = SimpleKDJIndicator(period=9, k_period=3, d_period=3)
    
    # 模拟价格数据
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
    
    print("价格数据: (最高价, 最低价, 收盘价)")
    print("-" * 40)
    
    for i, (high, low, close) in enumerate(test_data):
        kdj.update(high, low, close)
        
        print(f"数据点 {i+1:2d}: ({high:6.1f}, {low:6.1f}, {close:6.1f})")
        
        if kdj.initialized:
            print(f"  K: {kdj.value_k:6.2f}, D: {kdj.value_d:6.2f}, J: {kdj.value_j:6.2f}")
        else:
            print(f"  初始化中... (需要 {kdj.period} 个数据点)")
        
        print()
    
    print("✅ KDJ指标测试完成")


if __name__ == "__main__":
    test_simple_kdj() 