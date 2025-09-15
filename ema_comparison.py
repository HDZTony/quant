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

import time
from decimal import Decimal

from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.indicators.average.ema import ExponentialMovingAverage
from nautilus_trader.indicators import Indicator
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import PriceType


class PyExponentialMovingAverage(Indicator):
    """
    Python版本的EMA指标实现（与ema_python.py相同）
    """

    def __init__(self, period: int, price_type: PriceType = PriceType.LAST):
        PyCondition.positive_int(period, "period")
        super().__init__(params=[period])

        self.period = period
        self.price_type = price_type
        self.alpha = 2.0 / (period + 1.0)
        self.value = 0.0
        self.count = 0

    def handle_quote_tick(self, tick: QuoteTick):
        PyCondition.not_none(tick, "tick")
        self.update_raw(tick.extract_price(self.price_type).as_double())

    def handle_trade_tick(self, tick: TradeTick):
        PyCondition.not_none(tick, "tick")
        self.update_raw(tick.price.as_double())

    def handle_bar(self, bar: Bar):
        PyCondition.not_none(bar, "bar")
        self.update_raw(bar.close.as_double())

    def update_raw(self, value: float):
        if not self.has_inputs:
            self.value = value

        self.value = self.alpha * value + ((1.0 - self.alpha) * self.value)
        self.count += 1

        if not self.initialized:
            self._set_has_inputs(True)
            if self.count >= self.period:
                self._set_initialized(True)

    def _reset(self):
        self.value = 0.0
        self.count = 0


def performance_test():
    """性能测试：比较内置EMA和Python版本EMA的性能"""
    print("=" * 60)
    print("EMA指标对比测试")
    print("=" * 60)
    
    # 测试数据
    test_prices = [100.0, 101.0, 99.0, 102.0, 98.0, 103.0, 97.0, 104.0, 96.0, 105.0] * 1000  # 10000个数据点
    
    # 测试内置EMA
    print("\n1. 测试内置EMA（Cython实现）")
    builtin_ema = ExponentialMovingAverage(period=10)
    
    start_time = time.time()
    for price in test_prices:
        builtin_ema.update_raw(price)
    builtin_time = time.time() - start_time
    
    print(f"   处理 {len(test_prices)} 个数据点耗时: {builtin_time:.4f} 秒")
    print(f"   最终EMA值: {builtin_ema.value:.4f}")
    print(f"   是否已初始化: {builtin_ema.initialized}")
    
    # 测试Python版本EMA
    print("\n2. 测试Python版本EMA")
    python_ema = PyExponentialMovingAverage(period=10)
    
    start_time = time.time()
    for price in test_prices:
        python_ema.update_raw(price)
    python_time = time.time() - start_time
    
    print(f"   处理 {len(test_prices)} 个数据点耗时: {python_time:.4f} 秒")
    print(f"   最终EMA值: {python_ema.value:.4f}")
    print(f"   是否已初始化: {python_ema.initialized}")
    
    # 性能对比
    print("\n3. 性能对比")
    if python_time > 0:
        speedup = python_time / builtin_time
        print(f"   内置EMA比Python版本快 {speedup:.2f} 倍")
    
    # 结果验证
    print("\n4. 结果验证")
    diff = abs(builtin_ema.value - python_ema.value)
    print(f"   两个EMA值的差异: {diff:.8f}")
    if diff < 1e-10:
        print("   ✅ 两个实现的结果一致")
    else:
        print("   ❌ 两个实现的结果不一致")


def usage_examples():
    """使用示例：展示如何使用内置EMA"""
    print("\n" + "=" * 60)
    print("内置EMA使用示例")
    print("=" * 60)
    
    # 1. 基本使用
    print("\n1. 基本使用")
    ema = ExponentialMovingAverage(period=10)
    
    # 模拟价格数据
    prices = [100.0, 101.0, 99.0, 102.0, 98.0, 103.0, 97.0, 104.0, 96.0, 105.0]
    
    for i, price in enumerate(prices):
        ema.update_raw(price)
        print(f"   价格 {i+1}: {price:.2f}, EMA: {ema.value:.4f}")
    
    print(f"   最终EMA值: {ema.value:.4f}")
    
    # 2. 重置功能
    print("\n2. 重置功能")
    ema.reset()
    print(f"   重置后EMA值: {ema.value:.4f}")
    print(f"   重置后是否已初始化: {ema.initialized}")
    
    # 3. 不同价格类型
    print("\n3. 不同价格类型")
    ema_bid = ExponentialMovingAverage(period=5, price_type=PriceType.BID)
    ema_ask = ExponentialMovingAverage(period=5, price_type=PriceType.ASK)
    ema_mid = ExponentialMovingAverage(period=5, price_type=PriceType.MID)
    
    print("   创建了三个EMA指标，分别使用BID、ASK、MID价格类型")


def feature_comparison():
    """功能对比：内置EMA vs Python版本EMA"""
    print("\n" + "=" * 60)
    print("功能对比：内置EMA vs Python版本EMA")
    print("=" * 60)
    
    print("\n内置EMA（Cython实现）:")
    print("✅ 高性能 - 用Cython编写，编译为C扩展")
    print("✅ 内存效率高 - 优化的内存管理")
    print("✅ 生产就绪 - 经过充分测试和优化")
    print("✅ 类型安全 - 编译时类型检查")
    print("✅ 与平台深度集成 - 直接使用平台的数据类型")
    
    print("\nPython版本EMA:")
    print("✅ 易于理解 - 纯Python代码，逻辑清晰")
    print("✅ 易于修改 - 可以快速调整算法")
    print("✅ 教学价值 - 帮助理解EMA的工作原理")
    print("❌ 性能较低 - 解释执行，速度较慢")
    print("❌ 内存效率低 - Python对象开销")
    print("❌ 不适合生产 - 仅用于学习和原型开发")


if __name__ == "__main__":
    print("Nautilus Trader EMA指标对比分析")
    print("=" * 60)
    
    # 运行性能测试
    performance_test()
    
    # 显示使用示例
    usage_examples()
    
    # 功能对比
    feature_comparison()
    
    print("\n" + "=" * 60)
    print("总结")
    print("=" * 60)
    print("1. Nautilus Trader确实有内置的EMA指标（ExponentialMovingAverage）")
    print("2. 内置版本用Cython编写，性能远优于Python版本")
    print("3. 建议在生产环境中使用内置版本")
    print("4. Python版本主要用于学习和理解算法原理")
    print("5. 两个版本的API接口完全一致，可以无缝切换") 