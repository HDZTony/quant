#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
集成系统测试脚本
用于验证实时数据、技术指标计算和交易功能
"""

import time
import threading
from datetime import datetime
from collections import deque

# 导入自定义模块
from trading_system import JVQuantTradingClient

# 导入Nautilus Trader指标
try:
    from nautilus_trader.indicators.average.ema import ExponentialMovingAverage
    from nautilus_trader.indicators.momentum.rsi import RelativeStrengthIndex
    from nautilus_trader.indicators.macd import MovingAverageConvergenceDivergence  # 修复：正确的导入路径
    from kdj_indicator import KDJIndicator
    NAUTILUS_AVAILABLE = True
except ImportError:
    print("警告: Nautilus Trader不可用，将使用自定义指标")
    NAUTILUS_AVAILABLE = False
    # 这里可以添加自定义指标的导入作为备选


class SimpleEMA:
    """简化的EMA实现"""
    
    def __init__(self, period: int):
        self.period = period
        self.alpha = 2.0 / (period + 1.0)
        self.value = 0.0
        self.initialized = False
        self.count = 0
    
    def update(self, price: float):
        if not self.initialized:
            self.value = price
            self.count += 1
            if self.count >= self.period:
                self.initialized = True
        else:
            self.value = self.alpha * price + (1.0 - self.alpha) * self.value


class SimpleRSI:
    """简化的RSI实现"""
    
    def __init__(self, period: int = 14):
        self.period = period
        self.gains = deque(maxlen=period)
        self.losses = deque(maxlen=period)
        self.prev_price = None
        self.value = 50.0
        self.initialized = False
    
    def update(self, price: float):
        if self.prev_price is not None:
            change = price - self.prev_price
            if change > 0:
                self.gains.append(change)
                self.losses.append(0)
            else:
                self.gains.append(0)
                self.losses.append(abs(change))
            
            if len(self.gains) == self.period:
                self.initialized = True
                avg_gain = sum(self.gains) / self.period
                avg_loss = sum(self.losses) / self.period
                
                if avg_loss > 0:
                    rs = avg_gain / avg_loss
                    self.value = 100 - (100 / (1 + rs))
                else:
                    self.value = 100
        
        self.prev_price = price





class VolumeRatio:
    """量比计算器"""
    
    def __init__(self, period: int = 5):
        self.period = period
        self.volumes = deque(maxlen=period)
        self.volume_ratio = 1.0
    
    def update(self, volume: float):
        self.volumes.append(volume)
        
        if len(self.volumes) == self.period:
            avg_volume = sum(self.volumes) / len(self.volumes)
            if avg_volume > 0:
                self.volume_ratio = volume / avg_volume
            else:
                self.volume_ratio = 1.0


class TestDataProcessor:
    """测试数据处理器"""
    
    def __init__(self, token: str, stock_code: str):
        self.token = token
        self.stock_code = stock_code
        
        # 技术指标
        if NAUTILUS_AVAILABLE:
            self.macd = MovingAverageConvergenceDivergence(12, 26)  # 修复：移除第三个参数9
            self.rsi = RelativeStrengthIndex(14)
            self.kdj = KDJIndicator(9, 3, 3)
        else:
            # 使用自定义指标作为备选
            self.macd = None
            self.rsi = SimpleRSI(14)
            self.kdj = None
        
        self.volume_ratio = VolumeRatio(5)
        
        # 添加MACD相关存储
        self.macd_history = deque(maxlen=100)  # 存储DIF值
        self.signal_period = 9  # DEA计算周期
        
        # 数据存储
        self.price_history = deque(maxlen=100)
        self.volume_history = deque(maxlen=100)
        
        # 最新数据
        self.latest_price = 0.0
        self.latest_volume = 0.0
        self.latest_time = None
    
    def get_macd_signal(self) -> float:
        """获取DEA值（信号线）"""
        if not NAUTILUS_AVAILABLE or not self.macd or len(self.macd_history) < self.signal_period:
            return 0.0
        
        # 简单的EMA计算
        alpha = 2.0 / (self.signal_period + 1)
        dea = self.macd_history[0]
        for dif in self.macd_history[1:]:
            dea = alpha * dif + (1 - alpha) * dea
        return dea
    
    def get_macd_histogram(self) -> float:
        """获取MACD柱值"""
        if not NAUTILUS_AVAILABLE or not self.macd or not self.macd.initialized:
            return 0.0
        return self.macd.value - self.get_macd_signal()
    
    def update_data(self, price: float, volume: float, timestamp: str = None):
        """更新数据"""
        self.latest_price = price
        self.latest_volume = volume
        self.latest_time = timestamp or datetime.now().strftime("%H:%M:%S")
        
        # 添加到历史数据
        self.price_history.append(price)
        self.volume_history.append(volume)
        
        # 更新技术指标
        if NAUTILUS_AVAILABLE:
            self.macd.update_raw(price)
            # 更新MACD历史数据
            if self.macd.initialized:
                self.macd_history.append(self.macd.value)
            self.rsi.update_raw(price)
            self.kdj.update(price, price, price)  # 简化处理
        else:
            if self.rsi:
                self.rsi.update(price)
        
        self.volume_ratio.update(volume)
    
    def generate_signal(self) -> str:
        """生成交易信号"""
        if len(self.price_history) < 30:
            return "HOLD"
        
        # MACD信号
        macd_signal = "HOLD"
        if NAUTILUS_AVAILABLE and self.macd and self.macd.initialized:
            if self.macd.value > self.get_macd_signal() and self.get_macd_histogram() > 0:
                macd_signal = "BUY"
            elif self.macd.value < self.get_macd_signal() and self.get_macd_histogram() < 0:
                macd_signal = "SELL"
        
        # RSI信号
        rsi_signal = "HOLD"
        if self.rsi and self.rsi.initialized:
            if self.rsi.value < 30:
                rsi_signal = "BUY"
            elif self.rsi.value > 70:
                rsi_signal = "SELL"
        
        # KDJ信号
        kdj_signal = "HOLD"
        if NAUTILUS_AVAILABLE and self.kdj and self.kdj.initialized:
            if self.kdj.value_k < 20 and self.kdj.value_d < 20:
                kdj_signal = "BUY"
            elif self.kdj.value_k > 80 and self.kdj.value_d > 80:
                kdj_signal = "SELL"
        
        # 综合信号
        buy_count = sum(1 for signal in [macd_signal, rsi_signal, kdj_signal] if signal == "BUY")
        sell_count = sum(1 for signal in [macd_signal, rsi_signal, kdj_signal] if signal == "SELL")
        
        if buy_count >= 2:
            return "BUY"
        elif sell_count >= 2:
            return "SELL"
        else:
            return "HOLD"
    
    def print_status(self):
        """打印当前状态"""
        print("\n" + "="*60)
        print(f"时间: {self.latest_time}")
        print(f"价格: {self.latest_price:.2f}")
        print(f"成交量: {self.latest_volume:.0f}")
        
        # MACD
        if NAUTILUS_AVAILABLE and self.macd and self.macd.initialized:
            print(f"MACD: {self.macd.value:.4f}, Signal: {self.get_macd_signal():.4f}, Histogram: {self.get_macd_histogram():.4f}")
        
        # RSI
        if self.rsi and self.rsi.initialized:
            print(f"RSI: {self.rsi.value:.2f}")
        
        # KDJ
        if NAUTILUS_AVAILABLE and self.kdj and self.kdj.initialized:
            print(f"KDJ: K={self.kdj.value_k:.2f}, D={self.kdj.value_d:.2f}, J={self.kdj.value_j:.2f}")
        
        # 量比
        print(f"量比: {self.volume_ratio.volume_ratio:.2f}")
        
        # 信号
        signal = self.generate_signal()
        print(f"交易信号: {signal}")
        print("="*60)


def simulate_market_data():
    """模拟市场数据"""
    # 模拟价格数据（简单的随机游走）
    import random
    
    base_price = 1.0
    prices = []
    
    for i in range(100):
        # 随机价格变化
        change = random.uniform(-0.02, 0.02)
        base_price += change
        base_price = max(0.1, base_price)  # 确保价格为正
        prices.append(base_price)
    
    return prices


def test_trading_client():
    """测试交易客户端"""
    print("测试交易客户端功能")
    print("="*40)
    
    TOKEN = "d0c519adcd47d266f1c96750d4e80aa6"
    trading_client = JVQuantTradingClient(TOKEN)
    
    # 测试获取交易服务器
    if trading_client.trade_server:
        print(f"✅ 交易服务器: {trading_client.trade_server}")
    else:
        print("❌ 无法获取交易服务器")
        return
    
    # 测试查询功能（不需要登录）
    print("\n测试查询功能（模拟）:")
    print("- 查询持仓")
    print("- 查询交易记录")
    print("- 买入/卖出委托")
    print("- 撤销委托")
    
    print("\n✅ 交易客户端功能测试完成")


def test_indicators():
    """测试技术指标"""
    print("\n测试技术指标计算")
    print("="*40)
    
    # 创建数据处理器
    processor = TestDataProcessor("159506")
    
    # 生成模拟数据
    prices = simulate_market_data()
    volumes = [random.randint(1000, 10000) for _ in range(len(prices))]
    
    print(f"生成 {len(prices)} 个数据点进行测试")
    
    # 逐步更新数据并观察指标变化
    for i, (price, volume) in enumerate(zip(prices, volumes)):
        processor.update_data(price, volume, f"T{i:02d}")
        
        # 每10个数据点打印一次状态
        if (i + 1) % 10 == 0 or i < 5:
            processor.print_status()
        
        time.sleep(0.1)  # 模拟实时数据
    
    print("\n✅ 技术指标测试完成")


def test_signal_generation():
    """测试信号生成"""
    print("\n测试交易信号生成")
    print("="*40)
    
    processor = TestDataProcessor("159506")
    
    # 模拟不同的市场情况
    scenarios = [
        ("上涨趋势", [1.0 + i * 0.01 for i in range(50)]),
        ("下跌趋势", [1.5 - i * 0.01 for i in range(50)]),
        ("震荡市场", [1.0 + 0.05 * (i % 10 - 5) for i in range(50)])
    ]
    
    for scenario_name, prices in scenarios:
        print(f"\n测试场景: {scenario_name}")
        print("-" * 30)
        
        for i, price in enumerate(prices):
            volume = random.randint(1000, 10000)
            processor.update_data(price, volume, f"T{i:02d}")
            
            if i >= 30:  # 等待指标初始化
                signal = processor.generate_signal()
                if signal != "HOLD":
                    print(f"时间: T{i:02d}, 价格: {price:.2f}, 信号: {signal}")
        
        # 重置处理器
        processor = TestDataProcessor("159506")
    
    print("\n✅ 信号生成测试完成")


def main():
    """主测试函数"""
    print("jvQuant集成系统功能测试")
    print("="*60)
    
    try:
        # 1. 测试交易客户端
        test_trading_client()
        
        # 2. 测试技术指标
        test_indicators()
        
        # 3. 测试信号生成
        test_signal_generation()
        
        print("\n" + "="*60)
        print("所有测试完成!")
        print("="*60)
        
    except Exception as e:
        print(f"测试过程中出现异常: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    import random
    main() 