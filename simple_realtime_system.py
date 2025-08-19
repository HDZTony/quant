#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
简化的jvQuant实时技术指标计算系统
先测试基本的数据接收和指标计算功能
"""

import requests
import websocket
import zlib
import time
import threading
from datetime import datetime
from collections import deque
import json

# 尝试导入Nautilus Trader指标，如果失败则使用简化版本
try:
    from nautilus_trader.indicators.average.ema import ExponentialMovingAverage
    from nautilus_trader.indicators.momentum.rsi import RelativeStrengthIndex
    from nautilus_trader.indicators.macd import MovingAverageConvergenceDivergence  # 修复：正确的导入路径
    from kdj_indicator import KDJIndicator
    NAUTILUS_AVAILABLE = True
except ImportError:
    print("警告: Nautilus Trader指标不可用，使用简化版本")
    NAUTILUS_AVAILABLE = False


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
    
    def reset(self):
        self.value = 0.0
        self.initialized = False
        self.count = 0


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
    
    def reset(self):
        self.gains.clear()
        self.losses.clear()
        self.prev_price = None
        self.value = 50.0
        self.initialized = False





class SimpleKDJ:
    """简化的KDJ实现"""
    
    def __init__(self, period: int = 9, k_period: int = 3, d_period: int = 3):
        self.period = period
        self.k_period = k_period
        self.d_period = d_period
        self.highs = deque(maxlen=period)
        self.lows = deque(maxlen=period)
        self.k_ema = SimpleEMA(k_period)
        self.d_ema = SimpleEMA(d_period)
        self.value_k = 50.0
        self.value_d = 50.0
        self.value_j = 50.0
        self.initialized = False
    
    def update(self, high: float, low: float, close: float):
        self.highs.append(high)
        self.lows.append(low)
        
        if len(self.highs) >= self.period:
            period_high = max(self.highs)
            period_low = min(self.lows)
            
            if period_high == period_low:
                rsv = 50.0
            else:
                rsv = (close - period_low) / (period_high - period_low) * 100.0
            
            self.k_ema.update(rsv)
            self.value_k = self.k_ema.value
            
            self.d_ema.update(self.value_k)
            self.value_d = self.d_ema.value
            
            self.value_j = 3.0 * self.value_k - 2.0 * self.value_d
            
            if self.k_ema.initialized and self.d_ema.initialized:
                self.initialized = True
    
    def reset(self):
        self.highs.clear()
        self.lows.clear()
        self.k_ema.reset()
        self.d_ema.reset()
        self.value_k = 50.0
        self.value_d = 50.0
        self.value_j = 50.0
        self.initialized = False


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
    
    def reset(self):
        self.volumes.clear()
        self.volume_ratio = 1.0


class JVQuantSimpleClient:
    """简化的jvQuant客户端"""
    
    def __init__(self, token: str, stock_code: str):
        self.token = token
        self.stock_code = stock_code
        self.ws = None
        self.is_connected = False
        
        # 技术指标 - 使用Nautilus Trader的指标
        self.macd = MovingAverageConvergenceDivergence(12, 26)  # 修复：移除第三个参数9
        self.rsi = RelativeStrengthIndex(14)
        # 使用自定义KDJ
        self.kdj = SimpleKDJ(9, 3, 3)
        
        self.volume_ratio = VolumeRatio(5)
        
        # 添加MACD相关存储
        self.macd_history = deque(maxlen=100)  # 存储DIF值
        self.signal_period = 9  # DEA计算周期
        
        # 数据存储
        self.latest_data = {
            'price': 0.0,
            'volume': 0.0,
            'time': None,
            'high': 0.0,
            'low': 0.0,
            'open': 0.0,
            'close': 0.0
        }
        
        # 历史数据
        self.price_history = deque(maxlen=1000)
        self.volume_history = deque(maxlen=1000)
        
        # 数据锁
        self.data_lock = threading.Lock()
    
    def get_server(self):
        """获取服务器地址"""
        url = f"http://jvQuant.com/query/server?market=ab&type=websocket&token={self.token}"
        
        try:
            response = requests.get(url, timeout=10)
            data = response.json()
            
            if data.get("code") == "0":
                return data.get("server")
            else:
                print(f"获取服务器失败: {data}")
                return None
        except Exception as e:
            print(f"获取服务器地址失败: {e}")
            return None
    
    def connect(self):
        """连接WebSocket"""
        server = self.get_server()
        if not server:
            print("无法获取服务器地址")
            return False
        
        ws_url = f"ws://{server}/?token={self.token}"
        print(f"连接到: {ws_url}")
        
        self.ws = websocket.WebSocketApp(
            ws_url,
            on_open=self.on_open,
            on_data=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        
        self.ws.run_forever()
        return True
    
    def on_open(self, ws):
        """连接打开"""
        print("WebSocket连接已建立")
        self.is_connected = True
        
        # 订阅Level1数据
        subscription = f"all=lv1_{self.stock_code}"
        ws.send(subscription)
        print(f"已订阅: {subscription}")
    
    def on_message(self, ws, message, type, flag):
        """接收消息"""
        try:
            if type == websocket.ABNF.OPCODE_TEXT:
                print(f"文本消息: {message}")
            elif type == websocket.ABNF.OPCODE_BINARY:
                # 解压缩数据
                decompressed = zlib.decompress(message, -zlib.MAX_WBITS)
                data_str = decompressed.decode("utf-8")
                
                # 处理数据
                lines = data_str.strip().split('\n')
                for line in lines:
                    if line.strip():
                        self.process_data(line)
                        
        except Exception as e:
            print(f"处理消息失败: {e}")
    
    def process_data(self, data: str):
        """处理行情数据"""
        try:
            if data.startswith('lv1_'):
                # 解析Level1数据
                parts = data.split('=')
                if len(parts) != 2:
                    return
                
                code_part = parts[0]
                data_part = parts[1]
                
                stock_code = code_part[4:]  # 去掉'lv1_'前缀
                if stock_code != self.stock_code:
                    return
                
                fields = data_part.split(',')
                if len(fields) >= 6:
                    push_time = fields[0]
                    stock_name = fields[1]
                    latest_price = float(fields[2])
                    change_percent = float(fields[3])
                    turnover = float(fields[4])
                    volume = float(fields[5])
                    
                    with self.data_lock:
                        # 更新最新数据
                        self.latest_data['price'] = latest_price
                        self.latest_data['volume'] = volume
                        self.latest_data['time'] = push_time
                        self.latest_data['close'] = latest_price
                        
                        # 添加到历史数据
                        self.price_history.append(latest_price)
                        self.volume_history.append(volume)
                        
                        # 更新技术指标
                        self.update_indicators(latest_price, volume)
                        
                        # 打印指标
                        self.print_indicators()
                        
        except Exception as e:
            print(f"处理数据失败: {e}")
    
    def update_indicators(self, price: float, volume: float):
        """更新技术指标"""
        # 更新MACD - 使用Nautilus Trader的MACD
        self.macd.update_raw(price)
        
        # 更新MACD历史数据
        if self.macd.initialized:
            self.macd_history.append(self.macd.value)
        
        # 更新RSI - 使用Nautilus Trader的RSI
        self.rsi.update_raw(price)
        
        # 更新KDJ (使用当前价格作为high/low/close的近似)
        self.kdj.update(price, price, price)
        
        # 更新量比
        self.volume_ratio.update(volume)
    
    def print_indicators(self):
        """打印技术指标"""
        if len(self.price_history) < 10:
            return
        
        print("\n" + "="*60)
        print(f"时间: {self.latest_data['time']}")
        print(f"价格: {self.latest_data['price']:.2f}")
        print(f"成交量: {self.latest_data['volume']:.0f}")
        
        # MACD - 使用Nautilus Trader的MACD
        if self.macd.initialized:
            print(f"MACD: {self.macd.value:.4f}, Signal: {self.get_macd_signal():.4f}, Histogram: {self.get_macd_histogram():.4f}")
        
        # RSI - 使用Nautilus Trader的RSI
        if self.rsi.initialized:
            print(f"RSI: {self.rsi.value:.2f}")
        
        # KDJ
        if self.kdj.initialized:
            print(f"KDJ: K={self.kdj.value_k:.2f}, D={self.kdj.value_d:.2f}, J={self.kdj.value_j:.2f}")
        
        # 量比
        print(f"量比: {self.volume_ratio.volume_ratio:.2f}")
        print("="*60)
    
    def on_error(self, ws, error):
        """错误处理"""
        print(f"WebSocket错误: {error}")
        self.is_connected = False
    
    def on_close(self, ws, code, msg):
        """连接关闭"""
        print(f"WebSocket连接关闭: {code} - {msg}")
        self.is_connected = False
    
    def disconnect(self):
        """断开连接"""
        if self.ws:
            self.ws.close()

    def get_macd_signal(self) -> float:
        """获取DEA值（信号线）"""
        if len(self.macd_history) < self.signal_period:
            return 0.0
        
        # 简单的EMA计算
        alpha = 2.0 / (self.signal_period + 1)
        dea = self.macd_history[0]
        for dif in self.macd_history[1:]:
            dea = alpha * dif + (1 - alpha) * dea
        return dea
    
    def get_macd_histogram(self) -> float:
        """获取MACD柱值"""
        if not self.macd.initialized:
            return 0.0
        return self.macd.value - self.get_macd_signal()


def main():
    """主函数"""
    TOKEN = "d0c519adcd47d266f1c96750d4e80aa6"
    STOCK_CODE = "159506"
    
    print("简化的jvQuant实时技术指标计算系统")
    print("="*60)
    print(f"Token: {TOKEN}")
    print(f"股票代码: {STOCK_CODE}")
    print("="*60)
    
    client = JVQuantSimpleClient(TOKEN, STOCK_CODE)
    
    try:
        print("正在连接...")
        client.connect()
    except KeyboardInterrupt:
        print("\n用户中断，正在断开连接...")
        client.disconnect()
        print("已退出")


if __name__ == "__main__":
    main() 