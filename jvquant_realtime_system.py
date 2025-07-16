#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
jvQuant实时数据接入和技术指标计算系统
支持Level1、Level2、Level10数据，实时计算MACD、RSI、KDJ、量比等技术指标
"""

import json
import time
import threading
import requests
import websocket
import zlib
import pandas as pd
import numpy as np
import mplfinance as mpf
from datetime import datetime, timedelta
from collections import deque
import os
import pickle

from nautilus_trader.indicators.average.ema import ExponentialMovingAverage
from nautilus_trader.indicators.momentum.rsi import RelativeStrengthIndex
from nautilus_trader.indicators.trend.macd import MovingAverageConvergenceDivergence
from kdj_indicator import KDJIndicator


class JVQuantServerManager:
    """jvQuant服务器管理器"""
    
    def __init__(self, token: str):
        self.token = token
        self.base_url = "http://jvQuant.com/query/server"
    
    def get_server(self, market: str, type: str) -> str:
        """
        获取分配的服务器地址
        
        Parameters
        ----------
        market : str
            市场标识 (ab: 沪深, hk: 港股, us: 美股)
        type : str
            业务类型 (websocket: 实时行情, sql: 数据库服务, trade: 委托交易)
        """
        url = f"{self.base_url}?market={market}&type={type}&token={self.token}"
        
        try:
            response = requests.get(url, timeout=10)
            data = response.json()
            
            if data.get("code") == "0":
                return data.get("server")
            else:
                raise Exception(f"获取服务器失败: {data}")
        except Exception as e:
            print(f"获取服务器地址失败: {e}")
            return None


class VolumeRatioCalculator:
    """量比计算器"""
    
    def __init__(self, period: int = 5):
        self.period = period
        self.volumes = deque(maxlen=period)
        self.avg_volume = 0.0
        self.volume_ratio = 1.0
    
    def update(self, volume: float):
        """更新量比"""
        self.volumes.append(volume)
        
        if len(self.volumes) == self.period:
            self.avg_volume = sum(self.volumes) / len(self.volumes)
            if self.avg_volume > 0:
                self.volume_ratio = volume / self.avg_volume
            else:
                self.volume_ratio = 1.0
    
    def reset(self):
        """重置"""
        self.volumes.clear()
        self.avg_volume = 0.0
        self.volume_ratio = 1.0


class RealTimeDataProcessor:
    """实时数据处理器"""
    
    def __init__(self, stock_code: str):
        self.stock_code = stock_code
        
        # 技术指标
        self.macd = MovingAverageConvergenceDivergence(12, 26, 9)
        self.rsi = RelativeStrengthIndex(14)
        self.kdj = KDJIndicator(9, 3, 3)
        self.volume_ratio = VolumeRatioCalculator(5)
        
        # 数据存储
        self.ohlc_data = []
        self.tick_data = []
        self.level10_data = []
        
        # 最新数据
        self.latest_price = 0.0
        self.latest_volume = 0.0
        self.latest_time = None
        
        # 数据锁
        self.data_lock = threading.Lock()
    
    def process_level1_data(self, data: str):
        """处理Level1基础行情数据"""
        try:
            # 解析Level1数据: lv1_证券代码=推送时间,证券名称,最新价格,涨幅,成交额,成交量,买五档,卖五档
            parts = data.split('=')
            if len(parts) != 2:
                return
            
            code_part = parts[0]
            data_part = parts[1]
            
            if not code_part.startswith('lv1_'):
                return
            
            stock_code = code_part[4:]  # 去掉'lv1_'前缀
            if stock_code != self.stock_code:
                return
            
            # 解析数据字段
            fields = data_part.split(',')
            if len(fields) < 6:
                return
            
            push_time = fields[0]
            stock_name = fields[1]
            latest_price = float(fields[2])
            change_percent = float(fields[3])
            turnover = float(fields[4])
            volume = float(fields[5])
            
            with self.data_lock:
                self.latest_price = latest_price
                self.latest_volume = volume
                self.latest_time = push_time
                
                # 更新量比
                self.volume_ratio.update(volume)
                
                print(f"Level1 - {stock_code}: 价格={latest_price}, 成交量={volume}, 量比={self.volume_ratio.volume_ratio:.2f}")
                
        except Exception as e:
            print(f"处理Level1数据失败: {e}")
    
    def process_level2_data(self, data: str):
        """处理Level2逐笔成交数据"""
        try:
            # 解析Level2数据: lv2_证券代码=成交时间1,成交编号1,成交价格1,成交数量1|成交时间2,成交编号2,成交价格2,成交数量2...
            parts = data.split('=')
            if len(parts) != 2:
                return
            
            code_part = parts[0]
            data_part = parts[1]
            
            if not code_part.startswith('lv2_'):
                return
            
            stock_code = code_part[4:]  # 去掉'lv2_'前缀
            if stock_code != self.stock_code:
                return
            
            # 解析逐笔成交数据
            trades = data_part.split('|')
            for trade in trades:
                if not trade.strip():
                    continue
                
                fields = trade.split(',')
                if len(fields) >= 4:
                    trade_time = fields[0]
                    trade_id = fields[1]
                    trade_price = float(fields[2])
                    trade_volume = float(fields[3])
                    
                    # 存储逐笔数据
                    with self.data_lock:
                        self.tick_data.append({
                            'time': trade_time,
                            'price': trade_price,
                            'volume': trade_volume
                        })
                        
                        # 保持最近1000条记录
                        if len(self.tick_data) > 1000:
                            self.tick_data.pop(0)
                    
                    print(f"Level2 - {stock_code}: 成交价={trade_price}, 成交量={trade_volume}")
                    
        except Exception as e:
            print(f"处理Level2数据失败: {e}")
    
    def process_level10_data(self, data: str):
        """处理Level10十档盘口数据"""
        try:
            # 解析Level10数据: lv10_证券代码=推送时间,证券名称,最新价格,昨收,成交额,成交量,买十档,卖十档
            parts = data.split('=')
            if len(parts) != 2:
                return
            
            code_part = parts[0]
            data_part = parts[1]
            
            if not code_part.startswith('lv10_'):
                return
            
            stock_code = code_part[5:]  # 去掉'lv10_'前缀
            if stock_code != self.stock_code:
                return
            
            # 解析十档盘口数据
            fields = data_part.split(',')
            if len(fields) >= 6:
                push_time = fields[0]
                stock_name = fields[1]
                latest_price = float(fields[2])
                prev_close = float(fields[3])
                turnover = float(fields[4])
                volume = float(fields[5])
                
                with self.data_lock:
                    self.latest_price = latest_price
                    self.latest_volume = volume
                    self.latest_time = push_time
                    
                    # 存储十档盘口数据
                    self.level10_data.append({
                        'time': push_time,
                        'price': latest_price,
                        'volume': volume,
                        'turnover': turnover
                    })
                    
                    # 保持最近100条记录
                    if len(self.level10_data) > 100:
                        self.level10_data.pop(0)
                
                print(f"Level10 - {stock_code}: 价格={latest_price}, 成交量={volume}")
                
        except Exception as e:
            print(f"处理Level10数据失败: {e}")
    
    def update_indicators(self, bar_data: dict):
        """更新技术指标"""
        try:
            # 更新MACD
            self.macd.update_raw(bar_data['close'])
            
            # 更新RSI
            self.rsi.update_raw(bar_data['close'])
            
            # 更新KDJ
            self.kdj.update_raw(bar_data['high'], bar_data['low'], bar_data['close'])
            
            # 更新量比
            self.volume_ratio.update(bar_data['volume'])
            
        except Exception as e:
            print(f"更新技术指标失败: {e}")
    
    def get_indicators(self) -> dict:
        """获取当前技术指标值"""
        with self.data_lock:
            return {
                'macd': {
                    'macd': self.macd.value if self.macd.initialized else 0,
                    'signal': self.macd.signal if self.macd.initialized else 0,
                    'histogram': self.macd.histogram if self.macd.initialized else 0
                },
                'rsi': self.rsi.value if self.rsi.initialized else 50,
                'kdj': {
                    'k': self.kdj.value_k if self.kdj.initialized else 50,
                    'd': self.kdj.value_d if self.kdj.initialized else 50,
                    'j': self.kdj.value_j if self.kdj.initialized else 50
                },
                'volume_ratio': self.volume_ratio.volume_ratio,
                'latest_price': self.latest_price,
                'latest_volume': self.latest_volume,
                'latest_time': self.latest_time
            }
    
    def save_data(self, filename: str):
        """保存数据到文件"""
        with self.data_lock:
            data = {
                'ohlc_data': self.ohlc_data,
                'tick_data': self.tick_data,
                'level10_data': self.level10_data,
                'indicators': {
                    'macd': {
                        'macd': self.macd.value if self.macd.initialized else 0,
                        'signal': self.macd.signal if self.macd.initialized else 0,
                        'histogram': self.macd.histogram if self.macd.initialized else 0
                    },
                    'rsi': self.rsi.value if self.rsi.initialized else 50,
                    'kdj': {
                        'k': self.kdj.value_k if self.kdj.initialized else 50,
                        'd': self.kdj.value_d if self.kdj.initialized else 50,
                        'j': self.kdj.value_j if self.kdj.initialized else 50
                    }
                }
            }
            
            with open(filename, 'wb') as f:
                pickle.dump(data, f)
            
            print(f"数据已保存到: {filename}")


class JVQuantWebSocketClient:
    """jvQuant WebSocket客户端"""
    
    def __init__(self, token: str, stock_code: str):
        self.token = token
        self.stock_code = stock_code
        self.server_manager = JVQuantServerManager(token)
        self.data_processor = RealTimeDataProcessor(stock_code)
        self.ws = None
        self.is_connected = False
        
        # 图表更新线程
        self.chart_thread = None
        self.stop_chart = False
    
    def connect(self):
        """连接WebSocket服务器"""
        # 获取服务器地址
        server = self.server_manager.get_server("ab", "websocket")
        if not server:
            print("无法获取服务器地址")
            return False
        
        ws_url = f"ws://{server}/?token={self.token}"
        print(f"连接到WebSocket服务器: {ws_url}")
        
        self.ws = websocket.WebSocketApp(
            ws_url,
            on_open=self.on_open,
            on_data=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        
        # 启动WebSocket连接
        self.ws.run_forever()
        return True
    
    def on_open(self, ws):
        """连接打开回调"""
        print("WebSocket连接已建立")
        self.is_connected = True
        
        # 订阅Level1、Level2、Level10数据
        subscription = f"all=lv1_{self.stock_code},lv2_{self.stock_code},lv10_{self.stock_code}"
        ws.send(subscription)
        print(f"已订阅: {subscription}")
        
        # 启动图表更新线程
        self.start_chart_update()
    
    def on_message(self, ws, message, type, flag):
        """接收消息回调"""
        try:
            # 命令返回文本消息
            if type == websocket.ABNF.OPCODE_TEXT:
                print(f"Text响应: {message}")
            
            # 行情推送压缩二进制消息
            elif type == websocket.ABNF.OPCODE_BINARY:
                # 解压缩数据
                decompressed = zlib.decompress(message, -zlib.MAX_WBITS)
                data_str = decompressed.decode("utf-8")
                
                # 处理多行数据
                lines = data_str.strip().split('\n')
                for line in lines:
                    if line.strip():
                        self.process_market_data(line)
                        
        except Exception as e:
            print(f"处理消息失败: {e}")
    
    def process_market_data(self, data: str):
        """处理市场数据"""
        if data.startswith('lv1_'):
            self.data_processor.process_level1_data(data)
        elif data.startswith('lv2_'):
            self.data_processor.process_level2_data(data)
        elif data.startswith('lv10_'):
            self.data_processor.process_level10_data(data)
    
    def on_error(self, ws, error):
        """错误回调"""
        print(f"WebSocket错误: {error}")
        self.is_connected = False
    
    def on_close(self, ws, code, msg):
        """连接关闭回调"""
        print(f"WebSocket连接已关闭: {code} - {msg}")
        self.is_connected = False
        self.stop_chart = True
    
    def start_chart_update(self):
        """启动图表更新线程"""
        self.chart_thread = threading.Thread(target=self.update_charts)
        self.chart_thread.daemon = True
        self.chart_thread.start()
    
    def update_charts(self):
        """更新图表"""
        while not self.stop_chart:
            try:
                # 获取当前指标数据
                indicators = self.data_processor.get_indicators()
                
                # 生成图表
                self.generate_charts(indicators)
                
                # 每5秒更新一次图表
                time.sleep(5)
                
            except Exception as e:
                print(f"更新图表失败: {e}")
                time.sleep(5)
    
    def generate_charts(self, indicators: dict):
        """生成技术指标图表"""
        try:
            # 创建图表数据
            fig, axes = plt.subplots(4, 1, figsize=(15, 12))
            fig.suptitle(f'{self.stock_code} 实时技术指标', fontsize=16)
            
            # 获取历史数据用于绘图
            with self.data_processor.data_lock:
                if len(self.data_processor.ohlc_data) > 0:
                    df = pd.DataFrame(self.data_processor.ohlc_data)
                    df['datetime'] = pd.to_datetime(df['time'])
                    df.set_index('datetime', inplace=True)
                    
                    # 1. K线图
                    ax1 = axes[0]
                    mpf.plot(df, type='candle', style='charles', ax=ax1, volume=True, 
                            title=f'{self.stock_code} K线图')
                    
                    # 2. MACD
                    ax2 = axes[1]
                    if indicators['macd']['macd'] != 0:
                        ax2.plot(df.index, [indicators['macd']['macd']] * len(df), label='MACD', color='blue')
                        ax2.plot(df.index, [indicators['macd']['signal']] * len(df), label='Signal', color='red')
                        ax2.bar(df.index, [indicators['macd']['histogram']] * len(df), label='Histogram', alpha=0.3)
                    ax2.set_title('MACD')
                    ax2.legend()
                    
                    # 3. RSI
                    ax3 = axes[2]
                    ax3.plot(df.index, [indicators['rsi']] * len(df), label='RSI', color='purple')
                    ax3.axhline(y=70, color='r', linestyle='--', alpha=0.5)
                    ax3.axhline(y=30, color='g', linestyle='--', alpha=0.5)
                    ax3.set_title('RSI')
                    ax3.legend()
                    
                    # 4. KDJ
                    ax4 = axes[3]
                    ax4.plot(df.index, [indicators['kdj']['k']] * len(df), label='K', color='blue')
                    ax4.plot(df.index, [indicators['kdj']['d']] * len(df), label='D', color='red')
                    ax4.plot(df.index, [indicators['kdj']['j']] * len(df), label='J', color='green')
                    ax4.axhline(y=80, color='r', linestyle='--', alpha=0.5)
                    ax4.axhline(y=20, color='g', linestyle='--', alpha=0.5)
                    ax4.set_title('KDJ')
                    ax4.legend()
            
            # 显示当前指标值
            info_text = f"""
            最新价格: {indicators['latest_price']:.2f}
            成交量: {indicators['latest_volume']:.0f}
            量比: {indicators['volume_ratio']:.2f}
            RSI: {indicators['rsi']:.2f}
            K: {indicators['kdj']['k']:.2f} D: {indicators['kdj']['d']:.2f} J: {indicators['kdj']['j']:.2f}
            """
            
            plt.figtext(0.02, 0.02, info_text, fontsize=10, bbox=dict(boxstyle="round,pad=0.3", facecolor="lightgray"))
            
            plt.tight_layout()
            plt.show()
            
        except Exception as e:
            print(f"生成图表失败: {e}")
    
    def save_data(self):
        """保存数据"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"data_{self.stock_code}_{timestamp}.pkl"
        self.data_processor.save_data(filename)
    
    def disconnect(self):
        """断开连接"""
        self.stop_chart = True
        if self.ws:
            self.ws.close()
        print("已断开连接")


def main():
    """主函数"""
    # 配置参数
    TOKEN = "d0c519adcd47d266f1c96750d4e80aa6"
    STOCK_CODE = "159506"
    
    print("=" * 60)
    print("jvQuant实时技术指标计算系统")
    print("=" * 60)
    print(f"Token: {TOKEN}")
    print(f"股票代码: {STOCK_CODE}")
    print("=" * 60)
    
    # 创建WebSocket客户端
    client = JVQuantWebSocketClient(TOKEN, STOCK_CODE)
    
    try:
        # 连接服务器
        if client.connect():
            print("系统运行中... 按Ctrl+C退出")
            
            # 保持运行
            while client.is_connected:
                time.sleep(1)
                
    except KeyboardInterrupt:
        print("\n用户中断，正在保存数据...")
        client.save_data()
        client.disconnect()
        print("系统已退出")
    
    except Exception as e:
        print(f"系统错误: {e}")
        client.disconnect()


if __name__ == "__main__":
    main() 