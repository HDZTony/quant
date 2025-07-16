#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
159506 ETF集成系统
同时支持WebSocket数据永久保存和实时图表显示
"""

import json
import time
import threading
import requests
import websocket
import zlib
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from datetime import datetime, timedelta
from collections import deque
import os
from pathlib import Path
from typing import Dict, List, Optional, Any
import logging

# 导入技术指标
from nautilus_trader.indicators.average.ema import ExponentialMovingAverage
from nautilus_trader.indicators.momentum.rsi import RelativeStrengthIndex
from nautilus_trader.indicators.trend.macd import MovingAverageConvergenceDivergence

# 导入数据采集器组件
from etf_159506_data_collector import ETF159506ServerManager, ETF159506DataProcessor

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etf_159506_integrated.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class IntegratedChartManager:
    """集成图表管理器"""
    
    def __init__(self, stock_code: str = "159506"):
        self.stock_code = stock_code
        
        # 数据存储
        self.tick_data = deque(maxlen=10000)  # 最近1万条tick数据
        self.kline_data = deque(maxlen=1000)  # 最近1000根K线
        
        # 技术指标
        self.macd = MovingAverageConvergenceDivergence(12, 26, 9)
        self.rsi = RelativeStrengthIndex(14)
        self.ema_short = ExponentialMovingAverage(12)
        self.ema_long = ExponentialMovingAverage(26)
        
        # 图表相关
        self.fig = None
        self.axes = None
        self.animation = None
        
        # 数据锁
        self.data_lock = threading.Lock()
        
        # 最新数据
        self.latest_price = 0.0
        self.latest_time = None
        
        logger.info(f"初始化集成图表管理器: {stock_code}")
    
    def add_tick_data(self, timestamp: str, price: float, volume: float):
        """添加tick数据"""
        with self.data_lock:
            tick_record = {
                'timestamp': timestamp,
                'price': price,
                'volume': volume,
                'datetime': pd.to_datetime(timestamp)
            }
            
            self.tick_data.append(tick_record)
            self.latest_price = price
            self.latest_time = timestamp
            
            # 更新K线数据
            self._update_kline_data(tick_record)
    
    def _update_kline_data(self, tick_record: Dict):
        """更新K线数据"""
        current_time = tick_record['datetime']
        
        # 按分钟聚合K线
        minute_key = current_time.replace(second=0, microsecond=0)
        
        # 查找或创建当前分钟的K线
        current_kline = None
        for kline in self.kline_data:
            if kline['datetime'] == minute_key:
                current_kline = kline
                break
        
        if current_kline is None:
            # 创建新的K线
            current_kline = {
                'datetime': minute_key,
                'open': tick_record['price'],
                'high': tick_record['price'],
                'low': tick_record['price'],
                'close': tick_record['price'],
                'volume': tick_record['volume']
            }
            self.kline_data.append(current_kline)
        else:
            # 更新现有K线
            current_kline['high'] = max(current_kline['high'], tick_record['price'])
            current_kline['low'] = min(current_kline['low'], tick_record['price'])
            current_kline['close'] = tick_record['price']
            current_kline['volume'] += tick_record['volume']
    
    def update_indicators(self):
        """更新技术指标"""
        with self.data_lock:
            if len(self.kline_data) < 26:  # 需要足够的数据
                return
            
            # 获取最新的收盘价
            latest_close = self.kline_data[-1]['close']
            
            # 更新MACD
            self.macd.update_raw(latest_close)
            
            # 更新RSI
            self.rsi.update_raw(latest_close)
            
            # 更新EMA
            self.ema_short.update_raw(latest_close)
            self.ema_long.update_raw(latest_close)
    
    def get_indicators(self) -> Dict:
        """获取技术指标值"""
        with self.data_lock:
            return {
                'macd': {
                    'macd': self.macd.value if self.macd.initialized else 0,
                    'signal': self.macd.signal if self.macd.initialized else 0,
                    'histogram': self.macd.histogram if self.macd.initialized else 0
                },
                'rsi': self.rsi.value if self.rsi.initialized else 50,
                'ema_short': self.ema_short.value if self.ema_short.initialized else 0,
                'ema_long': self.ema_long.value if self.ema_long.initialized else 0,
                'latest_price': self.latest_price,
                'latest_time': self.latest_time
            }
    
    def create_charts(self):
        """创建图表"""
        # 创建图表
        self.fig, self.axes = plt.subplots(4, 1, figsize=(15, 12))
        self.fig.suptitle(f'{self.stock_code} 实时技术指标', fontsize=16)
        
        # 设置子图标题
        self.axes[0].set_title('K线图')
        self.axes[1].set_title('MACD')
        self.axes[2].set_title('RSI')
        self.axes[3].set_title('成交量')
        
        plt.tight_layout()
        plt.show(block=False)
    
    def update_charts(self, frame):
        """更新图表（动画回调）"""
        try:
            with self.data_lock:
                if len(self.kline_data) < 10:  # 需要足够的数据
                    return
                
                # 转换为DataFrame
                kline_list = list(self.kline_data)
                df = pd.DataFrame(kline_list)
                df.set_index('datetime', inplace=True)
                
                # 清空所有子图
                for ax in self.axes:
                    ax.clear()
                
                # 1. K线图
                ax1 = self.axes[0]
                ax1.plot(df.index, df['close'], label='收盘价', linewidth=1)
                
                # 添加EMA线
                if self.ema_short.initialized and self.ema_long.initialized:
                    ema_short_values = [self.ema_short.value] * len(df)
                    ema_long_values = [self.ema_long.value] * len(df)
                    ax1.plot(df.index, ema_short_values, label='EMA12', alpha=0.7)
                    ax1.plot(df.index, ema_long_values, label='EMA26', alpha=0.7)
                
                ax1.set_title('K线图')
                ax1.legend()
                ax1.grid(True, alpha=0.3)
                
                # 2. MACD
                ax2 = self.axes[1]
                if self.macd.initialized:
                    macd_values = [self.macd.value] * len(df)
                    signal_values = [self.macd.signal] * len(df)
                    histogram_values = [self.macd.histogram] * len(df)
                    
                    ax2.plot(df.index, macd_values, label='MACD', color='blue')
                    ax2.plot(df.index, signal_values, label='Signal', color='red')
                    ax2.bar(df.index, histogram_values, label='Histogram', alpha=0.3)
                
                ax2.set_title('MACD')
                ax2.legend()
                ax2.grid(True, alpha=0.3)
                
                # 3. RSI
                ax3 = self.axes[2]
                if self.rsi.initialized:
                    rsi_values = [self.rsi.value] * len(df)
                    ax3.plot(df.index, rsi_values, label='RSI', color='purple')
                    ax3.axhline(y=70, color='r', linestyle='--', alpha=0.5)
                    ax3.axhline(y=30, color='g', linestyle='--', alpha=0.5)
                
                ax3.set_title('RSI')
                ax3.legend()
                ax3.grid(True, alpha=0.3)
                ax3.set_ylim(0, 100)
                
                # 4. 成交量
                ax4 = self.axes[3]
                ax4.bar(df.index, df['volume'], alpha=0.6, label='成交量')
                ax4.set_title('成交量')
                ax4.legend()
                ax4.grid(True, alpha=0.3)
                
                # 显示最新指标值
                indicators = self.get_indicators()
                info_text = f"""
                最新价格: {indicators['latest_price']:.4f}
                RSI: {indicators['rsi']:.2f}
                MACD: {indicators['macd']['macd']:.4f}
                Signal: {indicators['macd']['signal']:.4f}
                """
                
                plt.figtext(0.02, 0.02, info_text, fontsize=10, 
                           bbox=dict(boxstyle="round,pad=0.3", facecolor="lightgray"))
                
        except Exception as e:
            logger.error(f"更新图表失败: {e}")
    
    def start_animation(self):
        """启动动画"""
        if self.fig is None:
            self.create_charts()
        
        # 创建动画
        self.animation = animation.FuncAnimation(
            self.fig, self.update_charts, interval=1000,  # 每秒更新一次
            blit=False, cache_frame_data=False
        )
        
        logger.info("实时图表动画已启动")
    
    def stop_animation(self):
        """停止动画"""
        if self.animation:
            self.animation.event_source.stop()
            logger.info("实时图表动画已停止")


class ETF159506IntegratedSystem:
    """159506 ETF集成系统"""
    
    def __init__(self, token: str, stock_code: str = "159506"):
        self.token = token
        self.stock_code = stock_code
        
        # 组件初始化
        self.server_manager = ETF159506ServerManager(token)
        self.data_processor = ETF159506DataProcessor(stock_code)
        self.chart_manager = IntegratedChartManager(stock_code)
        
        # WebSocket相关
        self.ws = None
        self.is_connected = False
        
        # 线程管理
        self.save_thread = None
        self.stop_save = False
        self.heartbeat_thread = None
        self.stop_heartbeat = False
        
        # 配置
        self.save_interval = 300  # 5分钟保存一次
        self.heartbeat_interval = 30  # 30秒心跳一次
        self.catalog_path = f"catalog/etf_159506"
        
        logger.info(f"初始化159506 ETF集成系统")
    
    def connect(self):
        """连接WebSocket服务器"""
        # 获取服务器地址
        server = self.server_manager.get_server("ab", "websocket")
        if not server:
            logger.error("无法获取服务器地址")
            return False
        
        # 修复URL格式
        if server.startswith('ws://'):
            ws_url = f"{server}/?token={self.token}"
        else:
            ws_url = f"ws://{server}/?token={self.token}"
        
        logger.info(f"连接到WebSocket服务器: {ws_url}")
        
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
        logger.info("WebSocket连接已建立")
        self.is_connected = True
        
        # 订阅Level1数据
        subscription = f"add=lv1_{self.stock_code}"
        ws.send(subscription)
        logger.info(f"已订阅: {subscription}")
        
        # 启动心跳
        self.start_heartbeat(ws)
        
        # 启动自动保存
        self.start_auto_save()
        
        # 启动实时图表
        self.chart_manager.start_animation()
    
    def on_message(self, ws, message, type, flag):
        """接收消息回调"""
        try:
            # 命令返回文本消息
            if type == websocket.ABNF.OPCODE_TEXT:
                logger.debug(f"Text响应: {message}")
            
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
            logger.error(f"处理消息失败: {e}")
    
    def process_market_data(self, data: str):
        """处理市场数据"""
        if data.startswith('lv1_'):
            self._process_level1_data(data)
    
    def _process_level1_data(self, data: str):
        """处理Level1数据"""
        try:
            # 解析Level1数据
            parts = data.split('=')
            if len(parts) != 2:
                return
            
            code_part = parts[0]
            data_part = parts[1]
            
            if not code_part.startswith('lv1_'):
                return
            
            stock_code = code_part[4:]
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
            
            # 处理时间戳
            if len(push_time.split(':')) == 3 and len(push_time) == 8:
                current_date = datetime.now().strftime('%Y-%m-%d')
                full_timestamp = f"{current_date} {push_time}"
            else:
                full_timestamp = push_time
            
            # 同时更新数据处理器和图表管理器
            self.data_processor.process_level1_data(data)
            self.chart_manager.add_tick_data(full_timestamp, latest_price, volume)
            
            # 更新技术指标
            self.chart_manager.update_indicators()
            
            # 实时输出（每100条输出一次）
            if self.data_processor.total_ticks % 100 == 0:
                indicators = self.chart_manager.get_indicators()
                logger.info(f"实时数据: 价格={latest_price:.4f}, RSI={indicators['rsi']:.2f}, "
                           f"MACD={indicators['macd']['macd']:.4f}, 总tick数={self.data_processor.total_ticks}")
                
        except Exception as e:
            logger.error(f"处理Level1数据失败: {e}")
    
    def start_heartbeat(self, ws):
        """启动心跳线程"""
        self.heartbeat_thread = threading.Thread(target=self.heartbeat_loop, args=(ws,))
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()
        logger.info("心跳线程已启动")
    
    def heartbeat_loop(self, ws):
        """心跳循环"""
        while not self.stop_heartbeat and self.is_connected:
            try:
                time.sleep(self.heartbeat_interval)
                
                if not self.stop_heartbeat and self.is_connected:
                    # 发送心跳包
                    heartbeat_msg = "ping"
                    ws.send(heartbeat_msg)
                    logger.debug("发送心跳包")
                    
            except Exception as e:
                logger.error(f"心跳发送失败: {e}")
                break
    
    def start_auto_save(self):
        """启动自动保存线程"""
        self.save_thread = threading.Thread(target=self.auto_save_loop)
        self.save_thread.daemon = True
        self.save_thread.start()
        logger.info("自动保存线程已启动")
    
    def auto_save_loop(self):
        """自动保存循环"""
        while not self.stop_save:
            try:
                time.sleep(self.save_interval)
                
                if not self.stop_save:
                    # 保存数据到catalog
                    saved_file = self.data_processor.save_data_to_catalog(self.catalog_path)
                    if saved_file:
                        logger.info(f"自动保存完成: {saved_file}")
                    
                    # 清理旧数据（保留24小时）
                    self.data_processor.clear_old_data(24)
                    
            except Exception as e:
                logger.error(f"自动保存失败: {e}")
    
    def on_error(self, ws, error):
        """错误回调"""
        logger.error(f"WebSocket错误: {error}")
        self.is_connected = False
        self.stop_heartbeat = True
        
        # 尝试重连
        if "Connection to remote host was lost" in str(error):
            logger.info("连接丢失，尝试重连...")
            time.sleep(5)  # 等待5秒后重连
            self.reconnect()
    
    def on_close(self, ws, code, msg):
        """连接关闭回调"""
        logger.info(f"WebSocket连接已关闭: {code} - {msg}")
        self.is_connected = False
        self.stop_save = True
        self.stop_heartbeat = True
        self.chart_manager.stop_animation()
    
    def reconnect(self):
        """重连方法"""
        try:
            logger.info("开始重连...")
            self.stop_heartbeat = True
            self.stop_save = True
            
            # 等待线程结束
            if self.heartbeat_thread:
                self.heartbeat_thread.join(timeout=2)
            if self.save_thread:
                self.save_thread.join(timeout=2)
            
            # 重新连接
            self.connect()
            
        except Exception as e:
            logger.error(f"重连失败: {e}")
    
    def get_status(self) -> Dict:
        """获取状态信息"""
        latest_data = self.data_processor.get_latest_data()
        indicators = self.chart_manager.get_indicators()
        
        return {
            'connected': self.is_connected,
            'stock_code': self.stock_code,
            'total_ticks': latest_data['total_ticks'],
            'latest_price': latest_data['price'],
            'latest_time': latest_data['time'],
            'rsi': indicators['rsi'],
            'macd': indicators['macd']['macd'],
            'catalog_path': self.catalog_path
        }
    
    def disconnect(self):
        """断开连接"""
        logger.info("正在断开连接...")
        self.stop_save = True
        self.stop_heartbeat = True
        self.chart_manager.stop_animation()
        
        if self.ws:
            self.ws.close()
        
        # 保存最终数据
        self.data_processor.save_data_to_catalog(self.catalog_path)
        logger.info("连接已断开")


def main():
    """主函数"""
    # 配置参数
    TOKEN = "d0c519adcd47d266f1c96750d4e80aa6"
    STOCK_CODE = "159506"
    
    print("=" * 60)
    print("159506 ETF集成系统")
    print("=" * 60)
    print(f"Token: {TOKEN}")
    print(f"股票代码: {STOCK_CODE}")
    print("功能:")
    print("  ✅ WebSocket数据永久保存到catalog")
    print("  ✅ 实时绘制K线图、MACD、RSI等技术指标")
    print("  ✅ 自动数据清理和重连机制")
    print("=" * 60)
    
    # 创建集成系统
    integrated_system = ETF159506IntegratedSystem(TOKEN, STOCK_CODE)
    
    try:
        # 连接服务器
        if integrated_system.connect():
            print("集成系统运行中... 按Ctrl+C退出")
            print("数据将自动保存到catalog/etf_159506目录")
            print("实时图表窗口已打开")
            
            # 保持运行
            while integrated_system.is_connected:
                time.sleep(1)
                
                # 每60秒显示一次状态
                if int(time.time()) % 60 == 0:
                    status = integrated_system.get_status()
                    print(f"状态: 连接={status['connected']}, "
                          f"总tick数={status['total_ticks']}, "
                          f"最新价格={status['latest_price']:.4f}, "
                          f"RSI={status['rsi']:.2f}, "
                          f"MACD={status['macd']:.4f}")
                
    except KeyboardInterrupt:
        print("\n用户中断，正在保存数据...")
        integrated_system.disconnect()
        print("系统已退出")
    
    except Exception as e:
        logger.error(f"系统错误: {e}")
        integrated_system.disconnect()


if __name__ == "__main__":
    main() 