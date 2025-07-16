#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
jvQuant集成交易系统
结合实时数据、技术指标计算和自动交易功能
"""

import requests
import websocket
import zlib
import time
import threading
import json
from datetime import datetime
from collections import deque
from typing import Dict, List, Optional
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import tkinter as tk
from tkinter import ttk, messagebox
import asyncio
import pandas as pd
import numpy as np
import logging
import websockets

# 导入自定义模块
from trading_system import JVQuantTradingClient, TradingStrategy
try:
    from nautilus_trader.indicators.stochastics import Stochastics as KDJIndicator
    NAUTILUS_KDJ = True
except ImportError:
    from simple_kdj_indicator import SimpleKDJIndicator as KDJIndicator
    NAUTILUS_KDJ = False

# 尝试导入Nautilus Trader指标
try:
    from nautilus_trader.indicators.average.ema import ExponentialMovingAverage
    from nautilus_trader.indicators.momentum.rsi import RelativeStrengthIndex
    from nautilus_trader.indicators.trend.macd import MovingAverageConvergenceDivergence
    NAUTILUS_AVAILABLE = True
except ImportError:
    print("警告: Nautilus Trader指标不可用，使用简化版本")
    NAUTILUS_AVAILABLE = False

# 导入自定义数据源
try:
    from custom_data_source import CustomDataSource, DataProcessor
    CUSTOM_DATA_SOURCE_AVAILABLE = True
except ImportError:
    CUSTOM_DATA_SOURCE_AVAILABLE = False
    print("自定义数据源不可用")

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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


class IntegratedDataProcessor:
    """集成数据处理器"""
    
    def __init__(self, stock_code: str):
        self.stock_code = stock_code
        
        # 技术指标 - 使用Nautilus Trader的指标
        self.macd = MovingAverageConvergenceDivergence(12, 26, 9)
        self.rsi = RelativeStrengthIndex(14)
        
        # KDJ优先用nautilus实现
        self.kdj = KDJIndicator(9, 3, 3)
        self.volume_ratio = VolumeRatio(5)
        
        # 数据存储
        self.price_history = deque(maxlen=1000)
        self.volume_history = deque(maxlen=1000)
        self.time_history = deque(maxlen=1000)
        
        # 最新数据
        self.latest_data = {
            'price': 0.0,
            'volume': 0.0,
            'time': None,
            'high': 0.0,
            'low': 0.0,
            'open': 0.0,
            'close': 0.0
        }
        
        # 数据锁
        self.data_lock = threading.Lock()
        
        # 回调函数
        self.on_data_update = None
        self.on_signal_generated = None
    
    def process_level1_data(self, data: str):
        """处理Level1数据"""
        try:
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
                    self.time_history.append(push_time)
                    
                    # 更新技术指标
                    self.update_indicators(latest_price, volume)
                    
                    # 生成交易信号
                    signal = self.generate_trading_signal()
                    
                    # 调用回调函数
                    if self.on_data_update:
                        self.on_data_update(self.get_current_data())
                    
                    if signal and self.on_signal_generated:
                        self.on_signal_generated(signal)
                    
                    print(f"数据更新: 价格={latest_price:.2f}, 成交量={volume:.0f}, 信号={signal}")
                    
        except Exception as e:
            print(f"处理Level1数据失败: {e}")
    
    def update_indicators(self, price: float, volume: float):
        """更新技术指标"""
        # 更新MACD - 使用Nautilus Trader的MACD
        self.macd.update_raw(price)
        
        # 更新RSI - 使用Nautilus Trader的RSI
        self.rsi.update_raw(price)
        
        # 更新KDJ (使用当前价格作为high/low/close的近似)
        self.kdj.update(price, price, price)
        
        # 更新量比
        self.volume_ratio.update(volume)
    
    def generate_trading_signal(self) -> Optional[str]:
        """生成交易信号"""
        if len(self.price_history) < 30:
            return None
        
        # MACD信号 - 使用Nautilus Trader的MACD
        macd_signal = "HOLD"
        if self.macd.initialized:
            if self.macd.value > self.macd.signal and self.macd.histogram > 0:
                macd_signal = "BUY"
            elif self.macd.value < self.macd.signal and self.macd.histogram < 0:
                macd_signal = "SELL"
        
        # RSI信号 - 使用Nautilus Trader的RSI
        rsi_signal = "HOLD"
        if self.rsi.initialized:
            if self.rsi.value < 30:
                rsi_signal = "BUY"
            elif self.rsi.value > 70:
                rsi_signal = "SELL"
        
        # KDJ信号
        kdj_signal = "HOLD"
        if self.kdj.initialized:
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
    
    def get_current_data(self) -> Dict:
        """获取当前数据"""
        with self.data_lock:
            return {
                'price': self.latest_data['price'],
                'volume': self.latest_data['volume'],
                'time': self.latest_data['time'],
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
                'price_history': list(self.price_history),
                'volume_history': list(self.volume_history),
                'time_history': list(self.time_history)
            }


class IntegratedTradingStrategy(TradingStrategy):
    """集成交易策略"""
    
    def __init__(self, trading_client: JVQuantTradingClient, data_processor: IntegratedDataProcessor, 
                 stock_code: str, stock_name: str, auto_trade: bool = False):
        super().__init__(trading_client)
        self.data_processor = data_processor
        self.stock_code = stock_code
        self.stock_name = stock_name
        self.auto_trade = auto_trade
        self.last_signal = None
        self.position = 0  # 当前持仓
        
        # 设置数据处理器回调
        self.data_processor.on_signal_generated = self.on_signal_received
    
    def on_signal_received(self, signal: str):
        """接收到交易信号"""
        if not self.auto_trade:
            return
        
        if signal != self.last_signal:
            print(f"收到交易信号: {signal}")
            
            if signal == "BUY" and self.position <= 0:
                # 买入信号
                self.execute_buy()
            elif signal == "SELL" and self.position >= 0:
                # 卖出信号
                self.execute_sell()
            
            self.last_signal = signal
    
    def execute_buy(self):
        """执行买入"""
        try:
            # 获取当前价格
            current_data = self.data_processor.get_current_data()
            price = current_data['price']
            
            if price <= 0:
                print("价格无效，无法执行买入")
                return
            
            # 计算买入数量（这里简化处理，实际应该根据资金计算）
            volume = 100
            
            order_id = self.trading_client.buy_stock(
                self.stock_code,
                self.stock_name,
                price,
                volume
            )
            
            if order_id:
                self.position += volume
                print(f"买入执行成功: {volume}股")
            
        except Exception as e:
            print(f"买入执行失败: {e}")
    
    def execute_sell(self):
        """执行卖出"""
        try:
            # 获取当前价格
            current_data = self.data_processor.get_current_data()
            price = current_data['price']
            
            if price <= 0:
                print("价格无效，无法执行卖出")
                return
            
            # 计算卖出数量
            volume = min(100, self.position)  # 最多卖出100股或全部持仓
            
            if volume <= 0:
                print("没有持仓可卖出")
                return
            
            order_id = self.trading_client.sell_stock(
                self.stock_code,
                self.stock_name,
                price,
                volume
            )
            
            if order_id:
                self.position -= volume
                print(f"卖出执行成功: {volume}股")
            
        except Exception as e:
            print(f"卖出执行失败: {e}")
    
    def _run_strategy(self):
        """策略运行逻辑"""
        while self.is_running:
            try:
                # 策略逻辑主要在数据处理器中实现
                # 这里主要负责监控和状态更新
                time.sleep(5)
                
            except Exception as e:
                print(f"策略运行异常: {e}")
                time.sleep(5)


class TradingGUI:
    """交易图形界面"""
    
    def __init__(self, token: str, stock_code: str, stock_name: str):
        self.token = token
        self.stock_code = stock_code
        self.stock_name = stock_name
        
        # 创建组件
        self.trading_client = JVQuantTradingClient(token)
        self.data_processor = IntegratedDataProcessor(stock_code)
        self.strategy = None
        
        # 创建GUI
        self.root = tk.Tk()
        self.root.title(f"jvQuant集成交易系统 - {stock_code}")
        self.root.geometry("1200x800")
        
        self.setup_gui()
        self.setup_charts()
        
        # 数据更新回调
        self.data_processor.on_data_update = self.update_display
    
    def setup_gui(self):
        """设置GUI界面"""
        # 主框架
        main_frame = ttk.Frame(self.root)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # 左侧控制面板
        control_frame = ttk.LabelFrame(main_frame, text="控制面板")
        control_frame.pack(side=tk.LEFT, fill=tk.Y, padx=(0, 10))
        
        # 连接状态
        status_frame = ttk.Frame(control_frame)
        status_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(status_frame, text="连接状态:").pack(anchor=tk.W)
        self.status_label = ttk.Label(status_frame, text="未连接", foreground="red")
        self.status_label.pack(anchor=tk.W)
        
        # 登录框架
        login_frame = ttk.LabelFrame(control_frame, text="交易登录")
        login_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(login_frame, text="资金账号:").pack(anchor=tk.W)
        self.account_entry = ttk.Entry(login_frame)
        self.account_entry.pack(fill=tk.X, pady=2)
        
        ttk.Label(login_frame, text="资金密码:").pack(anchor=tk.W)
        self.password_entry = ttk.Entry(login_frame, show="*")
        self.password_entry.pack(fill=tk.X, pady=2)
        
        ttk.Button(login_frame, text="登录", command=self.login).pack(fill=tk.X, pady=5)
        
        # 交易控制
        trade_frame = ttk.LabelFrame(control_frame, text="交易控制")
        trade_frame.pack(fill=tk.X, pady=5)
        
        ttk.Button(trade_frame, text="查询持仓", command=self.query_positions).pack(fill=tk.X, pady=2)
        ttk.Button(trade_frame, text="查询交易记录", command=self.query_orders).pack(fill=tk.X, pady=2)
        
        # 自动交易控制
        auto_frame = ttk.LabelFrame(control_frame, text="自动交易")
        auto_frame.pack(fill=tk.X, pady=5)
        
        self.auto_trade_var = tk.BooleanVar()
        ttk.Checkbutton(auto_frame, text="启用自动交易", variable=self.auto_trade_var).pack(anchor=tk.W)
        
        ttk.Button(auto_frame, text="启动策略", command=self.start_strategy).pack(fill=tk.X, pady=2)
        ttk.Button(auto_frame, text="停止策略", command=self.stop_strategy).pack(fill=tk.X, pady=2)
        
        # 右侧数据显示
        data_frame = ttk.LabelFrame(main_frame, text="实时数据")
        data_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)
        
        # 数据标签
        self.price_label = ttk.Label(data_frame, text="价格: --", font=("Arial", 12))
        self.price_label.pack(anchor=tk.W, pady=2)
        
        self.volume_label = ttk.Label(data_frame, text="成交量: --", font=("Arial", 12))
        self.volume_label.pack(anchor=tk.W, pady=2)
        
        self.macd_label = ttk.Label(data_frame, text="MACD: --", font=("Arial", 12))
        self.macd_label.pack(anchor=tk.W, pady=2)
        
        self.rsi_label = ttk.Label(data_frame, text="RSI: --", font=("Arial", 12))
        self.rsi_label.pack(anchor=tk.W, pady=2)
        
        self.kdj_label = ttk.Label(data_frame, text="KDJ: --", font=("Arial", 12))
        self.kdj_label.pack(anchor=tk.W, pady=2)
        
        self.signal_label = ttk.Label(data_frame, text="信号: --", font=("Arial", 12, "bold"))
        self.signal_label.pack(anchor=tk.W, pady=2)
    
    def setup_charts(self):
        """设置图表"""
        # 创建图表框架
        chart_frame = ttk.LabelFrame(self.root, text="技术指标图表")
        chart_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))
        
        # 创建图表
        self.fig, self.axes = plt.subplots(3, 1, figsize=(12, 8))
        self.canvas = FigureCanvasTkAgg(self.fig, chart_frame)
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        
        # 设置图表标题
        self.axes[0].set_title("价格走势")
        self.axes[1].set_title("MACD")
        self.axes[2].set_title("RSI & KDJ")
    
    def login(self):
        """登录交易系统"""
        account = self.account_entry.get().strip()
        password = self.password_entry.get().strip()
        
        if not account or not password:
            messagebox.showerror("错误", "请输入账户信息")
            return
        
        if self.trading_client.login(account, password):
            self.status_label.config(text="已登录", foreground="green")
            messagebox.showinfo("成功", "登录成功")
        else:
            messagebox.showerror("错误", "登录失败")
    
    def query_positions(self):
        """查询持仓"""
        positions = self.trading_client.query_positions()
        if positions:
            self.trading_client.print_positions(positions)
        else:
            messagebox.showinfo("提示", "暂无持仓")
    
    def query_orders(self):
        """查询交易记录"""
        orders = self.trading_client.query_orders()
        if orders:
            self.trading_client.print_orders(orders)
        else:
            messagebox.showinfo("提示", "暂无交易记录")
    
    def start_strategy(self):
        """启动策略"""
        if not self.trading_client.is_logged_in:
            messagebox.showerror("错误", "请先登录交易系统")
            return
        
        if self.strategy and self.strategy.is_running:
            messagebox.showinfo("提示", "策略已在运行中")
            return
        
        auto_trade = self.auto_trade_var.get()
        self.strategy = IntegratedTradingStrategy(
            self.trading_client,
            self.data_processor,
            self.stock_code,
            self.stock_name,
            auto_trade
        )
        
        self.strategy.start()
        messagebox.showinfo("成功", "策略已启动")
    
    def stop_strategy(self):
        """停止策略"""
        if self.strategy:
            self.strategy.stop()
            self.strategy = None
            messagebox.showinfo("成功", "策略已停止")
        else:
            messagebox.showinfo("提示", "没有运行中的策略")
    
    def update_display(self, data: Dict):
        """更新显示"""
        # 更新标签
        self.price_label.config(text=f"价格: {data['price']:.2f}")
        self.volume_label.config(text=f"成交量: {data['volume']:.0f}")
        
        # MACD
        macd_data = data['macd']
        self.macd_label.config(text=f"MACD: {macd_data['macd']:.4f}, Signal: {macd_data['signal']:.4f}")
        
        # RSI
        self.rsi_label.config(text=f"RSI: {data['rsi']:.2f}")
        
        # KDJ
        kdj_data = data['kdj']
        self.kdj_label.config(text=f"KDJ: K={kdj_data['k']:.2f}, D={kdj_data['d']:.2f}, J={kdj_data['j']:.2f}")
        
        # 信号
        signal = self.data_processor.generate_trading_signal()
        if signal == "BUY":
            self.signal_label.config(text="信号: 买入", foreground="red")
        elif signal == "SELL":
            self.signal_label.config(text="信号: 卖出", foreground="green")
        else:
            self.signal_label.config(text="信号: 持有", foreground="black")
        
        # 更新图表
        self.update_charts(data)
    
    def update_charts(self, data: Dict):
        """更新图表"""
        try:
            # 清除旧图表
            for ax in self.axes:
                ax.clear()
            
            if len(data['price_history']) > 0:
                # 价格走势
                self.axes[0].plot(data['price_history'], label='价格')
                self.axes[0].set_title("价格走势")
                self.axes[0].legend()
                self.axes[0].grid(True)
                
                # MACD
                if len(data['price_history']) >= 26:
                    macd_data = data['macd']
                    self.axes[1].plot([macd_data['macd']] * len(data['price_history']), label='MACD', color='blue')
                    self.axes[1].plot([macd_data['signal']] * len(data['price_history']), label='Signal', color='red')
                    self.axes[1].set_title("MACD")
                    self.axes[1].legend()
                    self.axes[1].grid(True)
                
                # RSI & KDJ
                if len(data['price_history']) >= 14:
                    rsi_value = data['rsi']
                    kdj_data = data['kdj']
                    
                    self.axes[2].plot([rsi_value] * len(data['price_history']), label='RSI', color='purple')
                    self.axes[2].plot([kdj_data['k']] * len(data['price_history']), label='K', color='blue')
                    self.axes[2].plot([kdj_data['d']] * len(data['price_history']), label='D', color='red')
                    self.axes[2].plot([kdj_data['j']] * len(data['price_history']), label='J', color='green')
                    
                    # 添加超买超卖线
                    self.axes[2].axhline(y=70, color='r', linestyle='--', alpha=0.5)
                    self.axes[2].axhline(y=30, color='g', linestyle='--', alpha=0.5)
                    
                    self.axes[2].set_title("RSI & KDJ")
                    self.axes[2].legend()
                    self.axes[2].grid(True)
            
            # 刷新画布
            self.canvas.draw()
            
        except Exception as e:
            print(f"更新图表失败: {e}")
    
    def run(self):
        """运行GUI"""
        self.root.mainloop()


def main():
    """主函数"""
    TOKEN = "d0c519adcd47d266f1c96750d4e80aa6"
    STOCK_CODE = "159506"
    STOCK_NAME = "ETF基金"
    
    print("jvQuant集成交易系统")
    print("="*60)
    print(f"Token: {TOKEN}")
    print(f"股票代码: {STOCK_CODE}")
    print(f"股票名称: {STOCK_NAME}")
    print("="*60)
    
    # 创建并运行GUI
    gui = TradingGUI(TOKEN, STOCK_CODE, STOCK_NAME)
    gui.run()


if __name__ == "__main__":
    main() 