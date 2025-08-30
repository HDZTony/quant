#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
测试新的极值点检测算法
基于时间窗口和一阶导数的检测方法，包含相对极值检测
支持实时每分钟分析当前极值
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import time
from collections import deque

def calculate_first_derivative(data, index):
    """计算数据序列中指定索引点的一阶导数"""
    if index < 1 or index >= len(data) - 1:
        return 0.0
    
    # 使用中心差分计算一阶导数
    # 一阶导数 = (f(x+h) - f(x-h)) / (2h)
    h = 1  # 步长
    f_plus_h = data[index + h]
    f_minus_h = data[index - h]
    
    derivative = (f_plus_h - f_minus_h) / (2 * h)
    return derivative

def calculate_second_derivative(data, index):
    """计算数据序列中指定索引点的二阶导数（用于确认极值类型）"""
    if index < 2 or index >= len(data) - 2:
        return 0.0
    
    # 使用中心差分计算二阶导数
    # 二阶导数 = (f(x+h) - 2f(x) + f(x-h)) / h^2
    h = 1  # 步长
    f_plus_h = data[index + h]
    f_minus_h = data[index - h]
    f_x = data[index]
    
    second_derivative = (f_plus_h - 2 * f_x + f_minus_h) / (h * h)
    return second_derivative

def is_extreme_by_derivative(data, index, derivative_threshold=0.001):
    """通过一阶导数检测是否为极值点"""
    if index < 2 or index >= len(data) - 2:
        return False, None
    
    # 计算当前点的一阶导数
    current_derivative = calculate_first_derivative(data, index)
    
    # 计算前后点的一阶导数
    prev_derivative = calculate_first_derivative(data, index - 1)
    next_derivative = calculate_first_derivative(data, index + 1)
    
    # 检测极值点：
    # 峰值：导数从正变负（从左到右，函数先增后减）
    # 谷值：导数从负变正（从左到右，函数先减后增）
    
    # 检查是否为峰值（导数从正变负）
    if (abs(prev_derivative) > derivative_threshold and 
        abs(next_derivative) > derivative_threshold and
        prev_derivative > derivative_threshold and 
        next_derivative < -derivative_threshold):
        return True, 'peak'
    
    # 检查是否为谷值（导数从负变正）
    elif (abs(prev_derivative) > derivative_threshold and 
          abs(next_derivative) > derivative_threshold and
          prev_derivative < -derivative_threshold and 
          next_derivative > derivative_threshold):
        return True, 'trough'
    
    return False, None

def is_extreme_in_time_window(timestamps, data, current_index, window_minutes=20, 
                             derivative_threshold=0.001):
    """在时间窗口内检测是否为极值点（使用一阶导数）"""
    if len(timestamps) < 3:
        return False, None
    
    current_timestamp = timestamps[current_index]
    
    # 计算时间窗口内的数据点
    window_start_time = current_timestamp - timedelta(minutes=window_minutes)
    window_end_time = current_timestamp + timedelta(minutes=window_minutes)
    
    # 找到时间窗口内的数据点索引
    window_indices = []
    for i, ts in enumerate(timestamps):
        if window_start_time <= ts <= window_end_time:
            window_indices.append(i)
    
    if len(window_indices) < 3:
        return False, None
    
    # 在时间窗口内使用导数检测极值点
    return is_extreme_by_derivative(data, current_index, derivative_threshold)

def is_relative_extreme(extreme_type, current_value, current_timestamp, 
                       all_extremes, min_extreme_distance=0.1):
    """检查当前值是否相对于上一个极值点是真正的极值
    
    规则：
    1. 如果新极值与上一个不同类型极值差异太小，则略过当前极值
    2. 如果新极值与上一个同类型极值，则保留绝对值更大的那个
    """
    # 如果没有历史数据，直接返回True
    if len(all_extremes) == 0:
        return True, 'keep'
    
    # 获取上一个极值点
    last_extreme = all_extremes[-1]
    last_extreme_type = last_extreme[2]  # 极值类型
    last_extreme_value = last_extreme[1]  # 极值数值
    
    # 如果新极值与上一个极值类型不同
    if extreme_type != last_extreme_type:
        # 计算与上一个极值的绝对差异
        diff_absolute = abs(current_value - last_extreme_value)
        
        # 如果差异太小，略过当前极值
        if diff_absolute < min_extreme_distance:
            return False, None
        
        # 差异足够大，保留新极值
        return True, 'keep'
    
    # 如果新极值与上一个极值类型相同
    else:
        # 计算与上一个极值的绝对差异
        diff_absolute = abs(current_value - last_extreme_value)
        
        # 如果差异太小，略过当前极值
        if diff_absolute < min_extreme_distance:
            return False, None
        
        # 根据极值类型决定保留策略
        if extreme_type == 'peak':
            # 峰值：保留更大的值
            if current_value > last_extreme_value:
                return True, 'keep'  # 保留新的
            else:
                return True, 'replace'  # 替换旧的
        else:  # extreme_type == 'trough'
            # 谷值：保留更小的值
            if current_value < last_extreme_value:
                return True, 'keep'  # 保留新的
            else:
                return True, 'replace'  # 替换旧的
    
    return False, None

class RealTimeExtremeDetector:
    """实时极值检测器，每分钟分析当前极值"""
    
    def __init__(self, window_minutes=20, derivative_threshold=0.0001, max_history=1000):
        self.window_minutes = window_minutes
        self.derivative_threshold = derivative_threshold
        self.max_history = max_history
        
        # 使用deque来存储历史数据，自动限制大小
        self.timestamps = deque(maxlen=max_history)
        self.price_data = deque(maxlen=max_history)
        self.macd_data = deque(maxlen=max_history)
        
        # 存储检测到的极值点
        self.macd_peaks = []
        self.macd_troughs = []
        
        # 按时间排序的所有极值点列表（用于比较上一个极值点）
        self.all_extremes = []
        
        # 统计信息
        self.total_peaks_detected = 0
        self.total_troughs_detected = 0
        self.last_analysis_time = None
    
    def add_data_point(self, timestamp, price, macd):
        """添加新的数据点"""
        self.timestamps.append(timestamp)
        self.price_data.append(price)
        self.macd_data.append(macd)
        
        # 如果数据点足够多，进行极值检测
        if len(self.timestamps) >= 3:
            self.analyze_current_extremes()
    
    def analyze_current_extremes(self):
        """分析当前的极值点"""
        if len(self.timestamps) < 3:
            return
        
        current_index = len(self.timestamps) - 2  # 当前点（排除边界点）
        current_timestamp = self.timestamps[current_index]
        current_price = self.price_data[current_index]
        current_macd = self.macd_data[current_index]
        
        # 检查是否需要分析（每分钟分析一次）
        if (self.last_analysis_time is None or 
            (current_timestamp - self.last_analysis_time).total_seconds() >= 60):
            
            self.last_analysis_time = current_timestamp
            
            # DEBUG: 打印当前分析状态
            print(f"\n[DEBUG] 分析时间: {current_timestamp.strftime('%H:%M:%S')}")
            print(f"[DEBUG] 当前索引: {current_index}, 数据点总数: {len(self.timestamps)}")
            print(f"[DEBUG] 当前MACD值: {current_macd:.4f}")
            
            # 检测MACD极值点
            macd_extreme, macd_type = self._detect_extreme(
                list(self.macd_data), current_index
            )
            
            # DEBUG: 打印极值检测结果
            print(f"[DEBUG] 极值检测结果: {macd_extreme}, 类型: {macd_type}")
            
            if macd_extreme:
                self._handle_extreme_detection(
                    macd_type, current_macd, current_timestamp, 
                    self.macd_peaks, self.macd_troughs, 'MACD'
                )
                
                # 打印当前状态
                self._print_current_status(current_timestamp, current_price, current_macd)
            else:
                # DEBUG: 如果没有检测到极值，打印更多信息
                if current_index >= 2:
                    prev_macd = self.macd_data[current_index - 1]
                    next_macd = self.macd_data[current_index + 1]
                    print(f"[DEBUG] 前后值: 前={prev_macd:.4f}, 当前={current_macd:.4f}, 后={next_macd:.4f}")
                    print(f"[DEBUG] 峰值条件: {current_macd > prev_macd and current_macd > next_macd}")
                    print(f"[DEBUG] 谷值条件: {current_macd < prev_macd and current_macd < next_macd}")
    
    def _detect_extreme(self, data, current_index):
        """检测极值点"""
        # 修复索引检查逻辑：确保有足够的前后点进行比较
        if current_index < 1 or current_index >= len(data) - 1:
            print(f"[DEBUG] 索引超出范围: current_index={current_index}, data_length={len(data)}")
            return False, None
        
        # 使用更简单的极值检测逻辑：比较当前点与前后点的值
        current_value = data[current_index]
        prev_value = data[current_index - 1]
        next_value = data[current_index + 1]
        
        # DEBUG: 打印检测详情
        print(f"[DEBUG] 极值检测详情: 前值={prev_value:.4f}, 当前值={current_value:.4f}, 后值={next_value:.4f}")
        
        # 检测峰值：当前值比前后值都大
        if current_value > prev_value and current_value > next_value:
            print(f"[DEBUG] 检测到峰值!")
            return True, 'peak'
        
        # 检测谷值：当前值比前后值都小
        elif current_value < prev_value and current_value < next_value:
            print(f"[DEBUG] 检测到谷值!")
            return True, 'trough'
        
        print(f"[DEBUG] 未检测到极值")
        return False, None
    
    def _handle_extreme_detection(self, extreme_type, current_value, current_timestamp, 
                                 peaks, troughs, data_name):
        """处理极值检测结果"""
        # 使用新的相对极值检测逻辑
        is_extreme, action = is_relative_extreme(extreme_type, current_value, current_timestamp, self.all_extremes, 0.1)
        
        if is_extreme:
            if action == 'replace':
                # 替换上一个极值点
                if len(self.all_extremes) > 0:
                    removed_extreme = self.all_extremes.pop()
                    removed_type = removed_extreme[2]
                    removed_value = removed_extreme[1]
                    
                    # 从对应的极值列表中删除
                    if removed_type == 'peak' and len(peaks) > 0:
                        peaks.pop()
                    elif removed_type == 'trough' and len(troughs) > 0:
                        troughs.pop()
                    
                    print(f"替换{data_name}{removed_type}: 时间{removed_extreme[0].strftime('%H:%M:%S')}, 值{removed_value:.2f} -> 时间{current_timestamp.strftime('%H:%M:%S')}, 值{current_value:.2f}")
                else:
                    print(f"检测到新{data_name}{extreme_type}: 时间{current_timestamp.strftime('%H:%M:%S')}, 值{current_value:.2f}")
                
                # 添加新极值点
                self._add_extreme_point(extreme_type, current_timestamp, current_value, peaks, troughs)
                
            elif action == 'keep':
                # 保留当前极值点
                self._add_extreme_point(extreme_type, current_timestamp, current_value, peaks, troughs)
                print(f"检测到新{data_name}{extreme_type}: 时间{current_timestamp.strftime('%H:%M:%S')}, 值{current_value:.2f}")
        else:
            print(f"略过{data_name}{extreme_type}: 时间{current_timestamp.strftime('%H:%M:%S')}, 值{current_value:.2f} (差异太小)")
    
    def _add_extreme_point(self, extreme_type, timestamp, value, peaks, troughs):
        """添加极值点到相应的列表和总列表"""
        # 添加到对应的极值列表
        if extreme_type == 'peak':
            peaks.append((timestamp, value))
            self.total_peaks_detected += 1
        else:  # extreme_type == 'trough'
            troughs.append((timestamp, value))
            self.total_troughs_detected += 1
        
        # 添加到总极值列表（按时间排序）
        self.all_extremes.append((timestamp, value, extreme_type))
    
    def _print_current_status(self, timestamp, price, macd):
        """打印当前状态"""
        print(f"\n[{timestamp.strftime('%H:%M:%S')}] 当前状态:")
        print(f"  价格: {price:.2f}, MACD: {macd:.3f}")
        print(f"  累计检测: 峰值{self.total_peaks_detected}个, 谷值{self.total_troughs_detected}个")
        print(f"  MACD峰值{len(self.macd_peaks)}个, MACD谷值{len(self.macd_troughs)}个")
    
    def get_statistics(self):
        """获取统计信息"""
        return {
            'total_peaks': self.total_peaks_detected,
            'total_troughs': self.total_troughs_detected,
            'current_macd_peaks': len(self.macd_peaks),
            'current_macd_troughs': len(self.macd_troughs),
            'data_points': len(self.timestamps)
        }
    
    def plot_current_results(self, save_path='realtime_macd_extreme_detection.png'):
        """绘制当前检测结果"""
        if len(self.timestamps) < 3:
            print("数据点不足，无法绘图")
            return
        
        timestamps = list(self.timestamps)
        prices = list(self.price_data)
        macd_data = list(self.macd_data)
        
        plt.figure(figsize=(15, 8))
        
        # 价格图（仅显示，不检测极值）
        plt.subplot(2, 1, 1)
        plt.plot(timestamps, prices, 'b-', label='价格', linewidth=1, alpha=0.7)
        plt.title('价格走势（仅显示）', fontsize=14)
        plt.xlabel('时间')
        plt.ylabel('价格')
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        # MACD图
        plt.subplot(2, 1, 2)
        plt.plot(timestamps, macd_data, 'purple', label='MACD (DIF)', linewidth=1, alpha=0.7)
        
        # 标记极值点并标注DIF值
        for ts, macd in self.macd_peaks:
            plt.scatter(ts, macd, color='red', s=80, marker='^', label='MACD峰值' if ts == self.macd_peaks[0][0] else "", zorder=5)
            # 在峰值点上方标注DIF值
            plt.annotate(f'{macd:.2f}', (ts, macd), 
                        xytext=(0, 10), textcoords='offset points',
                        ha='center', va='bottom', fontsize=8,
                        bbox=dict(boxstyle='round,pad=0.3', facecolor='red', alpha=0.7))
        
        for ts, macd in self.macd_troughs:
            plt.scatter(ts, macd, color='green', s=80, marker='v', label='MACD谷值' if ts == self.macd_troughs[0][0] else "", zorder=5)
            # 在谷值点下方标注DIF值
            plt.annotate(f'{macd:.2f}', (ts, macd), 
                        xytext=(0, -15), textcoords='offset points',
                        ha='center', va='top', fontsize=8,
                        bbox=dict(boxstyle='round,pad=0.3', facecolor='green', alpha=0.7))
        
        plt.title('实时MACD极值点检测', fontsize=14)
        plt.xlabel('时间')
        plt.ylabel('MACD值')
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.show()

def simulate_realtime_analysis():
    """模拟实时MACD极值分析"""
    print("开始模拟实时MACD极值检测...")
    print("每分钟分析一次当前MACD极值...")
    
    # 创建实时检测器
    detector = RealTimeExtremeDetector(window_minutes=20, derivative_threshold=0.001)
    
    # 生成模拟数据（每分钟一个数据点）
    start_time = datetime.now().replace(second=0, microsecond=0)
    print(f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 模拟100分钟的数据
    for i in range(100):
        # 生成当前时间点
        current_time = start_time + timedelta(minutes=i)
        
        # 生成更明显的极值数据
        t = i / 5.0  # 更快的周期，产生更多极值
        
        # 价格数据：多个正弦波叠加，产生明显的峰谷
        base_price = 100 + 15 * np.sin(t) + 8 * np.sin(2.5*t) + 5 * np.sin(4*t)
        noise = np.random.normal(0, 0.3)  # 减少噪声
        price = base_price + noise
        
        # MACD数据：更明显的极值模式
        # 使用多个不同频率的正弦波，产生明显的峰谷交替
        macd = 3 * np.sin(t) + 2 * np.sin(1.5*t) + 1.5 * np.sin(3*t) + np.random.normal(0, 0.05)
        
        # 添加一些随机极值点
        if i % 15 == 0:  # 每15个点添加一个随机极值
            macd += np.random.choice([2, -2]) * np.random.uniform(0.5, 1.5)
        
        # 添加数据点并进行分析
        detector.add_data_point(current_time, price, macd)
        
        # 模拟实时数据流（每秒添加一个数据点）
        if i < 99:  # 不是最后一个数据点
            time.sleep(0.1)  # 模拟0.1秒的延迟
    
    # 打印最终统计
    print("\n" + "="*50)
    print("模拟完成！最终统计:")
    stats = detector.get_statistics()
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    # 绘制结果
    detector.plot_current_results()
    
    return detector




if __name__ == "__main__":
    print("实时MACD极值检测算法测试程序")
    print("="*50)
    print("每分钟分析一次当前MACD极值...")
    
    # 运行实时MACD极值检测模拟
    detector = simulate_realtime_analysis()
