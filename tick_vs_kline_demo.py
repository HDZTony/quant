#!/usr/bin/env python3
"""
Tick数据 vs K线数据演示
展示Tick数据的详细信息和K线数据的聚合过程
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pathlib import Path

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False


def create_sample_tick_data():
    """创建示例Tick数据"""
    print("=== 创建示例Tick数据 ===")
    
    # 模拟1分钟内的tick数据
    base_time = datetime(2024, 1, 2, 9, 30, 0)
    
    tick_data = [
        # 时间戳, 成交价, 成交量, 类型
        (base_time + timedelta(milliseconds=123), 150.25, 100, "成交"),
        (base_time + timedelta(milliseconds=456), 150.30, 200, "成交"),
        (base_time + timedelta(milliseconds=789), 150.20, 150, "成交"),
        (base_time + timedelta(milliseconds=1012), 150.35, 300, "成交"),
        (base_time + timedelta(milliseconds=1234), 150.28, 250, "成交"),
        (base_time + timedelta(milliseconds=1456), 150.32, 180, "成交"),
        (base_time + timedelta(milliseconds=1678), 150.29, 120, "成交"),
        (base_time + timedelta(milliseconds=1890), 150.33, 400, "成交"),
    ]
    
    df_ticks = pd.DataFrame(tick_data, columns=['timestamp', 'price', 'volume', 'type'])
    df_ticks = df_ticks.set_index('timestamp')
    
    print("Tick数据详情:")
    print(df_ticks)
    print()
    
    return df_ticks


def create_sample_quote_tick_data():
    """创建示例报价Tick数据（包含bid/ask）"""
    print("=== 创建示例报价Tick数据 ===")
    
    base_time = datetime(2024, 1, 2, 9, 30, 0)
    
    quote_data = [
        # 时间戳, bid价格, ask价格, bid数量, ask数量
        (base_time + timedelta(milliseconds=100), 150.24, 150.26, 100, 200),
        (base_time + timedelta(milliseconds=300), 150.25, 150.27, 150, 180),
        (base_time + timedelta(milliseconds=500), 150.23, 150.26, 200, 160),
        (base_time + timedelta(milliseconds=700), 150.26, 150.28, 120, 220),
        (base_time + timedelta(milliseconds=900), 150.25, 150.27, 180, 190),
        (base_time + timedelta(milliseconds=1100), 150.27, 150.29, 160, 210),
        (base_time + timedelta(milliseconds=1300), 150.26, 150.28, 140, 200),
        (base_time + timedelta(milliseconds=1500), 150.28, 150.30, 170, 180),
    ]
    
    df_quotes = pd.DataFrame(quote_data, columns=['timestamp', 'bid', 'ask', 'bid_size', 'ask_size'])
    df_quotes = df_quotes.set_index('timestamp')
    
    print("报价Tick数据详情:")
    print(df_quotes)
    print()
    
    return df_quotes


def aggregate_to_kline(df_ticks, interval='1min'):
    """将Tick数据聚合为K线数据"""
    print(f"=== 聚合为{interval}K线数据 ===")
    
    # 按时间间隔重采样
    df_resampled = df_ticks.resample(interval).agg({
        'price': {
            'open': 'first',    # 第一个价格作为开盘价
            'high': 'max',      # 最高价格
            'low': 'min',       # 最低价格
            'close': 'last'     # 最后一个价格作为收盘价
        },
        'volume': 'sum'         # 成交量求和
    })
    
    # 扁平化列名
    df_resampled.columns = ['open', 'high', 'low', 'close', 'volume']
    
    print("聚合后的K线数据:")
    print(df_resampled)
    print()
    
    return df_resampled


def explain_ohlcv():
    """详细解释OHLCV"""
    print("=== OHLCV详细解释 ===")
    
    print("OHLCV是K线数据的五个基本要素:")
    print()
    print("O (Open) - 开盘价:")
    print("  • 该时间段内第一笔成交的价格")
    print("  • 反映市场开盘时的价格水平")
    print()
    
    print("H (High) - 最高价:")
    print("  • 该时间段内所有成交中的最高价格")
    print("  • 反映该时间段内的价格上限")
    print()
    
    print("L (Low) - 最低价:")
    print("  • 该时间段内所有成交中的最低价格")
    print("  • 反映该时间段内的价格下限")
    print()
    
    print("C (Close) - 收盘价:")
    print("  • 该时间段内最后一笔成交的价格")
    print("  • 通常是最重要的价格，用于计算涨跌幅")
    print()
    
    print("V (Volume) - 成交量:")
    print("  • 该时间段内所有成交的数量总和")
    print("  • 反映市场活跃度和流动性")
    print()


def visualize_tick_vs_kline(df_ticks, df_kline):
    """可视化Tick数据和K线数据"""
    print("=== 创建可视化图表 ===")
    
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
    
    # 绘制Tick数据
    ax1.plot(df_ticks.index, df_ticks['price'], 'o-', markersize=6, linewidth=1, alpha=0.7, label='Tick价格')
    ax1.set_title('Tick数据 - 每笔成交的详细价格', fontsize=14, fontweight='bold')
    ax1.set_ylabel('价格')
    ax1.grid(True, alpha=0.3)
    ax1.legend()
    
    # 添加成交量柱状图
    ax1_twin = ax1.twinx()
    ax1_twin.bar(df_ticks.index, df_ticks['volume'], alpha=0.3, color='orange', label='成交量')
    ax1_twin.set_ylabel('成交量', color='orange')
    ax1_twin.tick_params(axis='y', labelcolor='orange')
    
    # 绘制K线数据
    for idx, row in df_kline.iterrows():
        # 绘制K线实体
        if row['close'] >= row['open']:
            color = 'red'  # 上涨
            body_bottom = row['open']
            body_top = row['close']
        else:
            color = 'green'  # 下跌
            body_bottom = row['close']
            body_top = row['open']
        
        # 绘制影线（最高价到最低价）
        ax2.plot([idx, idx], [row['low'], row['high']], color='black', linewidth=1)
        
        # 绘制实体
        ax2.bar(idx, body_top - body_bottom, bottom=body_bottom, 
                color=color, alpha=0.7, width=0.6)
    
    ax2.set_title('K线数据 - 聚合后的OHLCV', fontsize=14, fontweight='bold')
    ax2.set_ylabel('价格')
    ax2.grid(True, alpha=0.3)
    
    # 添加成交量
    ax2_twin = ax2.twinx()
    ax2_twin.bar(df_kline.index, df_kline['volume'], alpha=0.3, color='blue', label='成交量')
    ax2_twin.set_ylabel('成交量', color='blue')
    ax2_twin.tick_params(axis='y', labelcolor='blue')
    
    plt.tight_layout()
    
    # 保存图片
    output_path = Path("tick_vs_kline_comparison.png")
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"图表已保存到: {output_path}")
    
    plt.show()


def demonstrate_data_differences():
    """演示数据差异"""
    print("=== 数据差异对比 ===")
    
    print("1. 数据粒度:")
    print("   Tick数据: 每笔报价/成交，时间精度到毫秒")
    print("   K线数据: 聚合后的时间段数据，如1分钟、5分钟、日线")
    print()
    
    print("2. 数据量:")
    print("   Tick数据: 数据量巨大，1天可能有数万条记录")
    print("   K线数据: 数据量相对较小，1天只有几条到几十条记录")
    print()
    
    print("3. 用途:")
    print("   Tick数据: 高频交易、精确执行、市场微观结构分析")
    print("   K线数据: 技术分析、趋势识别、策略回测")
    print()
    
    print("4. 存储需求:")
    print("   Tick数据: 需要大量存储空间和高速处理能力")
    print("   K线数据: 存储需求相对较小，处理速度要求较低")
    print()


def main():
    """主函数"""
    print("Tick数据 vs K线数据演示")
    print("=" * 50)
    
    # 创建示例数据
    df_ticks = create_sample_tick_data()
    df_quotes = create_sample_quote_tick_data()
    
    # 聚合为K线
    df_kline = aggregate_to_kline(df_ticks)
    
    # 解释OHLCV
    explain_ohlcv()
    
    # 演示数据差异
    demonstrate_data_differences()
    
    # 可视化
    visualize_tick_vs_kline(df_ticks, df_kline)
    
    print("演示完成！")
    print("\n关键要点:")
    print("• Tick数据包含每笔报价/成交的详细信息")
    print("• K线数据是Tick数据的聚合，包含OHLCV五个要素")
    print("• 两者各有用途，根据交易策略选择合适的数据类型")


if __name__ == "__main__":
    main() 