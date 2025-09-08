#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
根据tick数据计算每分钟成交量
"""

import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta

def calculate_minute_volume(file_path):
    """
    根据tick数据计算每分钟成交量
    
    Args:
        file_path: parquet文件路径
    
    Returns:
        DataFrame: 包含每分钟成交量统计的数据
    """
    
    print(f"正在读取文件: {file_path}")
    
    # 读取tick数据
    df = pd.read_parquet(file_path)
    print(f"原始数据形状: {df.shape}")
    print(f"列名: {df.columns.tolist()}")
    
    # 将UTC时间转换为北京时间 (UTC+8)
    print("正在转换时区...")
    df['beijing_time'] = df['timestamp'].dt.tz_localize('UTC').dt.tz_convert('Asia/Shanghai')
    
    # 创建分钟级别的时间戳（向下取整到分钟）
    df['minute_time'] = df['beijing_time'].dt.floor('min')
    
    # 按分钟分组计算成交量统计
    print("正在计算每分钟成交量...")
    minute_stats = df.groupby('minute_time').agg({
        'size': ['sum', 'count', 'mean', 'std'],  # 总成交量、交易次数、平均成交量、成交量标准差
        'price': ['first', 'last', 'min', 'max', 'mean'],  # 开盘价、收盘价、最低价、最高价、平均价
        'trade_id': 'count'  # 交易次数
    }).round(4)
    
    # 重命名列
    minute_stats.columns = [
        '总成交量', '交易次数', '平均成交量', '成交量标准差',
        '开盘价', '收盘价', '最低价', '最高价', '平均价', '交易笔数'
    ]
    
    # 重置索引，使时间成为列
    minute_stats = minute_stats.reset_index()
    
    return minute_stats, df

def print_minute_volume_data(minute_stats, df, start_time=None, end_time=None):
    """
    打印分钟成交量数据，包括每笔交易的详情
    
    Args:
        minute_stats: 分钟统计数据
        df: 原始tick数据
        start_time: 开始时间 (可选)
        end_time: 结束时间 (可选)
    """
    
    # 如果指定了时间范围，进行筛选
    if start_time and end_time:
        mask = (minute_stats['minute_time'] >= start_time) & (minute_stats['minute_time'] <= end_time)
        filtered_stats = minute_stats[mask]
        print(f"\n筛选时间范围: {start_time} 到 {end_time}")
        print(f"筛选到的分钟数: {len(filtered_stats)}")
    else:
        filtered_stats = minute_stats
        print(f"\n总分钟数: {len(filtered_stats)}")
    
    print("\n每分钟成交量统计:")
    print("=" * 80)
    print(f"{'时间':<20} {'每分钟成交量':<15} {'交易次数':<8}")
    print("-" * 80)
    
    for _, row in filtered_stats.iterrows():
        print(f"{row['minute_time'].strftime('%Y-%m-%d %H:%M'):<20} "
              f"{row['总成交量']:<15} "
              f"{row['交易次数']:<8}")
        
        # 显示该分钟内的每笔交易详情
        minute_start = row['minute_time']
        minute_end = minute_start + pd.Timedelta(minutes=1)
        
        # 筛选该分钟内的所有交易
        minute_trades = df[(df['minute_time'] >= minute_start) & (df['minute_time'] < minute_end)]
        
        if len(minute_trades) > 0:
            print(f"  该分钟内的交易详情:")
            print(f"  {'时间':<20} {'价格':<10} {'成交量':<12} {'该分钟内累计':<12}")
            print(f"  {'-'*20} {'-'*10} {'-'*12} {'-'*12}")
            
            minute_cumulative = 0
            for _, trade in minute_trades.iterrows():
                minute_cumulative += trade['size']
                print(f"  {trade['beijing_time'].strftime('%H:%M:%S.%f')[:-3]:<20} "
                      f"{trade['price']:<10} "
                      f"{trade['size']:<12} "
                      f"{minute_cumulative:<12}")
            print(f"  该分钟总成交量: {row['总成交量']}")
            print()

def main():
    """主函数"""
    
    # 文件路径
    file_path = 'catalog/etf_159506_cache/cache_data_20250821.parquet'
    
    try:
        # 计算每分钟成交量
        minute_stats, df = calculate_minute_volume(file_path)
        
        # 打印所有数据
        print_minute_volume_data(minute_stats, df)
        
        # 特别打印北京时间9:30到15:00的数据
        start_time = pd.Timestamp('2025-08-21 9:30:00', tz='Asia/Shanghai')
        end_time = pd.Timestamp('2025-08-21 15:00:00', tz='Asia/Shanghai')
        
        print("\n" + "="*80)
        print("北京时间9:30到15:00的每分钟成交量:")
        print("="*80)
        print_minute_volume_data(minute_stats, df, start_time, end_time)
        
        # 保存结果到CSV文件
        output_file = 'minute_volume_20250821.csv'
        minute_stats.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"\n结果已保存到: {output_file}")
        
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
