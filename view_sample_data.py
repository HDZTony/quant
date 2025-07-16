#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
样本数据查看器
用于查看下载的Nautilus Trader样本数据
"""

import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime


def load_fx_data(filename: str) -> pd.DataFrame:
    """
    加载FX数据
    
    Args:
        filename: CSV文件路径
        
    Returns:
        处理后的DataFrame
    """
    print(f"正在加载数据: {filename}")
    
    # 读取CSV文件
    df = pd.read_csv(filename, header=None, names=['datetime', 'bid', 'ask', 'volume'])
    
    # 转换时间格式
    df['datetime'] = pd.to_datetime(df['datetime'], format='%Y%m%d %H%M%S%f')
    
    # 计算中间价
    df['mid_price'] = (df['bid'] + df['ask']) / 2
    
    # 计算点差
    df['spread'] = df['ask'] - df['bid']
    
    print(f"数据加载完成，共 {len(df)} 条记录")
    print(f"时间范围: {df['datetime'].min()} 到 {df['datetime'].max()}")
    
    return df


def analyze_data(df: pd.DataFrame):
    """分析数据"""
    print("\n数据统计分析:")
    print("="*50)
    
    # 基本统计
    print("价格统计:")
    print(f"  中间价范围: {df['mid_price'].min():.5f} - {df['mid_price'].max():.5f}")
    print(f"  中间价均值: {df['mid_price'].mean():.5f}")
    print(f"  中间价标准差: {df['mid_price'].std():.5f}")
    
    print(f"\n点差统计:")
    print(f"  点差范围: {df['spread'].min():.5f} - {df['spread'].max():.5f}")
    print(f"  点差均值: {df['spread'].mean():.5f}")
    print(f"  点差标准差: {df['spread'].std():.5f}")
    
    # 时间分析
    print(f"\n时间分析:")
    print(f"  数据频率: 平均每 {(df['datetime'].max() - df['datetime'].min()).total_seconds() / len(df):.2f} 秒一条")
    
    # 按小时统计
    df['hour'] = df['datetime'].dt.hour
    hourly_counts = df['hour'].value_counts().sort_index()
    print(f"\n按小时分布:")
    for hour, count in hourly_counts.items():
        print(f"  {hour:02d}:00 - {hour:02d}:59: {count:6d} 条")


def plot_data(df: pd.DataFrame, sample_size: int = 1000):
    """绘制数据图表"""
    print(f"\n正在绘制图表 (使用前 {sample_size} 条数据)...")
    
    # 采样数据以减少绘图时间
    if len(df) > sample_size:
        df_sample = df.head(sample_size)
    else:
        df_sample = df
    
    # 创建图表
    fig, axes = plt.subplots(3, 1, figsize=(12, 10))
    fig.suptitle('EUR/USD 样本数据分析', fontsize=16)
    
    # 1. 价格走势
    axes[0].plot(df_sample['datetime'], df_sample['mid_price'], label='中间价', linewidth=0.5)
    axes[0].plot(df_sample['datetime'], df_sample['bid'], label='买价', alpha=0.7, linewidth=0.3)
    axes[0].plot(df_sample['datetime'], df_sample['ask'], label='卖价', alpha=0.7, linewidth=0.3)
    axes[0].set_title('价格走势')
    axes[0].set_ylabel('价格')
    axes[0].legend()
    axes[0].grid(True, alpha=0.3)
    
    # 格式化x轴时间
    axes[0].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    axes[0].xaxis.set_major_locator(mdates.MinuteLocator(interval=5))
    plt.setp(axes[0].xaxis.get_majorticklabels(), rotation=45)
    
    # 2. 点差
    axes[1].plot(df_sample['datetime'], df_sample['spread'], color='red', linewidth=0.5)
    axes[1].set_title('点差变化')
    axes[1].set_ylabel('点差')
    axes[1].grid(True, alpha=0.3)
    
    # 格式化x轴时间
    axes[1].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    axes[1].xaxis.set_major_locator(mdates.MinuteLocator(interval=5))
    plt.setp(axes[1].xaxis.get_majorticklabels(), rotation=45)
    
    # 3. 成交量（这里都是0，所以显示为直方图）
    axes[2].hist(df_sample['volume'], bins=20, alpha=0.7, color='green')
    axes[2].set_title('成交量分布')
    axes[2].set_xlabel('成交量')
    axes[2].set_ylabel('频次')
    axes[2].grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('sample_data_analysis.png', dpi=300, bbox_inches='tight')
    print("图表已保存为: sample_data_analysis.png")
    plt.show()


def main():
    """主函数"""
    print("Nautilus Trader样本数据查看器")
    print("="*50)
    
    # 检查文件是否存在
    filename = "DAT_ASCII_EURUSD_T_202001.csv"
    if not Path(filename).exists():
        print(f"❌ 文件不存在: {filename}")
        print("请先运行 download_sample_data.py 下载数据")
        return
    
    # 加载数据
    df = load_fx_data(filename)
    
    # 显示前几行
    print("\n数据预览:")
    print("="*50)
    print(df.head(10))
    
    # 分析数据
    analyze_data(df)
    
    # 绘制图表
    try:
        plot_data(df)
    except Exception as e:
        print(f"❌ 绘图失败: {e}")
        print("请确保已安装 matplotlib")


if __name__ == "__main__":
    main() 