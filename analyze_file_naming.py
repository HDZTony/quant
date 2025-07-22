#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
分析catalog文件命名格式
"""

from pathlib import Path
from datetime import datetime
import re

def analyze_file_naming():
    """分析文件命名格式"""
    print("=" * 60)
    print("📁 CATALOG文件命名格式分析")
    print("=" * 60)
    
    catalog_path = Path("catalog/etf_159506_cache")
    
    if not catalog_path.exists():
        print(f"❌ Catalog目录不存在: {catalog_path}")
        return
    
    # 获取所有parquet文件
    parquet_files = list(catalog_path.glob("*.parquet"))
    
    if not parquet_files:
        print("❌ 没有找到parquet文件")
        return
    
    print(f"找到 {len(parquet_files)} 个parquet文件")
    
    # 分析命名模式
    naming_patterns = {
        'cache_data_YYYYMMDD_HHMM.parquet': [],
        'cache_data_YYYYMMDD_HH00_merged.parquet': [],
        'final_cache_data_YYYYMMDD_HHMMSS.parquet': [],
        'other': []
    }
    
    for file_path in parquet_files:
        filename = file_path.name
        
        # 匹配不同的命名模式
        if re.match(r'cache_data_\d{8}_\d{4}\.parquet$', filename):
            naming_patterns['cache_data_YYYYMMDD_HHMM.parquet'].append(file_path)
        elif re.match(r'cache_data_\d{8}_\d{4}_merged\.parquet$', filename):
            naming_patterns['cache_data_YYYYMMDD_HH00_merged.parquet'].append(file_path)
        elif re.match(r'final_cache_data_\d{8}_\d{6}\.parquet$', filename):
            naming_patterns['final_cache_data_YYYYMMDD_HHMMSS.parquet'].append(file_path)
        else:
            naming_patterns['other'].append(file_path)
    
    # 显示分析结果
    for pattern, files in naming_patterns.items():
        print(f"\n📋 {pattern}: {len(files)} 个文件")
        if files:
            # 按修改时间排序
            files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
            
            # 显示前5个文件
            for i, file_path in enumerate(files[:5]):
                file_size = file_path.stat().st_size / 1024  # KB
                mod_time = datetime.fromtimestamp(file_path.stat().st_mtime)
                print(f"  {i+1}. {file_path.name} ({file_size:.1f} KB, {mod_time.strftime('%Y-%m-%d %H:%M:%S')})")
            
            if len(files) > 5:
                print(f"  ... 还有 {len(files) - 5} 个文件")
    
    # 分析命名逻辑
    print("\n" + "=" * 60)
    print("🔍 文件命名逻辑分析")
    print("=" * 60)
    
    print("1. cache_data_YYYYMMDD_HHMM.parquet")
    print("   - 来源: 自动保存循环 (_save_buffer_data)")
    print("   - 生成时机: 每10秒保存一次")
    print("   - 特点: 包含实时收集的交易数据")
    
    print("\n2. cache_data_YYYYMMDD_HH00_merged.parquet")
    print("   - 来源: 文件合并 (_merge_daily_files)")
    print("   - 生成时机: 每小时合并一次")
    print("   - 特点: 合并当天所有cache_data文件，删除原始文件")
    
    print("\n3. final_cache_data_YYYYMMDD_HHMMSS.parquet")
    print("   - 来源: 手动保存或程序退出时保存")
    print("   - 生成时机: 调用save_to_parquet方法")
    print("   - 特点: 包含完整的quote和trade数据")
    
    # 分析时间分布
    print("\n" + "=" * 60)
    print("⏰ 文件时间分布分析")
    print("=" * 60)
    
    # 按日期分组
    date_groups = {}
    for file_path in parquet_files:
        # 从文件名提取日期
        match = re.search(r'(\d{8})', file_path.name)
        if match:
            date_str = match.group(1)
            if date_str not in date_groups:
                date_groups[date_str] = []
            date_groups[date_str].append(file_path)
    
    # 显示每日文件数量
    for date_str in sorted(date_groups.keys(), reverse=True):
        files = date_groups[date_str]
        date_obj = datetime.strptime(date_str, '%Y%m%d')
        print(f"{date_obj.strftime('%Y-%m-%d')}: {len(files)} 个文件")
        
        # 按类型统计
        type_counts = {}
        for file_path in files:
            if '_merged' in file_path.name:
                file_type = 'merged'
            elif 'final_cache_data' in file_path.name:
                file_type = 'final'
            else:
                file_type = 'cache'
            
            type_counts[file_type] = type_counts.get(file_type, 0) + 1
        
        for file_type, count in type_counts.items():
            print(f"  - {file_type}: {count} 个")


def analyze_file_content():
    """分析文件内容"""
    print("\n" + "=" * 60)
    print("📊 文件内容分析")
    print("=" * 60)
    
    try:
        import pandas as pd
        
        catalog_path = Path("catalog/etf_159506_cache")
        parquet_files = list(catalog_path.glob("*.parquet"))
        
        if not parquet_files:
            print("❌ 没有找到parquet文件")
            return
        
        # 选择几个代表性文件进行分析
        sample_files = []
        
        # 找一个merged文件
        merged_files = [f for f in parquet_files if '_merged' in f.name]
        if merged_files:
            sample_files.append(merged_files[0])
        
        # 找一个final文件
        final_files = [f for f in parquet_files if 'final_cache_data' in f.name]
        if final_files:
            sample_files.append(final_files[0])
        
        # 找一个普通cache文件
        cache_files = [f for f in parquet_files if 'cache_data_' in f.name and '_merged' not in f.name and 'final_cache_data' not in f.name]
        if cache_files:
            sample_files.append(cache_files[0])
        
        for file_path in sample_files:
            print(f"\n📄 分析文件: {file_path.name}")
            
            try:
                df = pd.read_parquet(file_path)
                
                print(f"  数据行数: {len(df)}")
                print(f"  数据列数: {len(df.columns)}")
                print(f"  列名: {list(df.columns)}")
                
                # 分析数据类型
                if 'type' in df.columns:
                    type_counts = df['type'].value_counts()
                    print(f"  数据类型分布:")
                    for data_type, count in type_counts.items():
                        print(f"    {data_type}: {count} 条")
                
                # 分析时间范围
                if 'timestamp' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    start_time = df['timestamp'].min()
                    end_time = df['timestamp'].max()
                    print(f"  时间范围: {start_time} 到 {end_time}")
                
                # 分析成交量
                if 'size' in df.columns:
                    size_data = df['size'].dropna()
                    if len(size_data) > 0:
                        print(f"  成交量统计:")
                        print(f"    总成交量: {size_data.sum():,.0f}")
                        print(f"    平均成交量: {size_data.mean():.2f}")
                        print(f"    最大成交量: {size_data.max():,.0f}")
                
            except Exception as e:
                print(f"  ❌ 读取文件失败: {e}")
    
    except ImportError:
        print("❌ 需要安装pandas和pyarrow来分析文件内容")


def main():
    """主函数"""
    print("=" * 60)
    print("📁 CATALOG文件命名格式分析工具")
    print("=" * 60)
    
    # 分析文件命名
    analyze_file_naming()
    
    # 分析文件内容
    analyze_file_content()
    
    print("\n" + "=" * 60)
    print("✅ 分析完成")
    print("=" * 60)


if __name__ == "__main__":
    main() 