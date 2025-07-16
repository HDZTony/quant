#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
样本数据下载脚本 - Windows版本
用于下载Nautilus Trader官方示例数据
基于官方脚本: https://raw.githubusercontent.com/nautechsystems/nautilus_data/main/nautilus_data/hist_data_to_catalog.py
"""

import os
import requests
import gzip
import shutil
from pathlib import Path
from urllib.parse import urlparse

# 检查Nautilus Trader是否可用
try:
    from nautilus_trader.persistence.catalog import ParquetDataCatalog
    from nautilus_trader.persistence.wranglers import QuoteTickDataWrangler
    from nautilus_trader.test_kit.providers import CSVTickDataLoader
    from nautilus_trader.test_kit.providers import TestInstrumentProvider
    NAUTILUS_AVAILABLE = True
except ImportError:
    print("警告: Nautilus Trader不可用，将只下载原始数据")
    NAUTILUS_AVAILABLE = False


def download_file(url: str, filename: str = None) -> str:
    """
    下载文件到本地
    
    Args:
        url: 下载链接
        filename: 保存的文件名，如果为None则从URL中提取
        
    Returns:
        保存的文件路径
    """
    if filename is None:
        filename = url.rsplit("/", maxsplit=1)[1]
    
    print(f"正在下载: {url}")
    print(f"保存到: {filename}")
    
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        with open(filename, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        print(f"✅ 下载完成: {filename}")
        return filename
        
    except Exception as e:
        print(f"❌ 下载失败: {e}")
        return None


def extract_gzip(gz_filename: str) -> str:
    """
    解压gzip文件
    
    Args:
        gz_filename: gzip文件路径
        
    Returns:
        解压后的文件路径
    """
    if not gz_filename.endswith('.gz'):
        return gz_filename
    
    csv_filename = gz_filename[:-3]  # 去掉.gz后缀
    
    print(f"正在解压: {gz_filename}")
    
    try:
        with gzip.open(gz_filename, 'rb') as f_in:
            with open(csv_filename, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        print(f"✅ 解压完成: {csv_filename}")
        return csv_filename
        
    except Exception as e:
        print(f"❌ 解压失败: {e}")
        return None


def load_fx_hist_data(filename: str, currency: str, catalog_path: str) -> bool:
    """
    加载FX历史数据到Nautilus目录
    
    Args:
        filename: CSV数据文件路径
        currency: 货币对名称
        catalog_path: 目录保存路径
        
    Returns:
        是否成功
    """
    if not NAUTILUS_AVAILABLE:
        print("❌ Nautilus Trader不可用，无法处理数据")
        return False
    
    try:
        print(f"正在处理数据: {filename}")
        
        # 创建测试工具
        instrument = TestInstrumentProvider.default_fx_ccy(currency)
        wrangler = QuoteTickDataWrangler(instrument)

        # 加载CSV数据
        df = CSVTickDataLoader.load(
            filename,
            index_col=0,
            datetime_format="%Y%m%d %H%M%S%f",
        )
        df.columns = ["bid_price", "ask_price", "size"]
        print(f"数据预览:\n{df.head()}")
        print(f"数据形状: {df.shape}")

        # 处理tick数据
        print("正在准备tick数据...")
        ticks = wrangler.process(df)
        print(f"生成了 {len(ticks)} 个tick")

        # 写入目录
        print("正在写入数据到目录...")
        catalog = ParquetDataCatalog(catalog_path)
        catalog.write_data([instrument])
        catalog.write_data(ticks)

        print("✅ 数据处理完成")
        return True
        
    except Exception as e:
        print(f"❌ 数据处理失败: {e}")
        return False


def main():
    """主函数"""
    print("Nautilus Trader样本数据下载工具")
    print("="*50)
    
    # 创建数据目录
    data_dir = Path("sample_data")
    data_dir.mkdir(exist_ok=True)
    
    catalog_dir = Path("catalog")
    catalog_dir.mkdir(exist_ok=True)
    
    print(f"数据目录: {data_dir.absolute()}")
    print(f"目录路径: {catalog_dir.absolute()}")
    
    # 下载EUR/USD样本数据
    url = "https://raw.githubusercontent.com/nautechsystems/nautilus_data/main/raw_data/fx_hist_data/DAT_ASCII_EURUSD_T_202001.csv.gz"
    
    # 下载文件
    gz_filename = download_file(url)
    if not gz_filename:
        print("❌ 下载失败，退出")
        return
    
    # 解压文件
    csv_filename = extract_gzip(gz_filename)
    if not csv_filename:
        print("❌ 解压失败，退出")
        return
    
    # 处理数据（如果Nautilus Trader可用）
    if NAUTILUS_AVAILABLE:
        success = load_fx_hist_data(
            filename=csv_filename,
            currency="EUR/USD",
            catalog_path=str(catalog_dir)
        )
        
        if success:
            print("\n🎉 样本数据准备完成！")
            print(f"目录位置: {catalog_dir.absolute()}")
            print("现在可以使用这些数据进行回测了")
        else:
            print("\n⚠️ 数据处理失败，但原始数据已下载")
    else:
        print("\n⚠️ Nautilus Trader不可用")
        print(f"原始数据已下载到: {csv_filename}")
        print("请安装Nautilus Trader后重新运行此脚本")
    
    print("\n文件说明:")
    print(f"- {gz_filename}: 压缩的原始数据")
    print(f"- {csv_filename}: 解压后的CSV数据")
    if NAUTILUS_AVAILABLE:
        print(f"- catalog/: Nautilus Trader格式的数据目录")


if __name__ == "__main__":
    main() 