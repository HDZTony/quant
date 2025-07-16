#!/usr/bin/env python3
"""
修复版本的hist_data_to_catalog.py
正确处理时间格式并添加错误处理
"""

import pandas as pd
from pathlib import Path
import traceback

from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.persistence.wranglers import QuoteTickDataWrangler
from nautilus_trader.test_kit.providers import TestInstrumentProvider


def load_fx_hist_data_fixed(
    filename: str,
    currency: str,
    catalog_path: Path,
) -> None:
    """修复版本的数据加载函数"""
    print(f"=== 处理文件: {filename} ===")
    
    try:
        # 创建工具对象
        instrument = TestInstrumentProvider.default_fx_ccy(currency)
        print(f"创建工具: {instrument.id}")
        
        # 创建wrangler
        wrangler = QuoteTickDataWrangler(instrument)
        
        # 手动读取CSV文件
        print("读取CSV文件...")
        df = pd.read_csv(filename, header=None, names=["timestamp", "bid_price", "ask_price", "size"])
        
        print(f"原始数据形状: {df.shape}")
        print("前5行数据:")
        print(df.head())
        
        # 处理时间戳
        print("处理时间戳...")
        # 时间格式: 20200101 170000065 -> 2020-01-01 17:00:00.065
        df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y%m%d %H%M%S%f')
        
        # 设置时间戳为索引
        df = df.set_index('timestamp')
        
        print("处理后的数据:")
        print(df.head())
        print(f"数据范围: {df.index.min()} 到 {df.index.max()}")
        print(f"总记录数: {len(df)}")
        
        # 检查数据质量
        print("检查数据质量...")
        print(f"Bid价格范围: {df['bid_price'].min():.5f} - {df['bid_price'].max():.5f}")
        print(f"Ask价格范围: {df['ask_price'].min():.5f} - {df['ask_price'].max():.5f}")
        print(f"价差范围: {(df['ask_price'] - df['bid_price']).min():.5f} - {(df['ask_price'] - df['bid_price']).max():.5f}")
        
        # 转换为tick对象
        print("转换为tick对象...")
        ticks = wrangler.process(df)
        print(f"成功转换 {len(ticks)} 个tick对象")
        
        # 写入catalog
        print("写入catalog...")
        catalog = ParquetDataCatalog(catalog_path)
        
        # 写入工具信息
        catalog.write_data([instrument])
        print("✅ 工具信息已写入")
        
        # 写入tick数据
        catalog.write_data(ticks)
        print("✅ Tick数据已写入")
        
        # 验证写入结果
        print("验证写入结果...")
        written_instruments = catalog.instruments()
        written_ticks = catalog.quote_ticks()
        
        print(f"Catalog中的工具数量: {len(written_instruments)}")
        print(f"Catalog中的tick数量: {len(written_ticks)}")
        
        if written_ticks:
            print(f"第一个tick: {written_ticks[0]}")
            print(f"最后一个tick: {written_ticks[-1]}")
        
        print("✅ 数据转换完成！")
        
    except Exception as e:
        print(f"❌ 错误: {e}")
        print("详细错误信息:")
        traceback.print_exc()


def main():
    """主函数"""
    print("修复版本的hist_data_to_catalog.py")
    print("=" * 50)
    
    # 设置路径
    catalog_path = Path("catalog")
    catalog_path.mkdir(exist_ok=True)
    
    # 使用本地CSV文件
    csv_file = "DAT_ASCII_EURUSD_T_202001.csv"
    
    if not Path(csv_file).exists():
        print(f"❌ 文件不存在: {csv_file}")
        return
    
    # 处理数据
    load_fx_hist_data_fixed(
        filename=csv_file,
        currency="EUR/USD",
        catalog_path=catalog_path,
    )


if __name__ == "__main__":
    main() 