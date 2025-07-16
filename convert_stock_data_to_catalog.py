#!/usr/bin/env python3
"""
通用股票/ETF数据转换脚本
支持多种CSV格式转换为Nautilus Trader catalog格式
"""

import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Any
from decimal import Decimal

from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.persistence.wranglers import QuoteTickDataWrangler, BarDataWrangler
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import BarAggregation
from nautilus_trader.model.enums import PriceType
from nautilus_trader.test_kit.providers import TestInstrumentProvider


class StockDataConverter:
    """股票/ETF数据转换器"""
    
    def __init__(self, catalog_path: str = "catalog"):
        self.catalog_path = Path(catalog_path)
        self.catalog_path.mkdir(exist_ok=True)
        self.catalog = ParquetDataCatalog(self.catalog_path)
    
    def create_equity_instrument(self, symbol: str, venue: str = "XNAS") -> Any:
        """创建股票/ETF工具对象"""
        return TestInstrumentProvider.equity(symbol=symbol, venue=venue)
    
    def detect_data_format(self, csv_file: str) -> Dict[str, Any]:
        """自动检测CSV数据格式"""
        df = pd.read_csv(csv_file, nrows=10)
        columns = df.columns.tolist()
        
        # 检测时间列
        time_columns = [col for col in columns if any(keyword in col.lower() 
                        for keyword in ['time', 'date', 'timestamp'])]
        
        # 检测价格列
        price_columns = [col for col in columns if any(keyword in col.lower() 
                         for keyword in ['price', 'bid', 'ask', 'last'])]
        
        # 检测OHLC列
        ohlc_columns = [col for col in columns if col.lower() in ['open', 'high', 'low', 'close']]
        
        # 检测成交量列
        volume_columns = [col for col in columns if any(keyword in col.lower() 
                        for keyword in ['volume', 'size', 'qty'])]
        
        return {
            'time_column': time_columns[0] if time_columns else None,
            'price_columns': price_columns,
            'ohlc_columns': ohlc_columns,
            'volume_columns': volume_columns[0] if volume_columns else None,
            'is_tick_data': len(ohlc_columns) == 0 and len(price_columns) > 0,
            'is_bar_data': len(ohlc_columns) >= 4,
            'separator': self._detect_separator(csv_file),
            'sample_data': df.head(3)
        }
    
    def _detect_separator(self, csv_file: str) -> str:
        """检测CSV分隔符"""
        with open(csv_file, 'r') as f:
            first_line = f.readline()
            if ',' in first_line:
                return ','
            elif ';' in first_line:
                return ';'
            elif '\t' in first_line:
                return '\t'
            else:
                return ','
    
    def convert_tick_data(self, 
                         csv_file: str, 
                         symbol: str, 
                         venue: str = "XNAS",
                         time_column: Optional[str] = None,
                         bid_column: Optional[str] = None,
                         ask_column: Optional[str] = None,
                         volume_column: Optional[str] = None,
                         time_format: str = "auto") -> None:
        """转换tick数据"""
        
        # 创建工具对象
        instrument = self.create_equity_instrument(symbol, venue)
        
        # 检测格式
        if not all([time_column, bid_column, ask_column]):
            format_info = self.detect_data_format(csv_file)
            time_column = time_column or format_info['time_column']
            price_cols = format_info['price_columns']
            bid_column = bid_column or (price_cols[0] if len(price_cols) >= 1 else 'bid')
            ask_column = ask_column or (price_cols[1] if len(price_cols) >= 2 else 'ask')
            volume_column = volume_column or format_info['volume_columns']
        
        # 读取数据
        separator = self._detect_separator(csv_file)
        df = pd.read_csv(csv_file, sep=separator)
        
        # 重命名列
        column_mapping = {
            time_column: 'timestamp',
            bid_column: 'bid_price',
            ask_column: 'ask_price'
        }
        if volume_column:
            column_mapping[volume_column] = 'size'
        
        df = df.rename(columns=column_mapping)
        
        # 处理时间列
        if time_format == "auto":
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        else:
            df['timestamp'] = pd.to_datetime(df['timestamp'], format=time_format)
        
        # 设置索引
        df = df.set_index('timestamp')
        
        # 确保必要的列存在
        required_columns = ['bid_price', 'ask_price']
        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"缺少必要的列: {col}")
        
        # 添加成交量列（如果不存在）
        if 'size' not in df.columns:
            df['size'] = 0
        
        print(f"处理tick数据: {len(df)} 条记录")
        print(f"数据范围: {df.index.min()} 到 {df.index.max()}")
        print(f"列: {df.columns.tolist()}")
        
        # 转换为tick对象
        wrangler = QuoteTickDataWrangler(instrument)
        ticks = wrangler.process(df)
        
        # 写入catalog
        self.catalog.write_data([instrument])
        self.catalog.write_data(ticks)
        
        print(f"成功转换 {len(ticks)} 条tick数据到 catalog")
    
    def convert_bar_data(self,
                        csv_file: str,
                        symbol: str,
                        venue: str = "XNAS",
                        time_column: Optional[str] = None,
                        open_column: Optional[str] = None,
                        high_column: Optional[str] = None,
                        low_column: Optional[str] = None,
                        close_column: Optional[str] = None,
                        volume_column: Optional[str] = None,
                        time_format: str = "auto",
                        bar_aggregation: BarAggregation = BarAggregation.MINUTE,
                        price_type: PriceType = PriceType.LAST) -> None:
        """转换K线数据"""
        
        # 创建工具对象
        instrument = self.create_equity_instrument(symbol, venue)
        
        # 检测格式
        if not all([time_column, open_column, high_column, low_column, close_column]):
            format_info = self.detect_data_format(csv_file)
            time_column = time_column or format_info['time_column']
            ohlc_cols = format_info['ohlc_columns']
            
            if len(ohlc_cols) >= 4:
                open_column = open_column or 'open'
                high_column = high_column or 'high'
                low_column = low_column or 'low'
                close_column = close_column or 'close'
            else:
                raise ValueError("CSV文件缺少OHLC列")
            
            volume_column = volume_column or format_info['volume_columns']
        
        # 读取数据
        separator = self._detect_separator(csv_file)
        df = pd.read_csv(csv_file, sep=separator)
        
        # 重命名列
        column_mapping = {
            time_column: 'timestamp',
            open_column: 'open',
            high_column: 'high',
            low_column: 'low',
            close_column: 'close'
        }
        if volume_column:
            column_mapping[volume_column] = 'volume'
        
        df = df.rename(columns=column_mapping)
        
        # 处理时间列
        if time_format == "auto":
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        else:
            df['timestamp'] = pd.to_datetime(df['timestamp'], format=time_format)
        
        # 设置索引
        df = df.set_index('timestamp')
        
        # 确保必要的列存在
        required_columns = ['open', 'high', 'low', 'close']
        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"缺少必要的列: {col}")
        
        # 添加成交量列（如果不存在）
        if 'volume' not in df.columns:
            df['volume'] = 0
        
        print(f"处理K线数据: {len(df)} 条记录")
        print(f"数据范围: {df.index.min()} 到 {df.index.max()}")
        print(f"列: {df.columns.tolist()}")
        
        # 创建BarType
        bar_type = BarType(
            instrument_id=instrument.id,
            bar_aggregation=bar_aggregation,
            price_type=price_type
        )
        
        # 转换为bar对象
        wrangler = BarDataWrangler(bar_type, instrument)
        bars = wrangler.process(df)
        
        # 写入catalog
        self.catalog.write_data([instrument])
        self.catalog.write_data(bars)
        
        print(f"成功转换 {len(bars)} 条K线数据到 catalog")
    
    def auto_convert(self, csv_file: str, symbol: str, venue: str = "XNAS") -> None:
        """自动检测并转换数据"""
        format_info = self.detect_data_format(csv_file)
        
        print(f"检测到数据格式:")
        print(f"  时间列: {format_info['time_column']}")
        print(f"  价格列: {format_info['price_columns']}")
        print(f"  OHLC列: {format_info['ohlc_columns']}")
        print(f"  成交量列: {format_info['volume_columns']}")
        print(f"  数据类型: {'Tick数据' if format_info['is_tick_data'] else 'K线数据'}")
        print(f"  分隔符: '{format_info['separator']}'")
        
        if format_info['is_tick_data']:
            self.convert_tick_data(csv_file, symbol, venue)
        elif format_info['is_bar_data']:
            self.convert_bar_data(csv_file, symbol, venue)
        else:
            raise ValueError("无法确定数据类型，请手动指定转换方法")


def main():
    """示例用法"""
    converter = StockDataConverter()
    
    # 示例1: 自动转换股票数据
    # converter.auto_convert("AAPL_data.csv", "AAPL", "XNAS")
    
    # 示例2: 手动转换tick数据
    # converter.convert_tick_data(
    #     csv_file="stock_ticks.csv",
    #     symbol="TSLA",
    #     venue="XNAS",
    #     time_column="timestamp",
    #     bid_column="bid",
    #     ask_column="ask",
    #     volume_column="size"
    # )
    
    # 示例3: 手动转换K线数据
    # converter.convert_bar_data(
    #     csv_file="stock_bars.csv", 
    #     symbol="SPY",
    #     venue="XNAS",
    #     time_column="Date",
    #     open_column="Open",
    #     high_column="High", 
    #     low_column="Low",
    #     close_column="Close",
    #     volume_column="Volume"
    # )
    
    print("请根据你的数据格式调用相应的方法")


if __name__ == "__main__":
    main() 