#!/usr/bin/env python3
"""
股票数据转换使用示例
展示如何处理不同格式的股票/ETF数据
"""

from convert_stock_data_to_catalog import StockDataConverter
from nautilus_trader.model.enums import BarAggregation, PriceType


def example_yahoo_finance_format():
    """示例：处理Yahoo Finance格式的数据"""
    print("=== Yahoo Finance格式示例 ===")
    
    # Yahoo Finance格式：Date,Open,High,Low,Close,Adj Close,Volume
    yahoo_csv_content = """Date,Open,High,Low,Close,Adj Close,Volume
2024-01-02,150.20,151.30,149.15,150.25,150.25,1000000
2024-01-03,150.30,152.40,150.20,151.80,151.80,1200000
2024-01-04,151.90,153.50,151.50,152.90,152.90,1100000
2024-01-05,153.00,154.20,152.80,153.50,153.50,950000"""
    
    # 保存示例数据
    with open("yahoo_sample.csv", "w") as f:
        f.write(yahoo_csv_content)
    
    converter = StockDataConverter()
    
    # 自动转换
    converter.convert_bar_data(
        csv_file="yahoo_sample.csv",
        symbol="AAPL",
        venue="XNAS",
        time_column="Date",
        open_column="Open", 
        high_column="High",
        low_column="Low",
        close_column="Close",
        volume_column="Volume",
        bar_aggregation=BarAggregation.DAY,
        price_type=PriceType.LAST
    )


def example_alpha_vantage_format():
    """示例：处理Alpha Vantage格式的数据"""
    print("=== Alpha Vantage格式示例 ===")
    
    # Alpha Vantage格式：timestamp,open,high,low,close,volume
    alpha_csv_content = """timestamp,open,high,low,close,volume
2024-01-02 09:30:00,150.20,151.30,149.15,150.25,1000000
2024-01-02 09:31:00,150.30,151.40,150.20,151.10,500000
2024-01-02 09:32:00,151.15,152.20,151.00,151.80,600000
2024-01-02 09:33:00,151.85,152.50,151.70,152.30,450000"""
    
    # 保存示例数据
    with open("alpha_sample.csv", "w") as f:
        f.write(alpha_csv_content)
    
    converter = StockDataConverter()
    
    # 自动转换
    converter.convert_bar_data(
        csv_file="alpha_sample.csv",
        symbol="TSLA", 
        venue="XNAS",
        time_column="timestamp",
        open_column="open",
        high_column="high", 
        low_column="low",
        close_column="close",
        volume_column="volume",
        bar_aggregation=BarAggregation.MINUTE,
        price_type=PriceType.LAST
    )


def example_tick_data_format():
    """示例：处理tick数据格式"""
    print("=== Tick数据格式示例 ===")
    
    # Tick数据格式：timestamp,bid,ask,size
    tick_csv_content = """timestamp,bid,ask,size
2024-01-02 09:30:00.123,150.25,150.26,100
2024-01-02 09:30:00.456,150.24,150.27,200
2024-01-02 09:30:00.789,150.26,150.28,150
2024-01-02 09:30:01.012,150.25,150.29,300"""
    
    # 保存示例数据
    with open("tick_sample.csv", "w") as f:
        f.write(tick_csv_content)
    
    converter = StockDataConverter()
    
    # 自动转换
    converter.convert_tick_data(
        csv_file="tick_sample.csv",
        symbol="SPY",
        venue="XNAS", 
        time_column="timestamp",
        bid_column="bid",
        ask_column="ask",
        volume_column="size"
    )


def example_auto_detection():
    """示例：自动检测数据格式"""
    print("=== 自动检测格式示例 ===")
    
    # 混合格式数据
    mixed_csv_content = """Date,Open,High,Low,Close,Volume
2024-01-02,150.20,151.30,149.15,150.25,1000000
2024-01-03,150.30,152.40,150.20,151.80,1200000
2024-01-04,151.90,153.50,151.50,152.90,1100000"""
    
    # 保存示例数据
    with open("mixed_sample.csv", "w") as f:
        f.write(mixed_csv_content)
    
    converter = StockDataConverter()
    
    # 自动检测并转换
    converter.auto_convert("mixed_sample.csv", "QQQ", "XNAS")


def example_custom_format():
    """示例：处理自定义格式数据"""
    print("=== 自定义格式示例 ===")
    
    # 自定义格式：使用分号分隔符
    custom_csv_content = """Time;Price;Volume;Type
2024-01-02 09:30:00;150.25;1000;BUY
2024-01-02 09:30:05;150.26;500;SELL
2024-01-02 09:30:10;150.24;750;BUY
2024-01-02 09:30:15;150.27;1200;SELL"""
    
    # 保存示例数据
    with open("custom_sample.csv", "w") as f:
        f.write(custom_csv_content)
    
    converter = StockDataConverter()
    
    # 手动指定列映射（因为这是交易数据，不是标准OHLC）
    # 这里需要根据实际数据格式调整
    print("注意：自定义格式需要根据实际数据结构调整列映射")


def main():
    """运行所有示例"""
    print("股票数据转换示例")
    print("=" * 50)
    
    try:
        # 运行各种格式的示例
        example_yahoo_finance_format()
        print()
        
        example_alpha_vantage_format() 
        print()
        
        example_tick_data_format()
        print()
        
        example_auto_detection()
        print()
        
        example_custom_format()
        print()
        
        print("所有示例完成！")
        print("转换后的数据已保存到 catalog/ 目录")
        
    except Exception as e:
        print(f"示例运行出错: {e}")
        print("请检查数据格式和参数设置")


if __name__ == "__main__":
    main() 