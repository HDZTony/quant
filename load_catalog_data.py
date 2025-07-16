#!/usr/bin/env python3
"""
加载catalog数据并演示回测
展示如何用Nautilus Trader API加载catalog数据进行回测/研究
"""

import pandas as pd
from pathlib import Path
from decimal import Decimal
import time

from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.config import BacktestEngineConfig, LoggingConfig
from nautilus_trader.model.identifiers import TraderId, Venue
from nautilus_trader.model.enums import AccountType, OmsType
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.objects import Money
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import BarAggregation, PriceType
from nautilus_trader.test_kit.providers import TestInstrumentProvider


def check_catalog_data():
    """检查catalog数据"""
    print("=== 检查Catalog数据 ===")
    
    catalog_path = Path("catalog")
    if not catalog_path.exists():
        print("❌ catalog目录不存在")
        return None
    
    catalog = ParquetDataCatalog(catalog_path)
    
    try:
        # 检查可用的数据
        print(f"Catalog路径: {catalog_path.absolute()}")
        
        # 列出所有可用的数据
        instruments = catalog.instruments()
        print(f"可用工具: {len(instruments)} 个")
        for instrument in instruments:
            print(f"  - {instrument.id}")
        
        # 检查tick数据
        tick_data = catalog.quote_ticks()
        if tick_data:
            print(f"Tick数据: {len(tick_data)} 条记录")
            print(f"时间范围: {tick_data[0].ts_init} 到 {tick_data[-1].ts_init}")
        else:
            print("❌ 没有找到tick数据")
        
        # 检查bar数据
        bar_data = catalog.bars()
        if bar_data:
            print(f"Bar数据: {len(bar_data)} 条记录")
        else:
            print("❌ 没有找到bar数据")
        
        return catalog
        
    except Exception as e:
        print(f"❌ 检查catalog数据时出错: {e}")
        return None


def create_simple_backtest_engine():
    """创建简单的回测引擎"""
    print("\n=== 创建回测引擎 ===")
    
    # 配置回测引擎
    config = BacktestEngineConfig(
        trader_id=TraderId("BACKTEST-001"),
        logging=LoggingConfig(log_level="INFO"),
    )
    
    engine = BacktestEngine(config=config)
    
    # 添加交易场所
    venue = Venue("SIM")
    engine.add_venue(
        venue=venue,
        oms_type=OmsType.NETTING,
        account_type=AccountType.MARGIN,
        starting_balances=[Money(100_000, USD)],
        base_currency=USD,
    )
    
    return engine, venue


def load_data_from_catalog(catalog, engine):
    """从catalog加载数据到回测引擎"""
    print("\n=== 从Catalog加载数据 ===")
    
    try:
        # 获取EUR/USD工具
        instruments = catalog.instruments()
        if not instruments:
            print("❌ 没有找到工具数据")
            return False
        
        eurusd_instrument = None
        for instrument in instruments:
            if "EUR/USD" in str(instrument.id):
                eurusd_instrument = instrument
                break
        
        if not eurusd_instrument:
            print("❌ 没有找到EUR/USD工具")
            return False
        
        print(f"找到工具: {eurusd_instrument.id}")
        engine.add_instrument(eurusd_instrument)
        
        # 加载tick数据
        ticks = catalog.quote_ticks()
        if ticks:
            print(f"加载 {len(ticks)} 条tick数据")
            engine.add_data(ticks)
            return True
        
        # 如果没有tick数据，尝试加载bar数据
        bars = catalog.bars()
        if bars:
            print(f"加载 {len(bars)} 条bar数据")
            engine.add_data(bars)
            return True
        
        print("❌ 没有找到任何数据")
        return False
        
    except Exception as e:
        print(f"❌ 加载数据时出错: {e}")
        return False


def run_simple_backtest(engine):
    """运行简单回测"""
    print("\n=== 运行回测 ===")
    
    try:
        # 运行回测
        engine.run()
        
        # 生成报告
        print("\n=== 回测结果 ===")
        
        # 账户报告
        for venue in engine.venues:
            account = engine.trader.account(venue)
            if account:
                print(f"账户余额: {account.balance_total()}")
                print(f"可用余额: {account.balance_free()}")
        
        # 订单报告
        orders = engine.trader.generate_orders_report()
        if not orders.empty:
            print(f"\n订单数量: {len(orders)}")
            print(orders.head())
        
        # 持仓报告
        positions = engine.trader.generate_positions_report()
        if not positions.empty:
            print(f"\n持仓数量: {len(positions)}")
            print(positions.head())
        
        print("\n✅ 回测完成")
        
    except Exception as e:
        print(f"❌ 回测运行时出错: {e}")


def analyze_catalog_data(catalog):
    """分析catalog数据"""
    print("\n=== 分析Catalog数据 ===")
    
    try:
        # 分析tick数据
        ticks = catalog.quote_ticks()
        if ticks:
            print(f"Tick数据分析:")
            print(f"  总记录数: {len(ticks)}")
            print(f"  时间范围: {ticks[0].ts_init} 到 {ticks[-1].ts_init}")
            
            # 统计bid/ask价格
            bid_prices = [tick.bid_price for tick in ticks[:1000]]  # 取前1000条
            ask_prices = [tick.ask_price for tick in ticks[:1000]]
            
            print(f"  Bid价格范围: {min(bid_prices)} - {max(bid_prices)}")
            print(f"  Ask价格范围: {min(ask_prices)} - {max(ask_prices)}")
            print(f"  平均价差: {sum(ask - bid for ask, bid in zip(ask_prices, bid_prices)) / len(bid_prices)}")
        
        # 分析bar数据
        bars = catalog.bars()
        if bars:
            print(f"\nBar数据分析:")
            print(f"  总记录数: {len(bars)}")
            print(f"  时间范围: {bars[0].ts_init} 到 {bars[-1].ts_init}")
            
            # 统计OHLC
            opens = [bar.open for bar in bars[:100]]  # 取前100条
            highs = [bar.high for bar in bars[:100]]
            lows = [bar.low for bar in bars[:100]]
            closes = [bar.close for bar in bars[:100]]
            
            print(f"  开盘价范围: {min(opens)} - {max(opens)}")
            print(f"  最高价范围: {min(highs)} - {max(highs)}")
            print(f"  最低价范围: {min(lows)} - {max(lows)}")
            print(f"  收盘价范围: {min(closes)} - {max(closes)}")
        
    except Exception as e:
        print(f"❌ 分析数据时出错: {e}")


def main():
    """主函数"""
    print("Catalog数据加载和回测演示")
    print("=" * 50)
    
    # 1. 检查catalog数据
    catalog = check_catalog_data()
    if not catalog:
        print("❌ 无法访问catalog数据，请先运行hist_data_to_catalog.py")
        return
    
    # 2. 分析数据
    analyze_catalog_data(catalog)
    
    # 3. 创建回测引擎
    engine, venue = create_simple_backtest_engine()
    
    # 4. 加载数据
    if not load_data_from_catalog(catalog, engine):
        print("❌ 无法加载数据到回测引擎")
        return
    
    # 5. 运行回测
    run_simple_backtest(engine)
    
    # 6. 清理资源
    engine.dispose()
    
    print("\n🎉 演示完成！")


if __name__ == "__main__":
    main() 