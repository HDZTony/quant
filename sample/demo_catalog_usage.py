#!/usr/bin/env python3
"""
Nautilus Trader 官方快速入门完整实现
基于官方文档: https://nautilustrader.io/docs/latest/getting_started/quickstart/
包含完整的 MACD 策略和高级回测配置
"""

from pathlib import Path
from decimal import Decimal

from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.backtest.node import BacktestNode
from nautilus_trader.backtest.node import BacktestDataConfig
from nautilus_trader.backtest.node import BacktestEngineConfig
from nautilus_trader.backtest.node import BacktestRunConfig
from nautilus_trader.backtest.node import BacktestVenueConfig
from nautilus_trader.backtest.results import BacktestResult
from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import ImportableStrategyConfig
from nautilus_trader.model.identifiers import TraderId, Venue
from nautilus_trader.model.enums import AccountType, OmsType
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.objects import Money
from nautilus_trader.model import QuoteTick

# 导入 MACD 策略
from macd_strategy import MACDStrategy, MACDConfig


def demo_catalog_loading():
    """演示catalog数据加载"""
    print("=== Catalog数据加载演示 ===")
    
    # 加载catalog
    catalog = ParquetDataCatalog(Path("catalog"))
    
    # 获取工具信息
    instruments = catalog.instruments()
    print(f"可用工具: {len(instruments)} 个")
    for instrument in instruments:
        print(f"  - {instrument.id}")
        print(f"    基础货币: {instrument.base_currency}")
        print(f"    报价货币: {instrument.quote_currency}")
        print(f"    最小价格变动: {instrument.price_precision}")
    
    # 获取tick数据
    ticks = catalog.quote_ticks()
    print(f"\nTick数据: {len(ticks)} 条记录")
    if ticks:
        print(f"时间范围: {ticks[0].ts_init} 到 {ticks[-1].ts_init}")
        print(f"第一个tick: {ticks[0]}")
        print(f"最后一个tick: {ticks[-1]}")
    
    return catalog, instruments, ticks


def demo_simple_backtest(catalog, instruments, ticks):
    """演示简单回测（无策略）"""
    print("\n=== 简单回测演示（无策略） ===")
    
    # 创建回测引擎
    config = BacktestEngineConfig(
        trader_id=TraderId("DEMO-001"),
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
    
    # 添加工具
    if instruments:
        engine.add_instrument(instruments[0])
        print(f"添加工具: {instruments[0].id}")
    
    # 添加数据
    if ticks:
        # 只使用前1000条tick数据进行快速演示
        demo_ticks = ticks[:1000]
        engine.add_data(demo_ticks)
        print(f"添加 {len(demo_ticks)} 条tick数据")
    
    # 运行回测
    print("运行回测...")
    engine.run()
    
    # 显示结果
    print("\n=== 回测结果 ===")
    for venue in engine.list_venues():
        account = engine.portfolio.account(venue)
        if account:
            print(f"账户余额: {account.balance_total()}")
            print(f"可用余额: {account.balance_free()}")
    
    # 清理资源
    engine.dispose()
    print("✅ 简单回测完成")


def demo_official_quickstart(catalog, instruments):
    """演示官方快速入门完整功能"""
    print("\n" + "="*60)
    print("=== 官方快速入门完整实现 ===")
    print("="*60)
    
    if not instruments:
        print("❌ 没有可用的工具")
        return
    
    instrument = instruments[0]
    print(f"使用工具: {instrument.id}")
    
    # 1. 配置交易场所 (Venue)
    print("\n1. 配置交易场所...")
    venue = BacktestVenueConfig(
        name="SIM",
        oms_type="NETTING",
        account_type="MARGIN",
        base_currency="USD",
        starting_balances=["1_000_000 USD"]
    )
    print(f"✅ 交易场所配置完成: {venue.name}")
    
    # 2. 配置数据
    print("\n2. 配置数据...")
    data = BacktestDataConfig(
        catalog_path=str(catalog.path),
        data_cls=QuoteTick,
        instrument_id=instrument.id,
        end_time="2020-01-10",  # 限制数据范围
    )
    print(f"✅ 数据配置完成: {data.instrument_id}")
    
    # 3. 配置引擎和策略
    print("\n3. 配置引擎和策略...")
    engine = BacktestEngineConfig(
        strategies=[
            ImportableStrategyConfig(
                strategy_path="macd_strategy:MACDStrategy",
                config_path="macd_strategy:MACDConfig",
                config={
                    "instrument_id": instrument.id,
                    "fast_period": 12,
                    "slow_period": 26,
                    "trade_size": 1_000_000,
                    "entry_threshold": 0.00010,
                },
            )
        ],
        logging=LoggingConfig(log_level="INFO"),
    )
    print("✅ 引擎和策略配置完成")
    
    # 4. 创建回测配置
    print("\n4. 创建回测配置...")
    config = BacktestRunConfig(
        engine=engine,
        venues=[venue],
        data=[data],
    )
    print("✅ 回测配置创建完成")
    
    # 5. 运行回测
    print("\n5. 运行回测...")
    node = BacktestNode(configs=[config])
    results: list[BacktestResult] = node.run()
    
    if results:
        result = results[0]
        print(f"✅ 回测完成！")
        print(f"   回测ID: {result.run_id}")
        print(f"   运行开始: {result.run_started}")
        print(f"   运行结束: {result.run_finished}")
        print(f"   回测开始: {result.backtest_start}")
        print(f"   回测结束: {result.backtest_end}")
        print(f"   总事件数: {result.total_events}")
        print(f"   总订单数: {result.total_orders}")
        print(f"   总持仓数: {result.total_positions}")
        print(f"   迭代次数: {result.iterations}")
        print(f"   运行时间: {result.elapsed_time:.2f} 秒")
        
        # 6. 分析结果
        print("\n6. 分析结果...")
        engine_instance = node.get_engine(config.id)
        
        # 生成详细报告
        print("\n=== 详细报告 ===")
        
        # 订单报告
        orders_report = engine_instance.trader.generate_orders_report()
        if not orders_report.empty:
            print(f"\n📊 订单报告 (前5条):")
            print(orders_report.head())
        else:
            print("\n📊 订单报告: 无订单")
        
        # 持仓报告
        positions_report = engine_instance.trader.generate_positions_report()
        if not positions_report.empty:
            print(f"\n📊 持仓报告 (前5条):")
            print(positions_report.head())
        else:
            print("\n📊 持仓报告: 无持仓")
        
        # 账户报告
        account_report = engine_instance.trader.generate_account_report(Venue("SIM"))
        if not account_report.empty:
            print(f"\n📊 账户报告:")
            print(account_report)
        else:
            print("\n📊 账户报告: 无数据")
        
        # 填充报告
        fills_report = engine_instance.trader.generate_fills_report()
        if not fills_report.empty:
            print(f"\n📊 填充报告 (前5条):")
            print(fills_report.head())
        else:
            print("\n📊 填充报告: 无填充")
        
        print("\n✅ 官方快速入门演示完成！")
    else:
        print("❌ 回测运行失败")


def demo_data_analysis(catalog):
    """演示数据分析"""
    print("\n=== 数据分析演示 ===")
    
    ticks = catalog.quote_ticks()
    if not ticks:
        print("❌ 没有tick数据")
        return
    
    # 基本统计
    print(f"数据统计:")
    print(f"  总记录数: {len(ticks)}")
    print(f"  时间跨度: {ticks[-1].ts_init - ticks[0].ts_init} 纳秒")
    
    # 价格统计
    bid_prices = [tick.bid_price for tick in ticks[:1000]]
    ask_prices = [tick.ask_price for tick in ticks[:1000]]
    
    print(f"  价格统计 (前1000条):")
    print(f"    Bid价格范围: {min(bid_prices)} - {max(bid_prices)}")
    print(f"    Ask价格范围: {min(ask_prices)} - {max(ask_prices)}")
    print(f"    平均价差: {sum(ask - bid for ask, bid in zip(ask_prices, bid_prices)) / len(bid_prices)}")
    
    # 时间分布
    print(f"  时间分布:")
    print(f"    开始时间: {ticks[0].ts_init}")
    print(f"    结束时间: {ticks[-1].ts_init}")
    
    print("✅ 数据分析完成")


def main():
    """主函数"""
    print("Nautilus Trader 官方快速入门完整实现")
    print("=" * 60)
    
    try:
        # 1. 加载catalog数据
        catalog, instruments, ticks = demo_catalog_loading()
        
        # 2. 数据分析
        demo_data_analysis(catalog)
        
        # 3. 简单回测（无策略）
        demo_simple_backtest(catalog, instruments, ticks)
        
        # 4. 官方快速入门完整功能（包含MACD策略）
        demo_official_quickstart(catalog, instruments)
        
        print("\n🎉 完整演示完成！")
        print("\n关键要点:")
        print("• Catalog成功存储了工具信息和tick数据")
        print("• 数据可以用于回测和策略研究")
        print("• Nautilus Trader提供了完整的回测框架")
        print("• 支持高级API配置和策略导入")
        print("• 包含完整的报告生成功能")
        
    except Exception as e:
        print(f"❌ 演示过程中出错: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main() 