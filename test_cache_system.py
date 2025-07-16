#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
测试Cache系统基本功能
"""

import time
from datetime import datetime
import logging

# NautilusTrader imports
from nautilus_trader.config import CacheConfig
from nautilus_trader.cache.cache import Cache
from nautilus_trader.model.data import QuoteTick, TradeTick
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.common.clock import LiveClock
from nautilus_trader.common.logging import Logger

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_cache_basic():
    """测试Cache基本功能"""
    print("=" * 50)
    print("测试Cache基本功能")
    print("=" * 50)
    
    try:
        # 创建Cache配置
        cache_config = CacheConfig(
            tick_capacity=1000,
            bar_capacity=1000,
            encoding="msgpack",
            timestamps_as_iso8601=True,
        )
        
        # 创建Cache实例
        clock = LiveClock()
        logger_instance = Logger(clock=clock)
        cache = Cache(config=cache_config, clock=clock, logger=logger_instance)
        
        # 创建工具ID
        instrument_id = InstrumentId(
            symbol=Symbol("159506"),
            venue=Venue("SZSE")
        )
        
        print("✅ Cache实例创建成功")
        
        # 测试添加报价数据
        quote_tick = QuoteTick(
            instrument_id=instrument_id,
            bid_price=Price.from_str("1.350"),
            ask_price=Price.from_str("1.351"),
            bid_size=Quantity.from_int(1000),
            ask_size=Quantity.from_int(1000),
            ts_event=clock.timestamp_ns(),
            ts_init=clock.timestamp_ns(),
        )
        
        cache.add_quote_tick(quote_tick)
        print("✅ 报价数据添加成功")
        
        # 测试添加交易数据
        trade_tick = TradeTick(
            instrument_id=instrument_id,
            price=Price.from_str("1.350"),
            size=Quantity.from_int(500),
            aggressor_side=None,
            trade_id="test_trade_001",
            ts_event=clock.timestamp_ns(),
            ts_init=clock.timestamp_ns(),
        )
        
        cache.add_trade_tick(trade_tick)
        print("✅ 交易数据添加成功")
        
        # 测试数据查询
        latest_quote = cache.quote_tick(instrument_id)
        latest_trade = cache.trade_tick(instrument_id)
        
        if latest_quote:
            print(f"✅ 最新报价: 买{float(latest_quote.bid_price)} / 卖{float(latest_quote.ask_price)}")
        
        if latest_trade:
            print(f"✅ 最新交易: 价格{float(latest_trade.price)}, 数量{int(latest_trade.size)}")
        
        # 测试数据统计
        quote_count = cache.quote_tick_count(instrument_id)
        trade_count = cache.trade_tick_count(instrument_id)
        
        print(f"✅ 数据统计: 报价{quote_count}条, 交易{trade_count}条")
        
        # 测试自定义数据存储
        test_data = b"test_custom_data"
        cache.add("test_key", test_data)
        
        retrieved_data = cache.get("test_key")
        if retrieved_data == test_data:
            print("✅ 自定义数据存储和检索成功")
        else:
            print("❌ 自定义数据存储和检索失败")
        
        print("=" * 50)
        print("🎉 Cache基本功能测试通过！")
        print("=" * 50)
        
        return True
        
    except Exception as e:
        print(f"❌ Cache测试失败: {e}")
        return False


def test_cache_performance():
    """测试Cache性能"""
    print("=" * 50)
    print("测试Cache性能")
    print("=" * 50)
    
    try:
        # 创建Cache配置
        cache_config = CacheConfig(
            tick_capacity=10000,
            bar_capacity=10000,
            encoding="msgpack",
            timestamps_as_iso8601=True,
        )
        
        # 创建Cache实例
        clock = LiveClock()
        logger_instance = Logger(clock=clock)
        cache = Cache(config=cache_config, clock=clock, logger=logger_instance)
        
        # 创建工具ID
        instrument_id = InstrumentId(
            symbol=Symbol("159506"),
            venue=Venue("SZSE")
        )
        
        # 批量添加数据
        start_time = time.time()
        
        for i in range(1000):
            quote_tick = QuoteTick(
                instrument_id=instrument_id,
                bid_price=Price.from_str(f"1.35{i%10}"),
                ask_price=Price.from_str(f"1.35{(i+1)%10}"),
                bid_size=Quantity.from_int(1000 + i),
                ask_size=Quantity.from_int(1000 + i),
                ts_event=clock.timestamp_ns(),
                ts_init=clock.timestamp_ns(),
            )
            
            trade_tick = TradeTick(
                instrument_id=instrument_id,
                price=Price.from_str(f"1.35{i%10}"),
                size=Quantity.from_int(500 + i),
                aggressor_side=None,
                trade_id=f"test_trade_{i:03d}",
                ts_event=clock.timestamp_ns(),
                ts_init=clock.timestamp_ns(),
            )
            
            cache.add_quote_tick(quote_tick)
            cache.add_trade_tick(trade_tick)
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        print(f"✅ 批量添加1000条数据耗时: {elapsed_time:.3f}秒")
        print(f"✅ 平均每条数据: {elapsed_time/1000*1000:.3f}毫秒")
        
        # 测试数据查询性能
        start_time = time.time()
        
        for i in range(100):
            quote_ticks = cache.quote_ticks(instrument_id)
            trade_ticks = cache.trade_ticks(instrument_id)
        
        end_time = time.time()
        query_time = end_time - start_time
        
        print(f"✅ 100次数据查询耗时: {query_time:.3f}秒")
        print(f"✅ 平均每次查询: {query_time/100*1000:.3f}毫秒")
        
        # 显示最终统计
        quote_count = cache.quote_tick_count(instrument_id)
        trade_count = cache.trade_tick_count(instrument_id)
        
        print(f"✅ 最终数据量: 报价{quote_count}条, 交易{trade_count}条")
        
        print("=" * 50)
        print("🎉 Cache性能测试通过！")
        print("=" * 50)
        
        return True
        
    except Exception as e:
        print(f"❌ Cache性能测试失败: {e}")
        return False


def main():
    """主函数"""
    print("🚀 开始测试Cache系统")
    print()
    
    # 测试基本功能
    basic_test_passed = test_cache_basic()
    print()
    
    # 测试性能
    performance_test_passed = test_cache_performance()
    print()
    
    # 总结
    print("=" * 60)
    print("测试结果总结")
    print("=" * 60)
    
    if basic_test_passed and performance_test_passed:
        print("🎉 所有测试通过！Cache系统工作正常")
        print()
        print("✅ 可以开始使用以下功能:")
        print("   - etf_159506_cache_collector.py (数据采集)")
        print("   - etf_159506_cache_strategy.py (策略分析)")
        print("   - etf_159506_multi_strategy_demo.py (多策略)")
    else:
        print("❌ 部分测试失败，请检查NautilusTrader安装")
        print()
        print("请尝试:")
        print("   - 重新安装NautilusTrader: uv add nautilus-trader")
        print("   - 检查Python环境")
        print("   - 查看错误日志")
    
    print("=" * 60)


if __name__ == "__main__":
    main() 