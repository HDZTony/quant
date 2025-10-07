#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
159506 港股通医疗ETF富国工具定义
基于NautilusTrader标准架构设计
"""

from decimal import Decimal
from typing import Optional

from nautilus_trader.model.currencies import CNY
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue
from nautilus_trader.model.instruments import Equity
from nautilus_trader.model.objects import Money, Price, Quantity


def create_etf_159506_instrument(
    symbol: str = "159506",
    venue: str = "SZSE",
    currency: str = "CNY",
    price_precision: int = 3,
    price_increment: str = "0.001",
    lot_size: int = 100,
    isin: Optional[str] = None,
    ts_event: int = 0,
    ts_init: int = 0,
) -> Equity:
    """
    创建159506 港股通医疗ETF富国工具
    
    根据NautilusTrader标准架构，使用Equity类表示股票/ETF工具
    参考官方文档：https://nautilustrader.io/docs/latest/concepts/instruments/
    参考官方示例：TestInstrumentProvider.equity()
    
    Parameters
    ----------
    symbol : str, default "159506"
        交易代码
    venue : str, default "SZSE"
        交易场所
    currency : str, default "CNY"
        货币代码
    price_precision : int, default 3
        价格精度（小数位数）
    price_increment : str, default "0.001"
        最小价格变动单位
    lot_size : int, default 100
        标准手数
    isin : Optional[str], default None
        ISIN代码
    ts_event : int, default 0
        事件时间戳
    ts_init : int, default 0
        初始化时间戳
    
    Returns
    -------
    Equity
        159506 港股通医疗ETF富国工具对象
    """
    
    # 创建工具ID
    instrument_id = InstrumentId(
        symbol=Symbol(symbol),
        venue=Venue(venue)
    )
    
    # 根据货币代码获取货币对象
    currency_obj = CNY if currency == "CNY" else CNY  # 可以根据需要扩展
    
    # 创建Equity工具
    # 参考官方TestInstrumentProvider.equity()的实现
    etf_instrument = Equity(
        instrument_id=instrument_id,
        raw_symbol=Symbol(symbol),
        currency=currency_obj,
        price_precision=price_precision,
        price_increment=Price.from_str(price_increment),
        lot_size=Quantity.from_int(lot_size),
        isin=isin,
        ts_event=ts_event,
        ts_init=ts_init,
    )
    
    return etf_instrument


def create_etf_159506_bar_type(
    symbol: str = "159506",
    venue: str = "SZSE",
    aggregation: str = "1-MINUTE",
    price_type: str = "LAST",
    source: str = "EXTERNAL"
) -> str:
    """
    创建159506 ETF的K线类型字符串
    
    Parameters
    ----------
    symbol : str, default "159506"
        交易代码
    venue : str, default "SZSE"
        交易场所
    aggregation : str, default "1-MINUTE"
        聚合周期
    price_type : str, default "LAST"
        价格类型
    source : str, default INTERNAL "EXTERNAL"
        数据源
    
    Returns
    -------
    str
        K线类型字符串，格式：{symbol}.{venue}-{aggregation}-{price_type}-{source}
    """
    return f"{symbol}.{venue}-{aggregation}-{price_type}-{source}"


def get_etf_159506_info(
    symbol: str = "159506",
    name: str = "港股通医疗ETF富国",
    exchange: str = "SZSE",
    currency: str = "CNY",
    tick_size: float = 0.001,
    lot_size: int = 100,
    price_precision: int = 3,
    description: str = "港股通医疗ETF富国交易型开放式指数证券投资基金"
) -> dict:
    """
    获取159506 ETF的基本信息
    
    Parameters
    ----------
    symbol : str, default "159506"
        交易代码
    name : str, default "港股通医疗ETF富国"
        基金名称
    exchange : str, default "SZSE"
        交易所
    currency : str, default "CNY"
        货币
    tick_size : float, default 0.001
        最小价格变动
    lot_size : int, default 100
        标准手数
    price_precision : int, default 3
        价格精度
    description : str, default "港股通医疗ETF富国交易型开放式指数证券投资基金"
        描述
    
    Returns
    -------
    dict
        ETF基本信息
    """
    return {
        "symbol": symbol,
        "name": name,
        "exchange": exchange,
        "currency": currency,
        "tick_size": tick_size,
        "lot_size": lot_size,
        "price_precision": price_precision,
        "description": description
    }


# 预设配置
def create_etf_159506_default() -> Equity:
    """
    创建159506 港股通医疗ETF富国的默认配置工具
    
    Returns
    -------
    Equity
        159506 港股通医疗ETF富国工具对象（默认配置）
    """
    return create_etf_159506_instrument()


def create_etf_159506_high_precision() -> Equity:
    """
    创建159506 港股通医疗ETF富国的高精度配置工具（用于精确交易）
    
    Returns
    -------
    Equity
        159506 港股通医疗ETF富国工具对象（高精度配置）
    """
    return create_etf_159506_instrument(
        price_precision=3,
        price_increment="0.001",
    )


def create_etf_159506_low_fee() -> Equity:
    """
    创建159506 港股通医疗ETF富国的低费用配置工具（用于高频交易）
    
    Returns
    -------
    Equity
        159506 港股通医疗ETF富国工具对象（低费用配置）
    """
    return create_etf_159506_instrument(
        price_precision=2,
        price_increment="0.01",
    )


# 使用示例
if __name__ == "__main__":
    print("159506 港股通医疗ETF富国工具创建示例")
    print("="*60)
    
    # 1. 默认配置
    print("\n1. 默认配置:")
    etf_default = create_etf_159506_default()
    print(f"   工具ID: {etf_default.id}")
    print(f"   价格精度: {etf_default.price_precision}")
    print(f"   最小价格变动: {etf_default.price_increment}")
    print(f"   标准手数: {etf_default.lot_size}")
    
    # 2. 高精度配置
    print("\n2. 高精度配置:")
    etf_high_precision = create_etf_159506_high_precision()
    print(f"   工具ID: {etf_high_precision.id}")
    print(f"   价格精度: {etf_high_precision.price_precision}")
    print(f"   最小价格变动: {etf_high_precision.price_increment}")
    
    # 3. 低费用配置
    print("\n3. 低费用配置:")
    etf_low_fee = create_etf_159506_low_fee()
    print(f"   工具ID: {etf_low_fee.id}")
    print(f"   价格精度: {etf_low_fee.price_precision}")
    print(f"   最小价格变动: {etf_low_fee.price_increment}")
    
    # 4. 自定义配置
    print("\n4. 自定义配置:")
    etf_custom = create_etf_159506_instrument(
        symbol="159506",
        venue="SZSE",
        price_precision=2,
        price_increment="0.01",
    )
    print(f"   工具ID: {etf_custom.id}")
    print(f"   价格精度: {etf_custom.price_precision}")
    print(f"   最小价格变动: {etf_custom.price_increment}")
    
    # 5. 测试价格和数量创建
    print("\n5. 价格和数量创建测试:")
    test_price = etf_default.make_price(1.234)
    test_quantity = etf_default.make_qty(1000)
    print(f"   测试价格: {test_price}")
    print(f"   测试数量: {test_quantity}")
    
    # 6. 获取K线类型
    bar_type_str = create_etf_159506_bar_type()
    print(f"\n6. K线类型: {bar_type_str}")
    
    # 7. 获取基本信息
    info = get_etf_159506_info()
    print(f"\n7. 基本信息: {info}")
    
    print("\n✅ 所有工具创建成功！")
    print("�� 可以根据实际需要调整参数配置") 