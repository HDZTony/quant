#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
159506 ETF 自定义数据类
基于NautilusTrader Actor-Based Data发布/订阅模式
参考官方示例: examples/backtest/example_10_messaging_with_actor_data
"""

from nautilus_trader.core.data import Data
from nautilus_trader.model.custom import customdataclass
from nautilus_trader.model.data import DataType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.model.data import BarType
from typing import Optional


class ETF159506TickData(Data):
    """
    159506 ETF Tick数据类
    
    这个类继承自Data类，用于使用Actor/Strategy的publish/subscribe方法。
    Data类继承自动提供：
    - `ts_event` 属性：用于回测中的正确数据排序
    - `ts_init` 属性：用于初始化时间跟踪
    
    由于不使用@customdataclass装饰器，可以包含任何Python类型的属性。
    这适用于不需要序列化、节点间传输或持久化的策略数据。
    """
    
    def __init__(
        self,
        instrument_id: InstrumentId,
        price: Price,
        volume: Quantity,
        timestamp: int,
        ts_event: int,
        ts_init: int,
    ):
        self.instrument_id = instrument_id
        self.price = price
        self.volume = volume
        self.timestamp = timestamp
        super().__init__(ts_event=ts_event, ts_init=ts_init)
    
    def __str__(self) -> str:
        return (f"ETF159506TickData("
                f"instrument_id={self.instrument_id}, "
                f"price={self.price}, "
                f"volume={self.volume}, "
                f"timestamp={self.timestamp})")


@customdataclass
class ETF159506TickDataSerializable(Data):
    """
    159506 ETF Tick数据类（可序列化版本）
    
    这个类使用@customdataclass装饰器，添加了序列化功能，支持：
    - 数据目录系统中的数据持久化
    - 不同节点间的数据传输
    - 自动序列化方法：to_dict(), from_dict(), to_bytes(), to_arrow()
    
    注意：使用@customdataclass时，属性必须是支持的类型：
    - InstrumentId
    - 基本类型：str, bool, float, int, bytes, ndarray
    """
    
    instrument_id: InstrumentId
    price: float  # 使用float而不是Price对象
    volume: float  # 使用float而不是Quantity对象
    timestamp: int
    
    def __str__(self) -> str:
        return (f"ETF159506TickDataSerializable("
                f"instrument_id={self.instrument_id}, "
                f"price={self.price}, "
                f"volume={self.volume}, "
                f"timestamp={self.timestamp})")


class ETF159506BarData(Data):
    """
    159506 ETF K线数据类
    """
    
    def __init__(
        self,
        bar_type: BarType,
        open_price: Price,
        high_price: Price,
        low_price: Price,
        close_price: Price,
        volume: Quantity,
        ts_event: int,
        ts_init: int,
    ):
        self.bar_type = bar_type
        self.open_price = open_price
        self.high_price = high_price
        self.low_price = low_price
        self.close_price = close_price
        self.volume = volume
        super().__init__(ts_event=ts_event, ts_init=ts_init)
    
    def __str__(self) -> str:
        return (f"ETF159506BarData("
                f"bar_type={self.bar_type}, "
                f"open={self.open_price}, "
                f"high={self.high_price}, "
                f"low={self.low_price}, "
                f"close={self.close_price}, "
                f"volume={self.volume})")


@customdataclass
class ETF159506BarDataSerializable(Data):
    """
    159506 ETF K线数据类（可序列化版本）
    """
    
    bar_type_str: str  # 使用字符串而不是BarType对象
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float
    
    def __str__(self) -> str:
        return (f"ETF159506BarDataSerializable("
                f"bar_type={self.bar_type_str}, "
                f"open={self.open_price}, "
                f"high={self.high_price}, "
                f"low={self.low_price}, "
                f"close={self.close_price}, "
                f"volume={self.volume})")


class ETF159506MarketStatusData(Data):
    """
    159506 ETF 市场状态数据类
    """
    
    def __init__(
        self,
        instrument_id: InstrumentId,
        market_status: str,  # "OPEN", "CLOSED", "PRE_OPEN", "POST_CLOSE"
        trading_volume: float,
        last_price: Optional[Price],
        ts_event: int,
        ts_init: int,
    ):
        self.instrument_id = instrument_id
        self.market_status = market_status
        self.trading_volume = trading_volume
        self.last_price = last_price
        super().__init__(ts_event=ts_event, ts_init=ts_init)
    
    def __str__(self) -> str:
        return (f"ETF159506MarketStatusData("
                f"instrument_id={self.instrument_id}, "
                f"market_status={self.market_status}, "
                f"trading_volume={self.trading_volume}, "
                f"last_price={self.last_price})")





# 便捷函数
def create_tick_data(
    instrument_id: InstrumentId,
    price: Price,
    volume: Quantity,
    timestamp: int,
    ts_event: int,
    ts_init: int,
) -> ETF159506TickData:
    """创建ETF159506TickData实例"""
    return ETF159506TickData(
        instrument_id=instrument_id,
        price=price,
        volume=volume,
        timestamp=timestamp,
        ts_event=ts_event,
        ts_init=ts_init,
    )


def create_tick_data_serializable(
    instrument_id: InstrumentId,
    price: float,
    volume: float,
    timestamp: int,
    ts_event: int,
    ts_init: int,
) -> ETF159506TickDataSerializable:
    """创建ETF159506TickDataSerializable实例"""
    return ETF159506TickDataSerializable(
        instrument_id=instrument_id,
        price=price,
        volume=volume,
        timestamp=timestamp,
        ts_event=ts_event,
        ts_init=ts_init,
    )


def create_bar_data(
    bar_type: BarType,
    open_price: Price,
    high_price: Price,
    low_price: Price,
    close_price: Price,
    volume: Quantity,
    ts_event: int,
    ts_init: int,
) -> ETF159506BarData:
    """创建ETF159506BarData实例"""
    return ETF159506BarData(
        bar_type=bar_type,
        open_price=open_price,
        high_price=high_price,
        low_price=low_price,
        close_price=close_price,
        volume=volume,
        ts_event=ts_event,
        ts_init=ts_init,
    )

def create_market_status_data(
    instrument_id: InstrumentId,
    market_status: str,
    trading_volume: float,
    last_price: Optional[Price],
    ts_event: int,
    ts_init: int,
) -> ETF159506MarketStatusData:
    """创建ETF159506MarketStatusData实例"""
    return ETF159506MarketStatusData(
        instrument_id=instrument_id,
        market_status=market_status,
        trading_volume=trading_volume,
        last_price=last_price,
        ts_event=ts_event,
        ts_init=ts_init,
    )
