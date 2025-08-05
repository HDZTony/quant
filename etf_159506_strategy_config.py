#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
159506 港股通医疗ETF富国策略配置文件
基于NautilusTrader标准架构设计
"""

from decimal import Decimal
from typing import Optional

from nautilus_trader.config import StrategyConfig
from nautilus_trader.config import PositiveFloat
from nautilus_trader.config import PositiveInt
from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import InstrumentId, Venue


class ETF159506Config(StrategyConfig, frozen=True):
    """
    159506 港股通医疗ETF富国策略配置
    
    基于NautilusTrader标准架构，参考官方EMACrossBracketConfig设计
    
    Parameters
    ----------
    instrument_id : InstrumentId
        交易工具ID
    bar_type : BarType
        K线类型
    trade_size : Decimal
        每次交易数量
    fast_ema_period : PositiveInt, default 10
        快速EMA周期
    slow_ema_period : PositiveInt, default 20
        慢速EMA周期
    volume_threshold : PositiveInt, default 500000
        成交量阈值
    stop_loss_pct : PositiveFloat, default 0.02
        止损百分比
    take_profit_pct : PositiveFloat, default 0.015
        止盈百分比
    max_daily_trades : PositiveInt, default 10
        每日最大交易次数
    lookback_period : PositiveInt, default 10
        回看周期
    price_threshold : PositiveFloat, default 0.003
        价格变化阈值
    emulation_trigger : str, default 'NO_TRIGGER'
        订单模拟触发器
    """
    
    instrument_id: InstrumentId
    bar_type: BarType
    venue: Venue
    trade_size: Decimal
    fast_ema_period: PositiveInt = 10
    slow_ema_period: PositiveInt = 20
    volume_threshold: PositiveInt = 500000
    stop_loss_pct: PositiveFloat = 0.02
    take_profit_pct: PositiveFloat = 0.015
    max_daily_trades: PositiveInt = 10
    lookback_period: PositiveInt = 10
    price_threshold: PositiveFloat = 0.003
    emulation_trigger: str = "NO_TRIGGER"
    
    # 背离检测参数
    dea_trend_period: PositiveInt = 5  # DEA趋势判断周期
    divergence_threshold: PositiveFloat = 0.0002  # 背离检测阈值
    advance_trading_bars: PositiveInt = 2  # 提前交易K线数
    confirmation_bars: PositiveInt = 3  # 确认K线数
    max_divergence_duration: PositiveInt = 10  # 最大背离持续时间（分钟） 