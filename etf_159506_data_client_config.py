#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
159506 ETF数据客户端配置
符合NautilusTrader架构的自定义数据客户端配置
"""

from nautilus_trader.live.config import LiveDataClientConfig
from nautilus_trader.config import InstrumentProviderConfig, RoutingConfig
from nautilus_trader.model.identifiers import Venue
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class ETF159506DataClientConfig(LiveDataClientConfig, frozen=True):
    """159506 ETF数据客户端配置"""
    
    venue: Venue = Venue("SZSE")
    handle_revised_bars: bool = False
    instrument_provider: InstrumentProviderConfig = InstrumentProviderConfig()
    routing: RoutingConfig = RoutingConfig()


def create_etf_159506_data_client_config() -> ETF159506DataClientConfig:
    """创建159506 ETF数据客户端配置"""
    return ETF159506DataClientConfig(
        venue=Venue("SZSE"),
        handle_revised_bars=False,
        instrument_provider=InstrumentProviderConfig(),
        routing=RoutingConfig(),
    )
