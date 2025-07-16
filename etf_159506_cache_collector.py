#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
159506 ETF基于NautilusTrader Cache的实时数据采集器
支持实时数据存储、策略回测、Redis持久化和多策略共享
"""

import json
import time
import threading
import requests
import websocket
import zlib
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from collections import deque
import os
from pathlib import Path
import pickle
from typing import Dict, List, Optional, Any
import logging

# NautilusTrader imports
from nautilus_trader.config import CacheConfig, DatabaseConfig
from nautilus_trader.cache.cache import Cache
from nautilus_trader.model.data import Bar, BarType, BarSpecification
from nautilus_trader.model.data import QuoteTick, TradeTick
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue, TradeId
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.model.enums import BarAggregation, PriceType, AssetClass, InstrumentClass, AggressorSide
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.objects import Currency
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import Logger
from decimal import Decimal

# 配置日志
logging.basicConfig(
    level=logging.INFO,  # 改回INFO级别，减少详细输出
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etf_159506_cache_collector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ETF159506CacheManager:
    """159506 ETF Cache管理器"""
    
    def __init__(self, use_redis: bool = True, redis_host: str = "localhost", redis_port: int = 6379):
        self.use_redis = use_redis
        self.redis_host = redis_host
        self.redis_port = redis_port
        
        # 创建Cache配置
        if use_redis:
            cache_config = CacheConfig(
                database=DatabaseConfig(
                    type="redis",
                    host=redis_host,
                    port=redis_port,
                    timeout=2,
                ),
                tick_capacity=100_000,  # 存储10万条tick
                bar_capacity=50_000,    # 存储5万根K线
                encoding="msgpack",
                timestamps_as_iso8601=True,
                use_trader_prefix=True,
                use_instance_id=False,
                flush_on_start=False,
                drop_instruments_on_reset=True,
            )
        else:
            cache_config = CacheConfig(
                tick_capacity=100_000,
                bar_capacity=50_000,
                encoding="msgpack",
                timestamps_as_iso8601=True,
            )
        
        # 创建Cache实例
        self.clock = LiveClock()
        self.logger = Logger("ETF159506CacheManager")
        self.cache = Cache(config=cache_config)
        
        # 初始化159506 ETF工具
        self._init_instrument()
        
        # 数据统计
        self.tick_count = 0
        self.bar_count = 0
        self.start_time = datetime.now()
        
        logger.info(f"Cache管理器初始化完成 - Redis: {use_redis}")
    
    def _init_instrument(self):
        """初始化159506 ETF工具"""
        # 创建159506 ETF工具
        self.instrument_id = InstrumentId(
            symbol=Symbol("159506"),
            venue=Venue("SZSE")  # 深圳证券交易所
        )
        
        # 创建工具对象
        self.instrument = Instrument(
            instrument_id=self.instrument_id,
            raw_symbol=Symbol("159506"),
            asset_class=AssetClass.EQUITY,
            instrument_class=InstrumentClass.SPOT,
            quote_currency=Currency.from_str("CNY"),
            is_inverse=False,
            price_precision=3,
            size_precision=0,
            size_increment=Quantity(1, precision=0),
            multiplier=Quantity(1, precision=0),
            margin_init=Decimal("0.0"),
            margin_maint=Decimal("0.0"),
            maker_fee=Decimal("0.0"),
            taker_fee=Decimal("0.0"),
            ts_event=0,
            ts_init=0,
        )
        
        # 将工具添加到Cache
        self.cache.add_instrument(self.instrument)
        logger.info(f"工具已添加到Cache: {self.instrument_id}")
    
    def store_quote_tick(self, data: Dict):
        """存储报价tick数据到Cache"""
        try:
            # 安全地转换数据类型
            bid_price = data.get('bid_price', data.get('price', 0))
            ask_price = data.get('ask_price', data.get('price', 0))
            bid_size = data.get('bid_size', 0)
            ask_size = data.get('ask_size', 0)
            
            # 确保价格和数量是有效的数值
            if not isinstance(bid_price, (int, float)) or bid_price <= 0:
                bid_price = 0
            if not isinstance(ask_price, (int, float)) or ask_price <= 0:
                ask_price = 0
            if not isinstance(bid_size, (int, float)) or bid_size < 0:
                bid_size = 0
            if not isinstance(ask_size, (int, float)) or ask_size < 0:
                ask_size = 0
            
            # 创建QuoteTick对象 - 使用instrument的方法
            quote_tick = QuoteTick(
                instrument_id=self.instrument_id,
                bid_price=self.instrument.make_price(bid_price),  # 使用instrument的make_price方法
                ask_price=self.instrument.make_price(ask_price),  # 使用instrument的make_price方法
                bid_size=Quantity.from_int(int(float(bid_size))),  # 使用from_int方法
                ask_size=Quantity.from_int(int(float(ask_size))),  # 使用from_int方法
                ts_event=self.clock.timestamp_ns(),
                ts_init=self.clock.timestamp_ns(),
            )
            
            # 添加到Cache
            self.cache.add_quote_tick(quote_tick)
            self.tick_count += 1
                
        except Exception as e:
            logger.error(f"存储QuoteTick失败: {e}")
            logger.error(f"数据内容: {data}")
    
    def store_trade_tick(self, data: Dict):
        """存储交易tick数据到Cache"""
        try:
            # 安全地转换数据类型
            price = data.get('price', 0)
            volume = data.get('volume', 0)
            
            # 确保价格和成交量是有效的数值
            if not isinstance(price, (int, float)) or price <= 0:
                price = 0
            if not isinstance(volume, (int, float)) or volume <= 0:
                volume = 0
            
            # 将成交量转换为整数（避免数据类型问题）
            volume_int = int(float(volume))  # 确保是整数
            
            # 创建TradeTick对象
            current_time_ns = self.clock.timestamp_ns()
            
            trade_tick = TradeTick(
                instrument_id=self.instrument_id,
                price=self.instrument.make_price(price),  # 使用instrument的make_price方法
                size=Quantity.from_int(volume_int),  # 使用from_int方法
                aggressor_side=AggressorSide.NO_AGGRESSOR,  # 使用枚举值
                trade_id=TradeId(str(data.get('trade_id', f"trade_{self.tick_count}"))),
                ts_event=current_time_ns,
                ts_init=current_time_ns,
            )
            
            # 添加到Cache
            self.cache.add_trade_tick(trade_tick)
            self.tick_count += 1
            
        except Exception as e:
            logger.error(f"存储TradeTick失败: {e}")
            logger.error(f"数据内容: {data}")
            import traceback
            logger.error(f"详细错误信息: {traceback.format_exc()}")
    
    def create_bars_from_ticks(self, bar_type: BarType):
        """从tick数据创建K线 - 使用成交价"""
        try:
            # 获取交易tick数据（成交价）
            trade_ticks = self.cache.trade_ticks(self.instrument_id)
            if len(trade_ticks) < 2:
                return
            
            # 创建K线数据 - 使用成交价
            latest_tick = trade_ticks[-1]
            
            bar = Bar(
                bar_type=bar_type,
                open=latest_tick.price,      # 使用成交价
                high=latest_tick.price,      # 使用成交价
                low=latest_tick.price,       # 使用成交价
                close=latest_tick.price,     # 使用成交价
                volume=latest_tick.size,     # 使用成交量
                ts_event=latest_tick.ts_event,
                ts_init=latest_tick.ts_init,
            )
            
            # 添加到Cache
            self.cache.add_bar(bar)
            self.bar_count += 1
            
            if self.bar_count % 100 == 0:
                logger.info(f"已创建 {self.bar_count} 根K线（基于成交价）")
                
        except Exception as e:
            logger.error(f"创建K线失败: {e}")
    
    def create_enhanced_bars_from_ticks(self, bar_type: BarType):
        """创建增强K线 - 结合成交价和买卖价差"""
        try:
            # 获取交易tick数据（成交价）
            trade_ticks = self.cache.trade_ticks(self.instrument_id)
            quote_ticks = self.cache.quote_ticks(self.instrument_id)
            
            if len(trade_ticks) < 2:
                return
            
            # 按时间窗口聚合数据
            window_trades = self._get_window_trades(trade_ticks, bar_type)
            window_quotes = self._get_window_quotes(quote_ticks, bar_type)
            
            if len(window_trades) == 0:
                return
            
            # 计算基于成交价的OHLC
            trade_prices = [float(tick.price) for tick in window_trades]
            trade_volumes = [int(tick.size) for tick in window_trades]
            
            # 计算基于买卖价差的增强信息
            spread_info = self._calculate_spread_info(window_quotes)
            
            # 检查总成交量是否超过最大值
            total_volume = sum(trade_volumes)
            max_quantity = 18_446_744_073  # Quantity类型的最大值
            
            # 添加调试信息
            logger.debug(f"K线生成: 窗口内交易数={len(window_trades)}, 总成交量={total_volume}")
            
            if total_volume > max_quantity:
                logger.warning(f"总成交量 {total_volume} 超过最大值 {max_quantity}，将使用最大值")
                total_volume = max_quantity
            
            # 创建增强K线
            bar = Bar(
                bar_type=bar_type,
                open=self.instrument.make_price(trade_prices[0]),
                high=self.instrument.make_price(max(trade_prices)),
                low=self.instrument.make_price(min(trade_prices)),
                close=self.instrument.make_price(trade_prices[-1]),
                volume=Quantity.from_int(total_volume),
                ts_event=window_trades[-1].ts_event,
                ts_init=window_trades[-1].ts_init,
            )
            
            # 添加到Cache
            self.cache.add_bar(bar)
            self.bar_count += 1
            
            # 记录增强信息
            if self.bar_count % 100 == 0:
                logger.info(f"已创建 {self.bar_count} 根增强K线, "
                           f"平均价差: {spread_info.get('avg_spread', 0):.4f}")
                
        except Exception as e:
            logger.error(f"创建增强K线失败: {e}")
            import traceback
            logger.error(f"详细错误信息: {traceback.format_exc()}")
    
    def _get_window_trades(self, trade_ticks, bar_type):
        """获取时间窗口内的交易数据"""
        if len(trade_ticks) == 0:
            return []
        
        # 根据bar_type的时间窗口来聚合数据
        # 对于1分钟K线，获取最近1分钟的数据
        current_time = trade_ticks[-1].ts_event
        window_start = current_time - (60 * 1_000_000_000)  # 1分钟 = 60秒 * 10^9纳秒
        
        # 过滤时间窗口内的数据
        window_trades = [tick for tick in trade_ticks if tick.ts_event >= window_start]
        
        # 如果数据量仍然过大，进一步限制
        max_records = 100
        if len(window_trades) > max_records:
            window_trades = window_trades[-max_records:]
            logger.debug(f"时间窗口数据过多，限制为最近{max_records}条记录")
        
        return window_trades
    
    def _get_window_quotes(self, quote_ticks, bar_type):
        """获取时间窗口内的报价数据"""
        if len(quote_ticks) == 0:
            return []
        
        # 根据bar_type的时间窗口来聚合数据
        # 对于1分钟K线，获取最近1分钟的数据
        current_time = quote_ticks[-1].ts_event
        window_start = current_time - (60 * 1_000_000_000)  # 1分钟 = 60秒 * 10^9纳秒
        
        # 过滤时间窗口内的数据
        window_quotes = [tick for tick in quote_ticks if tick.ts_event >= window_start]
        
        # 如果数据量仍然过大，进一步限制
        max_records = 100
        if len(window_quotes) > max_records:
            window_quotes = window_quotes[-max_records:]
            logger.debug(f"时间窗口数据过多，限制为最近{max_records}条记录")
        
        return window_quotes
    
    def _calculate_spread_info(self, quote_ticks):
        """计算价差信息"""
        if len(quote_ticks) == 0:
            return {}
        
        spreads = []
        bid_prices = []
        ask_prices = []
        
        for tick in quote_ticks:
            spread = float(tick.ask_price) - float(tick.bid_price)
            spreads.append(spread)
            bid_prices.append(float(tick.bid_price))
            ask_prices.append(float(tick.ask_price))
        
        return {
            'avg_spread': np.mean(spreads) if spreads else 0,
            'max_spread': max(spreads) if spreads else 0,
            'min_spread': min(spreads) if spreads else 0,
            'avg_bid': np.mean(bid_prices) if bid_prices else 0,
            'avg_ask': np.mean(ask_prices) if ask_prices else 0,
            'spread_volatility': np.std(spreads) if len(spreads) > 1 else 0
        }
    
    def get_latest_data(self) -> Dict:
        """获取最新数据"""
        try:
            latest_quote = self.cache.quote_tick(self.instrument_id)
            latest_trade = self.cache.trade_tick(self.instrument_id)
            
            return {
                'tick_count': self.tick_count,
                'bar_count': self.bar_count,
                'latest_quote': latest_quote,
                'latest_trade': latest_trade,
                'quote_count': self.cache.quote_tick_count(self.instrument_id),
                'trade_count': self.cache.trade_tick_count(self.instrument_id),
                'runtime': datetime.now() - self.start_time
            }
        except Exception as e:
            logger.error(f"获取最新数据失败: {e}")
            return {}
    
    def get_historical_data(self, limit: int = 1000) -> Dict:
        """获取历史数据"""
        try:
            quote_ticks = self.cache.quote_ticks(self.instrument_id)[-limit:]
            trade_ticks = self.cache.trade_ticks(self.instrument_id)[-limit:]
            
            return {
                'quote_ticks': quote_ticks,
                'trade_ticks': trade_ticks,
                'total_quotes': len(quote_ticks),
                'total_trades': len(trade_ticks)
            }
        except Exception as e:
            logger.error(f"获取历史数据失败: {e}")
            return {}
    
    def save_to_parquet(self, filepath: str):
        """将Cache数据保存为Parquet格式"""
        try:
            # 获取所有数据
            quote_ticks = self.cache.quote_ticks(self.instrument_id)
            trade_ticks = self.cache.trade_ticks(self.instrument_id)
            
            # 转换为DataFrame
            quote_data = []
            for tick in quote_ticks:
                quote_data.append({
                    'timestamp': pd.to_datetime(tick.ts_event, unit='ns'),
                    'bid_price': float(tick.bid_price),
                    'ask_price': float(tick.ask_price),
                    'bid_size': int(tick.bid_size),
                    'ask_size': int(tick.ask_size),
                    'type': 'quote'
                })
            
            trade_data = []
            for tick in trade_ticks:
                trade_data.append({
                    'timestamp': pd.to_datetime(tick.ts_event, unit='ns'),
                    'price': float(tick.price),
                    'size': int(tick.size),
                    'trade_id': str(tick.trade_id),  # 转换为字符串避免序列化问题
                    'type': 'trade'
                })
            
            # 合并数据
            all_data = quote_data + trade_data
            df = pd.DataFrame(all_data)
            
            if not df.empty:
                df = df.sort_values('timestamp')
                df.to_parquet(filepath, index=False)
                logger.info(f"数据已保存到: {filepath} ({len(df)} 条记录)")
                return filepath
            else:
                logger.warning("没有数据可保存")
                return None
                
        except Exception as e:
            logger.error(f"保存Parquet文件失败: {e}")
            return None
    
    def clear_old_data(self, keep_hours: int = 24):
        """清理旧数据"""
        try:
            cutoff_time = datetime.now() - timedelta(hours=keep_hours)
            cutoff_ns = int(cutoff_time.timestamp() * 1e9)
            
            # 获取所有数据
            quote_ticks = self.cache.quote_ticks(self.instrument_id)
            trade_ticks = self.cache.trade_ticks(self.instrument_id)
            
            # 过滤新数据
            new_quotes = [tick for tick in quote_ticks if tick.ts_event >= cutoff_ns]
            new_trades = [tick for tick in trade_ticks if tick.ts_event >= cutoff_ns]
            
            # 重新设置数据（这里简化处理，实际应该使用Cache的清理方法）
            logger.info(f"数据清理: 报价 {len(quote_ticks)} -> {len(new_quotes)}, "
                       f"交易 {len(trade_ticks)} -> {len(new_trades)}")
            
        except Exception as e:
            logger.error(f"清理旧数据失败: {e}")


class ETF159506ServerManager:
    """159506 ETF服务器管理器"""
    
    def __init__(self, token: str):
        self.token = token
        self.base_url = "http://jvQuant.com/query/server"
    
    def get_server(self, market: str = "ab", type: str = "websocket") -> str:
        """获取分配的服务器地址"""
        url = f"{self.base_url}?market={market}&type={type}&token={self.token}"
        
        try:
            response = requests.get(url, timeout=10)
            data = response.json()
            
            if data.get("code") == "0":
                server = data.get("server")
                logger.info(f"获取服务器地址成功: {server}")
                return server
            else:
                raise Exception(f"获取服务器失败: {data}")
        except Exception as e:
            logger.error(f"获取服务器地址失败: {e}")
            return None


class ETF159506CacheDataProcessor:
    """159506 ETF Cache数据处理器"""
    
    def __init__(self, stock_code: str = "159506", cache_manager: ETF159506CacheManager = None):
        self.stock_code = stock_code
        self.cache_manager = cache_manager
        
        # 数据统计
        self.total_processed = 0
        self.start_time = datetime.now()
        
        logger.info(f"初始化159506 ETF Cache数据处理器")
    
    def process_level1_data(self, data: str):
        """处理Level1数据并存储到Cache"""
        try:
            # 解析Level1数据
            parts = data.split('=')
            if len(parts) != 2:
                return
            
            code_part = parts[0]
            data_part = parts[1]
            
            if not code_part.startswith('lv1_'):
                return
            
            stock_code = code_part[4:]
            if stock_code != self.stock_code:
                return
            
            # 解析数据字段
            fields = data_part.split(',')
            if len(fields) < 6:
                return
            
            push_time = fields[0]
            stock_name = fields[1]
            latest_price = float(fields[2])
            change_percent = float(fields[3])
            turnover = float(fields[4])
            volume = float(fields[5])
            
            # 解析买卖五档数据
            bid_asks = self._parse_bid_ask_data(fields[6:])
            
            # 准备Cache数据
            cache_data = {
                'timestamp': push_time,
                'stock_code': stock_code,
                'stock_name': stock_name,
                'price': latest_price,
                'change_percent': change_percent,
                'turnover': turnover,
                'volume': volume,
                'bid_asks': bid_asks,
                'collect_time': datetime.now().isoformat()
            }
            
            # 存储到Cache
            if self.cache_manager:
                # 存储报价数据（买卖五档）
                if bid_asks.get('bids') and bid_asks.get('asks'):
                    # 使用买一卖一作为主要QuoteTick
                    best_bid = bid_asks['bids'][0] if bid_asks['bids'] else {'price': latest_price, 'volume': 0}
                    best_ask = bid_asks['asks'][0] if bid_asks['asks'] else {'price': latest_price, 'volume': 0}
                    
                    quote_data = {
                        'bid_price': best_bid['price'],      # 买一价
                        'ask_price': best_ask['price'],      # 卖一价
                        'bid_size': best_bid['volume'],      # 买一量
                        'ask_size': best_ask['volume'],      # 卖一量
                    }
                    self.cache_manager.store_quote_tick(quote_data)
                    
                    # 计算并记录价差信息
                    spread = best_ask['price'] - best_bid['price']
                    if self.total_processed % 50 == 0:  # 每50条记录一次价差
                        logger.info(f"价差信息: 买一{best_bid['price']:.3f}({best_bid['volume']}) "
                                   f"卖一{best_ask['price']:.3f}({best_ask['volume']}) "
                                   f"价差{spread:.4f}")
                
                # 存储交易数据（成交价）
                trade_data = {
                    'price': latest_price,                   # 成交价
                    'volume': volume,                        # 成交量
                    'trade_id': f"trade_{self.total_processed}"
                }
                self.cache_manager.store_trade_tick(trade_data)
            
            self.total_processed += 1
            
            # 每100条输出一次统计
            if self.total_processed % 100 == 0:
                logger.info(f"已处理 {self.total_processed} 条数据, "
                           f"最新价格: {latest_price}, 成交量: {volume}")
                
        except Exception as e:
            logger.error(f"处理Level1数据失败: {e}")
    
    def _parse_bid_ask_data(self, fields: List[str]) -> List[Dict]:
        """解析买卖五档数据"""
        try:
            bid_asks = {
                'bids': [],
                'asks': []
            }
            
            if len(fields) >= 20:
                # 解析买五档
                for i in range(0, 10, 2):
                    if i + 1 < len(fields):
                        try:
                            volume = float(fields[i])
                            price = float(fields[i + 1])
                            bid_asks['bids'].append({
                                'volume': volume,
                                'price': price,
                                'level': i // 2 + 1
                            })
                        except (ValueError, IndexError):
                            continue
                
                # 解析卖五档
                for i in range(10, 20, 2):
                    if i + 1 < len(fields):
                        try:
                            volume = float(fields[i])
                            price = float(fields[i + 1])
                            bid_asks['asks'].append({
                                'volume': volume,
                                'price': price,
                                'level': (i - 10) // 2 + 1
                            })
                        except (ValueError, IndexError):
                            continue
            
            return bid_asks
            
        except Exception as e:
            logger.error(f"解析买卖档数据失败: {e}")
            return {'bids': [], 'asks': []}


class ETF159506CacheWebSocketClient:
    """159506 ETF基于Cache的WebSocket客户端"""
    
    def __init__(self, token: str, stock_code: str = "159506", 
                 use_redis: bool = True, redis_host: str = "localhost", redis_port: int = 6379):
        self.token = token
        self.stock_code = stock_code
        
        # 初始化组件
        self.server_manager = ETF159506ServerManager(token)
        self.cache_manager = ETF159506CacheManager(use_redis, redis_host, redis_port)
        self.data_processor = ETF159506CacheDataProcessor(stock_code, self.cache_manager)
        
        # WebSocket相关
        self.ws = None
        self.is_connected = False
        
        # 线程控制
        self.save_thread = None
        self.stop_save = False
        self.heartbeat_thread = None
        self.stop_heartbeat = False
        self.monitor_thread = None
        self.stop_monitor = False
        
        # 统计信息
        self.connection_count = 0
        self.disconnection_count = 0
        self.data_receive_count = 0
        self.last_data_time = None
        self.start_time = datetime.now()
        
        # 配置
        self.save_interval = 60  # 1分钟保存一次
        self.heartbeat_interval = 30  # 30秒心跳一次
        self.catalog_path = f"catalog/etf_159506_cache"
        
        logger.info(f"初始化159506 ETF Cache WebSocket客户端")
    
    def connect(self):
        """连接WebSocket服务器"""
        server = self.server_manager.get_server("ab", "websocket")
        if not server:
            logger.error("无法获取服务器地址")
            return False
        
        if server.startswith('ws://'):
            ws_url = f"{server}/?token={self.token}"
        else:
            ws_url = f"ws://{server}/?token={self.token}"
        
        logger.info(f"连接到WebSocket服务器: {ws_url}")
        
        self.ws = websocket.WebSocketApp(
            ws_url,
            on_open=self.on_open,
            on_data=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        
        self.ws.run_forever()
        return True
    
    def on_open(self, ws):
        """连接打开回调"""
        self.connection_count += 1
        self.is_connected = True
        logger.info(f"WebSocket连接已建立 (第{self.connection_count}次连接)")
        
        subscription = f"add=lv1_{self.stock_code}"
        ws.send(subscription)
        logger.info(f"已订阅: {subscription}")
        
        self.start_heartbeat(ws)
        self.start_auto_save()
        self.start_diagnostic_monitor()
    
    def on_message(self, ws, message, type, flag):
        """接收消息回调"""
        try:
            if type == websocket.ABNF.OPCODE_TEXT:
                logger.info(f"收到文本消息: {message}")
            
            elif type == websocket.ABNF.OPCODE_BINARY:
                decompressed = zlib.decompress(message, -zlib.MAX_WBITS)
                data_str = decompressed.decode("utf-8")
                
                self.data_receive_count += 1
                self.last_data_time = datetime.now()
                
                # 减少日志输出，只在每10条数据时输出一次
                if self.data_receive_count % 10 == 0:
                    logger.info(f"收到二进制数据 (第{self.data_receive_count}条): {data_str[:100]}...")
                
                lines = data_str.strip().split('\n')
                for line in lines:
                    if line.strip():
                        self.process_market_data(line)
                        
        except Exception as e:
            logger.error(f"处理消息失败: {e}")
    
    def process_market_data(self, data: str):
        """处理市场数据"""
        if data.startswith('lv1_'):
            self.data_processor.process_level1_data(data)
    
    def on_error(self, ws, error):
        """错误回调"""
        self.disconnection_count += 1
        self.is_connected = False
        self.stop_heartbeat = True
        logger.error(f"WebSocket错误 (第{self.disconnection_count}次断开): {error}")
        
        if "Connection to remote host was lost" in str(error):
            logger.info("连接丢失，尝试重连...")
            time.sleep(5)
            self.reconnect()
    
    def on_close(self, ws, code, msg):
        """连接关闭回调"""
        logger.info(f"WebSocket连接已关闭: {code} - {msg}")
        self.is_connected = False
        self.stop_save = True
        self.stop_heartbeat = True
    
    def start_auto_save(self):
        """启动自动保存线程"""
        self.save_thread = threading.Thread(target=self.auto_save_loop)
        self.save_thread.daemon = True
        self.save_thread.start()
        logger.info("自动保存线程已启动")
    
    def start_heartbeat(self, ws):
        """启动心跳线程"""
        self.heartbeat_thread = threading.Thread(target=self.heartbeat_loop, args=(ws,))
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()
        logger.info("心跳线程已启动")
    
    def start_diagnostic_monitor(self):
        """启动诊断监控线程"""
        self.monitor_thread = threading.Thread(target=self.diagnostic_monitor_loop)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        logger.info("诊断监控线程已启动")
    
    def diagnostic_monitor_loop(self):
        """诊断监控循环"""
        while not self.stop_monitor:
            try:
                time.sleep(300)  # 每5分钟输出一次诊断状态
                
                if not self.stop_monitor:
                    self.print_diagnostic_status()
                    
            except Exception as e:
                logger.error(f"诊断监控循环错误: {e}")
                break
    
    def print_diagnostic_status(self):
        """打印诊断状态信息"""
        runtime = datetime.now() - self.start_time
        last_data_ago = "无数据" if not self.last_data_time else str(datetime.now() - self.last_data_time)
        
        # 获取Cache状态
        cache_status = self.cache_manager.get_latest_data()
        
        # 获取最新价格信息
        latest_quote = cache_status.get('latest_quote')
        latest_trade = cache_status.get('latest_trade')
        
        # 计算价差信息
        spread_info = ""
        if latest_quote:
            spread = float(latest_quote.ask_price) - float(latest_quote.bid_price)
            spread_info = f"价差: {spread:.4f}"
        
        # 获取最新成交价
        trade_price = ""
        if latest_trade:
            trade_price = f"成交价: {float(latest_trade.price):.4f}"
        
        status = f"""
=== WebSocket Cache诊断状态 ===
运行时间: {runtime}
连接状态: {'已连接' if self.is_connected else '未连接'}
连接次数: {self.connection_count}
断开次数: {self.disconnection_count}
数据接收: {self.data_receive_count} 条
最后数据: {last_data_ago} 前
Cache统计:
  总tick数: {cache_status.get('tick_count', 0)}
  总K线数: {cache_status.get('bar_count', 0)}
  报价数: {cache_status.get('quote_count', 0)}
  交易数: {cache_status.get('trade_count', 0)}
最新数据:
  {trade_price}
  {spread_info}
===============================
"""
        logger.info(status)
    
    def heartbeat_loop(self, ws):
        """心跳循环"""
        while not self.stop_heartbeat and self.is_connected:
            try:
                time.sleep(self.heartbeat_interval)
                
                if not self.stop_heartbeat and self.is_connected:
                    heartbeat_msg = "ping"
                    ws.send(heartbeat_msg)
                    logger.debug("发送心跳包")
                    
            except Exception as e:
                logger.error(f"心跳发送失败: {e}")
                break
    
    def auto_save_loop(self):
        """自动保存循环"""
        while not self.stop_save:
            try:
                time.sleep(self.save_interval)
                
                if not self.stop_save:
                    # 创建增强K线
                    try:
                        from nautilus_trader.model.data import BarType, BarSpecification
                        from nautilus_trader.model.enums import BarAggregation, PriceType
                        
                        # 创建1分钟K线 - 修复构造函数参数
                        bar_spec = BarSpecification(
                            1,  # step: 1分钟
                            BarAggregation.MINUTE,
                            PriceType.LAST,
                        )
                        bar_type = BarType(
                            self.cache_manager.instrument_id,
                            bar_spec,
                        )
                        
                        # 生成增强K线
                        self.cache_manager.create_enhanced_bars_from_ticks(bar_type)
                        
                    except Exception as e:
                        logger.error(f"生成增强K线失败: {e}")
                    
                    # 保存数据到Parquet文件
                    catalog_dir = Path(self.catalog_path)
                    catalog_dir.mkdir(parents=True, exist_ok=True)
                    
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    filename = f"cache_data_{timestamp}.parquet"
                    filepath = catalog_dir / filename
                    
                    saved_file = self.cache_manager.save_to_parquet(str(filepath))
                    if saved_file:
                        logger.info(f"自动保存完成: {saved_file}")
                    
                    # 清理旧数据（保留24小时）
                    self.cache_manager.clear_old_data(24)
                    
            except Exception as e:
                logger.error(f"自动保存失败: {e}")
    
    def get_status(self) -> Dict:
        """获取状态信息"""
        cache_status = self.cache_manager.get_latest_data()
        return {
            'connected': self.is_connected,
            'stock_code': self.stock_code,
            'connection_count': self.connection_count,
            'disconnection_count': self.disconnection_count,
            'data_receive_count': self.data_receive_count,
            'last_data_time': self.last_data_time.isoformat() if self.last_data_time else None,
            'cache_status': cache_status
        }
    
    def disconnect(self):
        """断开连接"""
        logger.info("正在断开连接...")
        self.stop_save = True
        self.stop_heartbeat = True
        self.stop_monitor = True
        if self.ws:
            self.ws.close()
        
        # 保存最终数据
        catalog_dir = Path(self.catalog_path)
        catalog_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"final_cache_data_{timestamp}.parquet"
        filepath = catalog_dir / filename
        self.cache_manager.save_to_parquet(str(filepath))
        
        self.print_diagnostic_status()
        logger.info("连接已断开")
    
    def reconnect(self):
        """重连方法"""
        try:
            logger.info("开始重连...")
            self.stop_heartbeat = True
            self.stop_save = True
            self.stop_monitor = True
            
            if self.heartbeat_thread:
                self.heartbeat_thread.join(timeout=2)
            if self.save_thread:
                self.save_thread.join(timeout=2)
            if self.monitor_thread:
                self.monitor_thread.join(timeout=2)
            
            self.connect()
            
        except Exception as e:
            logger.error(f"重连失败: {e}")


def main():
    """主函数"""
    # 配置参数
    TOKEN = "d0c519adcd47d266f1c96750d4e80aa6"
    STOCK_CODE = "159506"
    USE_REDIS = True  # 是否使用Redis持久化
    REDIS_HOST = "localhost"
    REDIS_PORT = 6379
    
    print("=" * 60)
    print("159506 ETF基于NautilusTrader Cache的实时数据采集器")
    print("=" * 60)
    print(f"Token: {TOKEN}")
    print(f"股票代码: {STOCK_CODE}")
    print(f"Redis持久化: {USE_REDIS}")
    if USE_REDIS:
        print(f"Redis地址: {REDIS_HOST}:{REDIS_PORT}")
    print(f"Catalog路径: catalog/etf_159506_cache")
    print("=" * 60)
    
    # 创建WebSocket客户端
    client = ETF159506CacheWebSocketClient(
        TOKEN, STOCK_CODE, USE_REDIS, REDIS_HOST, REDIS_PORT
    )
    
    try:
        if client.connect():
            print("Cache数据采集系统运行中... 按Ctrl+C退出")
            print("数据将自动保存到Cache和catalog/etf_159506_cache目录")
            
            while client.is_connected:
                time.sleep(1)
                
                if int(time.time()) % 300 == 0:  # 每5分钟输出一次状态
                    status = client.get_status()
                    cache_status = status.get('cache_status', {})
                    
                    # 获取最新价格信息
                    latest_quote = cache_status.get('latest_quote')
                    latest_trade = cache_status.get('latest_trade')
                    
                    # 显示价格信息
                    price_info = ""
                    if latest_trade:
                        price_info = f"成交价: {float(latest_trade.price):.4f}"
                    
                    if latest_quote:
                        spread = float(latest_quote.ask_price) - float(latest_quote.bid_price)
                        price_info += f", 价差: {spread:.4f}"
                    
                    print(f"状态: 连接={status['connected']}, "
                          f"总tick数={cache_status.get('tick_count', 0)}, "
                          f"报价数={cache_status.get('quote_count', 0)}, "
                          f"交易数={cache_status.get('trade_count', 0)}, "
                          f"{price_info}")
                
    except KeyboardInterrupt:
        print("\n用户中断，正在保存数据...")
        client.disconnect()
        print("系统已退出")
    
    except Exception as e:
        logger.error(f"系统错误: {e}")
        client.disconnect()


if __name__ == "__main__":
    main() 