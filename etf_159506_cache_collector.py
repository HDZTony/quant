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

# Redis连接检测和健康检查
REDIS_AVAILABLE = False
REDIS_HEALTHY = False
REDIS_TEST_HOST = "localhost"
REDIS_TEST_PORT = 6379
try:
    import redis
    REDIS_AVAILABLE = True
    print(f"[Redis] redis-py已安装，开始检测Redis服务...")
    try:
        r = redis.StrictRedis(host=REDIS_TEST_HOST, port=REDIS_TEST_PORT, socket_connect_timeout=3)
        result = r.ping()
        REDIS_HEALTHY = True
        print(f"[Redis] Redis服务检测成功: {result}")
        logging.info("[Redis] redis-py已安装，且Redis服务可用，优先使用Redis持久化模式。")
    except Exception as e:
        print(f"[Redis] Redis服务检测失败: {e}")
        logging.warning(f"[Redis] redis-py已安装，但Redis服务不可用({REDIS_TEST_HOST}:{REDIS_TEST_PORT})，将自动切换为内存缓存模式。错误: {e}")
except ImportError:
    print("[Redis] 未安装redis-py，将使用内存缓存模式")
    logging.warning("[Redis] 未安装redis-py，将使用内存缓存模式。建议: pip install redis")

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
        # 动态切换逻辑
        print(f"[CacheManager] 初始化参数: use_redis={use_redis}, REDIS_AVAILABLE={REDIS_AVAILABLE}, REDIS_HEALTHY={REDIS_HEALTHY}")
        self.use_redis = use_redis and REDIS_AVAILABLE and REDIS_HEALTHY
        self.redis_host = redis_host
        self.redis_port = redis_port
        print(f"[CacheManager] 最终Redis使用状态: {self.use_redis}")
        if use_redis and not self.use_redis:
            print("[CacheManager] 请求使用Redis，但Redis不可用，已自动切换为内存缓存模式！")
            logging.warning("[CacheManager] 请求使用Redis，但Redis不可用，已自动切换为内存缓存模式！")
        
        # 创建Cache配置
        if self.use_redis:
            print(f"[CacheManager] 创建Redis配置: host={redis_host}, port={redis_port}")
            cache_config = CacheConfig(
                database=DatabaseConfig(
                    type="redis",
                    host=redis_host,
                    port=redis_port,
                    timeout=5,
                ),
                tick_capacity=100_000,  # 存储10万条tick
                bar_capacity=50_000,    # 存储5万根K线
                encoding="msgpack",
                timestamps_as_iso8601=True,
                use_trader_prefix=True,  # 启用trader前缀，这是默认行为
                use_instance_id=True,    # 启用实例ID，确保数据隔离
                flush_on_start=False,
                drop_instruments_on_reset=True,
            )
            print("[CacheManager] Redis配置创建成功")
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
        
        # 如果需要Redis，创建数据库适配器
        if self.use_redis:
            from nautilus_trader.cache.database import CacheDatabaseAdapter
            from nautilus_trader.model.identifiers import TraderId
            from nautilus_trader.core.uuid import UUID4
            from nautilus_trader.serialization.serializer import MsgSpecSerializer
            import msgspec
            
            # 创建必要的组件
            trader_id = TraderId("TRADER-001")
            instance_id = UUID4()
            serializer = MsgSpecSerializer(
                encoding=msgspec.msgpack, 
                timestamps_as_str=True,
                timestamps_as_iso8601=False  # 只使用timestamps_as_str
            )
            
            # 创建数据库适配器
            database = CacheDatabaseAdapter(
                trader_id=trader_id,
                instance_id=instance_id,
                serializer=serializer,
                config=cache_config,
            )
            
            # 创建带数据库的Cache
            self.cache = Cache(database=database, config=cache_config)
            print("[CacheManager] 已创建带Redis后端的Cache")
        else:
            # 创建内存Cache
            self.cache = Cache(config=cache_config)
            print("[CacheManager] 已创建内存Cache")
        
        # 初始化159506 ETF工具
        self._init_instrument()
        
        # 数据统计
        self.tick_count = 0
        self.start_time = datetime.now()
        
        logger.info(f"Cache管理器初始化完成 - Redis: {self.use_redis}")
    
    def _init_instrument(self):
        """初始化159506 ETF工具"""
        # 创建159506 ETF工具
        self.instrument_id = InstrumentId(
            symbol=Symbol("159506"),
            venue=Venue("SZSE")  # 深圳证券交易所
        )
        
        # 创建工具对象 - 使用Equity类型而不是Instrument基类
        from nautilus_trader.model.instruments import Equity
        from nautilus_trader.model.objects import Price
        
        self.instrument = Equity(
            instrument_id=self.instrument_id,
            raw_symbol=Symbol("159506"),
            currency=Currency.from_str("CNY"),
            price_precision=3,
            price_increment=Price.from_str("0.001"),  # 最小价格变动0.001
            lot_size=Quantity.from_int(1),  # 最小交易单位1股
            ts_event=self.clock.timestamp_ns(),
            ts_init=self.clock.timestamp_ns(),
            margin_init=Decimal("0.0"),
            margin_maint=Decimal("0.0"),
            maker_fee=Decimal("0.0"),
            taker_fee=Decimal("0.0"),
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
                logger.warning(f"无效价格: {price}")
                return
            if not isinstance(volume, (int, float)) or volume <= 0:
                logger.warning(f"无效成交量: {volume}")
                return
            
            # 将成交量转换为整数（避免数据类型问题）
            volume_int = int(float(volume))  # 确保是整数
            
            # 验证成交量必须大于0
            if volume_int <= 0:
                logger.warning(f"成交量为0或负数，跳过存储: {volume_int}")
                return
            
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
    

    
    def get_latest_data(self) -> Dict:
        """获取最新数据"""
        try:
            latest_quote = self.cache.quote_tick(self.instrument_id)
            latest_trade = self.cache.trade_tick(self.instrument_id)
            
            return {
                'tick_count': self.tick_count,
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
        
        # 用于计算增量成交量
        self.last_volume = 0  # 上次的累计成交量
        
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
                
                # 计算增量成交量（当前累计成交量 - 上次累计成交量）
                volume_increment = max(0, volume - self.last_volume)
                self.last_volume = volume
                
                # 添加调试信息（每100条记录一次）
                if self.total_processed % 100 == 0:
                    logger.debug(f"成交量计算: 累计={volume}, 增量={volume_increment}, 上次={self.last_volume}")
                
                # 只有当增量成交量大于0时才存储交易数据
                if volume_increment > 0:
                    trade_data = {
                        'price': latest_price,                   # 成交价
                        'volume': volume_increment,              # 增量成交量
                        'trade_id': f"trade_{self.total_processed}"
                    }
                    self.cache_manager.store_trade_tick(trade_data)
                else:
                    logger.debug(f"增量成交量为0，跳过存储交易tick: 累计={volume}, 增量={volume_increment}")
            
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
        self.ws_thread = None
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
        
        # 在后台线程中运行WebSocket连接
        self.ws_thread = threading.Thread(target=self.ws.run_forever)
        self.ws_thread.daemon = True
        self.ws_thread.start()
        
        # 等待连接建立或超时
        timeout = 30  # 30秒超时
        start_time = time.time()
        while not self.is_connected and (time.time() - start_time) < timeout:
            time.sleep(0.1)
        
        if self.is_connected:
            logger.info("WebSocket连接成功建立")
            return True
        else:
            logger.error("WebSocket连接超时")
            return False
    
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
    USE_REDIS = True  # 启用Redis，系统会自动检测并切换
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
                
                # 每10秒打印一次数据状态
                if int(time.time()) % 10 == 0:
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
                    
                    current_time = datetime.now().strftime('%H:%M:%S')
                    print(f"[{current_time}] 数据状态: 连接={status['connected']}, "
                          f"总tick数={cache_status.get('tick_count', 0)}, "
                          f"报价数={cache_status.get('quote_count', 0)}, "
                          f"交易数={cache_status.get('trade_count', 0)}, "
                          f"{price_info}")
                
                # 每1分钟打印一次详细状态信息
                if int(time.time()) % 60 == 0:
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
                    
                    current_time = datetime.now().strftime('%H:%M:%S')
                    print(f"\n{'='*60}")
                    print(f"[{current_time}] 详细状态信息:")
                    print(f"连接状态: {status['connected']}")
                    print(f"连接次数: {status['connection_count']}")
                    print(f"断开次数: {status['disconnection_count']}")
                    print(f"数据接收: {status['data_receive_count']} 条")
                    print(f"Cache统计:")
                    print(f"  总tick数: {cache_status.get('tick_count', 0)}")
                    print(f"  报价数: {cache_status.get('quote_count', 0)}")
                    print(f"  交易数: {cache_status.get('trade_count', 0)}")
                    print(f"最新数据: {price_info}")
                    print(f"{'='*60}\n")
                
    except KeyboardInterrupt:
        print("\n用户中断，正在保存数据...")
        client.disconnect()
        print("系统已退出")
    
    except Exception as e:
        logger.error(f"系统错误: {e}")
        client.disconnect()


if __name__ == "__main__":
    main() 