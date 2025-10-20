#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
159506 ETF适配器
基于NautilusTrader适配器架构设计，集成jvquant平台数据接入
"""

import asyncio
import logging
import os
import threading
import time
import zlib
import requests
import websocket
import aiohttp
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from decimal import Decimal
import json

# NautilusTrader imports
from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.model.data import QuoteTick, TradeTick, Bar
from nautilus_trader.model.enums import BarAggregation, PriceType, AggressorSide, OmsType, AccountType, OrderStatus
from nautilus_trader.model.objects import Quantity, AccountBalance, Money
from nautilus_trader.model.currencies import CNY
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import (
    GenerateOrderStatusReports,
    GenerateFillReports,
    GeneratePositionStatusReports,
    CancelAllOrders,
    BatchCancelOrders,
    ModifyOrder,
)
from nautilus_trader.execution.reports import OrderStatusReport, FillReport, PositionStatusReport
from nautilus_trader.model.enums import OrderSide, OrderType, LiquiditySide, PositionSide
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.serialization.arrow.serializer import register_arrow
from nautilus_trader.live.data_client import LiveMarketDataClient
from nautilus_trader.live.factories import LiveDataClientFactory
from nautilus_trader.live.execution_client import LiveExecutionClient
from nautilus_trader.live.factories import LiveExecClientFactory
from nautilus_trader.common.component import MessageBus, LiveClock
from nautilus_trader.cache.cache import Cache
from nautilus_trader.model.identifiers import ClientId
import pyarrow as pa

logger = logging.getLogger(__name__)


class ETF159506DataSaver:
    """159506 ETF数据保存管理器 - 支持流式保存到磁盘"""
    
    def __init__(self, catalog_path: str = "./data_catalog"):
        """初始化数据保存器"""
        self.catalog_path = catalog_path
        self.catalog = None
        self.instrument_id = InstrumentId.from_str("159506.SZSE")
        # 分离缓冲区，避免混合存储导致的数据保存问题
        self.quote_buffer = []  # QuoteTick专用缓冲区
        self.trade_buffer = []  # TradeTick专用缓冲区
        self.last_save_time = time.time()
       
        
        # 初始化数据目录
        self._init_catalog()
        
    def _init_catalog(self):
        """初始化Parquet数据目录"""
        try:
            # 检查现有文件
            self._check_existing_files()
            
            self.catalog = ParquetDataCatalog(self.catalog_path)
            logger.info(f"✅ 数据目录初始化成功: {self.catalog_path}")
        except Exception as e:
            logger.error(f"❌ 数据目录初始化失败: {e}")
            self.catalog = None
    
    def _check_existing_files(self):
        """检查现有文件，找出可能的重叠"""
        try:
            quote_tick_path = os.path.join(self.catalog_path, "data", "quote_tick", "159506.SZSE")
            if os.path.exists(quote_tick_path):
                files = os.listdir(quote_tick_path)
                logger.debug(f"🔍 现有QuoteTick文件数量: {len(files)}")
                if files:
                    # 显示最近几个文件
                    files.sort()
                    recent_files = files[-3:] if len(files) >= 3 else files
                    logger.debug(f"🔍 最近的QuoteTick文件: {recent_files}")
                    
                    # 检查文件名中的时间戳
                    for file in recent_files:
                        if file.endswith('.parquet'):
                            logger.debug(f"🔍 文件: {file}")
        except Exception as e:
            logger.debug(f"检查现有文件失败: {e}")
    
    def add_trade_tick(self, trade_tick: TradeTick):
        """添加TradeTick到专用缓冲区"""
        if not self.catalog:
            return
            
        try:
            # 添加到TradeTick专用缓冲区
            self.trade_buffer.append(trade_tick)
            
            # 检查TradeTick是否需要保存
            self._check_trade_save()
            
        except Exception as e:
            logger.error(f"❌ 添加TradeTick失败: {e}")
    
    def add_quote_tick(self, quote_tick: QuoteTick):
        """添加QuoteTick到专用缓冲区"""
        if not self.catalog:
            return
            
        try:
            # 添加到QuoteTick专用缓冲区
            self.quote_buffer.append(quote_tick)
            
            # 检查QuoteTick是否需要保存
            self._check_quote_save()
            
        except Exception as e:
            logger.error(f"❌ 添加QuoteTick失败: {e}")
    
    def _check_quote_save(self):
        """检查QuoteTick是否需要保存"""
        current_time = time.time()
        
        # 按数量保存 - QuoteTick至少20条数据才保存
        if len(self.quote_buffer) >= 20:
            self._flush_quote_buffer()
    
    def _check_trade_save(self):
        """检查TradeTick是否需要保存"""
        current_time = time.time()
        
        # 按数量保存 - TradeTick至少20条数据才保存
        if len(self.trade_buffer) >= 20:
            self._flush_trade_buffer()
    
    def _flush_quote_buffer(self):
        """独立保存QuoteTick缓冲区"""
        if not self.quote_buffer or not self.catalog:
            return
            
        try:
            logger.debug(f"🔍 准备保存QuoteTick数据: {len(self.quote_buffer)} 条")
            
            # 显示时间戳范围
            ts_events = [tick.ts_event for tick in self.quote_buffer]
            ts_inits = [tick.ts_init for tick in self.quote_buffer]
            logger.debug(f"   - QuoteTick ts_event范围: {min(ts_events)} - {max(ts_events)}")
            logger.debug(f"   - QuoteTick ts_init范围: {min(ts_inits)} - {max(ts_inits)}")
            
            # 保存QuoteTick数据
            self.catalog.write_data(self.quote_buffer)
            saved_count = len(self.quote_buffer)
            logger.debug(f"💾 已保存 {saved_count} 条QuoteTick数据")
            logger.info(f"💾 QuoteTick保存完成: {saved_count} 条")
            
            # 清空QuoteTick缓冲区
            self.quote_buffer.clear()
            
        except Exception as e:
            logger.error(f"❌ QuoteTick保存失败: {e}")
            logger.error(f"❌ 错误详情: {type(e).__name__}: {str(e)}")
            # 即使保存失败，也清空缓冲区避免重复保存
            self.quote_buffer.clear()
    
    def _flush_trade_buffer(self):
        """独立保存TradeTick缓冲区"""
        if not self.trade_buffer or not self.catalog:
            return
            
        try:
            logger.debug(f"🔍 准备保存TradeTick数据: {len(self.trade_buffer)} 条")
            
            # 显示时间戳范围
            ts_events = [tick.ts_event for tick in self.trade_buffer]
            ts_inits = [tick.ts_init for tick in self.trade_buffer]
            logger.debug(f"   - TradeTick ts_event范围: {min(ts_events)} - {max(ts_events)}")
            logger.debug(f"   - TradeTick ts_init范围: {min(ts_inits)} - {max(ts_inits)}")
            
            # 保存TradeTick数据
            self.catalog.write_data(self.trade_buffer)
            saved_count = len(self.trade_buffer)
            logger.debug(f"💾 已保存 {saved_count} 条TradeTick数据")
            logger.info(f"💾 TradeTick保存完成: {saved_count} 条")
            
            # 清空TradeTick缓冲区
            self.trade_buffer.clear()
            
        except Exception as e:
            logger.error(f"❌ TradeTick保存失败: {e}")
            logger.error(f"❌ 错误详情: {type(e).__name__}: {str(e)}")
            # 即使保存失败，也清空缓冲区避免重复保存
            self.trade_buffer.clear()
    
    def force_save(self):
        """强制保存所有缓冲数据"""
        self._flush_quote_buffer()
        self._flush_trade_buffer()
    
    def get_saved_data_count(self) -> int:
        """获取已保存的数据条数"""
        try:
            if not self.catalog:
                return 0
                
            # 查询QuoteTick数据
            quote_data = self.catalog.quote_ticks(
                instrument_ids=[self.instrument_id],
                start=None,
                end=None
            )
            
            # 查询TradeTick数据
            trade_data = self.catalog.trade_ticks(
                instrument_ids=[self.instrument_id],
                start=None,
                end=None
            )
            
            return len(quote_data) + len(trade_data)
            
        except Exception as e:
            logger.error(f"❌ 获取保存数据统计失败: {e}")
            return 0

    def get_data_save_stats(self) -> dict:
        """获取数据保存统计信息 - 分别统计QuoteTick和TradeTick"""
        try:
            if not self.catalog:
                return {
                    'quote_ticks': 0,
                    'trade_ticks': 0,
                    'total_saved': 0,
                    'buffer_size': len(self.quote_buffer) + len(self.trade_buffer),
                    'quote_buffer_size': len(self.quote_buffer),
                    'trade_buffer_size': len(self.trade_buffer),
                    'catalog_path': self.catalog_path,
                    'last_save_time': self.last_save_time
                }
            
            # 查询QuoteTick数据
            quote_data = self.catalog.quote_ticks(
                instrument_ids=[self.instrument_id],
                start=None,
                end=None
            )
            
            # 查询TradeTick数据
            trade_data = self.catalog.trade_ticks(
                instrument_ids=[self.instrument_id],
                start=None,
                end=None
            )
            
            quote_count = len(list(quote_data)) if quote_data else 0
            trade_count = len(list(trade_data)) if trade_data else 0
            
            return {
                'quote_ticks': quote_count,
                'trade_ticks': trade_count,
                'total_saved': quote_count + trade_count,
                'buffer_size': len(self.quote_buffer) + len(self.trade_buffer),
                'quote_buffer_size': len(self.quote_buffer),
                'trade_buffer_size': len(self.trade_buffer),
                'catalog_path': self.catalog_path,
                'last_save_time': self.last_save_time
            }
            
        except Exception as e:
            logger.error(f"获取数据保存统计信息失败: {e}")
            return {
                'quote_ticks': 0,
                'trade_ticks': 0,
                'total_saved': 0,
                'buffer_size': 0,
                'quote_buffer_size': 0,
                'trade_buffer_size': 0,
                'catalog_path': self.catalog_path,
                'last_save_time': None,
                'error': str(e)
            }


class ETF159506ServerManager:
    """159506 ETF服务器管理器"""
    
    def __init__(self, token: str):
        self.token = token
        self.base_url = "http://jvQuant.com/query/server"
    
    def get_server(self, market: str = "ab", type: str = "websocket") -> str:
        """获取分配的服务器地址"""
        url = f"{self.base_url}?market={market}&type={type}&token={self.token}"
        
        # 配置代理
        proxies = {
            'http': 'http://127.0.0.1:10809',
            'https': 'http://127.0.0.1:10809'
        }
        
        try:
            response = requests.get(url, timeout=30, proxies=proxies)
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


class ETF159506HttpClient:
    """159506 ETF HTTP客户端 - 基于jvquant平台"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.logger = logging.getLogger("ETF159506HttpClient")
        self.token = config.get('token')
        self.server_manager = ETF159506ServerManager(self.token) if self.token else None
        self.is_connected = False
        
    async def connect(self) -> bool:
        """连接HTTP客户端"""
        try:
            if not self.token:
                self.logger.error("缺少token配置")
                return False
                
            # 测试服务器连接
            server = self.server_manager.get_server("ab", "websocket")
            if server:
                self.is_connected = True
                self.logger.info("HTTP客户端连接成功")
                return True
            else:
                self.logger.error("无法获取服务器地址")
                return False
        except Exception as e:
            self.logger.error(f"HTTP客户端连接失败: {e}")
            return False
    
    async def disconnect(self) -> None:
        """断开HTTP连接"""
        self.is_connected = False
        self.logger.info("HTTP客户端已断开")
    
    async def get_instruments(self) -> List[Dict]:
        """获取可用工具列表"""
        # 返回159506 ETF信息
        return [{
            'symbol': '159506',
            'name': '华夏中证500ETF',
            'exchange': 'SZSE',
            'currency': 'CNY',
            'tick_size': 0.001,
            'lot_size': 100
        }]
    
    async def get_historical_data(self, symbol: str, start_date: str, end_date: str, 
                                 kline_type: str = "day", fq: str = "前复权", limit: int = 240) -> List[Dict]:
        """获取历史数据 - 基于jvquant K线数据接口"""
        try:
            if not self.token:
                self.logger.error("缺少token配置")
                return []
            
            # 获取数据库服务器地址 - 使用正确的参数
            server = self.server_manager.get_server("ab", "sql")  # 数据库服务使用sql类型
            if not server:
                self.logger.error("无法获取数据库服务器地址")
                return []
            
            # 构建请求URL - 根据官方文档格式
            if server.startswith('http://') or server.startswith('https://'):
                url = f"{server}/sql"
            else:
                url = f"http://{server}/sql"
            params = {
                "mode": "kline",
                "cate": "etf",  # 159506是ETF
                "code": symbol,
                "type": kline_type,
                "fq": fq,
                "limit": limit,
                "token": self.token
            }
            
            # 调试信息
            self.logger.info(f"请求URL: {url}")
            self.logger.info(f"请求参数: {params}")
            
            # 发送请求 - 使用requests作为备选方案
            try:
                import requests
                # 配置代理
                proxies = {
                    'http': 'http://127.0.0.1:10809',
                    'https': 'http://127.0.0.1:10809'
                }
                response = requests.get(url, params=params, timeout=30, proxies=proxies)
                if response.status_code == 200:
                    data = response.json()
                    
                    if data.get("code") == 0 and data.get("data"):
                        return self._parse_kline_data(data["data"])
                    else:
                        self.logger.error(f"API返回错误: {data}")
                        return []
                else:
                    self.logger.error(f"HTTP请求失败: {response.status_code}")
                    return []
            except ImportError:
                # 如果没有requests，使用aiohttp
                # 配置代理
                connector = aiohttp.TCPConnector()
                proxy = 'http://127.0.0.1:10809'
                async with aiohttp.ClientSession(connector=connector) as session:
                    async with session.get(url, params=params, proxy=proxy) as response:
                        if response.status == 200:
                            data = await response.json()
                            
                            if data.get("code") == 0 and data.get("data"):
                                return self._parse_kline_data(data["data"])
                            else:
                                self.logger.error(f"API返回错误: {data}")
                                return []
                        else:
                            self.logger.error(f"HTTP请求失败: {response.status}")
                            return []
                        
        except Exception as e:
            self.logger.error(f"获取历史数据失败: {e}")
            return []
    
    def _parse_kline_data(self, data: Dict[str, Any]) -> List[Dict]:
        """解析K线数据"""
        try:
            # 字段映射 - 根据实际API响应调整
            field_mapping = {
                "日期": "timestamp",
                "开盘": "open",
                "收盘": "close", 
                "最高": "high",
                "最低": "low",
                "成交量": "volume",
                "成交额": "amount",
                "振幅": "amplitude",
                "涨跌幅": "change_pct",
                "涨跌额": "change_amount",
                "换手率": "turnover_rate"
            }
            
            # 获取字段列表和数据列表
            fields = data.get("fields", [])
            kline_list = data.get("list", [])  # 注意：实际返回的是"list"而不是"列表"
            
            if not kline_list:
                return []
            
            # 转换数据
            result = []
            for kline in kline_list:
                bar_data = {}
                for i, field in enumerate(fields):
                    if i < len(kline):
                        mapped_field = field_mapping.get(field, field)
                        value = kline[i]
                        
                        # 转换数据类型
                        if mapped_field in ["open", "close", "high", "low", "volume", "amount", 
                                          "amplitude", "change_pct", "change_amount", "turnover_rate"]:
                            try:
                                value = float(value) if value else 0.0
                            except:
                                value = 0.0
                        
                        bar_data[mapped_field] = value
                
                result.append(bar_data)
            
            return result
            
        except Exception as e:
            self.logger.error(f"解析K线数据失败: {e}")
            return []


class ETF159506DataProcessor:
    """159506 ETF数据处理器"""
    
    def __init__(self, stock_code: str = "159506", data_saver=None):
        self.stock_code = stock_code
        self.data_saver = data_saver
        self.logger = logging.getLogger("ETF159506DataProcessor")
        self.total_processed = 0
        self.start_time = datetime.now()
        self.last_volume = 0
        self.last_date = None  # 用于检测交易日变化
        self.is_initialized = False  # 用于处理程序启动时的第一条数据
        
        # 尝试从catalog恢复last_volume（如果可能）
        self._load_last_volume_from_catalog()
    
    def _load_last_volume_from_catalog(self):
        """从catalog恢复最后的累计成交量（用于程序重启后继续）"""
        try:
            if not self.data_saver or not hasattr(self.data_saver, 'catalog') or not self.data_saver.catalog:
                self.logger.info("📊 数据保存器未初始化，last_volume使用默认值0")
                return
            
            from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue
            
            # 查询今天的TradeTick数据
            instrument_id = InstrumentId(Symbol(self.stock_code), Venue("SZSE"))
            today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            
            try:
                # 查询今天的trade_ticks
                trade_ticks = self.data_saver.catalog.trade_ticks(
                    instrument_ids=[instrument_id],
                    start=int(today_start.timestamp() * 1e9),
                    end=None
                )
                
                if trade_ticks and len(trade_ticks) > 0:
                    # 计算今天已保存的所有TradeTick的成交量总和
                    total_volume = sum(tick.size.as_double() for tick in trade_ticks)
                    self.last_volume = total_volume
                    self.is_initialized = True
                    self.logger.info(f"✅ 从catalog恢复last_volume: {self.last_volume:,.0f} (基于{len(trade_ticks)}条TradeTick)")
                else:
                    self.logger.info("📊 catalog中无今日数据，last_volume使用默认值0")
                    
            except Exception as e:
                self.logger.debug(f"查询catalog数据失败: {e}")
                
        except Exception as e:
            self.logger.error(f"从catalog恢复last_volume失败: {e}")
        
    def process_level1_data(self, data: str) -> Optional[Dict]:
        """处理Level1数据"""
        try:
            # 解析Level1数据
            parts = data.split('=')
            if len(parts) != 2:
                return None
            
            code_part = parts[0]
            data_part = parts[1]
            
            if not code_part.startswith('lv1_'):
                return None
            
            stock_code = code_part[4:]
            if stock_code != self.stock_code:
                return None
            
            # 解析数据字段
            fields = data_part.split(',')
            if len(fields) < 6:
                return None
            
            push_time = fields[0]
            stock_name = fields[1]
            latest_price = float(fields[2])
            change_percent = float(fields[3])
            turnover = float(fields[4])
            volume = float(fields[5])
            
            # 解析买卖五档数据
            bid_asks = self._parse_bid_ask_data(fields[6:])
            
            # 准备数据
            quote_data = {
                'timestamp': push_time,
                'stock_code': stock_code,
                'stock_name': stock_name,
                'price': latest_price,
                'change_percent': change_percent,
                'turnover': turnover,
                'volume': volume,
                'bid_asks': bid_asks,
                'collect_time': datetime.now().isoformat(),
                'raw_websocket_data': data  # 添加原始WebSocket数据
            }
            
            self.total_processed += 1
            
            # 尝试保存数据到数据保存器
            if self.data_saver:
                try:
                    # 转换为NautilusTrader格式并保存
                    self.logger.debug(f"🔍 准备保存数据: {quote_data.get('timestamp')}, price={quote_data.get('price')}")
                    self._save_to_data_saver(self.data_saver, quote_data)
                except Exception as save_error:
                    self.logger.error(f"❌ 保存数据到数据保存器失败: {save_error}")
            else:
                self.logger.error("❌ 数据保存器未初始化")
            
            return quote_data
            
        except Exception as e:
            self.logger.error(f"处理Level1数据失败: {e}")
            return None
    
    def _parse_bid_ask_data(self, fields: List[str]) -> Dict:
        """解析买卖五档数据94300,1.620,72600,1.619,148300,1.618,53100,1.617,50500,1.616,387600,1.621,35500,1.622,549100,1.623,303800,1.624,560700,1.625
        数据格式：数量,价格,数量,价格...（前10个字段是买五档，后10个字段是卖五档）
        """
        try:
            bid_asks = {'bids': [], 'asks': []}
            
            # 买五档（前10个字段：数量,价格,数量,价格...）
            for i in range(0, 10, 2):
                if i + 1 < len(fields):
                    volume = float(fields[i])     # 偶数索引：数量
                    price = float(fields[i + 1])   # 奇数索引：价格
                    if price > 0 and volume > 0:
                        bid_asks['bids'].append({'price': price, 'volume': volume})
            
            # 卖五档（后10个字段：数量,价格,数量,价格...）
            for i in range(10, 20, 2):
                if i + 1 < len(fields):
                    volume = float(fields[i])     # 偶数索引：数量
                    price = float(fields[i + 1])  # 奇数索引：价格
                    if price > 0 and volume > 0:
                        bid_asks['asks'].append({'price': price, 'volume': volume})
            
            return bid_asks
            
        except Exception as e:
            self.logger.error(f"解析买卖五档数据失败: {e}")
            return {'bids': [], 'asks': []}
    
    def _save_to_data_saver(self, data_saver, quote_data: Dict):
        """保存数据到数据保存器 - 同时保存QuoteTick和TradeTick"""
        try:
            from nautilus_trader.model.data import QuoteTick, TradeTick
            from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue, TradeId
            from nautilus_trader.model.objects import Price, Quantity
            from nautilus_trader.model.enums import AggressorSide
            import time
            
            # 创建InstrumentId
            instrument_id = InstrumentId(Symbol(self.stock_code), Venue("SZSE"))
            
            # 获取买卖价格和数量
            bid_asks = quote_data.get('bid_asks', {'bids': [], 'asks': []})
            
            # 检查是否有有效的买卖数据
            if not bid_asks['bids'] or not bid_asks['asks']:
                self.logger.debug(f"⚠️ 买卖数据为空，跳过保存: bids={len(bid_asks['bids'])}, asks={len(bid_asks['asks'])}")
                return
            
            # 确保价格精度一致（都使用3位小数）
            bid_price_value = round(float(bid_asks['bids'][0]['price']), 3)
            ask_price_value = round(float(bid_asks['asks'][0]['price']), 3)
            
            bid_price = Price.from_str(f"{bid_price_value:.3f}")
            ask_price = Price.from_str(f"{ask_price_value:.3f}")
            bid_size = Quantity.from_int(int(bid_asks['bids'][0]['volume']))
            ask_size = Quantity.from_int(int(bid_asks['asks'][0]['volume']))
        
            
            # 使用服务器返回的时间戳（jvquant格式: HH:MM:SS）
            server_timestamp = quote_data.get('timestamp', '')
            try:
                if ':' in server_timestamp and len(server_timestamp.split(':')) == 3:
                    # jvquant标准格式: HH:MM:SS
                    time_parts = server_timestamp.split(':')
                    hours, minutes, seconds = map(int, time_parts)
                    
                    # 使用当前日期 + 服务器时间
                    today = datetime.now().date()
                    server_datetime = datetime.combine(today, datetime.min.time().replace(
                        hour=hours, minute=minutes, second=seconds
                    ))
                    current_time_ns = int(server_datetime.timestamp() * 1e9)
                    
                    self.logger.debug(f"✅ 使用服务器时间戳: {server_timestamp} -> {server_datetime}")
                else:
                    # 无法解析，使用本地时间
                    current_time_ns = int(time.time() * 1e9)
                    self.logger.debug(f"⚠️ 无法解析时间戳格式: {server_timestamp}, 使用本地时间")
            except Exception as e:
                # 解析失败，使用本地时间
                self.logger.debug(f"❌ 解析服务器时间戳失败: {e}, 使用本地时间")
                current_time_ns = int(time.time() * 1e9)
            
            # 1. 创建并保存QuoteTick
            quote_tick = QuoteTick(
                instrument_id=instrument_id,
                bid_price=bid_price,
                ask_price=ask_price,
                bid_size=bid_size,
                ask_size=ask_size,
                ts_event=current_time_ns,
                ts_init=current_time_ns
            )
            data_saver.add_quote_tick(quote_tick)
            self.logger.debug(f"✅ 已添加QuoteTick到缓冲区: bid={bid_price}, ask={ask_price}")
            
            # 2. 创建并保存TradeTick（只有当成交量有变化时才保存）
            latest_price = quote_data.get('price', 0)
            volume = quote_data.get('volume', 0)
            
            # 只有当有成交量和价格变化时才创建TradeTick
            if volume > 0 and latest_price > 0:
                # 【改进1】检测交易日变化 - 新的一天重置last_volume
                current_date = datetime.now().date()
                if self.last_date is None:
                    self.last_date = current_date
                    self.logger.info(f"📅 初始化交易日: {current_date}")
                elif current_date != self.last_date:
                    self.logger.info(f"📅 检测到新交易日: {self.last_date} -> {current_date}")
                    self.logger.info(f"   重置last_volume: {self.last_volume:,.0f} -> 0")
                    self.last_volume = 0
                    self.last_date = current_date
                    self.is_initialized = False
                
                # 【改进2】检测成交量回退 - 可能是新交易日或程序重启
                if volume < self.last_volume:
                    self.logger.warning(f"⚠️  检测到成交量回退: {self.last_volume:,.0f} -> {volume:,.0f}")
                    self.logger.warning(f"   可能原因: 新交易日开始或数据源重置")
                    self.logger.warning(f"   重置last_volume为0")
                    self.last_volume = 0
                    self.is_initialized = False
                
                # 【改进3】处理程序启动时的第一条数据
                if not self.is_initialized:
                    # 第一条数据：直接设置last_volume，不创建TradeTick
                    self.last_volume = volume
                    self.is_initialized = True
                    self.logger.info(f"🔧 初始化成交量基准: {volume:,.0f} (跳过第一条TradeTick)")
                else:
                    # 计算成交量变化（增量）
                    volume_change = volume - self.last_volume
                    
                    # 只有当成交量有增量变化时才创建TradeTick
                    if volume_change > 0:
                        # 创建TradeTick
                        trade_tick = TradeTick(
                            instrument_id=instrument_id,
                            price=Price.from_str(f"{latest_price:.3f}"),
                            size=Quantity.from_int(int(volume_change)),
                            aggressor_side=AggressorSide.NO_AGGRESSOR,  # Level1数据无法确定主动方
                            trade_id=TradeId(f"{self.stock_code}_{current_time_ns}"),
                            ts_event=current_time_ns,
                            ts_init=current_time_ns
                        )
                        data_saver.add_trade_tick(trade_tick)
                        self.logger.debug(f"✅ 已添加TradeTick: price={latest_price}, size={volume_change:,.0f} (累计={volume:,.0f})")
                        
                        # 更新最后成交量
                        self.last_volume = volume
                    elif volume_change < 0:
                        # 理论上不应该出现，因为前面已经检测过回退
                        self.logger.warning(f"⚠️  成交量异常减少: {self.last_volume:,.0f} -> {volume:,.0f}, 跳过")
                    # volume_change == 0 时不做任何操作（成交量未变化）
            
        except Exception as e:
            self.logger.debug(f"转换并保存数据失败: {e}")


class ETF159506WebSocketClient:
    """159506 ETF WebSocket客户端 - 基于jvquant平台"""
    
    def __init__(self, config: Dict, data_saver=None):
        self.config = config
        self.logger = logging.getLogger("ETF159506WebSocketClient")
        self.token = config.get('token')
        self.stock_code = config.get('stock_code', '159506')
        self.server_manager = ETF159506ServerManager(self.token) if self.token else None
        self.data_processor = ETF159506DataProcessor(self.stock_code, data_saver)
        
        # WebSocket相关
        self.ws = None
        self.is_connected = False
        self.callbacks = {}
        
        # 线程控制
        self.ws_thread = None
        self.connection_count = 0
        self.data_receive_count = 0
        self.last_data_time = None
        
    async def connect(self) -> bool:
        """连接WebSocket"""
        try:
            if not self.token:
                self.logger.error("缺少token配置")
                return False
            
            # 获取服务器地址
            server = self.server_manager.get_server("ab", "websocket")
            if not server:
                self.logger.error("无法获取服务器地址")
                return False
            
            # 构建WebSocket URL
            if server.startswith('ws://'):
                ws_url = f"{server}/?token={self.token}"
            else:
                ws_url = f"ws://{server}/?token={self.token}"
            
            self.logger.info(f"连接到WebSocket服务器: {ws_url}")
            
            # 创建WebSocket连接
            self.ws = websocket.WebSocketApp(
                ws_url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close
            )
            
            # 在后台线程中运行WebSocket连接
            self.ws_thread = threading.Thread(target=self.ws.run_forever)
            self.ws_thread.daemon = True
            self.ws_thread.start()
            
            # 等待连接建立或超时
            timeout = 15
            start_time = time.time()
            while not self.is_connected and (time.time() - start_time) < timeout:
                await asyncio.sleep(0.1)
            
            if self.is_connected:
                self.logger.info("WebSocket连接成功建立")
                return True
            else:
                self.logger.error("WebSocket连接超时")
                return False
                
        except Exception as e:
            self.logger.error(f"WebSocket连接失败: {e}")
            return False
    
    async def disconnect(self) -> None:
        """断开WebSocket连接"""
        if self.ws:
            try:
                self.ws.close()
            except:
                pass
            self.ws = None
        
        if self.ws_thread and self.ws_thread.is_alive():
            try:
                self.ws_thread.join(timeout=2)
            except:
                pass
        
        self.is_connected = False
        self.logger.info("WebSocket客户端已断开")
    
    def subscribe_quotes(self, symbol: str, callback) -> None:
        """订阅报价数据"""
        self.callbacks[f'quote_{symbol}'] = callback
        self.logger.info(f"已订阅{symbol}报价数据")
    
    def subscribe_trades(self, symbol: str, callback) -> None:
        """订阅成交数据"""
        self.callbacks[f'trade_{symbol}'] = callback
        self.logger.info(f"已订阅{symbol}成交数据")
    
    def _on_open(self, ws):
        """连接打开回调"""
        self.connection_count += 1
        self.is_connected = True
        self.logger.info(f"WebSocket连接已建立 (第{self.connection_count}次连接)")
        
        # 订阅数据
        subscription = f"add=lv1_{self.stock_code}"
        ws.send(subscription)
        self.logger.info(f"已订阅: {subscription}")
    
    def _on_message(self, ws, message, *args):
        """接收消息回调"""
        try:
            if isinstance(message, str):
                if message.startswith('-1#'):
                    self.logger.error(f"收到服务器错误消息: {message}")
                    if "账户连接数已达并发上限" in message:
                        self.logger.error("⚠️ 账户连接数已达并发上限！")
                        ws.close()
                        return
                elif message.startswith('lv1_'):
                    # 处理Level1数据
                    self.data_receive_count += 1
                    self.last_data_time = datetime.now()
                    
                    # 处理数据
                    quote_data = self.data_processor.process_level1_data(message)
                    if quote_data and f'quote_{self.stock_code}' in self.callbacks:
                        # 同步调用回调函数（WebSocket在非异步线程中运行）
                        try:
                            loop = asyncio.get_event_loop()
                            if loop.is_running():
                                asyncio.create_task(self._call_callback(f'quote_{self.stock_code}', quote_data))
                            else:
                                # 如果没有运行的事件循环，直接调用回调
                                callback = self.callbacks[f'quote_{self.stock_code}']
                                if asyncio.iscoroutinefunction(callback):
                                    # 创建新的事件循环来运行协程
                                    asyncio.run(callback(quote_data))
                                else:
                                    callback(quote_data)
                        except RuntimeError:
                            # 如果没有事件循环，直接调用回调
                            callback = self.callbacks[f'quote_{self.stock_code}']
                            if not asyncio.iscoroutinefunction(callback):
                                callback(quote_data)
                        
            else:
                # 处理二进制数据
                try:
                    decompressed = zlib.decompress(message, -zlib.MAX_WBITS)
                    data_str = decompressed.decode("utf-8")
                    
                    self.data_receive_count += 1
                    self.last_data_time = datetime.now()
                    
                    lines = data_str.strip().split('\n')
                    for line in lines:
                        if line.strip() and line.startswith('lv1_'):
                            quote_data = self.data_processor.process_level1_data(line)
                            if quote_data and f'quote_{self.stock_code}' in self.callbacks:
                                # 同步调用回调函数（WebSocket在非异步线程中运行）
                                try:
                                    loop = asyncio.get_event_loop()
                                    if loop.is_running():
                                        asyncio.create_task(self._call_callback(f'quote_{self.stock_code}', quote_data))
                                    else:
                                        # 如果没有运行的事件循环，直接调用回调
                                        callback = self.callbacks[f'quote_{self.stock_code}']
                                        if asyncio.iscoroutinefunction(callback):
                                            # 创建新的事件循环来运行协程
                                            asyncio.run(callback(quote_data))
                                        else:
                                            callback(quote_data)
                                except RuntimeError:
                                    # 如果没有事件循环，直接调用回调
                                    callback = self.callbacks[f'quote_{self.stock_code}']
                                    if not asyncio.iscoroutinefunction(callback):
                                        callback(quote_data)
                                
                except Exception as decompress_error:
                    self.logger.error(f"解压缩数据失败: {decompress_error}")
                    
        except Exception as e:
            self.logger.error(f"处理消息失败: {e}")
    
    def _on_error(self, ws, error):
        """错误回调"""
        self.logger.error(f"WebSocket错误: {error}")
    
    def _on_close(self, ws, code, msg):
        """连接关闭回调"""
        self.is_connected = False
        self.logger.info(f"WebSocket连接已关闭: {code} - {msg}")
    
    async def _call_callback(self, callback_name: str, data: Dict):
        """异步调用回调函数"""
        try:
            callback = self.callbacks.get(callback_name)
            if callback:
                if asyncio.iscoroutinefunction(callback):
                    await callback(data)
                else:
                    callback(data)
        except Exception as e:
            self.logger.error(f"回调函数执行失败: {e}")


class ETF159506InstrumentProvider(InstrumentProvider):
    """159506 ETF工具提供者"""
    
    def __init__(self, http_client: ETF159506HttpClient):
        self.http_client = http_client
        self.logger = logging.getLogger("ETF159506InstrumentProvider")
        
        # 初始化基类
        super().__init__(InstrumentProviderConfig())
        
    async def load_all_async(self, filters: dict | None = None) -> None:
        """异步加载所有工具"""
        try:
            # 创建标准的NautilusTrader Equity工具
            from etf_159506_instrument import create_etf_159506_default
            
            # 创建159506 ETF工具
            etf_instrument = create_etf_159506_default()
            
            # 使用基类的add方法注册instrument
            self.add(etf_instrument)
            
            self.logger.info(f"已加载{self.count}个工具")
            self.logger.info(f"工具ID: {etf_instrument.id}")
            self.logger.info(f"工具类型: {type(etf_instrument)}")
            self.logger.info(f"工具价格精度: {etf_instrument.price_precision}")
            self.logger.info(f"工具最小价格变动: {etf_instrument.price_increment}")
            
            # 验证工具是否正确添加
            found_instrument = self.find(etf_instrument.id)
            if found_instrument:
                self.logger.info(f"✅ 工具验证成功: {found_instrument.id}")
            else:
                self.logger.error(f"❌ 工具验证失败: {etf_instrument.id}")
            
        except Exception as e:
            self.logger.error(f"加载工具失败: {e}")
            import traceback
            self.logger.error(f"错误详情: {traceback.format_exc()}")
    
    def find(self, instrument_id) -> Optional['Instrument']:
        """查找工具"""
        # 使用基类的find方法
        return super().find(instrument_id)
    
    def get_all(self) -> dict[InstrumentId, Instrument]:
        """获取所有工具"""
        # 返回基类的_instruments字典
        return super().get_all()


class ETF159506Instrument:
    """159506 ETF工具类"""
    
    def __init__(self, symbol: str, name: str, exchange: str, currency: str, 
                 tick_size: Decimal, lot_size: int):
        self.symbol = symbol
        self.name = name
        self.exchange = exchange
        self.currency = currency
        self.tick_size = tick_size
        self.lot_size = lot_size
        self.id = f"{symbol}.{exchange}"
    
    def __str__(self) -> str:
        return f"ETF159506Instrument({self.id})"
    
    def __repr__(self) -> str:
        return self.__str__()


class ETF159506Adapter:
    """159506 ETF适配器主类"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.logger = logging.getLogger("ETF159506Adapter")
        
        # 初始化组件
        self.http_client = ETF159506HttpClient(config)
        
        # 初始化数据保存器
        catalog_path = config.get('catalog_path', './data_catalog')
        self.data_saver = ETF159506DataSaver(catalog_path)
        
        # 创建WebSocket客户端，传入数据保存器
        self.ws_client = ETF159506WebSocketClient(config, self.data_saver)
        self.instrument_provider = ETF159506InstrumentProvider(self.http_client)
        # 注意：数据客户端和执行客户端现在都由NautilusTrader管理，不再在这里初始化
        
        # 初始化邮件通知器（可选）
        email_config = config.get('email_notification', {})
        if email_config and email_config.get('enabled', False):
            try:
                from email_notifier import EmailNotifier
                self.email_notifier = EmailNotifier(email_config)
                self.logger.info("邮件通知功能已启用")
            except Exception as e:
                self.logger.warning(f"邮件通知功能初始化失败: {e}")
                self.email_notifier = None
        else:
            self.email_notifier = None
            self.logger.info("邮件通知功能未启用")
        
        self.is_connected = False
        self.connection_attempts = 0
        self.max_connection_attempts = 1
        
    async def connect(self) -> bool:
        """连接适配器"""
        try:
            # 检查连接次数
            if self.connection_attempts >= self.max_connection_attempts:
                self.logger.error(f"连接尝试次数已达上限({self.max_connection_attempts})，请检查网络或等待一段时间后重试")
                return False
            
            self.connection_attempts += 1
            self.logger.info(f"第{self.connection_attempts}次尝试连接适配器...")
            
            # 连接基础组件（数据客户端和执行客户端现在都由NautilusTrader管理）
            # 连接HTTP客户端和WebSocket客户端
            http_connected = await self.http_client.connect()
            ws_connected = await self.ws_client.connect()
            
            if http_connected and ws_connected:
                # 加载工具信息
                await self.instrument_provider.load_all_async()
                
                self.is_connected = True
                self.connection_attempts = 0  # 重置连接计数
                self.logger.info("159506 ETF适配器连接成功（基础组件）")
                return True
            else:
                self.logger.error("159506 ETF适配器连接失败")
                return False
                
        except Exception as e:
            self.logger.error(f"159506 ETF适配器连接失败: {e}")
            return False
    
    async def disconnect(self) -> None:
        """断开适配器"""
        await self.http_client.disconnect()
        await self.ws_client.disconnect()
        # 注意：数据客户端和执行客户端现在都由NautilusTrader管理，不需要在这里断开
        self.is_connected = False
        self.logger.info("159506 ETF适配器已断开（基础组件）")
    
    def get_instrument_provider(self) -> ETF159506InstrumentProvider:
        """获取工具提供者"""
        return self.instrument_provider
    
    # 注意：get_data_client和get_execution_client方法已移除，数据客户端和执行客户端现在都由NautilusTrader管理
    
    async def get_status(self) -> Dict:
        """获取适配器状态"""
        return {
            'adapter_name': 'ETF159506Adapter',
            'is_connected': self.is_connected,
            'connection_attempts': self.connection_attempts,
            'max_connection_attempts': self.max_connection_attempts,
            'instruments_count': self.instrument_provider.count,
            'http_client_connected': self.http_client.is_connected,
            # 注意：数据客户端和执行客户端状态现在都由NautilusTrader管理
        }
    
    async def reset_connection_attempts(self) -> None:
        """重置连接尝试次数"""
        self.connection_attempts = 0
        self.logger.info("连接尝试次数已重置")
    
    async def check_connection_health(self) -> Dict:
        """检查连接健康状态"""
        health_status = {
            'adapter_connected': self.is_connected,
            'http_client_connected': self.http_client.is_connected,
            'ws_client_connected': self.ws_client.is_connected,
            'connection_attempts': self.connection_attempts,
            'can_retry': self.connection_attempts < self.max_connection_attempts
        }
        
        if not health_status['can_retry']:
            health_status['suggestion'] = "连接尝试次数已达上限，建议等待一段时间后重试"
        
        return health_status


# 使用示例
async def main():
    """适配器使用示例 - 基于jvquant平台"""
    config = {
        'token': 'd0c519adcd47d266f1c96750d4e80aa6',  # 使用collector中的实际token
        'stock_code': '159506'
    }
    
    adapter = ETF159506Adapter(config)
    
    # 连接适配器
    if await adapter.connect():
        print("✅ 适配器连接成功")
        
        # 获取工具信息
        instruments = adapter.get_instrument_provider().get_all()
        print(f"📋 可用工具: {instruments}")
        
        # 获取历史数据 - 现在通过HTTP客户端直接获取
        print("\n📈 获取历史数据...")
        historical_data = await adapter.http_client.get_historical_data(
            symbol='159506',
            start_date='2024-01-01',
            end_date='2024-01-31',
            kline_type='day',
            fq='前复权',
            limit=5
        )
        
        if historical_data:
            print(f"✅ 历史数据获取成功: {len(historical_data)}条记录")
            latest_data = historical_data[0]
            print(f"   最新数据: {latest_data.get('timestamp')} 收盘价: {latest_data.get('close')}")
        else:
            print("❌ 历史数据获取失败")
        
        # 订阅实时数据
        print("\n📡 订阅实时数据...")
        
        # 数据统计
        data_count = 0
        last_price = None
        start_time = datetime.now()
        
        def quote_callback(data):
            nonlocal data_count, last_price
            data_count += 1
            current_price = data.get('price')
            volume = data.get('volume')
            timestamp = data.get('timestamp', 'N/A')
            raw_data = data.get('raw_websocket_data', 'N/A')
            
            # 计算价格变化
            price_change = ""
            if last_price is not None and current_price is not None:
                change = current_price - last_price
                change_pct = (change / last_price) * 100 if last_price > 0 else 0
                price_change = f" ({change:+.3f}, {change_pct:+.2f}%)"
            
            # 显示实时数据和原始WebSocket内容
            print(f"📊 [{data_count:04d}] {timestamp} | 价格: {current_price}{price_change} | 成交量: {volume}")
            print(f"🔗 WebSocket原始数据: {raw_data}")
            
            last_price = current_price
            
            # 每100条数据显示一次统计和数据保存状态
            if data_count % 100 == 0:
                runtime = datetime.now() - start_time
                # 现在通过WebSocket客户端的数据处理器获取统计信息
                save_stats = adapter.ws_client.data_processor.data_saver.get_data_save_stats()
                print(f"📈 统计: 已接收{data_count}条数据, 运行时间: {runtime}")
                print(f"💾 数据保存: QuoteTick {save_stats['quote_ticks']}条, TradeTick {save_stats['trade_ticks']}条")
                print(f"📊 总计: {save_stats['total_saved']}条, 缓冲区{save_stats['buffer_size']}条")
        
        # 订阅实时数据 - 现在通过WebSocket客户端直接订阅
        adapter.ws_client.subscribe_quotes('159506', quote_callback)
        
        
        # 获取状态
        status = await adapter.get_status()
        print(f"\n📊 适配器状态: {status}")
        
        # 生产环境：持续运行
        print("\n🚀 进入生产模式 - 持续获取实时数据...")
        print("💡 按 Ctrl+C 停止程序")
        
        try:
            # 持续运行，每30秒显示一次状态
            while True:
                await asyncio.sleep(30)
                
                # 显示状态信息
                current_status = await adapter.get_status()
                runtime = datetime.now() - start_time
                save_stats = adapter.ws_client.data_processor.data_saver.get_data_save_stats()
                
                print(f"\n📊 状态更新 [{runtime}] - 已接收数据: {data_count}条")
                print(f"   连接状态: {'✅ 正常' if current_status['is_connected'] else '❌ 断开'}")
                print(f"   HTTP客户端: {'✅ 正常' if current_status['http_client_connected'] else '❌ 断开'}")
                print(f"   数据保存: QuoteTick {save_stats['quote_ticks']}条, TradeTick {save_stats['trade_ticks']}条")
                print(f"   总计: {save_stats['total_saved']}条, 缓冲区{save_stats['buffer_size']}条")
                print(f"   保存路径: {save_stats['catalog_path']}")
                
                # 检查连接健康状态
                health = await adapter.check_connection_health()
                if not health['adapter_connected']:
                    print("⚠️  检测到连接问题，尝试重连...")
                    if await adapter.connect():
                        print("✅ 重连成功")
                    else:
                        print("❌ 重连失败")
                
        except KeyboardInterrupt:
            print("\n\n🛑 收到停止信号，正在关闭...")
        except Exception as e:
            print(f"\n❌ 运行时错误: {e}")
        finally:
            # 强制保存所有缓冲数据 - 现在通过WebSocket客户端的数据处理器
            print("\n💾 正在保存所有缓冲数据...")
            adapter.ws_client.data_processor.data_saver.force_save()
            
            # 断开连接
            await adapter.disconnect()
            print("✅ 适配器已断开连接")
            
            # 最终统计
            final_save_stats = adapter.ws_client.data_processor.data_saver.get_data_save_stats()
            print(f"📊 最终统计:")
            print(f"   总共接收: {data_count} 条数据")
            print(f"   已保存到磁盘: {final_save_stats['total_saved']} 条数据")
            print(f"   保存路径: {final_save_stats['catalog_path']}")
            print(f"   数据格式: Parquet (支持NautilusTrader)")
    else:
        print("❌ 适配器连接失败")


# =============================================================================
# NautilusTrader适配器集成
# =============================================================================

from nautilus_trader.live.data_client import LiveMarketDataClient
from nautilus_trader.live.factories import LiveDataClientFactory
from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.model.data import Bar, BarType, QuoteTick, TradeTick
from nautilus_trader.model.identifiers import InstrumentId, Venue, ClientId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.enums import BarAggregation, PriceType
from nautilus_trader.core.data import Data
from nautilus_trader.core.message import Event
from nautilus_trader.common.component import MessageBus, LiveClock
from nautilus_trader.cache.cache import Cache
from nautilus_trader.data.messages import SubscribeBars

# 全局变量存储适配器实例
_global_adapter_instance = None

def set_global_adapter(adapter):
    """设置全局适配器实例"""
    global _global_adapter_instance
    _global_adapter_instance = adapter

def get_global_adapter():
    """获取全局适配器实例"""
    return _global_adapter_instance


class ETF159506NautilusDataClient(LiveMarketDataClient):
    """159506 ETF NautilusTrader数据客户端包装器"""
    
    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        client_id,
        venue,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        instrument_provider: InstrumentProvider,
        config=None,
    ):
        super().__init__(
            loop=loop,
            client_id=client_id,
            venue=venue,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=instrument_provider,
            config=config,
        )
        
        # 强制使用全局适配器实例
        global_adapter = get_global_adapter()
        logger.info(f"全局适配器状态: {global_adapter is not None}")
        if global_adapter is not None:
            self.adapter = global_adapter
            logger.info(f"使用全局ETF159506适配器实例，连接状态: {self.adapter.is_connected}")
        else:
            logger.error("全局适配器未设置！这不应该发生。")
            raise RuntimeError("全局适配器未设置，请确保在创建TradingNode之前设置全局适配器")
        
        # 初始化instrument_id - 参考etf_159506_cache_collector.py
        from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue
        self.instrument_id = InstrumentId(
            symbol=Symbol("159506"),
            venue=Venue("SZSE")
        )
        
        # 配置catalog路径用于读取历史数据
        # 默认使用项目根目录下的data_catalog文件夹
        import os
        # 当前文件在quant目录下，所以只需要一层dirname
        project_root = os.path.dirname(os.path.abspath(__file__))
        self._catalog_path = os.path.join(project_root, "data_catalog")
        
        logger.info(f"配置catalog路径: {self._catalog_path}")
        
        # 初始化jvquant API相关组件（从ETF159506DataClient集成）
        self.config = global_adapter.config
        self.logger = logging.getLogger("ETF159506NautilusDataClient")
        self.http_client = ETF159506HttpClient(self.config)
        
        # 初始化数据保存器
        catalog_path = self.config.get('catalog_path', './data_catalog')
        self.data_saver = ETF159506DataSaver(catalog_path)
        
        # 创建WebSocket客户端，传入数据保存器
        self.ws_client = ETF159506WebSocketClient(self.config, self.data_saver)
        
        # 实时Bar成交量增量计算（匹配历史数据格式）
        self.last_minute_cumulative_volume = {}  # {bar_type: 上一分钟结束时的累计成交量}
        self.current_minute_start_volume = {}    # {bar_type: 当前分钟开始时的累计成交量}
        self.current_minute_key = {}             # {bar_type: 当前分钟标识"HH:MM"}
        
        # 1分钟Bar聚合器：从tick数据聚合成分钟Bar
        self.current_minute_bar_data = {}        # {bar_type: {open, high, low, close, volume, ts_start}}
        self.completed_minute_bar = {}           # {bar_type: Bar对象} 存储上一分钟完成的Bar
        
        self._set_connected(False)
        
    async def _connect(self) -> None:
        """连接到数据源 - 使用集成的jvquant连接方法"""
        try:
            logger.info("连接ETF159506合并数据客户端...")
            
            # 连接HTTP和WebSocket客户端（从ETF159506DataClient集成）
            http_connected = await self.http_client.connect()
            ws_connected = await self.ws_client.connect()
            
            if http_connected and ws_connected:
                self._set_connected(True)
                logger.info("ETF159506合并数据客户端连接成功")
            else:
                logger.error("ETF159506合并数据客户端连接失败")
            
        except Exception as e:
            logger.error(f"连接ETF159506合并数据客户端失败: {e}")
            raise
    
    async def _disconnect(self) -> None:
        """断开数据源连接 - 使用集成的jvquant断开方法"""
        try:
            logger.info("断开ETF159506合并数据客户端...")
            
            # 断开HTTP和WebSocket客户端（从ETF159506DataClient集成）
            await self.http_client.disconnect()
            await self.ws_client.disconnect()
            
            self._set_connected(False)
            logger.info("ETF159506合并数据客户端已断开")
            
        except Exception as e:
            logger.error(f"断开ETF159506合并数据客户端失败: {e}")
    
    async def _subscribe_bars(self, command: SubscribeBars) -> None:
        """订阅K线数据"""
        try:
            logger.info(f"订阅K线数据: {command.bar_type}")
            
            # 设置数据回调
            def bar_callback(data: Dict[str, Any]):
                try:
                    # 将适配器数据转换为NautilusTrader Bar对象
                    bar = self._convert_to_bar(data, command.bar_type)
                    if bar:
                        # 发送到消息总线
                        self._handle_bar(bar)
                except Exception as e:
                    logger.error(f"处理K线数据失败: {e}")
            
            # 订阅实时数据 - 使用我们初始化的instrument_id
            instrument_id = self.instrument_id
            symbol = instrument_id.symbol.value
            
            # 使用WebSocket客户端订阅数据
            self.ws_client.subscribe_quotes(symbol, bar_callback)
            
            logger.info(f"K线数据订阅成功: {command.bar_type}")
            
        except Exception as e:
            logger.error(f"订阅K线数据失败: {e}")
            raise
    
    async def _subscribe_quotes(self, instrument_id: InstrumentId) -> None:
        """订阅报价数据"""
        try:
            logger.info(f"订阅报价数据: {instrument_id}")
            
            def quote_callback(data: Dict[str, Any]):
                try:
                    # 将适配器数据转换为NautilusTrader QuoteTick对象
                    quote = self._convert_to_quote_tick(data, instrument_id)
                    if quote:
                        # 发送到消息总线
                        self._handle_quote_tick(quote)
                except Exception as e:
                    logger.error(f"处理报价数据失败: {e}")
            
            # 订阅实时数据
            symbol = instrument_id.symbol.value
            self.ws_client.subscribe_quotes(symbol, quote_callback)
            
            logger.info(f"报价数据订阅成功: {instrument_id}")
            
        except Exception as e:
            logger.error(f"订阅报价数据失败: {e}")
            raise
    
    def _convert_to_bar(self, data: Dict[str, Any], bar_type: BarType) -> Optional[Bar]:
        """将tick级别的实时数据聚合为1分钟Bar
        
        核心逻辑：
        1. 同一分钟内：聚合多个tick数据（更新OHLC和volume），返回None（不触发on_bar）
        2. 分钟切换时：返回上一分钟的完整Bar（触发on_bar），开始聚合新分钟
        3. 结果：on_bar每分钟只被调用1次
        
        注意：实时数据的volume是累计成交量，需要计算增量以匹配历史数据格式
        """
        try:
            from nautilus_trader.model.objects import Price, Quantity
            from nautilus_trader.core.datetime import dt_to_unix_nanos
            from datetime import time, datetime
            import pandas as pd
            
            # ====== 使用数据时间而不是系统时间 ======
            # 从data中获取时间戳（jvquant格式: "HH:MM:SS"）
            data_timestamp = data.get('timestamp', '')
            
            if not data_timestamp or ':' not in data_timestamp:
                # 如果没有数据时间戳，回退到系统时间（但记录警告）
                logger.warning(f"数据中缺少timestamp字段，回退到系统时间: {data}")
                current_time = pd.Timestamp.now(tz='UTC')
                beijing_time = current_time.tz_convert('Asia/Shanghai')
            else:
                # 解析数据时间戳 "HH:MM:SS"
                try:
                    time_parts = data_timestamp.split(':')
                    hours, minutes, seconds = map(int, time_parts)
                    
                    # 使用当前日期 + 数据时间构建完整时间戳
                    today = datetime.now().date()
                    data_datetime = datetime.combine(today, datetime.min.time().replace(
                        hour=hours, minute=minutes, second=seconds
                    ))
                    
                    # 转换为pandas Timestamp（假设数据时间是北京时间）
                    beijing_time = pd.Timestamp(data_datetime, tz='Asia/Shanghai')
                    current_time = beijing_time.tz_convert('UTC')
                    
                    logger.debug(f"使用数据时间: {data_timestamp} -> {beijing_time}")
                except Exception as e:
                    logger.warning(f"解析数据时间戳失败: {data_timestamp}, 错误: {e}, 回退到系统时间")
                    current_time = pd.Timestamp.now(tz='UTC')
                    beijing_time = current_time.tz_convert('Asia/Shanghai')
            
            current_time_only = beijing_time.time()
            
            # 午休时间：11:30-13:00，不处理数据
            if time(11, 30) <= current_time_only < time(13, 0):
                logger.debug(f"数据时间 {beijing_time.strftime('%H:%M:%S')} 在午休时间，跳过Bar聚合")
                return None
            
            # 获取价格和累计成交量
            price = data.get('price', 0)
            cumulative_volume = data.get('volume', 0)  # WebSocket原始数据是累计值
            
            # 确保是数字类型
            if isinstance(price, str):
                price = float(price)
            if isinstance(cumulative_volume, str):
                cumulative_volume = float(cumulative_volume)
            
            # 使用数据时间提取分钟标识
            minute_key = beijing_time.strftime('%H:%M')  # "09:30"
            
            # bar_type的字符串表示作为key
            bar_type_key = str(bar_type)
            
            # 获取instrument（用于创建Price和Quantity）
            instrument = self._instrument_provider.find(bar_type.instrument_id)
            if not instrument:
                instrument = self.cache.instrument(bar_type.instrument_id)
            if not instrument:
                raise RuntimeError(f"无法找到instrument: {bar_type.instrument_id}")
            
            # 创建当前价格对象
            price_obj = instrument.make_price(price)
            
            # 检测分钟变化
            is_new_minute = False
            old_minute = None  # 用于日志输出
            
            if bar_type_key not in self.current_minute_key:
                # 第一次接收数据 - 初始化第一个分钟
                is_new_minute = True
                self.current_minute_key[bar_type_key] = minute_key
                self.current_minute_start_volume[bar_type_key] = cumulative_volume
                self.last_minute_cumulative_volume[bar_type_key] = cumulative_volume
                
                # 创建第一分钟的Bar数据结构（包含start_volume）
                self.current_minute_bar_data[bar_type_key] = {
                    'open': price,
                    'high': price,
                    'low': price,
                    'close': price,
                    'ts_start': dt_to_unix_nanos(current_time),
                    'start_volume': cumulative_volume  # 保存起始累计量
                }
                logger.info(f"[Bar聚合] 首次初始化: {minute_key} (数据时间: {data_timestamp}), 起始累计量={cumulative_volume:,.0f}")
                
            elif self.current_minute_key[bar_type_key] != minute_key:
                # 进入新的一分钟
                old_minute = self.current_minute_key[bar_type_key]
                is_new_minute = True
                
                # 关键修复：使用上一次的累计量作为新分钟的起始
                # 这样可以避免丢失分钟边界的数据
                last_volume = self.last_minute_cumulative_volume.get(bar_type_key, cumulative_volume)
                
                logger.info(f"[Bar聚合] 分钟切换: {old_minute} -> {minute_key} (数据时间: {data_timestamp})")
                logger.info(f"  上次累计量: {last_volume:,.0f}")
                logger.info(f"  当前累计量: {cumulative_volume:,.0f}")
                logger.info(f"  边界增量: {cumulative_volume - last_volume:,.0f} (将计入新分钟)")
                
                # 更新分钟key和起始累计量
                self.current_minute_key[bar_type_key] = minute_key
                self.current_minute_start_volume[bar_type_key] = last_volume  # 使用上次累计量
                
            # 更新last_minute_cumulative_volume（每次tick都更新）
            self.last_minute_cumulative_volume[bar_type_key] = cumulative_volume
            
            # 变量用于保存将要返回的Bar（如果有）
            bar_to_return = None
            
            # 如果是新分钟，先完成上一分钟的Bar
            if is_new_minute and old_minute is not None:
                # 只有在非首次初始化时，才有上一分钟的Bar可以完成
                old_bar_data = self.current_minute_bar_data.get(bar_type_key)
                if old_bar_data:
                    # 计算上一分钟的成交量
                    # 在分钟切换前，last_volume还保存着上一分钟最后一个tick的累计量
                    # old_minute_start_volume是上一分钟开始时的累计量
                    # 注意：这里的last_volume在line 1552已经被获取了
                    old_minute_start = old_bar_data.get('start_volume', 0)  # 上一分钟开始时的累计量
                    old_minute_end = last_volume  # 上一分钟结束时的累计量（line 1552的last_volume）
                    
                    old_minute_volume = old_minute_end - old_minute_start
                    if old_minute_volume < 0:
                        old_minute_volume = 0
                    
                    # 创建上一分钟的完整Bar
                    bar_to_return = Bar(
                        bar_type=bar_type,
                        open=instrument.make_price(old_bar_data['open']),
                        high=instrument.make_price(old_bar_data['high']),
                        low=instrument.make_price(old_bar_data['low']),
                        close=instrument.make_price(old_bar_data['close']),
                        volume=instrument.make_qty(int(old_minute_volume)),
                        ts_event=old_bar_data.get('ts_start', dt_to_unix_nanos(current_time)),
                        ts_init=dt_to_unix_nanos(current_time),
                    )
                    logger.info(f"[Bar完成] {old_minute}: O={old_bar_data['open']:.3f} H={old_bar_data['high']:.3f} L={old_bar_data['low']:.3f} C={old_bar_data['close']:.3f} V={int(old_minute_volume):,}")
                    logger.info(f"  → 将在下一个tick触发on_bar")
                
                # 初始化新分钟的Bar数据
                self.current_minute_bar_data[bar_type_key] = {
                    'open': price,
                    'high': price,
                    'low': price,
                    'close': price,
                    'ts_start': dt_to_unix_nanos(current_time),
                    'start_volume': last_volume  # 保存该分钟开始时的累计成交量
                }
                logger.debug(f"[Bar聚合] 开始新分钟: {minute_key}, 初始价格={price:.3f}")
                
            else:
                # 同一分钟内，更新OHLC（不返回Bar）
                bar_data = self.current_minute_bar_data.get(bar_type_key)
                if bar_data:
                    bar_data['high'] = max(bar_data['high'], price)
                    bar_data['low'] = min(bar_data['low'], price)
                    bar_data['close'] = price
                    logger.debug(f"[Bar聚合] 更新 {minute_key}: H={bar_data['high']:.3f} L={bar_data['low']:.3f} C={price:.3f}")
            
            # 返回完成的Bar（如果有），否则返回None
            # 同一分钟内返回None，不触发on_bar
            # 只有新分钟才返回上一分钟的完整Bar，触发on_bar
            return bar_to_return
            
        except Exception as e:
            logger.error(f"转换Bar数据失败: {e}")
            import traceback
            logger.error(f"详细错误: {traceback.format_exc()}")
            return None
    
    def _convert_to_quote_tick(self, data: Dict[str, Any], instrument_id: InstrumentId) -> Optional[QuoteTick]:
        """将适配器数据转换为NautilusTrader QuoteTick对象"""
        try:
            # 这里需要根据实际的数据格式进行转换
            # 假设data包含: bid_price, ask_price, bid_size, ask_size, timestamp
            
            from nautilus_trader.model.objects import Price, Quantity
            
            quote = QuoteTick(
                instrument_id=instrument_id,
                bid_price=Price.from_str(str(data.get('bid_price', 0))),
                ask_price=Price.from_str(str(data.get('ask_price', 0))),
                bid_size=Quantity.from_str(str(data.get('bid_size', 0))),
                ask_size=Quantity.from_str(str(data.get('ask_size', 0))),
                ts_event=datetime.now(),
                ts_init=datetime.now(),
            )
            
            return quote
            
        except Exception as e:
            logger.error(f"转换QuoteTick数据失败: {e}")
            return None
    
    def _handle_bar(self, bar: Bar) -> None:
        """处理Bar数据"""
        try:
            # 使用NautilusTrader官方方法发送数据
            self._handle_data(bar)
        except Exception as e:
            logger.error(f"处理Bar数据失败: {e}")
    
    def _handle_quote_tick(self, quote: QuoteTick) -> None:
        """处理QuoteTick数据"""
        try:
            # 使用NautilusTrader官方方法发送数据
            self._handle_data(quote)
        except Exception as e:
            logger.error(f"处理QuoteTick数据失败: {e}")
    
    async def _request_bars(self, request) -> None:
        """请求历史K线数据"""
        try:
            logger.info(f"请求历史K线数据: {request.bar_type}")
            
            # 打印request所有属性
            logger.info(f"Request对象类型: {type(request)}")
            
            # 使用dir()获取所有属性，然后过滤掉私有属性
            all_attrs = [attr for attr in dir(request) if not attr.startswith('_')]
            logger.info(f"Request所有公共属性: {all_attrs}")
            
            
            
            # 获取请求参数
            bar_type = request.bar_type
            start = request.start
            end = request.end
            limit = request.limit if hasattr(request, 'limit') else 0
            
            logger.info(f"请求参数 - bar_type: {bar_type}, start: {start}, end: {end}, limit: {limit}")
            
            # 检查价格类型
            if bar_type.spec.price_type != PriceType.LAST:
                logger.error(
                    f"Cannot request {bar_type} bars: "
                    f"only historical bars for LAST price type available from ETF159506 adapter",
                )
                return
            
            # 尝试从本地parquet文件读取历史数据
            bars = await self._load_bars_from_catalog(bar_type, start, end, limit)
            
            if bars:
                logger.info(f"从本地catalog加载了 {len(bars)} 条历史K线数据")
            else:
                logger.info(f"未找到历史K线数据，返回空数据")
            
            # 使用NautilusTrader官方方法处理数据
            self._handle_bars(
                bar_type=bar_type,
                bars=bars,  # 历史数据或空列表
                partial=None,  # 没有部分数据
                correlation_id=request.id,
                start=start,
                end=end,
                params=request.params if hasattr(request, 'params') else None,
            )
            
            logger.info(f"✅ _handle_bars调用完成，发送了 {len(bars)} 条历史数据")
                
        except Exception as e:
            logger.error(f"请求历史K线数据失败: {e}")
            raise
    
    async def _load_bars_from_catalog(self, bar_type, start, end, limit):
        """从本地parquet catalog加载历史K线数据 - 支持动态聚合"""
        try:
            # 检查是否有catalog路径配置
            catalog_path = getattr(self, '_catalog_path', None)
            if not catalog_path:
                logger.debug("未配置catalog路径，跳过本地数据加载")
                return []
            
            # 导入ParquetDataCatalog
            from nautilus_trader.persistence.catalog import ParquetDataCatalog
            
            # 创建catalog实例
            catalog = ParquetDataCatalog(catalog_path)
            
            # 2. 如果没找到bar数据，尝试从trade_tick聚合生成
            bars_list = await self._generate_bars_from_ticks(catalog, bar_type, start, end, limit)
            
            if bars_list:
                logger.info(f"🔄 从trade_tick聚合生成了 {len(bars_list)} 条K线数据")
                return bars_list
            else:
                logger.info(f"⚠️ 未找到历史K线数据，返回空数据")
                return []
            
        except Exception as e:
            logger.warning(f"从catalog加载历史数据失败: {e}")
            return []
    
    # ========== 从ETF159506DataClient集成的方法 ==========
    
    async def request_instrument(self, request) -> None:
        """请求工具信息"""
        try:
            instrument = self.instrument_provider.find(request.instrument_id)
            if instrument:
                # 这里应该调用回调函数返回工具信息
                self.logger.info(f"返回工具信息: {instrument}")
            else:
                self.logger.error(f"未找到工具: {request.instrument_id}")
                
        except Exception as e:
            self.logger.error(f"请求工具信息失败: {e}")
    
    def subscribe_quotes(self, symbol: str, callback) -> None:
        """订阅报价数据 - 同时保存QuoteTick和TradeTick"""
        def enhanced_callback(data):
            if callback:
                callback(data)
        
        self.ws_client.subscribe_quotes(symbol, enhanced_callback)
    
    def subscribe_trades(self, symbol: str, callback) -> None:
        """订阅成交数据"""
        def enhanced_callback(data):
            if callback:
                callback(data)
        
        self.ws_client.subscribe_trades(symbol, enhanced_callback)
    
    async def get_historical_data(self, symbol: str, start_date: str, end_date: str, 
                                 kline_type: str = "day", fq: str = "前复权", limit: int = 240) -> List[Dict]:
        """获取历史数据"""
        return await self.http_client.get_historical_data(symbol, start_date, end_date, kline_type, fq, limit)
    
    async def _generate_bars_from_ticks(self, catalog, bar_type, start, end, limit=0):
        """从tick数据生成K线数据 - 手动聚合trade tick为分钟bar"""
        try:
            from nautilus_trader.model.data import TradeTick, Bar
            from nautilus_trader.model.objects import Price, Quantity
            import pandas as pd
            from datetime import datetime
            
            logger.info(f"🔄 开始从tick数据生成K线: {bar_type}")
            
            # 获取instrument_id
            instrument_id = bar_type.instrument_id
            
            # 查询trade_tick数据
            try:
                logger.info(f"🔍 查询参数: instrument_id={instrument_id} (类型: {type(instrument_id)})")
                logger.info(f"🔍 时间范围: start={start}, end={end}")
                logger.info(f"🔍 catalog路径: {catalog.path}")
                
                # 检查catalog中是否有trade_tick数据
                try:
                    # 先查询所有trade_tick文件，不限制时间
                    all_files_query = catalog.query(
                        data_cls=TradeTick,
                        identifiers=[str(instrument_id)],
                    )
                    all_files = list(all_files_query) if all_files_query else []
                    logger.info(f"🔍 catalog中总共有 {len(all_files)} 条trade_tick记录")
                    
                    if all_files:
                        # 显示第一条记录的时间
                        first_tick = all_files[0]
                        logger.info(f"🔍 第一条trade_tick时间: {first_tick.ts_event}")
                except Exception as e:
                    logger.warning(f"⚠️ 查询所有trade_tick失败: {e}")
                
                # 使用catalog.query()查询trade_tick数据
                trade_ticks = catalog.query(
                    data_cls=TradeTick,
                    identifiers=[str(instrument_id)],  # 添加identifiers参数
                    start=start,
                    end=end,
                )
                
                ticks_list = list(trade_ticks) if trade_ticks else []
                logger.info(f"📊 查询到 {len(ticks_list)} 条trade_tick数据")
                    
            except Exception as e:
                logger.error(f"❌ 查询trade_tick数据失败: {e}")
                import traceback
                logger.error(f"详细错误: {traceback.format_exc()}")
                return []
            
            # 改进1: 添加空检查
            if not ticks_list:
                logger.warning("⚠️  没有查询到trade_tick数据，无法生成Bar")
                return []
            
            # 手动从trade tick合成分钟bar数据
            logger.info(f"🔄 开始手动合成分钟K线数据，共 {len(ticks_list)} 条tick")
            
            # 将tick数据转换为DataFrame进行聚合
            tick_data = []
            for tick in ticks_list:
                tick_data.append({
                    'timestamp': pd.to_datetime(tick.ts_event, unit='ns'),
                    'price': float(tick.price),
                    'volume': int(tick.size),  # TradeTick.size是增量成交量
                    'ts_event': tick.ts_event
                })
            
            df = pd.DataFrame(tick_data)
            df.set_index('timestamp', inplace=True)
            
            # 改进2: 修复FutureWarning - 使用'1min'而不是'1T'
            # 按1分钟时间窗口聚合数据
            resampled = df.resample('1min').agg({
                'price': ['first', 'max', 'min', 'last'],
                'volume': 'sum'  # 将增量累加得到该分钟的总成交量
            }).dropna()
            
            # 改进3: 检查聚合结果
            if resampled.empty:
                logger.warning("⚠️  聚合后没有数据，无法生成Bar")
                return []
            
            # 重命名列
            resampled.columns = ['open', 'high', 'low', 'close', 'volume']
            
            # 创建Bar对象列表
            bars = []
            for timestamp, row in resampled.iterrows():
                try:
                    # 创建Bar对象
                    bar = Bar(
                        bar_type=bar_type,
                        open=Price.from_str(f"{row['open']:.3f}"),
                        high=Price.from_str(f"{row['high']:.3f}"),
                        low=Price.from_str(f"{row['low']:.3f}"),
                        close=Price.from_str(f"{row['close']:.3f}"),
                        volume=Quantity.from_int(int(row['volume'])),  # 该分钟总成交量
                        ts_event=int(timestamp.timestamp() * 1_000_000_000),  # 纳秒
                        ts_init=int(timestamp.timestamp() * 1_000_000_000),
                    )
                    bars.append(bar)
                    
                except Exception as e:
                    logger.warning(f"转换K线数据失败: {e}, 数据: {row}")
                    continue
            
            logger.info(f"✅ 成功从tick数据生成 {len(bars)} 条分钟Bar对象")
            return bars
            
        except Exception as e:
            logger.error(f"❌ 从tick数据生成K线失败: {e}")
            import traceback
            logger.error(f"错误详情: {traceback.format_exc()}")
            return []
    
    def get_data_save_stats(self) -> dict:
        """获取数据保存统计信息 - 分别统计QuoteTick和TradeTick"""
        try:
            if not self.data_saver.catalog:
                return {
                    'quote_ticks': 0,
                    'trade_ticks': 0,
                    'total_saved': 0,
                    'buffer_size': len(self.data_saver.quote_buffer) + len(self.data_saver.trade_buffer),
                    'quote_buffer_size': len(self.data_saver.quote_buffer),
                    'trade_buffer_size': len(self.data_saver.trade_buffer),
                    'catalog_path': self.data_saver.catalog_path,
                    'last_save_time': self.data_saver.last_save_time
                }
            
            # 查询QuoteTick数据
            quote_data = self.data_saver.catalog.quote_ticks(
                instrument_ids=[self.data_saver.instrument_id],
                start=None,
                end=None
            )
            
            # 查询TradeTick数据
            trade_data = self.data_saver.catalog.trade_ticks(
                instrument_ids=[self.data_saver.instrument_id],
                start=None,
                end=None
            )
            
            quote_count = len(list(quote_data)) if quote_data else 0
            trade_count = len(list(trade_data)) if trade_data else 0
            
            return {
                'quote_ticks': quote_count,
                'trade_ticks': trade_count,
                'total_saved': quote_count + trade_count,
                'buffer_size': len(self.data_saver.quote_buffer) + len(self.data_saver.trade_buffer),
                'quote_buffer_size': len(self.data_saver.quote_buffer),
                'trade_buffer_size': len(self.data_saver.trade_buffer),
                'catalog_path': self.data_saver.catalog_path,
                'last_save_time': self.data_saver.last_save_time
            }
            
        except Exception as e:
            self.logger.error(f"获取数据保存统计信息失败: {e}")
            return {
                'quote_ticks': 0,
                'trade_ticks': 0,
                'total_saved': 0,
                'buffer_size': 0,
                'quote_buffer_size': 0,
                'trade_buffer_size': 0,
                'catalog_path': self.data_saver.catalog_path,
                'last_save_time': None,
                'error': str(e)
            }
    
    def force_save_data(self):
        """强制保存所有缓冲数据"""
        self.data_saver.force_save()




class ETF159506LiveDataClientFactory(LiveDataClientFactory):
    """159506 ETF数据客户端工厂"""
    
    @staticmethod
    def create(
        loop: asyncio.AbstractEventLoop,
        name: str,
        config,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
    ) -> ETF159506NautilusDataClient:
        """
        创建159506 ETF数据客户端
        
        Parameters
        ----------
        loop : asyncio.AbstractEventLoop
            事件循环
        name : str
            客户端名称
        config
            客户端配置
        msgbus : MessageBus
            消息总线
        cache : Cache
            缓存
        clock : LiveClock
            时钟
            
        Returns
        -------
        ETF159506NautilusDataClient
        """
        # 使用全局适配器的instrument_provider
        global_adapter = get_global_adapter()
        instrument_provider = global_adapter.instrument_provider
        logger.info(f"使用全局适配器的instrument_provider，已加载{instrument_provider.count}个工具")
        
        
        return ETF159506NautilusDataClient(
            loop=loop,
            client_id=ClientId(name),
            venue=config.venue if hasattr(config, 'venue') else None,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=instrument_provider,
            config=config,
        )


class ETF159506NautilusExecClient(LiveExecutionClient):
    """159506 ETF 合并的执行客户端 - 集成jvquant API和NautilusTrader接口"""
    
    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        client_id,
        venue,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        instrument_provider: InstrumentProvider,
        config=None,
    ):
        super().__init__(
            loop=loop,
            client_id=client_id,
            venue=venue,
            oms_type=OmsType.NETTING,  # 使用净额结算模式
            account_type=AccountType.CASH,  # 使用现金账户类型
            base_currency=None,  # 多币种账户，不指定基础货币
            instrument_provider=instrument_provider,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            config=config,
        )
        
        # 获取全局适配器配置
        global_adapter = get_global_adapter()
        if global_adapter is not None:
            jvquant_config = global_adapter.config
            logger.info(f"使用全局适配器配置初始化jvquant执行客户端")
        else:
            logger.error("全局适配器未设置！这不应该发生。")
            raise RuntimeError("全局适配器未设置，请确保在创建TradingNode之前设置全局适配器")
        
        # 初始化jvquant API相关属性
        self.config = jvquant_config
        self.logger = logging.getLogger("ETF159506NautilusExecClient")
        self.token = jvquant_config.get('token')
        self.http_client = ETF159506HttpClient(jvquant_config)
        self.orders = {}
        
        # 交易相关
        self.trade_server = None
        self.ticket = None
        self.ticket_expire = None
        self.is_logged_in = False
        
        # 交易账户信息（从config读取）
        self.trade_account = '541460031518'
        self.trade_password = '882200'
        
        self._set_connected(False)
        
        # 订单ID映射：NautilusTrader client_order_id -> jvquant order_id
        self._order_id_mapping = {}
        
        # 股票代码到名称的映射（可从配置读取）
        default_names = {
            '159506': '恒生医疗',
            # 可以添加更多股票映射
        }
        # 优先使用配置中的映射，否则使用默认映射
        self._instrument_names = jvquant_config.get('instrument_names', default_names)
        
        # 初始化邮件通知器（从全局适配器获取）
        if global_adapter and hasattr(global_adapter, 'email_notifier'):
            self.email_notifier = global_adapter.email_notifier
            if self.email_notifier:
                logger.info("执行客户端邮件通知功能已启用")
        else:
            self.email_notifier = None
        
        # 账户状态更新任务相关
        self._account_update_task = None
        self._account_update_interval = 10  # 每10秒更新一次账户状态
    
    # ========== jvquant API 方法 ==========
    
    async def _get_trade_server(self) -> bool:
        """获取交易服务器地址"""
        try:
            url = f"http://jvQuant.com/query/server?market=ab&type=trade&token={self.token}"
            response = requests.get(url, timeout=10)
            data = response.json()
            
            if data.get("code") == "0":
                self.trade_server = data.get("server")
                self.logger.info(f"获取交易服务器成功: {self.trade_server}")
                return True
            else:
                self.logger.error(f"获取交易服务器失败: {data}")
                return False
                
        except Exception as e:
            self.logger.error(f"获取交易服务器异常: {e}")
            return False
    
    async def login(self, account: str, password: str) -> bool:
        """登录交易柜台"""
        if not self.trade_server:
            self.logger.error("交易服务器地址未获取")
            return False
        
        try:
            # trade_server 已包含完整URL (如 http://121.43.57.182:21888)
            url = f"{self.trade_server}/login"
            params = {
                'token': self.token,
                'acc': account,
                'pass': password
            }
            
            self.logger.info(f"正在登录交易柜台: {account}")
            response = requests.get(url, params=params, timeout=10)
            data = response.json()
            
            if data.get("code") == "0":
                self.ticket = data.get("ticket")
                # expire是有效期秒数，需要转换为过期时间戳
                expire_seconds = data.get("expire")
                if expire_seconds:
                    expire_seconds = float(expire_seconds)
                    self.ticket_expire = time.time() + expire_seconds
                    self.logger.info(f"登录成功! 交易凭证: {self.ticket}")
                    self.logger.info(f"   凭证有效期: {expire_seconds}秒")
                else:
                    self.ticket_expire = None
                    self.logger.info(f"登录成功! 交易凭证: {self.ticket} (无过期时间)")
                
                self.is_logged_in = True
                return True
            else:
                self.logger.error(f"登录失败: {data}")
                return False
                
        except Exception as e:
            self.logger.error(f"登录异常: {e}")
            return False
    
    def _check_login_status(self) -> bool:
        """检查登录状态"""
        if not self.is_logged_in or not self.ticket:
            return False
        
        # 检查ticket是否过期
        if self.ticket_expire and time.time() > self.ticket_expire:
            self.logger.warning("交易凭证已过期，需要重新登录")
            self.is_logged_in = False
            self.ticket = None
            return False
        
        return True
    
    async def _buy_stock(self, code: str, name: str, price: float, volume: int) -> Optional[str]:
        """
        买入股票
        
        API参数规范：
        - type: 报单类别，买入为 'buy'
        - token: 用户认证token
        - ticket: 交易凭证
        - code: 证券代码
        - name: 证券名称
        - price: 委托价格
        - volume: 委托数量
        """
        try:
            url = f"{self.trade_server}/buy"
            params = {
                'type': 'buy',              # 报单类别：买入
                'token': self.token,        # 用户认证token
                'ticket': self.ticket,      # 交易凭证
                'code': code,               # 证券代码
                'name': name,               # 证券名称
                'price': str(price),        # 委托价格
                'volume': str(volume)       # 委托数量
            }
            
            self.logger.info(f"正在买入: {code} {name}, 价格: {price}, 数量: {volume}")
            response = requests.get(url, params=params, timeout=10)
            data = response.json()
            
            if data.get("code") == "0":
                order_id = data.get("order_id")
                self.logger.info(f"买入委托成功! 委托编号: {order_id}")
                
                # 记录订单
                self.orders[order_id] = {
                    'order_id': order_id,
                    'code': code,
                    'name': name,
                    'side': 'buy',
                    'price': price,
                    'volume': volume,
                    'status': 'submitted',
                    'submit_time': datetime.now().isoformat()
                }
                
                return order_id
            else:
                self.logger.error(f"买入委托失败: {data}")
                return None
                
        except Exception as e:
            self.logger.error(f"买入委托异常: {e}")
            return None
    
    async def _sell_stock(self, code: str, name: str, price: float, volume: int) -> Optional[str]:
        """
        卖出股票
        
        API参数规范：
        - type: 报单类别，卖出为 'sale'
        - token: 用户认证token
        - ticket: 交易凭证
        - code: 证券代码
        - name: 证券名称
        - price: 委托价格
        - volume: 委托数量
        """
        try:
            url = f"{self.trade_server}/sale"
            params = {
                'type': 'sale',             # 报单类别：卖出
                'token': self.token,        # 用户认证token
                'ticket': self.ticket,      # 交易凭证
                'code': code,               # 证券代码
                'name': name,               # 证券名称
                'price': str(price),        # 委托价格
                'volume': str(volume)       # 委托数量
            }
            
            self.logger.info(f"正在卖出: {code} {name}, 价格: {price}, 数量: {volume}")
            response = requests.get(url, params=params, timeout=10)
            data = response.json()
            
            if data.get("code") == "0":
                order_id = data.get("order_id")
                self.logger.info(f"卖出委托成功! 委托编号: {order_id}")
                
                # 记录订单
                self.orders[order_id] = {
                    'order_id': order_id,
                    'code': code,
                    'name': name,
                    'side': 'sell',
                    'price': price,
                    'volume': volume,
                    'status': 'submitted',
                    'submit_time': datetime.now().isoformat()
                }
                
                return order_id
            else:
                self.logger.error(f"卖出委托失败: {data}")
                return None
                
        except Exception as e:
            self.logger.error(f"卖出委托异常: {e}")
            return None
    
    async def _cancel_jvquant_order(self, order_id: str) -> bool:
        """
        取消jvquant订单（撤销委托）
        
        API参数规范：
        - token: 用户账户的认证token，用于验证请求权限
        - ticket: 交易凭证
        - order_id: 委托编号
        
        Returns
        -------
        bool
            True if 撤单成功, False otherwise
        """
        try:
            if not self._check_login_status():
                logger.error("未登录或登录已过期")
                return False
            
            url = f"{self.trade_server}/cancel"
            params = {
                'token': self.token,        # 用户认证token
                'ticket': self.ticket,      # 交易凭证
                'order_id': order_id        # 委托编号
            }
            
            logger.info(f"正在撤销委托: {order_id}")
            response = requests.get(url, params=params, timeout=10)
            data = response.json()
            
            if data.get("code") == "0":
                logger.info(f"撤单成功! {data.get('message', '')}")
                
                # 更新订单状态
                if order_id in self.orders:
                    self.orders[order_id]['status'] = 'cancelled'
                
                return True
            else:
                logger.error(f"撤单失败: {data}")
                return False
                
        except Exception as e:
            logger.error(f"撤单异常: {e}")
            return False
    
    async def _check_orders(self) -> Optional[List[Dict]]:
        """
        查询委托列表（查询交易）
        
        API参数规范：
        - token: 用户账户的认证token，用于验证请求权限
        - ticket: 交易凭证
        
        返回参数：
        - list: 交易列表
          - order_id: 委托编号
          - day: 委托日期
          - time: 委托时间
          - code: 证券代码
          - name: 证券名称
          - type: 委托类型
          - status: 委托状态
          - order_price: 委托价格
          - order_volume: 委托数量
          - deal_price: 成交价格
          - deal_volume: 成交数量
        
        Returns
        -------
        Optional[List[Dict]]
            委托列表，如果查询失败返回 None
        """
        try:
            if not self._check_login_status():
                logger.error("未登录或登录已过期")
                return None
            
            url = f"{self.trade_server}/check_order"
            params = {
                'token': self.token,        # 用户认证token
                'ticket': self.ticket,      # 交易凭证
            }
            
            logger.info("查询委托列表...")
            response = requests.get(url, params=params, timeout=10)
            data = response.json()
            
            if data.get("code") == "0":
                order_list = data.get('list', [])
                logger.info(f"查询到 {len(order_list)} 条委托记录")
                
                # 更新本地订单缓存
                for order_info in order_list:
                    order_id = order_info.get('order_id')
                    if order_id:
                        self.orders[order_id] = {
                            'order_id': order_id,
                            'day': order_info.get('day'),
                            'time': order_info.get('time'),
                            'code': order_info.get('code'),
                            'name': order_info.get('name'),
                            'type': order_info.get('type'),
                            'status': order_info.get('status'),
                            'order_price': float(order_info.get('order_price', 0)),
                            'order_volume': int(order_info.get('order_volume', 0)),
                            'deal_price': float(order_info.get('deal_price', 0)),
                            'deal_volume': int(order_info.get('deal_volume', 0)),
                        }
                
                return order_list
            else:
                logger.error(f"查询委托失败: {data.get('message', '')}")
                return None
                
        except Exception as e:
            logger.error(f"查询委托异常: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    async def _check_positions(self) -> Optional[Dict]:
        """
        查询持仓信息（查询持仓）
        
        API参数规范：
        - token: 用户账户的认证token，用于验证请求权限
        - ticket: 交易凭证
        
        返回参数：
        - total: 账户总资产
        - usable: 账户可用资金
        - day_earn: 账户当日盈亏
        - hold_earn: 账户持仓盈亏
        - hold_list: 账户持仓列表
          - code: 证券代码
          - name: 证券名称
          - hold_vol: 持仓数量
          - usable_vol: 可用数量
          - day_earn: 当日盈亏
          - hold_earn: 持仓盈亏
        
        Returns
        -------
        Optional[Dict]
            持仓信息字典，如果查询失败返回 None
        """
        try:
            if not self._check_login_status():
                logger.error("未登录或登录已过期")
                return None
            
            url = f"{self.trade_server}/check_hold"
            params = {
                'token': self.token,        # 用户认证token
                'ticket': self.ticket,      # 交易凭证
            }
            
            logger.info("查询持仓信息...")
            response = requests.get(url, params=params, timeout=10)
            data = response.json()
            
            if data.get("code") == "0":
                # 提取账户信息
                account_info = {
                    'total': float(data.get('total', 0)),           # 账户总资产
                    'usable': float(data.get('usable', 0)),         # 可用资金
                    'day_earn': float(data.get('day_earn', 0)),     # 当日盈亏
                    'hold_earn': float(data.get('hold_earn', 0)),   # 持仓盈亏
                    'hold_list': []
                }
                
                # 解析持仓列表
                hold_list = data.get('hold_list', [])
                for position in hold_list:
                    account_info['hold_list'].append({
                        'code': position.get('code'),
                        'name': position.get('name'),
                        'hold_vol': int(position.get('hold_vol', 0)),      # 持仓数量
                        'usable_vol': int(position.get('usable_vol', 0)),  # 可用数量
                        'day_earn': float(position.get('day_earn', 0)),    # 当日盈亏
                        'hold_earn': float(position.get('hold_earn', 0)),  # 持仓盈亏
                    })
                
                logger.info(f"查询持仓成功: 总资产={account_info['total']}, "
                          f"可用资金={account_info['usable']}, "
                          f"持仓数量={len(account_info['hold_list'])}")
                
                return account_info
            else:
                logger.error(f"查询持仓失败: {data.get('message', '')}")
                return None
                
        except Exception as e:
            logger.error(f"查询持仓异常: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    async def _update_account_state(self) -> None:
        """
        查询并更新账户状态到Nautilus Cache
        
        此方法负责：
        1. 从JVQuant API获取账户信息
        2. 构建AccountBalance对象
        3. 调用generate_account_state更新Cache
        """
        try:
            # 1. 查询账户信息
            account_info = await self._check_positions()
            if not account_info:
                self.logger.warning("无法获取账户信息，跳过账户状态更新")
                return
            
            # 2. 提取账户余额信息
            total = account_info['total']        # 总资产
            usable = account_info['usable']      # 可用资金
            locked = total - usable              # 冻结资金 = 总资产 - 可用资金
            
            # 3. 构建AccountBalance对象
            # 关键：free字段必须设置为usable（可用资金），这是策略中balance_free()返回的值
            balances = [
                AccountBalance(
                    total=Money(total, CNY),      # 总余额
                    locked=Money(locked, CNY),    # 冻结余额
                    free=Money(usable, CNY),      # ✅ 可用余额（最重要！）
                )
            ]
            
            # 4. 调用generate_account_state更新Cache
            self.generate_account_state(
                balances=balances,
                margins=[],                        # 现金账户，无保证金
                reported=True,                     # 数据来自券商API
                ts_event=self._clock.timestamp_ns(),
            )
            
            # 5. 记录日志
            self.logger.info(
                f"✅ 账户状态已更新: "
                f"总资产={total:.2f} CNY, "
                f"可用={usable:.2f} CNY, "
                f"冻结={locked:.2f} CNY"
            )
            
        except Exception as e:
            self.logger.error(f"❌ 更新账户状态失败: {e}")
            import traceback
            traceback.print_exc()
    
    async def _periodic_account_update(self) -> None:
        """
        定期更新账户状态的后台任务
        
        此任务在连接时启动，断开连接时自动取消
        每隔一定时间间隔（默认10秒）自动查询并更新账户状态
        """
        try:
            self.logger.info(f"✅ 账户状态定期更新任务已启动，更新间隔: {self._account_update_interval}秒")
            
            while self.is_connected:
                # 等待指定间隔
                await asyncio.sleep(self._account_update_interval)
                
                # 更新账户状态
                await self._update_account_state()
                
        except asyncio.CancelledError:
            self.logger.info("账户更新任务已取消")
            raise
        except Exception as e:
            self.logger.exception("账户更新任务异常", e)
    
    # ========== NautilusTrader 接口方法 ==========
    
    def _convert_nautilus_order_to_jvquant(self, order) -> Optional[Dict]:
        """将NautilusTrader订单转换为jvquant格式"""
        try:
            # 获取订单基本信息
            instrument_id = str(order.instrument_id)
            side = str(order.side).lower()
            quantity = int(order.quantity)
            
            # 解析股票代码（从159506.SZSE中提取159506）
            if '.' in instrument_id:
                code = instrument_id.split('.')[0]
            else:
                code = instrument_id
            
            # 动态获取股票名称
            name = self._get_instrument_name(code, order.instrument_id)
            
            # 确定委托价格
            price = 0.0  # 市价单价格为0
            if hasattr(order, 'price') and order.price is not None:
                price = float(order.price)
            
            # 确定买卖方向（转换为 API 要求的格式）
            # 官方 API: 买入用 'buy', 卖出用 'sale'
            if side == 'buy':
                order_type = 'buy'
            elif side == 'sell':
                order_type = 'sale'  # ← 注意：卖出是 'sale' 不是 'sell'
            else:
                logger.error(f"不支持的订单方向: {side}")
                return None
            
            # 构建jvquant订单格式（严格按照官方API参数规范）
            # 官方规范：type, token, ticket, code, name, price, volume
            jvquant_order = {
                'type': order_type,     # 报单类别：'buy' 或 'sale'
                'code': code,           # 证券代码
                'name': name,           # 证券名称（动态获取）
                'price': price,         # 委托价格
                'volume': quantity      # 委托数量
            }
            # 注意：token 和 ticket 在调用 API 时添加，不在这里
            
            logger.info(f"订单转换: {order} -> {jvquant_order}")
            return jvquant_order
            
        except Exception as e:
            logger.error(f"订单转换失败: {e}")
            return None
    
    def _get_instrument_name(self, code: str, instrument_id) -> str:
        """
        获取股票名称
        
        优先级：
        1. 从cache中查询instrument对象获取
        2. 从映射表获取
        3. 使用代码作为默认名称
        
        Parameters
        ----------
        code : str
            股票代码
        instrument_id
            NautilusTrader InstrumentId对象
            
        Returns
        -------
        str
            股票名称
        """
        try:
            # 方法1: 尝试从cache获取instrument
            if hasattr(self, 'cache') and self.cache:
                instrument = self.cache.instrument(instrument_id)
                if instrument:
                    # NautilusTrader的Equity可能没有name属性
                    # 但我们可以尝试从raw_symbol或其他属性获取
                    logger.debug(f"从cache获取到instrument: {instrument}")
            
            # 方法2: 从映射表获取
            if code in self._instrument_names:
                return self._instrument_names[code]
            
            # 方法3: 使用代码作为默认名称
            logger.warning(f"未找到代码{code}的名称映射，使用代码作为名称")
            return code
            
        except Exception as e:
            logger.error(f"获取股票名称失败: {e}, 使用代码作为默认名称")
            return code
        
    async def _connect(self) -> None:
        """连接到执行源 - 使用集成的jvquant连接方法"""
        try:
            logger.info("连接ETF159506合并执行客户端...")
            
            if not self.token:
                logger.error("缺少token配置")
                return
                
            # 获取交易服务器地址
            if not await self._get_trade_server():
                logger.error("无法获取交易服务器地址")
                return
                
            connected = await self.http_client.connect()
            if not connected:
                logger.error("ETF159506合并执行客户端连接失败")
                return
            
            logger.info(f"尝试自动登录交易柜台: {self.trade_account}")
            login_success = await self.login(self.trade_account, self.trade_password)
            
            if login_success:
                # 设置账户ID（格式：{venue}-{account_number}）
                # ✅ 修复: 使用venue而不是client_id，确保Portfolio能正确查找账户
                account_id = AccountId(f"{self.venue.value}-{self.trade_account}")
                self._set_account_id(account_id)
                self._set_connected(True)
                
                logger.info(f"✅ ETF159506执行客户端连接并登录成功")
                logger.info(f"   账户ID: {self.account_id}")
                logger.info(f"   交易凭证: {self.ticket}")
                if self.ticket_expire:
                    remaining = self.ticket_expire - time.time()
                    logger.info(f"   凭证剩余有效期: {remaining:.0f}秒")
                else:
                    logger.info(f"   凭证有效期: 无限制")
                
                # ✅ 初始化账户状态
                logger.info("📊 初始化账户状态...")
                await self._update_account_state()
                
                # ✅ 启动账户状态定期更新任务
                logger.info("🔄 启动账户状态定期更新任务...")
                self._account_update_task = self.create_task(
                    self._periodic_account_update()
                )
            else:
                logger.error("❌ 交易柜台登录失败，执行客户端无法使用")
                logger.warning("提示: 请检查trade_account和trade_password配置是否正确")
                # 不设置connected为True，因为没有ticket无法交易
                return
           
            
        except Exception as e:
            logger.error(f"连接ETF159506合并执行客户端失败: {e}")
            raise
    
    async def _disconnect(self) -> None:
        """断开执行客户端连接 - 使用集成的jvquant断开方法"""
        try:
            logger.info("断开ETF159506合并执行客户端...")
            
            # ✅ 取消账户状态更新任务
            if self._account_update_task is not None:
                logger.info("🛑 取消账户状态更新任务...")
                self._account_update_task.cancel()
                try:
                    await self._account_update_task
                except asyncio.CancelledError:
                    logger.info("账户更新任务已成功取消")
                except Exception as e:
                    logger.warning(f"取消账户更新任务时发生异常: {e}")
                finally:
                    self._account_update_task = None
            
            # 使用集成的jvquant断开方法
            await self.http_client.disconnect()
            self._set_connected(False)
            logger.info("ETF159506合并执行客户端已断开")
        except Exception as e:
            logger.error(f"断开ETF159506合并执行客户端失败: {e}")
    
    async def generate_order_status_reports(
        self,
        command: GenerateOrderStatusReports,
    ) -> list[OrderStatusReport]:
        """
        生成订单状态报告列表
        
        Parameters
        ----------
        command : GenerateOrderStatusReports
            生成订单状态报告的命令
            
        Returns
        -------
        list[OrderStatusReport]
            订单状态报告列表
        """
        reports = []
        
        # 遍历所有订单，生成状态报告
        for client_order_id, order_info in self.orders.items():
            try:
                # 创建订单状态报告
                report = OrderStatusReport(
                    account_id=command.account_id,
                    instrument_id=order_info.get('instrument_id'),
                    client_order_id=client_order_id,
                    venue_order_id=order_info.get('venue_order_id'),
                    order_side=order_info.get('order_side'),
                    order_type=order_info.get('order_type'),
                    time_in_force=order_info.get('time_in_force'),
                    order_status=order_info.get('order_status', OrderStatus.INITIALIZED),
                    quantity=order_info.get('quantity'),
                    filled_qty=order_info.get('filled_qty', Quantity.zero()),
                    avg_px=order_info.get('avg_px'),
                    last_px=order_info.get('last_px'),
                    currency=order_info.get('currency'),
                    report_id=UUID4(),
                    ts_accepted=order_info.get('ts_accepted'),
                    ts_triggered=order_info.get('ts_triggered'),
                    ts_last=order_info.get('ts_last'),
                    ts_init=self._clock.timestamp_ns(),
                )
                reports.append(report)
            except Exception as e:
                logger.error(f"生成订单状态报告失败 {client_order_id}: {e}")
                
        return reports
    
    async def _submit_order(self, command) -> None:
        """提交订单 - 使用集成的jvquant API"""
        try:
            logger.info(f"提交订单: {command.order}")
            
            # 检查执行客户端连接状态
            if not self.is_connected:
                logger.error("执行客户端未连接，无法提交订单")
                return
            
            # 将NautilusTrader订单转换为jvquant格式
            jvquant_order = self._convert_nautilus_order_to_jvquant(command.order)
            if not jvquant_order:
                logger.error("订单转换失败")
                return
            
            logger.info(f"转换后的jvquant订单: {jvquant_order}")
            
            # 提取订单参数
            order_type = jvquant_order.get('type')  # 'buy' 或 'sale'（报单类别）
            code = jvquant_order.get('code', '159506')
            name = jvquant_order.get('name', '华夏中证500ETF')
            price = jvquant_order.get('price', 0.0)
            volume = jvquant_order.get('volume', 0)
            
            # 根据 type 参数调用对应的 API
            # type='buy' 调用买入接口，type='sale' 调用卖出接口
            jvquant_order_id = None
            if order_type == 'buy':
                logger.info(f"调用买入接口: {code} {name}, 价格: {price}, 数量: {volume}")
                jvquant_order_id = await self._buy_stock(code, name, price, volume)
            elif order_type == 'sale':
                logger.info(f"调用卖出接口: {code} {name}, 价格: {price}, 数量: {volume}")
                jvquant_order_id = await self._sell_stock(code, name, price, volume)
            else:
                logger.error(f"不支持的报单类别: {order_type}")
                return
            
            if jvquant_order_id:
                logger.info(f"订单提交成功! jvquant订单ID: {jvquant_order_id}")
                
                # 建立订单ID映射
                nautilus_order_id = str(command.order.client_order_id)
                self._order_id_mapping[nautilus_order_id] = jvquant_order_id
                logger.info(f"订单ID映射: {nautilus_order_id} -> {jvquant_order_id}")
                
                # 发送综合邮件通知（订单 + 账户信息）
                if self.email_notifier:
                    try:
                        # 准备订单信息
                        notification_info = {
                            'code': code,
                            'name': name,
                            'type': order_type,  # 'buy' 或 'sale'
                            'price': price,
                            'volume': volume,
                            'order_id': jvquant_order_id
                        }
                        
                        # 查询账户信息（用于合并到邮件中）
                        account_info = None
                        try:
                            account_info = await self._check_positions()
                            if account_info:
                                logger.info(f"查询账户信息成功: 总资产={account_info.get('total')}")
                        except Exception as e:
                            logger.warning(f"查询账户信息失败（邮件将只包含订单信息）: {e}")
                        
                        # 发送综合通知邮件
                        self.email_notifier.send_order_with_account_notification(
                            notification_info,
                            account_info  # 传入账户信息（可能为None）
                        )
                        logger.info("📧 订单和账户综合邮件通知已发送")
                    except Exception as e:
                        logger.warning(f"发送邮件通知失败（不影响订单）: {e}")
                
            else:
                logger.error("订单提交失败")
            
        except Exception as e:
            logger.error(f"提交订单失败: {e}")
            raise
    
    async def _submit_order_list(self, command) -> None:
        """提交订单列表"""
        try:
            logger.info(f"提交订单列表: {command.order_list}")
            # TODO: 实现订单列表提交逻辑
            logger.info("订单列表提交成功（模拟）")
        except Exception as e:
            logger.error(f"提交订单列表失败: {e}")
            raise
    
    async def _modify_order(self, command: ModifyOrder) -> None:
        """
        修改订单 - JVQuant不支持订单修改
        
        JVQuant API不提供订单修改功能，需要先撤单再重新下单
        """
        self.logger.warning(
            f"JVQuant不支持订单修改，订单 {command.client_order_id} 修改请求被拒绝。"
            f"建议先撤单再重新下单"
        )
        
        # 生成订单修改拒绝事件
        self.generate_order_modify_rejected(
            strategy_id=command.strategy_id,
            instrument_id=command.instrument_id,
            client_order_id=command.client_order_id,
            venue_order_id=command.venue_order_id,
            reason="JVQuant不支持订单修改，请撤单后重新下单",
            ts_event=self._clock.timestamp_ns(),
        )
    
    async def _cancel_order(self, command) -> None:
        """取消订单 - 使用集成的jvquant API"""
        try:
            logger.info(f"取消订单: {command.order}")
            
            # 检查执行客户端连接状态
            if not self.is_connected:
                logger.error("执行客户端未连接，无法取消订单")
                return
            
            # 获取订单ID映射
            nautilus_order_id = str(command.order.client_order_id)
            jvquant_order_id = self._order_id_mapping.get(nautilus_order_id)
            
            if not jvquant_order_id:
                logger.error(f"未找到订单ID映射: {nautilus_order_id}")
                return
            
            logger.info(f"取消订单: NautilusTrader ID: {nautilus_order_id} -> jvquant ID: {jvquant_order_id}")
            
            # 调用集成的jvquant API取消订单
            success = await self._cancel_jvquant_order(jvquant_order_id)
            
            if success:
                logger.info(f"订单取消成功! jvquant订单ID: {jvquant_order_id}")
                # 从映射中移除已取消的订单
                self._order_id_mapping.pop(nautilus_order_id, None)
            else:
                logger.error(f"订单取消失败! jvquant订单ID: {jvquant_order_id}")
            
        except Exception as e:
            logger.error(f"取消订单失败: {e}")
            raise
    
    async def _cancel_all_orders(self, command: CancelAllOrders) -> None:
        """
        取消所有订单
        
        查询当前所有委托订单，然后逐个取消指定证券的订单
        """
        try:
            instrument_id = command.instrument_id
            logger.info(f"取消所有订单: {instrument_id}")
            
            # 检查连接状态
            if not self.is_connected:
                logger.error("执行客户端未连接，无法取消订单")
                return
            
            # 查询当前所有委托订单
            orders = await self._check_orders()
            if not orders:
                logger.info("没有需要取消的订单")
                return
            
            # 提取证券代码（如从'159506.SZSE'提取'159506'）
            code = str(instrument_id.symbol.value).split('.')[0]
            
            # 过滤出指定证券的未完成订单
            target_orders = [
                o for o in orders 
                if o.get('code') == code and o.get('status') not in ['已成', '已撤', '废单']
            ]
            
            if not target_orders:
                logger.info(f"证券 {code} 没有需要取消的订单")
                return
            
            logger.info(f"找到 {len(target_orders)} 个待取消订单")
            
            # 逐个取消订单
            success_count = 0
            failed_count = 0
            
            for order in target_orders:
                order_id = order.get('order_id')
                if order_id:
                    try:
                        success = await self._cancel_jvquant_order(order_id)
                        if success:
                            success_count += 1
                            logger.info(f"✅ 订单 {order_id} 取消成功")
                        else:
                            failed_count += 1
                            logger.warning(f"❌ 订单 {order_id} 取消失败")
                    except Exception as e:
                        failed_count += 1
                        logger.error(f"❌ 取消订单 {order_id} 异常: {e}")
            
            logger.info(
                f"取消所有订单完成: 成功 {success_count}/{len(target_orders)}, "
                f"失败 {failed_count}/{len(target_orders)}"
            )
            
        except Exception as e:
            logger.error(f"取消所有订单失败: {e}")
            raise
    
    async def _batch_cancel_orders(self, command: BatchCancelOrders) -> None:
        """
        批量取消订单
        
        JVQuant不支持批量取消API，通过循环调用单个取消订单实现
        """
        logger.info(
            f"批量取消订单: 共 {len(command.cancels)} 个订单 "
            f"(JVQuant不支持批量取消API，将逐个取消)"
        )
        
        success_count = 0
        failed_count = 0
        
        for cancel_command in command.cancels:
            try:
                await self._cancel_order(cancel_command)
                success_count += 1
            except Exception as e:
                failed_count += 1
                logger.warning(
                    f"取消订单失败: {cancel_command.client_order_id}, 错误: {e}"
                )
                # 继续处理其他订单，不要因为单个失败而中断
        
        logger.info(
            f"批量取消完成: 成功 {success_count}/{len(command.cancels)}, "
            f"失败 {failed_count}/{len(command.cancels)}"
        )
    
    async def generate_fill_reports(
        self, 
        command: GenerateFillReports
    ) -> list[FillReport]:
        """
        生成成交报告列表
        
        查询委托列表，提取已成交或部分成交的订单生成成交报告
        """
        try:
            logger.info(f"生成成交报告: {command}")
            
            # 查询所有委托订单
            orders = await self._check_orders()
            if not orders:
                logger.info("没有委托订单")
                return []
            
            fill_reports = []
            
            for order in orders:
                # 只处理有成交量的订单
                deal_volume = int(order.get('deal_volume', 0))
                if deal_volume <= 0:
                    continue
                
                try:
                    # 解析订单信息
                    code = order.get('code')
                    instrument_id = InstrumentId.from_str(f"{code}.SZSE")
                    
                    # 解析订单方向
                    order_type = order.get('type', '')
                    if order_type == '买入' or order_type == 'buy':
                        order_side = OrderSide.BUY
                    elif order_type == '卖出' or order_type == 'sale':
                        order_side = OrderSide.SELL
                    else:
                        logger.warning(f"未知订单类型: {order_type}")
                        continue
                    
                    # 解析价格和数量
                    deal_price = float(order.get('deal_price', 0))
                    if deal_price <= 0:
                        logger.warning(f"无效的成交价格: {deal_price}")
                        continue
                    
                    # 构建成交报告
                    report = FillReport(
                        account_id=self.account_id,
                        instrument_id=instrument_id,
                        venue_order_id=VenueOrderId(order.get('order_id')),
                        trade_id=TradeId(order.get('order_id')),  # JVQuant可能没有单独的成交ID
                        order_side=order_side,
                        last_qty=Quantity.from_int(deal_volume),
                        last_px=Price.from_str(str(deal_price)),
                        commission=Money(0, CNY),  # JVQuant API未提供佣金信息
                        liquidity_side=LiquiditySide.NO_LIQUIDITY_SIDE,
                        ts_event=self._clock.timestamp_ns(),
                        report_id=UUID4(),
                    )
                    
                    fill_reports.append(report)
                    logger.debug(f"生成成交报告: {code} {order_side} {deal_volume}@{deal_price}")
                    
                except Exception as e:
                    logger.warning(f"解析订单 {order.get('order_id')} 失败: {e}")
                    continue
            
            logger.info(f"✅ 生成了 {len(fill_reports)} 个成交报告")
            return fill_reports
            
        except Exception as e:
            logger.error(f"生成成交报告失败: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    async def generate_position_status_reports(
        self, 
        command: GeneratePositionStatusReports
    ) -> list[PositionStatusReport]:
        """
        生成持仓状态报告列表
        
        查询持仓信息，生成持仓状态报告
        """
        try:
            logger.info(f"生成持仓状态报告: {command}")
            
            # 查询持仓信息
            account_info = await self._check_positions()
            if not account_info or not account_info.get('hold_list'):
                logger.info("没有持仓")
                return []
            
            position_reports = []
            
            for position in account_info['hold_list']:
                try:
                    # 解析持仓信息
                    code = position.get('code')
                    hold_vol = int(position.get('hold_vol', 0))
                    hold_earn = float(position.get('hold_earn', 0))  # 持仓盈亏
                    
                    # 跳过空持仓
                    if hold_vol <= 0:
                        continue
                    
                    # 构建instrument_id
                    instrument_id = InstrumentId.from_str(f"{code}.SZSE")
                    
                    # ✅ 修复: 通过持仓盈亏计算平均开仓价格
                    # 公式: 平均成本价 = 当前价格 - (持仓盈亏 / 持仓数量)
                    avg_px_open = None
                    try:
                        # 获取当前价格
                        quote = self.cache.quote_tick(instrument_id)
                        if quote and hold_vol > 0:
                            current_price = (quote.bid_price.as_double() + quote.ask_price.as_double()) / 2
                            # 反推平均成本价
                            avg_cost_price = current_price - (hold_earn / hold_vol)
                            avg_px_open = Decimal(str(avg_cost_price))
                            logger.debug(
                                f"计算avg_px_open: 当前价={current_price:.3f}, "
                                f"持仓盈亏={hold_earn:.2f}, 持仓量={hold_vol}, "
                                f"平均成本价={avg_px_open}"
                            )
                    except Exception as e:
                        logger.debug(f"计算平均开仓价失败: {e}")
                    
                    # 构建持仓状态报告
                    # 现金账户使用FLAT position_side
                    report = PositionStatusReport(
                        account_id=self.account_id,
                        instrument_id=instrument_id,
                        position_side=PositionSide.FLAT,  # 现金账户使用FLAT
                        quantity=Quantity.from_int(hold_vol),
                        report_id=UUID4(),
                        ts_last=self._clock.timestamp_ns(),
                        ts_init=self._clock.timestamp_ns(),
                        avg_px_open=avg_px_open,  # ✅ 添加平均开仓价
                    )
                    
                    position_reports.append(report)
                    logger.debug(
                        f"生成持仓报告: {code} 持仓量={hold_vol}, "
                        f"可用量={position.get('usable_vol', 0)}, "
                        f"估算平均价={avg_px_open}"
                    )
                    
                except Exception as e:
                    logger.warning(f"解析持仓 {position.get('code')} 失败: {e}")
                    continue
            
            logger.info(f"✅ 生成了 {len(position_reports)} 个持仓报告")
            return position_reports
            
        except Exception as e:
            logger.error(f"生成持仓状态报告失败: {e}")
            import traceback
            traceback.print_exc()
            return []


class ETF159506LiveExecClientFactory(LiveExecClientFactory):
    """159506 ETF执行客户端工厂"""
    
    @staticmethod
    def create(
        loop: asyncio.AbstractEventLoop,
        name: str,
        config,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
    ) -> ETF159506NautilusExecClient:
        """
        创建159506 ETF执行客户端
        
        Parameters
        ----------
        loop : asyncio.AbstractEventLoop
            事件循环
        name : str
            客户端名称
        config
            客户端配置
        msgbus : MessageBus
            消息总线
        cache : Cache
            缓存
        clock : LiveClock
            时钟
            
        Returns
        -------
        ETF159506NautilusExecClient
        """
        # 使用全局适配器的instrument_provider
        global_adapter = get_global_adapter()
        instrument_provider = global_adapter.instrument_provider
        logger.info(f"使用全局适配器的instrument_provider，已加载{instrument_provider.count}个工具")
        
        return ETF159506NautilusExecClient(
            loop=loop,
            client_id=ClientId(name),
            venue=config.venue if hasattr(config, 'venue') else None,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=instrument_provider,
            config=config,
        )


if __name__ == "__main__":
    asyncio.run(main()) 