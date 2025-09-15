#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
159506 ETF适配器
基于NautilusTrader适配器架构设计，集成jvquant平台数据接入
"""

import asyncio
import logging
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

logger = logging.getLogger(__name__)


class ETF159506ServerManager:
    """159506 ETF服务器管理器"""
    
    def __init__(self, token: str):
        self.token = token
        self.base_url = "http://jvQuant.com/query/server"
    
    def get_server(self, market: str = "ab", type: str = "websocket") -> str:
        """获取分配的服务器地址"""
        url = f"{self.base_url}?market={market}&type={type}&token={self.token}"
        
        try:
            response = requests.get(url, timeout=10, proxies=None)
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
                response = requests.get(url, params=params, timeout=10)
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
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, params=params) as response:
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
    
    def __init__(self, stock_code: str = "159506"):
        self.stock_code = stock_code
        self.logger = logging.getLogger("ETF159506DataProcessor")
        self.total_processed = 0
        self.start_time = datetime.now()
        self.last_volume = 0
        
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
                'collect_time': datetime.now().isoformat()
            }
            
            self.total_processed += 1
            return quote_data
            
        except Exception as e:
            self.logger.error(f"处理Level1数据失败: {e}")
            return None
    
    def _parse_bid_ask_data(self, fields: List[str]) -> Dict:
        """解析买卖五档数据"""
        try:
            bid_asks = {'bids': [], 'asks': []}
            
            # 买五档
            for i in range(0, 10, 2):
                if i + 1 < len(fields):
                    price = float(fields[i]) if fields[i] else 0
                    volume = float(fields[i + 1]) if fields[i + 1] else 0
                    if price > 0 and volume > 0:
                        bid_asks['bids'].append({'price': price, 'volume': volume})
            
            # 卖五档
            for i in range(10, 20, 2):
                if i + 1 < len(fields):
                    price = float(fields[i]) if fields[i] else 0
                    volume = float(fields[i + 1]) if fields[i + 1] else 0
                    if price > 0 and volume > 0:
                        bid_asks['asks'].append({'price': price, 'volume': volume})
            
            return bid_asks
            
        except Exception as e:
            self.logger.error(f"解析买卖五档数据失败: {e}")
            return {'bids': [], 'asks': []}


class ETF159506WebSocketClient:
    """159506 ETF WebSocket客户端 - 基于jvquant平台"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.logger = logging.getLogger("ETF159506WebSocketClient")
        self.token = config.get('token')
        self.stock_code = config.get('stock_code', '159506')
        self.server_manager = ETF159506ServerManager(self.token) if self.token else None
        self.data_processor = ETF159506DataProcessor(self.stock_code)
        
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
            
        except Exception as e:
            self.logger.error(f"加载工具失败: {e}")
    
    def find(self, instrument_id) -> Optional['Instrument']:
        """查找工具"""
        # 使用基类的find方法
        return super().find(instrument_id)
    
    def get_all(self) -> List['Instrument']:
        """获取所有工具"""
        # 使用基类的list_all方法
        return self.list_all()


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


class ETF159506DataClient:
    """159506 ETF数据客户端"""
    
    def __init__(self, config: Dict, instrument_provider: ETF159506InstrumentProvider):
        self.config = config
        self.instrument_provider = instrument_provider
        self.logger = logging.getLogger("ETF159506DataClient")
        self.http_client = ETF159506HttpClient(config)
        self.ws_client = ETF159506WebSocketClient(config)
        self.is_connected = False
        
    async def connect(self) -> bool:
        """连接数据客户端"""
        try:
            # 连接HTTP和WebSocket客户端
            http_connected = await self.http_client.connect()
            ws_connected = await self.ws_client.connect()
            
            if http_connected and ws_connected:
                self.is_connected = True
                self.logger.info("数据客户端连接成功")
                return True
            else:
                self.logger.error("数据客户端连接失败")
                return False
                
        except Exception as e:
            self.logger.error(f"数据客户端连接失败: {e}")
            return False
    
    async def disconnect(self) -> None:
        """断开数据客户端"""
        await self.http_client.disconnect()
        await self.ws_client.disconnect()
        self.is_connected = False
        self.logger.info("数据客户端已断开")
    
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
        """订阅报价数据"""
        self.ws_client.subscribe_quotes(symbol, callback)
    
    def subscribe_trades(self, symbol: str, callback) -> None:
        """订阅成交数据"""
        self.ws_client.subscribe_trades(symbol, callback)
    
    async def get_historical_data(self, symbol: str, start_date: str, end_date: str, 
                                 kline_type: str = "day", fq: str = "前复权", limit: int = 240) -> List[Dict]:
        """获取历史数据"""
        return await self.http_client.get_historical_data(symbol, start_date, end_date, kline_type, fq, limit)


class ETF159506ExecutionClient:
    """159506 ETF执行客户端 - 基于jvquant平台"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.logger = logging.getLogger("ETF159506ExecutionClient")
        self.token = config.get('token')
        self.http_client = ETF159506HttpClient(config)
        self.is_connected = False
        self.orders = {}
        
        # 交易相关
        self.trade_server = None
        self.ticket = None
        self.ticket_expire = None
        self.is_logged_in = False
        
    async def connect(self) -> bool:
        """连接执行客户端"""
        try:
            if not self.token:
                self.logger.error("缺少token配置")
                return False
                
            # 获取交易服务器地址
            if not await self._get_trade_server():
                self.logger.error("无法获取交易服务器地址")
                return False
                
            connected = await self.http_client.connect()
            if connected:
                self.is_connected = True
                self.logger.info("执行客户端连接成功")
                return True
            else:
                self.logger.error("执行客户端连接失败")
                return False
                
        except Exception as e:
            self.logger.error(f"执行客户端连接失败: {e}")
            return False
    
    async def disconnect(self) -> None:
        """断开执行客户端"""
        await self.http_client.disconnect()
        self.is_connected = False
        self.logger.info("执行客户端已断开")
    
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
            url = f"http://{self.trade_server}/login"
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
                self.ticket_expire = data.get("expire")
                self.is_logged_in = True
                
                self.logger.info(f"登录成功! 交易凭证: {self.ticket}")
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
    
    async def submit_order(self, order: Dict) -> str:
        """提交订单 - 基于jvquant交易接口"""
        try:
            if not self._check_login_status():
                self.logger.error("未登录或登录已过期")
                return None
            
            order_type = order.get('type', 'market')
            side = order.get('side', 'buy')
            code = order.get('code', '159506')
            name = order.get('name', '华夏中证500ETF')
            price = order.get('price', 0.0)
            volume = order.get('quantity', 0)
            
            if side == 'buy':
                return await self._buy_stock(code, name, price, volume)
            elif side == 'sell':
                return await self._sell_stock(code, name, price, volume)
            else:
                self.logger.error(f"不支持的订单方向: {side}")
                return None
                
        except Exception as e:
            self.logger.error(f"订单提交失败: {e}")
            return None
    
    async def _buy_stock(self, code: str, name: str, price: float, volume: int) -> Optional[str]:
        """买入股票"""
        try:
            url = f"http://{self.trade_server}/buy"
            params = {
                'token': self.token,
                'ticket': self.ticket,
                'code': code,
                'name': name,
                'price': str(price),
                'volume': str(volume)
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
        """卖出股票"""
        try:
            url = f"http://{self.trade_server}/sale"
            params = {
                'token': self.token,
                'ticket': self.ticket,
                'code': code,
                'name': name,
                'price': str(price),
                'volume': str(volume)
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
    
    async def cancel_order(self, order_id: str) -> bool:
        """取消订单 - 基于jvquant交易接口"""
        try:
            if not self._check_login_status():
                self.logger.error("未登录或登录已过期")
                return False
            
            url = f"http://{self.trade_server}/cancel"
            params = {
                'token': self.token,
                'ticket': self.ticket,
                'order_id': order_id
            }
            
            self.logger.info(f"正在撤销委托: {order_id}")
            response = requests.get(url, params=params, timeout=10)
            data = response.json()
            
            if data.get("code") == "0":
                self.logger.info(f"撤单成功! {data.get('message', '')}")
                
                # 更新订单状态
                if order_id in self.orders:
                    self.orders[order_id]['status'] = 'cancelled'
                
                return True
            else:
                self.logger.error(f"撤单失败: {data}")
                return False
                
        except Exception as e:
            self.logger.error(f"撤单异常: {e}")
            return False
    
    async def get_order_status(self, order_id: str) -> Optional[Dict]:
        """获取订单状态"""
        # 先检查本地订单
        if order_id in self.orders:
            return self.orders[order_id]
        
        # 如果本地没有，尝试从服务器查询
        try:
            if not self._check_login_status():
                return None
            
            url = f"http://{self.trade_server}/check_order"
            params = {
                'token': self.token,
                'ticket': self.ticket
            }
            
            response = requests.get(url, params=params, timeout=10)
            data = response.json()
            
            if data.get("code") == "0":
                orders = data.get("list", [])
                for order in orders:
                    if order.get("order_id") == order_id:
                        return order
            
            return None
            
        except Exception as e:
            self.logger.error(f"查询订单状态异常: {e}")
            return None
    
    async def get_positions(self) -> List[Dict]:
        """获取持仓信息 - 基于jvquant交易接口"""
        try:
            if not self._check_login_status():
                self.logger.error("未登录或登录已过期")
                return []
            
            url = f"http://{self.trade_server}/check_position"
            params = {
                'token': self.token,
                'ticket': self.ticket
            }
            
            self.logger.info("正在查询持仓...")
            response = requests.get(url, params=params, timeout=10)
            data = response.json()
            
            if data.get("code") == "0":
                positions = data.get("list", [])
                self.logger.info(f"查询成功! 共{len(positions)}个持仓")
                return positions
            else:
                self.logger.error(f"查询持仓失败: {data}")
                return []
                
        except Exception as e:
            self.logger.error(f"查询持仓异常: {e}")
            return []
    
    async def get_orders(self) -> List[Dict]:
        """获取交易记录"""
        try:
            if not self._check_login_status():
                self.logger.error("未登录或登录已过期")
                return []
            
            url = f"http://{self.trade_server}/check_order"
            params = {
                'token': self.token,
                'ticket': self.ticket
            }
            
            self.logger.info("正在查询交易记录...")
            response = requests.get(url, params=params, timeout=10)
            data = response.json()
            
            if data.get("code") == "0":
                orders = data.get("list", [])
                self.logger.info(f"查询成功! 共{len(orders)}条交易记录")
                return orders
            else:
                self.logger.error(f"查询交易记录失败: {data}")
                return []
                
        except Exception as e:
            self.logger.error(f"查询交易记录异常: {e}")
            return []
    
    async def logout(self):
        """登出"""
        self.is_logged_in = False
        self.ticket = None
        self.ticket_expire = None
        self.logger.info("已登出交易系统")


class ETF159506Adapter:
    """159506 ETF适配器主类"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.logger = logging.getLogger("ETF159506Adapter")
        
        # 初始化组件
        self.http_client = ETF159506HttpClient(config)
        self.ws_client = ETF159506WebSocketClient(config)
        self.instrument_provider = ETF159506InstrumentProvider(self.http_client)
        self.data_client = ETF159506DataClient(config, self.instrument_provider)
        self.execution_client = ETF159506ExecutionClient(config)
        
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
            
            # 连接所有组件
            data_connected = await self.data_client.connect()
            execution_connected = await self.execution_client.connect()
            
            if data_connected and execution_connected:
                # 加载工具信息
                await self.instrument_provider.load_all_async()
                
                self.is_connected = True
                self.connection_attempts = 0  # 重置连接计数
                self.logger.info("159506 ETF适配器连接成功")
                return True
            else:
                self.logger.error("159506 ETF适配器连接失败")
                return False
                
        except Exception as e:
            self.logger.error(f"159506 ETF适配器连接失败: {e}")
            return False
    
    async def disconnect(self) -> None:
        """断开适配器"""
        await self.data_client.disconnect()
        await self.execution_client.disconnect()
        self.is_connected = False
        self.logger.info("159506 ETF适配器已断开")
    
    def get_instrument_provider(self) -> ETF159506InstrumentProvider:
        """获取工具提供者"""
        return self.instrument_provider
    
    def get_data_client(self) -> ETF159506DataClient:
        """获取数据客户端"""
        return self.data_client
    
    def get_execution_client(self) -> ETF159506ExecutionClient:
        """获取执行客户端"""
        return self.execution_client
    
    async def get_status(self) -> Dict:
        """获取适配器状态"""
        return {
            'adapter_name': 'ETF159506Adapter',
            'is_connected': self.is_connected,
            'connection_attempts': self.connection_attempts,
            'max_connection_attempts': self.max_connection_attempts,
            'instruments_count': self.instrument_provider.count,
            'data_client_connected': self.data_client.is_connected,
            'execution_client_connected': self.execution_client.is_connected,
            'orders_count': len(self.execution_client.orders)
        }
    
    async def reset_connection_attempts(self) -> None:
        """重置连接尝试次数"""
        self.connection_attempts = 0
        self.logger.info("连接尝试次数已重置")
    
    async def check_connection_health(self) -> Dict:
        """检查连接健康状态"""
        health_status = {
            'adapter_connected': self.is_connected,
            'data_client_connected': self.data_client.is_connected,
            'execution_client_connected': self.execution_client.is_connected,
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
        
        # 获取历史数据
        print("\n📈 获取历史数据...")
        historical_data = await adapter.get_data_client().get_historical_data(
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
        async def quote_callback(data):
            print(f"📊 实时数据: 价格={data.get('price')}, 成交量={data.get('volume')}")
        
        adapter.get_data_client().subscribe_quotes('159506', quote_callback)
        
        # 交易功能演示
        print("\n🏦 交易功能演示...")
        execution_client = adapter.get_execution_client()
        
        if execution_client.trade_server:
            print(f"✅ 交易服务器: {execution_client.trade_server}")
            print("💡 交易功能已集成，需要登录后使用:")
            print("   - execution_client.login(account, password)")
            print("   - execution_client.submit_order(order)")
            print("   - execution_client.get_positions()")
            print("   - execution_client.get_orders()")
        else:
            print("❌ 交易服务器连接失败")
        
        # 获取状态
        status = await adapter.get_status()
        print(f"\n📊 适配器状态: {status}")
        
        # 保持连接一段时间
        print("\n⏳ 保持连接10秒...")
        await asyncio.sleep(10)
        
        # 断开连接
        await adapter.disconnect()
        print("✅ 适配器已断开连接")
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
        
        self._set_connected(False)
        
    async def _connect(self) -> None:
        """连接到数据源"""
        try:
            logger.info("连接ETF159506数据客户端...")
            
            # 检查适配器是否已连接
            if self.adapter.is_connected:
                logger.info("适配器已连接，直接设置数据客户端状态")
                self._set_connected(True)
                return
            
            # 如果适配器未连接，说明全局适配器没有正确设置
            logger.error("适配器未连接，无法启动数据客户端")
            raise RuntimeError("适配器未连接，请确保全局适配器已正确设置")
            
        except Exception as e:
            logger.error(f"连接ETF159506数据客户端失败: {e}")
            raise
    
    async def _disconnect(self) -> None:
        """断开数据源连接"""
        try:
            if self.is_connected:
                await self.adapter.disconnect()
                self._set_connected(False)
                logger.info("ETF159506数据客户端已断开")
        except Exception as e:
            logger.error(f"断开ETF159506数据客户端失败: {e}")
    
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
            
            # 使用适配器订阅数据
            data_client = self.adapter.get_data_client()
            data_client.subscribe_quotes(symbol, bar_callback)
            
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
            data_client = self.adapter.get_data_client()
            data_client.subscribe_quotes(symbol, quote_callback)
            
            logger.info(f"报价数据订阅成功: {instrument_id}")
            
        except Exception as e:
            logger.error(f"订阅报价数据失败: {e}")
            raise
    
    def _convert_to_bar(self, data: Dict[str, Any], bar_type: BarType) -> Optional[Bar]:
        """将适配器数据转换为NautilusTrader Bar对象"""
        try:
            # 这里需要根据实际的数据格式进行转换
            # Level1数据包含: price, volume, timestamp等字段
            
            from nautilus_trader.model.objects import Price, Quantity
            from decimal import Decimal
            
            # 获取价格和成交量
            price = data.get('price', 0)
            volume = data.get('volume', 0)
            
            # 调试：打印原始数据
            logger.info(f"原始数据: {data}")
            logger.info(f"数据类型 - price: {type(price)}, volume: {type(volume)}")
            logger.info(f"数据值 - price: {price}, volume: {volume}")
            
            # 确保价格和成交量是数字类型
            if isinstance(price, str):
                price = float(price)
            if isinstance(volume, str):
                volume = float(volume)
            
            # 确保volume是整数，参考etf_159506_catalog_loader.py的处理方式
            volume_int = int(volume) if volume > 0 else 0            
            # 调试：打印转换后的值
            logger.info(f"转换后 - volume_int: {volume_int}, type: {type(volume_int)}")
            
            # 使用NautilusTrader官方方法获取instrument
            instrument = self._instrument_provider.find(bar_type.instrument_id)
            logger.info(f"从instrument_provider获取instrument: {instrument}")
            
            
            # 使用推荐的方法创建Price和Quantity - 通过instrument.make_price()和make_qty()
            price_obj = instrument.make_price(price)
            volume_quantity = instrument.make_qty(volume_int)
            logger.info(f"使用instrument创建 - price: {price_obj}, volume: {volume_quantity}")
           
            
            # 使用NautilusTrader官方方法获取时间戳
            from nautilus_trader.core.datetime import dt_to_unix_nanos
            import pandas as pd
            
            current_time = pd.Timestamp.now(tz='UTC')
            ts_event = dt_to_unix_nanos(current_time)
            ts_init = ts_event  # 使用相同的时间戳
            
            bar = Bar(
                bar_type=bar_type,
                open=price_obj,
                high=price_obj,
                low=price_obj,
                close=price_obj,
                volume=volume_quantity,
                ts_event=ts_event,
                ts_init=ts_init,
            )
            
            return bar
            
        except Exception as e:
            logger.error(f"转换Bar数据失败: {e}")
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


if __name__ == "__main__":
    asyncio.run(main()) 