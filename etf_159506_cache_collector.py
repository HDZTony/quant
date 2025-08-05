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


class TradingTimeManager:
    """A股交易时间管理器"""
    
    def __init__(self):
        # A股交易时间配置（周一至周五）
        self.trading_sessions = [
            # 上午交易时段：9:30-11:30
            {
                'start': '09:30:00',
                'end': '11:30:00',
                'name': '上午交易时段'
            },
            # 下午交易时段：13:00-15:00
            {
                'start': '13:00:00',
                'end': '15:00:00',
                'name': '下午交易时段'
            }
        ]
        
        # 节假日列表（2025年A股休市安排）
        self.holidays = [
            # 元旦：1月1日（星期三）休市，1月2日（星期四）起照常开市
            '2025-01-01',  # 元旦
            
            # 春节：1月28日（星期二）至2月4日（星期二）休市，2月5日（星期三）起照常开市
            # 另外，1月26日（星期日）、2月8日（星期六）为周末休市
            '2025-01-28', '2025-01-29', '2025-01-30', '2025-01-31',  # 春节
            '2025-02-01', '2025-02-02', '2025-02-03', '2025-02-04',  # 春节
            
            # 清明节：4月4日（星期五）至4月6日（星期日）休市，4月7日（星期一）起照常开市
            '2025-04-04', '2025-04-05', '2025-04-06',  # 清明节
            
            # 劳动节：5月1日（星期四）至5月5日（星期一）休市，5月6日（星期二）起照常开市
            # 另外，4月27日（星期日）为周末休市
            '2025-05-01', '2025-05-02', '2025-05-03', '2025-05-04', '2025-05-05',  # 劳动节
            
            # 端午节：5月31日（星期六）至6月2日（星期一）休市，6月3日（星期二）起照常开市
            '2025-05-31', '2025-06-01', '2025-06-02',  # 端午节
            
            # 国庆节、中秋节：10月1日（星期三）至10月8日（星期三）休市，10月9日（星期四）起照常开市
            # 另外，9月28日（星期日）、10月11日（星期六）为周末休市
            '2025-10-01', '2025-10-02', '2025-10-03', '2025-10-04',  # 国庆节、中秋节
            '2025-10-05', '2025-10-06', '2025-10-07', '2025-10-08',  # 国庆节、中秋节
        ]
        
        logger.info("交易时间管理器初始化完成")
    
    def is_trading_day(self, date: datetime = None) -> bool:
        """判断是否为交易日"""
        if date is None:
            date = datetime.now()
        
        # 检查是否为周末
        if date.weekday() >= 5:  # 5=周六, 6=周日
            return False
        
        # 检查是否为节假日
        date_str = date.strftime('%Y-%m-%d')
        if date_str in self.holidays:
            return False
        
        return True
    
    def is_trading_time(self, current_time: datetime = None) -> bool:
        """判断当前是否为交易时间"""
        if current_time is None:
            current_time = datetime.now()
        
        # 首先检查是否为交易日
        if not self.is_trading_day(current_time):
            return False
        
        # 获取当前时间字符串
        time_str = current_time.strftime('%H:%M:%S')
        
        # 检查是否在任一交易时段内
        for session in self.trading_sessions:
            if session['start'] <= time_str <= session['end']:
                return True
        
        return False
    
    def get_next_trading_time(self, current_time: datetime = None) -> datetime:
        """获取下一个交易时间"""
        if current_time is None:
            current_time = datetime.now()
        
        # 如果当前是交易时间，返回当前时间
        if self.is_trading_time(current_time):
            return current_time
        
        # 获取当前日期
        current_date = current_time.date()
        current_time_str = current_time.strftime('%H:%M:%S')
        
        # 检查今天剩余的交易时段
        for session in self.trading_sessions:
            if current_time_str < session['start']:
                # 今天还有交易时段
                next_time_str = f"{current_date} {session['start']}"
                return datetime.strptime(next_time_str, '%Y-%m-%d %H:%M:%S')
        
        # 今天没有交易时段了，查找下一个交易日
        next_date = current_date + timedelta(days=1)
        while not self.is_trading_day(datetime.combine(next_date, datetime.min.time())):
            next_date += timedelta(days=1)
        
        # 返回下一个交易日的第一个交易时段
        next_time_str = f"{next_date} {self.trading_sessions[0]['start']}"
        return datetime.strptime(next_time_str, '%Y-%m-%d %H:%M:%S')
    
    def get_trading_status(self) -> Dict:
        """获取交易状态信息"""
        current_time = datetime.now()
        is_trading = self.is_trading_time(current_time)
        is_trading_day = self.is_trading_day(current_time)
        next_trading_time = self.get_next_trading_time(current_time)
        
        return {
            'current_time': current_time.strftime('%Y-%m-%d %H:%M:%S'),
            'is_trading_day': is_trading_day,
            'is_trading_time': is_trading,
            'next_trading_time': next_trading_time.strftime('%Y-%m-%d %H:%M:%S'),
            'time_until_next': str(next_trading_time - current_time).split('.')[0] if next_trading_time > current_time else '0:00:00'
        }
    
    def wait_until_trading_time(self, check_interval: int = 60):
        """等待直到交易时间"""
        while not self.is_trading_time():
            status = self.get_trading_status()
            logger.info(f"当前非交易时间: {status['current_time']}")
            logger.info(f"下一个交易时间: {status['next_trading_time']} (等待 {status['time_until_next']})")
            time.sleep(check_interval)
        
        logger.info(f"已进入交易时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


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
                    timeout=2,  # 减少超时时间
                ),
                tick_capacity=500_000,  # 增加到50万条tick（提高容量）
                bar_capacity=100_000,   # 增加到10万根K线
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
    
    def cleanup_corrupted_files(self, catalog_path: str = None):
        """清理损坏的Parquet文件"""
        try:
            # 如果没有提供catalog_path，使用默认路径
            if catalog_path is None:
                catalog_path = "catalog/etf_159506_cache"
            
            catalog_path = Path(catalog_path)
            if not catalog_path.exists():
                logger.info(f"目录不存在，跳过清理: {catalog_path}")
                return
            
            # 查找所有.parquet文件
            parquet_files = list(catalog_path.glob("*.parquet"))
            corrupted_count = 0
            
            for file_path in parquet_files:
                try:
                    # 检查文件大小
                    file_size = file_path.stat().st_size
                    if file_size < 100:  # 小于100字节的文件可能是损坏的
                        logger.warning(f"发现损坏文件: {file_path} (大小: {file_size}字节)")
                        # 备份并删除
                        backup_path = file_path.with_suffix('.corrupted')
                        file_path.rename(backup_path)
                        corrupted_count += 1
                        continue
                    
                    # 尝试读取文件验证完整性
                    pd.read_parquet(file_path)
                    
                except Exception as e:
                    logger.warning(f"发现损坏文件: {file_path} (错误: {e})")
                    # 备份并删除
                    backup_path = file_path.with_suffix('.corrupted')
                    file_path.rename(backup_path)
                    corrupted_count += 1
            
            if corrupted_count > 0:
                logger.info(f"清理了 {corrupted_count} 个损坏的Parquet文件")
            else:
                logger.info("未发现损坏的Parquet文件")
            
        except Exception as e:
            logger.error(f"清理损坏文件失败: {e}")


class ETF159506ServerManager:
    """159506 ETF服务器管理器"""
    
    def __init__(self, token: str):
        self.token = token
        self.base_url = "http://jvQuant.com/query/server"
    
    def get_server(self, market: str = "ab", type: str = "websocket") -> str:
        """获取分配的服务器地址"""
        url = f"{self.base_url}?market={market}&type={type}&token={self.token}"
        
        try:
            # 移除代理配置，直接连接，避免代理问题
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
                 use_redis: bool = True, redis_host: str = "localhost", redis_port: int = 6379,
                 enable_trading_time_control: bool = True):
        self.token = token
        self.stock_code = stock_code
        self.enable_trading_time_control = enable_trading_time_control
        
        # 初始化组件
        self.server_manager = ETF159506ServerManager(token)
        self.cache_manager = ETF159506CacheManager(use_redis, redis_host, redis_port)
        self.data_processor = ETF159506CacheDataProcessor(stock_code, self.cache_manager)
        
        # 交易时间管理
        if self.enable_trading_time_control:
            self.trading_time_manager = TradingTimeManager()
            logger.info("已启用交易时间控制")
        else:
            self.trading_time_manager = None
            logger.info("已禁用交易时间控制")
        
        # WebSocket相关
        self.ws = None
        self.is_connected = False
        
        # 线程控制
        self.ws_thread = None
        self.save_thread = None
        self.stop_save = False
        # 移除心跳相关变量 - JVQuant服务器不支持心跳
        # self.heartbeat_thread = None
        # self.stop_heartbeat = False
        self.monitor_thread = None
        self.stop_monitor = False
        self.trading_time_thread = None
        self.stop_trading_time_check = False
        
        # 统计信息
        self.connection_count = 0
        self.disconnection_count = 0
        self.data_receive_count = 0
        self.last_data_time = None
        self.start_time = datetime.now()
        
        # 配置 - 优化数据保存
        self.save_interval = 10  # 5分钟保存一次（减少文件数量）
        self.merge_interval = 3600  # 1小时合并一次文件
        self.trading_time_check_interval = 60  # 改为60秒检查一次交易时间，减少检查频率
        self.catalog_path = f"catalog/etf_159506_cache"
        
        # 数据缓冲区
        self.data_buffer = []
        self.last_save_time = None
        self.last_merge_time = None
        
        logger.info(f"初始化159506 ETF Cache WebSocket客户端")
    
    def connect(self):
        """连接WebSocket服务器"""
        # 先清理之前的连接资源
        if hasattr(self, 'ws') and self.ws:
            try:
                self.ws.close()
            except:
                pass
            self.ws = None
        
        if hasattr(self, 'ws_thread') and self.ws_thread and self.ws_thread.is_alive():
            try:
                self.ws_thread.join(timeout=2)
            except:
                pass
        
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
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        
        # 在后台线程中运行WebSocket连接
        self.ws_thread = threading.Thread(target=self.ws.run_forever)
        self.ws_thread.daemon = True
        self.ws_thread.start()
        
        # 等待连接建立或超时
        timeout = 15  # 增加到15秒超时
        start_time = time.time()
        while not self.is_connected and (time.time() - start_time) < timeout:
            time.sleep(0.1)  # 增加到0.1秒检查间隔，减少CPU占用
        
        if self.is_connected:
            logger.info("WebSocket连接成功建立")
            return True
        else:
            logger.error("WebSocket连接超时")
            # 清理失败的连接
            if hasattr(self, 'ws') and self.ws:
                try:
                    self.ws.close()
                except:
                    pass
            return False
    
    def on_open(self, ws):
        """连接打开回调"""
        self.connection_count += 1
        self.is_connected = True
        logger.info(f"WebSocket连接已建立 (第{self.connection_count}次连接)")
        
        subscription = f"add=lv1_{self.stock_code}"
        ws.send(subscription)
        logger.info(f"已订阅: {subscription}")
        
        # 移除心跳机制 - JVQuant服务器不支持心跳
        # self.start_heartbeat(ws)
        
        # 启动时清理损坏的文件
        self.cache_manager.cleanup_corrupted_files(self.catalog_path)
        
        self.start_auto_save()
        self.start_diagnostic_monitor()
        self.start_trading_time_check()
    
    def on_message(self, ws, message, *args):
        """接收消息回调"""
        try:
            # 尝试解析消息类型
            if isinstance(message, str):
                # 减少日志输出，只记录重要信息
                if message.startswith('-1#'):
                    logger.error(f"收到服务器错误消息: {message}")
                    if "账户连接数已达并发上限" in message:
                        logger.error("⚠️  账户连接数已达并发上限！")
                        logger.error("解决方案:")
                        logger.error("1. 关闭其他使用相同token的连接")
                        logger.error("2. 等待一段时间后重试")
                        logger.error("3. 充值提升连接额度: https://jvquant.com/home.html#charge")
                        logger.error("4. 计算公式: 每100余额可增加1个并发连接额度")
                        # 主动关闭连接
                        ws.close()
                        return
                elif message.startswith('lv1_'):
                    # 处理Level1数据
                    self.data_receive_count += 1
                    self.last_data_time = datetime.now()
                    self.process_market_data(message)
                elif message == 'pong':
                    pass  # 静默处理心跳响应
                elif message == "":
                    pass  # 静默处理空消息
                else:
                    logger.debug(f"收到其他文本消息: {message}")
            else:
                # 处理二进制数据
                try:
                    decompressed = zlib.decompress(message, -zlib.MAX_WBITS)
                    data_str = decompressed.decode("utf-8")
                    
                    self.data_receive_count += 1
                    self.last_data_time = datetime.now()
                    
                    # 减少日志输出，只在每100条数据时输出一次（进一步减少日志开销）
                    if self.data_receive_count % 100 == 0:
                        logger.info(f"收到二进制数据 (第{self.data_receive_count}条): {data_str[:50]}...")
                    
                    lines = data_str.strip().split('\n')
                    for line in lines:
                        if line.strip():
                            self.process_market_data(line)
                            
                except Exception as decompress_error:
                    logger.error(f"解压缩数据失败: {decompress_error}")
                    # 尝试作为文本处理
                    try:
                        data_str = message.decode("utf-8")
                        logger.info(f"收到文本数据: {data_str[:100]}...")
                        self.process_market_data(data_str)
                    except:
                        logger.error(f"无法解析消息: {message[:100]}...")
                        
        except Exception as e:
            logger.error(f"处理消息失败: {e}")
            import traceback
            logger.error(f"详细错误: {traceback.format_exc()}")
    
    def process_market_data(self, data: str):
        """处理市场数据"""
        if data.startswith('lv1_'):
            self.data_processor.process_level1_data(data)
    
    def on_error(self, ws, error):
        """错误回调"""
        self.disconnection_count += 1
        self.is_connected = False
        
        # 记录详细的错误信息
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        error_str = str(error)
        error_type = type(error).__name__
        
        logger.error(f"[{current_time}] WebSocket错误 (第{self.disconnection_count}次断开)")
        logger.error(f"错误类型: {error_type}")
        logger.error(f"错误详情: {error_str}")
        logger.error(f"连接状态: {self.is_connected}")
        logger.error(f"数据接收计数: {self.data_receive_count}")
        
        # 分析错误类型并提供详细说明
        if "Connection to remote host was lost" in error_str:
            logger.error("错误分析: 远程主机连接丢失")
            logger.error("可能原因: 网络中断、服务器重启、防火墙阻止")
            logger.error("处理策略: 等待交易时间检查线程自动重连")
        elif "timeout" in error_str.lower():
            logger.error("错误分析: 连接超时")
            logger.error("可能原因: 网络延迟、服务器响应慢、心跳包丢失")
            logger.error("处理策略: 等待交易时间检查线程自动重连")
        elif "Connection refused" in error_str:
            logger.error("错误分析: 连接被拒绝")
            logger.error("可能原因: 服务器未启动、端口被占用、认证失败")
            logger.error("处理策略: 等待交易时间检查线程自动重连")
        elif "SSL" in error_str or "TLS" in error_str:
            logger.error("错误分析: SSL/TLS连接错误")
            logger.error("可能原因: 证书问题、协议版本不匹配")
            logger.error("处理策略: 等待交易时间检查线程自动重连")
        else:
            logger.error(f"错误分析: 其他未知错误")
            logger.error(f"错误详情: {error_str}")
            logger.error("处理策略: 等待交易时间检查线程自动重连")
        
        # 记录当前系统状态
        if hasattr(self, 'last_data_time') and self.last_data_time:
            time_since_last_data = datetime.now() - self.last_data_time
            logger.error(f"距离最后数据接收: {time_since_last_data}")
        else:
            logger.error("最后数据接收: 无数据")
        
        # 记录线程状态
        # logger.error(f"心跳线程状态: 已移除 (JVQuant服务器不支持心跳)")
        logger.error(f"保存线程状态: {'运行中' if not self.stop_save else '已停止'}")
        logger.error(f"监控线程状态: {'运行中' if not self.stop_monitor else '已停止'}")
        
        logger.error("=" * 60)
        
        # 移除心跳相关处理 - JVQuant服务器不支持心跳
        # def delayed_stop_heartbeat():
        #     time.sleep(2)  # 等待2秒
        #     if not self.is_connected:
        #         self.stop_heartbeat = True
        #         logger.info("延迟停止心跳线程")
        # 
        # threading.Thread(target=delayed_stop_heartbeat, daemon=True).start()
    
    def on_close(self, ws, code, msg):
        """连接关闭回调"""
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        logger.error(f"[{current_time}] WebSocket连接已关闭")
        logger.error(f"关闭代码: {code}")
        logger.error(f"关闭消息: {msg}")
        logger.error(f"连接状态: 已断开")
        logger.error(f"数据接收计数: {self.data_receive_count}")
        
        # 分析关闭代码
        if code == 1000:
            logger.error("关闭分析: 正常关闭")
            logger.error("可能原因: 服务器主动关闭、客户端主动断开")
        elif code == 1001:
            logger.error("关闭分析: 端点离开")
            logger.error("可能原因: 服务器关闭、网络中断")
        elif code == 1002:
            logger.error("关闭分析: 协议错误")
            logger.error("可能原因: 协议版本不匹配、消息格式错误")
        elif code == 1003:
            logger.error("关闭分析: 不支持的数据类型")
            logger.error("可能原因: 消息类型不支持")
        elif code == 1006:
            logger.error("关闭分析: 异常关闭")
            logger.error("可能原因: 网络中断、服务器崩溃")
        elif code == 1009:
            logger.error("关闭分析: 消息过大")
            logger.error("可能原因: 单条消息超过限制")
        elif code == 1011:
            logger.error("关闭分析: 服务器错误")
            logger.error("可能原因: 服务器内部错误")
        else:
            logger.error(f"关闭分析: 未知关闭代码 {code}")
        
        # 记录当前系统状态
        if hasattr(self, 'last_data_time') and self.last_data_time:
            time_since_last_data = datetime.now() - self.last_data_time
            logger.error(f"距离最后数据接收: {time_since_last_data}")
        else:
            logger.error("最后数据接收: 无数据")
        
        # 记录线程状态
        # logger.error(f"心跳线程状态: 已停止")  # 移除心跳线程状态
        logger.error(f"保存线程状态: 已停止")
        logger.error(f"监控线程状态: {'运行中' if not self.stop_monitor else '已停止'}")
        logger.error(f"交易时间检查线程状态: {'运行中' if not self.stop_trading_time_check else '已停止'}")
        
        logger.error("=" * 60)
        
        self.is_connected = False
        self.stop_save = True
        # self.stop_heartbeat = True  # 移除心跳停止
        # 不要停止交易时间检查线程，让它继续监控并自动重连
    
    def start_auto_save(self):
        """启动自动保存线程"""
        self.save_thread = threading.Thread(target=self.auto_save_loop)
        self.save_thread.daemon = True
        self.save_thread.start()
        logger.info("自动保存线程已启动")
    
    # 移除心跳机制 - JVQuant服务器不支持心跳
    # def start_heartbeat(self, ws):
    #     """启动心跳线程"""
    #     self.heartbeat_thread = threading.Thread(target=self.heartbeat_loop, args=(ws,))
    #     self.heartbeat_thread.daemon = True
    #     self.heartbeat_thread.start()
    #     logger.info("心跳线程已启动")
    
    def start_diagnostic_monitor(self):
        """启动诊断监控线程"""
        self.monitor_thread = threading.Thread(target=self.diagnostic_monitor_loop)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        logger.info("诊断监控线程已启动")
    
    def start_trading_time_check(self):
        """启动交易时间检查线程"""
        if self.enable_trading_time_control and self.trading_time_manager:
            self.trading_time_thread = threading.Thread(target=self.trading_time_check_loop)
            self.trading_time_thread.daemon = True
            self.trading_time_thread.start()
            logger.info("交易时间检查线程已启动")
    
    def diagnostic_monitor_loop(self):
        """诊断监控循环"""
        while not self.stop_monitor:
            try:
                time.sleep(120)  # 改为每2分钟输出一次诊断状态，减少日志输出频率
                
                if not self.stop_monitor:
                    self.print_diagnostic_status()
                    
            except Exception as e:
                logger.error(f"诊断监控循环错误: {e}")
                break
    
    def trading_time_check_loop(self):
        """交易时间检查循环"""
        last_reconnect_time = 0
        reconnect_cooldown = 300  # 5分钟冷却时间，防止疯狂重连
        consecutive_failures = 0
        max_consecutive_failures = 3  # 最大连续失败次数
        
        while not self.stop_trading_time_check:
            try:
                time.sleep(self.trading_time_check_interval)
                
                if not self.stop_trading_time_check and self.trading_time_manager:
                    current_status = self.trading_time_manager.get_trading_status()
                    current_time = time.time()
                    
                    # 如果当前是交易时间但未连接，则连接（增加冷却时间和失败次数控制）
                    if current_status['is_trading_time'] and not self.is_connected:
                        # 检查是否在冷却期内
                        if current_time - last_reconnect_time < reconnect_cooldown:
                            logger.debug(f"重连冷却中，剩余 {reconnect_cooldown - (current_time - last_reconnect_time):.0f} 秒")
                            continue
                        
                        # 检查连续失败次数
                        if consecutive_failures >= max_consecutive_failures:
                            logger.warning(f"连续失败 {consecutive_failures} 次，暂停重连 {reconnect_cooldown} 秒")
                            last_reconnect_time = current_time
                            consecutive_failures = 0
                            continue
                            
                        logger.info(f"🔗 检测到交易时间，开始连接: {current_status['current_time']}")
                        logger.info(f"   当前连接状态: {self.is_connected}")
                        logger.info(f"   连续失败次数: {consecutive_failures}")
                        
                        # 重置停止标志
                        self.stop_save = False
                        self.stop_monitor = False
                        self.stop_trading_time_check = False
                        
                        # 尝试连接
                        connect_result = self.connect()
                        if connect_result:
                            logger.info("✅ 自动重连成功")
                            consecutive_failures = 0  # 重置失败计数
                            last_reconnect_time = current_time
                        else:
                            logger.error("❌ 自动重连失败")
                            consecutive_failures += 1
                            last_reconnect_time = current_time
                    
                    # 如果当前不是交易时间但已连接，则断开
                    elif not current_status['is_trading_time'] and self.is_connected:
                        logger.info(f"🔌 检测到非交易时间，断开连接: {current_status['current_time']}")
                        logger.info(f"下一个交易时间: {current_status['next_trading_time']}")
                        self.disconnect()
                        consecutive_failures = 0  # 正常断开时重置失败计数
                    
                    # 每5分钟输出一次交易时间状态
                    if int(current_time) % 300 == 0:  # 5分钟 = 300秒
                        logger.info(f"📊 交易时间状态: {current_status}")
                        logger.info(f"   连接状态: {self.is_connected}")
                        logger.info(f"   连续失败次数: {consecutive_failures}")
                        logger.info(f"   距离下次重连: {max(0, reconnect_cooldown - (current_time - last_reconnect_time)):.0f}秒")
                    
            except Exception as e:
                logger.error(f"交易时间检查循环错误: {e}")
    
    def print_diagnostic_status(self):
        """打印诊断状态信息"""
        runtime = datetime.now() - self.start_time
        last_data_ago = "无数据" if not self.last_data_time else str(datetime.now() - self.last_data_time)
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
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
        
        # 基础状态信息
        status = f"""
============================================================
[{current_time}] 详细状态信息:
连接状态: {self.is_connected}
连接次数: {self.connection_count}
断开次数: {self.disconnection_count}
数据接收: {self.data_receive_count} 条
交易时间控制: {'启用' if self.enable_trading_time_control else '禁用'}"""
        
        # 如果连接状态为False，添加详细的调试信息
        if not self.is_connected:
            status += f"""
⚠️  连接断开状态详细分析:
  断开时间: {current_time}
  运行时间: {runtime}
  最后数据接收: {last_data_ago} 前
  线程状态:
    # 心跳线程: 已移除 (JVQuant服务器不支持心跳)
    保存线程: {'运行中' if not self.stop_save else '已停止'}
    监控线程: {'运行中' if not self.stop_monitor else '已停止'}
    交易时间检查线程: {'运行中' if not self.stop_trading_time_check else '已停止'}
  
  重连机制状态:
    交易时间控制: {'启用' if self.enable_trading_time_control else '禁用'}
    交易时间管理器: {'可用' if self.trading_time_manager else '不可用'}
    自动重连: {'启用' if self.enable_trading_time_control and self.trading_time_manager else '禁用'}
  
  连接历史:
    总连接次数: {self.connection_count}
    总断开次数: {self.disconnection_count}
    连接成功率: {(self.connection_count/(self.connection_count + self.disconnection_count)*100) if (self.connection_count + self.disconnection_count) > 0 else 0.0:.1f}%
  
  数据统计:
    累计接收数据: {self.data_receive_count} 条
    平均数据接收率: {self.data_receive_count/max(runtime.total_seconds()/3600, 1):.1f} 条/小时"""
        
        # 添加交易时间状态
        if self.trading_time_manager:
            trading_status = self.trading_time_manager.get_trading_status()
            status += f"""
交易时间状态:
  当前时间: {trading_status['current_time']}
  是否为交易日: {trading_status['is_trading_day']}
  是否为交易时间: {trading_status['is_trading_time']}
  下一个交易时间: {trading_status['next_trading_time']}
  距离下次交易: {trading_status['time_until_next']}"""
        
        # 添加Cache统计
        status += f"""
Cache统计:
  总tick数: {cache_status.get('tick_count', 0)}
  报价数: {cache_status.get('quote_count', 0)}
  交易数: {cache_status.get('trade_count', 0)}
最新数据: {trade_price}, {spread_info}
============================================================"""
        
        logger.info(status)
    
    # 移除心跳机制 - JVQuant服务器不支持心跳
    # def heartbeat_loop(self, ws):
    #     """心跳循环"""
    #     # 完整的心跳循环代码已移除
    
    def auto_save_loop(self):
        """自动保存循环"""
        while not self.stop_save:
            try:
                time.sleep(self.save_interval)
                
                if not self.stop_save:
                    # 增加数据量检查，避免频繁保存空数据
                    cache_data = self.cache_manager.get_historical_data(limit=100)
                    quote_count = len(cache_data.get('quote_ticks', []))
                    trade_count = len(cache_data.get('trade_ticks', []))
                    
                    if quote_count > 0 or trade_count > 0:
                        # 保存数据到Parquet文件
                        self._save_buffer_data()
                        
                        # 检查是否需要合并文件
                        self._merge_files_if_needed()
                        
                        # 清理旧数据（保留24小时）
                        self.cache_manager.clear_old_data(24)
                    else:
                        logger.debug("跳过保存：无新数据")
                    
            except Exception as e:
                logger.error(f"自动保存失败: {e}")
                time.sleep(30)  # 出错时等待30秒，减少错误频率
    
    def _save_buffer_data(self):
        """保存缓冲区数据"""
        try:
            # 获取当前缓存数据，减少数据量避免内存占用过高
            cache_data = self.cache_manager.get_historical_data(limit=1000)
            
            if not cache_data or not cache_data.get('trade_ticks'):
                return
            
            # 转换为DataFrame格式
            trade_data = []
            for tick in cache_data['trade_ticks']:
                trade_data.append({
                    'timestamp': pd.to_datetime(tick.ts_event, unit='ns'),
                    'price': float(tick.price),
                    'size': int(tick.size),
                    'trade_id': str(tick.trade_id),
                    'type': 'trade'
                })
            
            if not trade_data:
                return
            
            # 按日期分组保存
            today = datetime.now().date()
            filename = f"cache_data_{today.strftime('%Y%m%d')}.parquet"
            filepath = Path(self.catalog_path) / filename
            
            # 确保目录存在
            filepath.parent.mkdir(parents=True, exist_ok=True)
            
            # 如果文件已存在，读取并合并
            if filepath.exists():
                try:
                    # 检查文件大小，如果太小可能是损坏文件
                    file_size = filepath.stat().st_size
                    if file_size < 100:  # 小于100字节的文件可能是损坏的
                        logger.warning(f"文件太小({file_size}字节)，可能是损坏文件，创建新文件")
                        combined_df = pd.DataFrame(trade_data)
                    else:
                        existing_df = pd.read_parquet(filepath)
                        new_df = pd.DataFrame(trade_data)
                        
                        # 合并数据，去重
                        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                        combined_df = combined_df.drop_duplicates(subset=['timestamp', 'trade_id'], keep='last')
                        combined_df = combined_df.sort_values('timestamp')
                except Exception as read_error:
                    logger.warning(f"读取现有文件失败，创建新文件: {read_error}")
                    # 备份损坏的文件
                    try:
                        backup_path = filepath.with_suffix('.parquet.bak')
                        filepath.rename(backup_path)
                        logger.info(f"已备份损坏文件到: {backup_path}")
                    except Exception as backup_error:
                        logger.error(f"备份损坏文件失败: {backup_error}")
                    combined_df = pd.DataFrame(trade_data)
            else:
                combined_df = pd.DataFrame(trade_data)
            
            # 保存文件（使用临时文件避免并发写入问题）
            temp_filepath = filepath.with_suffix('.tmp')
            try:
                combined_df.to_parquet(temp_filepath, index=False)
                # 原子性替换文件
                if filepath.exists():
                    filepath.unlink()  # 删除原文件
                temp_filepath.rename(filepath)
                logger.info(f"数据保存完成: {filepath} ({len(combined_df)} 条记录)")
                self.last_save_time = datetime.now()
            except Exception as save_error:
                logger.error(f"保存文件失败: {save_error}")
                # 清理临时文件
                if temp_filepath.exists():
                    temp_filepath.unlink()
                return
            finally:
                # 确保临时文件被清理
                if temp_filepath.exists():
                    temp_filepath.unlink()
            self.last_save_time = datetime.now()
            
        except Exception as e:
            logger.error(f"保存缓冲区数据失败: {e}")
            # 出错时不抛出异常，避免影响主循环
    
    def _merge_files_if_needed(self):
        """如果需要，合并文件"""
        current_time = datetime.now()
        
        # 每小时合并一次
        if (self.last_merge_time is None or 
            (current_time - self.last_merge_time).total_seconds() >= self.merge_interval):
            
            self._merge_daily_files()
            self.last_merge_time = current_time
    
    def _merge_daily_files(self):
        """合并当天的文件"""
        try:
            today = datetime.now().date()
            today_str = today.strftime('%Y%m%d')
            
            # 查找当天的所有文件
            pattern = f"cache_data_{today_str}*.parquet"
            files = list(Path(self.catalog_path).glob(pattern))
            
            if len(files) <= 1:
                return  # 只有一个文件或没有文件，不需要合并
            
            logger.info(f"开始合并 {len(files)} 个文件...")
            
            # 读取所有文件
            all_dataframes = []
            for file_path in files:
                try:
                    df = pd.read_parquet(file_path)
                    all_dataframes.append(df)
                except Exception as e:
                    logger.warning(f"读取文件失败 {file_path}: {e}")
                    continue
            
            if not all_dataframes:
                return
            
            # 合并数据
            combined_df = pd.concat(all_dataframes, ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=['timestamp', 'trade_id'], keep='last')
            combined_df = combined_df.sort_values('timestamp')
            
            # 保存合并后的文件
            merged_filename = f"cache_data_{today_str}_merged.parquet"
            merged_filepath = Path(self.catalog_path) / merged_filename
            combined_df.to_parquet(merged_filepath, index=False)
            
            # 删除原始文件
            for file_path in files:
                try:
                    file_path.unlink()
                    logger.info(f"删除原始文件: {file_path.name}")
                except Exception as e:
                    logger.warning(f"删除文件失败 {file_path}: {e}")
            
            logger.info(f"文件合并完成: {merged_filename} ({len(combined_df)} 条记录)")
            
        except Exception as e:
            logger.error(f"合并文件失败: {e}")
    
    def get_status(self) -> Dict:
        """获取状态信息"""
        cache_status = self.cache_manager.get_latest_data()
        
        # 获取交易时间状态
        trading_status = None
        if self.trading_time_manager:
            trading_status = self.trading_time_manager.get_trading_status()
        
        return {
            'connected': self.is_connected,
            'stock_code': self.stock_code,
            'connection_count': self.connection_count,
            'disconnection_count': self.disconnection_count,
            'data_receive_count': self.data_receive_count,
            'last_data_time': self.last_data_time.isoformat() if self.last_data_time else None,
            'cache_status': cache_status,
            'trading_time_control_enabled': self.enable_trading_time_control,
            'trading_status': trading_status
        }
    
    def disconnect(self):
        """断开连接"""
        logger.info("正在断开连接...")
        self.stop_save = True
        # self.stop_heartbeat = True  # 移除心跳停止
        self.stop_monitor = True
        # 不要停止交易时间检测线程，让它继续监控并自动重连
        # self.stop_trading_time_check = True
        if self.ws:
            self.ws.close()
        
        # 保存最终数据
        self._save_buffer_data()
        
        self.print_diagnostic_status()
        logger.info("连接已断开")
    
    def reconnect(self):
        """重连方法"""
        try:
            logger.info("开始重连...")
            # self.stop_heartbeat = True  # 移除心跳停止
            self.stop_save = True
            self.stop_monitor = True
            # 不要停止交易时间检查线程，让它继续监控
            # self.stop_trading_time_check = True
            
            # if self.heartbeat_thread:  # 移除心跳线程处理
            #     self.heartbeat_thread.join(timeout=2)
            if self.save_thread:
                self.save_thread.join(timeout=2)
            if self.monitor_thread:
                self.monitor_thread.join(timeout=2)
            # 不要等待交易时间检查线程结束
            # if self.trading_time_thread:
            #     self.trading_time_thread.join(timeout=2)
            
            # 重置停止标志
            self.stop_save = False
            self.stop_monitor = False
            # self.stop_heartbeat = False  # 移除心跳标志
            
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
    
    # 交易时间控制配置
    ENABLE_TRADING_TIME_CONTROL = True  # 启用交易时间控制
    
    # 高频模式配置
    HIGH_FREQUENCY_MODE = True  # 启用高频模式
    
    print("=" * 60)
    print("159506 ETF基于NautilusTrader Cache的实时数据采集器")
    print("=" * 60)
    print(f"Token: {TOKEN}")
    print(f"股票代码: {STOCK_CODE}")
    print(f"Redis持久化: {USE_REDIS}")
    if USE_REDIS:
        print(f"Redis地址: {REDIS_HOST}:{REDIS_PORT}")
    print(f"交易时间控制: {'启用' if ENABLE_TRADING_TIME_CONTROL else '禁用'}")
    print(f"Catalog路径: catalog/etf_159506_cache")
    print(f"高频模式: {'启用' if HIGH_FREQUENCY_MODE else '禁用'}")
    print("=" * 60)
    
    # 创建WebSocket客户端
    client = ETF159506CacheWebSocketClient(
        TOKEN, STOCK_CODE, USE_REDIS, REDIS_HOST, REDIS_PORT, ENABLE_TRADING_TIME_CONTROL
    )
    
    try:
        # 如果启用交易时间控制，先检查当前是否为交易时间
        if ENABLE_TRADING_TIME_CONTROL:
            trading_manager = TradingTimeManager()
            current_status = trading_manager.get_trading_status()
            
            print(f"当前时间: {current_status['current_time']}")
            print(f"是否为交易日: {current_status['is_trading_day']}")
            print(f"是否为交易时间: {current_status['is_trading_time']}")
            
            if not current_status['is_trading_time']:
                print(f"当前非交易时间，下一个交易时间: {current_status['next_trading_time']}")
                print(f"等待时间: {current_status['time_until_next']}")
                print("系统将在交易时间自动连接...")
                
                # 等待直到交易时间
                trading_manager.wait_until_trading_time()
        
        if client.connect():
            print("Cache数据采集系统运行中... 按Ctrl+C退出")
            print("数据将自动保存到Cache和catalog/etf_159506_cache目录")
            
            # 主循环 - 根据是否启用交易时间控制来决定循环条件
            if ENABLE_TRADING_TIME_CONTROL:
                # 启用交易时间控制时，持续运行直到用户中断
                while True:
                    time.sleep(0.1)  # 减少到0.1秒检查间隔
                    
                    # 每5秒打印一次数据状态（提高监控频率）
                    if int(time.time()) % 5 == 0:
                        status = client.get_status()
                        cache_status = status.get('cache_status', {})
                        trading_status = status.get('trading_status')
                        
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
                        
                        # 显示交易时间状态
                        trading_info = ""
                        if trading_status:
                            trading_info = f", 交易时间: {'是' if trading_status['is_trading_time'] else '否'}"
                        
                        current_time = datetime.now().strftime('%H:%M:%S')
                        print(f"[{current_time}] 数据状态: 连接={status['connected']}, "
                              f"总tick数={cache_status.get('tick_count', 0)}, "
                              f"报价数={cache_status.get('quote_count', 0)}, "
                              f"交易数={cache_status.get('trade_count', 0)}, "
                              f"{price_info}{trading_info}")
                    
                    # 每30秒打印一次详细状态信息（提高监控频率）
                    if int(time.time()) % 30 == 0:
                        status = client.get_status()
                        cache_status = status.get('cache_status', {})
                        trading_status = status.get('trading_status')
                        
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
                        print(f"交易时间控制: {'启用' if status['trading_time_control_enabled'] else '禁用'}")
                        if trading_status:
                            print(f"交易时间状态:")
                            print(f"  当前时间: {trading_status['current_time']}")
                            print(f"  是否为交易日: {trading_status['is_trading_day']}")
                            print(f"  是否为交易时间: {trading_status['is_trading_time']}")
                            print(f"  下一个交易时间: {trading_status['next_trading_time']}")
                            print(f"  距离下次交易: {trading_status['time_until_next']}")
                        print(f"Cache统计:")
                        print(f"  总tick数: {cache_status.get('tick_count', 0)}")
                        print(f"  报价数: {cache_status.get('quote_count', 0)}")
                        print(f"  交易数: {cache_status.get('trade_count', 0)}")
                        print(f"最新数据: {price_info}")
                        print(f"{'='*60}\n")
                    
                    # 检查用户中断
                    try:
                        # 这里可以添加其他检查逻辑
                        pass
                    except KeyboardInterrupt:
                        raise
            else:
                # 不启用交易时间控制时，只在连接状态下运行
                while client.is_connected:
                    time.sleep(0.1)  # 减少到0.1秒检查间隔
                    
                    # 每5秒打印一次数据状态（提高监控频率）
                    if int(time.time()) % 5 == 0:
                        status = client.get_status()
                        cache_status = status.get('cache_status', {})
                        trading_status = status.get('trading_status')
                        
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
                        
                        # 显示交易时间状态
                        trading_info = ""
                        if trading_status:
                            trading_info = f", 交易时间: {'是' if trading_status['is_trading_time'] else '否'}"
                        
                        current_time = datetime.now().strftime('%H:%M:%S')
                        print(f"[{current_time}] 数据状态: 连接={status['connected']}, "
                              f"总tick数={cache_status.get('tick_count', 0)}, "
                              f"报价数={cache_status.get('quote_count', 0)}, "
                              f"交易数={cache_status.get('trade_count', 0)}, "
                              f"{price_info}{trading_info}")
                    
                    # 每30秒打印一次详细状态信息（提高监控频率）
                    if int(time.time()) % 30 == 0:
                        status = client.get_status()
                        cache_status = status.get('cache_status', {})
                        trading_status = status.get('trading_status')
                        
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
                        print(f"交易时间控制: {'启用' if status['trading_time_control_enabled'] else '禁用'}")
                        if trading_status:
                            print(f"交易时间状态:")
                            print(f"  当前时间: {trading_status['current_time']}")
                            print(f"  是否为交易日: {trading_status['is_trading_day']}")
                            print(f"  是否为交易时间: {trading_status['is_trading_time']}")
                            print(f"  下一个交易时间: {trading_status['next_trading_time']}")
                            print(f"  距离下次交易: {trading_status['time_until_next']}")
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