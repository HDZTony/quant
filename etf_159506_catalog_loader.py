#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
159506 ETF Redis K线生成器
专门用于从Redis读取实时数据并生成K线图
"""

import pandas as pd
import numpy as np
from pathlib import Path
import json
import matplotlib.pyplot as plt
import mplfinance as mpf
import seaborn as sns
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import logging
import time
import threading
import argparse

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

# Redis连接检测
REDIS_AVAILABLE = False
try:
    import redis
    REDIS_AVAILABLE = True
    print("[Redis] redis-py已安装，支持Redis数据读取")
except ImportError:
    print("[Redis] 未安装redis-py，无法使用Redis功能")

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 导入TradingTimeManager
try:
    from etf_159506_cache_collector import TradingTimeManager
    TRADING_TIME_MANAGER_AVAILABLE = True
    print("[TradingTimeManager] 已导入，支持交易日判断")
except ImportError as e:
    print(f"[TradingTimeManager] 导入失败: {e}")
    print("[TradingTimeManager] 将使用简单的工作日判断")
    TRADING_TIME_MANAGER_AVAILABLE = False

# NautilusTrader imports (如果可用)
NAUTILUS_AVAILABLE = False
try:
    from nautilus_trader.cache.cache import Cache
    from nautilus_trader.config import CacheConfig, DatabaseConfig
    from nautilus_trader.cache.database import CacheDatabaseAdapter
    from nautilus_trader.model.data import Bar, BarType, BarSpecification
    from nautilus_trader.model.data import QuoteTick, TradeTick
    from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue, TraderId
    from nautilus_trader.model.objects import Price, Quantity
    from nautilus_trader.model.enums import BarAggregation, PriceType, AssetClass, InstrumentClass, AggressorSide
    from nautilus_trader.model.instruments import Equity
    from nautilus_trader.model.objects import Currency
    from nautilus_trader.common.component import LiveClock
    from nautilus_trader.common.component import Logger
    from nautilus_trader.core.uuid import UUID4
    from nautilus_trader.serialization.serializer import MsgSpecSerializer
    from decimal import Decimal
    import msgspec
    
    NAUTILUS_AVAILABLE = True
    print("[NautilusTrader] 已安装，支持Redis数据读取和K线生成")
except ImportError as e:
    print(f"[NautilusTrader] 未安装或导入失败: {e}")
    print("[NautilusTrader] 无法使用Redis功能")


class ETF159506RedisKlineGenerator:
    """159506 ETF Redis K线生成器"""
    
    def __init__(self, redis_host: str = "localhost", redis_port: int = 6379, catalog_path: str = "catalog/etf_159506_cache"):
        if not REDIS_AVAILABLE or not NAUTILUS_AVAILABLE:
            raise RuntimeError("Redis或NautilusTrader不可用")
        
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.catalog_path = Path(catalog_path)
        self.cache = None
        self.instrument_id = None
        self.clock = LiveClock()
        self.logger = Logger("ETF159506RedisKlineGenerator")
        
        # 初始化Redis连接和Cache
        self._init_redis_cache()
        self._init_instrument()
        
        # K线数据存储
        self.kline_data = []
        self.last_update_time = None
        
        logger.info(f"Redis K线生成器初始化完成: {redis_host}:{redis_port}")
        logger.info(f"Catalog路径: {self.catalog_path}")
    
    def _init_redis_cache(self):
        """初始化Redis Cache"""
        try:
            # 创建Cache配置
            cache_config = CacheConfig(
                database=DatabaseConfig(
                    type="redis",
                    host=self.redis_host,
                    port=self.redis_port,
                    timeout=5,
                ),
                tick_capacity=100_000,
                bar_capacity=50_000,
                encoding="msgpack",
                timestamps_as_iso8601=True,
                use_trader_prefix=True,
                use_instance_id=True,
                flush_on_start=False,
                drop_instruments_on_reset=True,
            )
            
            # 创建数据库适配器
            trader_id = TraderId("TRADER-001")
            instance_id = UUID4()
            serializer = MsgSpecSerializer(
                encoding=msgspec.msgpack, 
                timestamps_as_str=True,
                timestamps_as_iso8601=False
            )
            
            database = CacheDatabaseAdapter(
                trader_id=trader_id,
                instance_id=instance_id,
                serializer=serializer,
                config=cache_config,
            )
            
            # 创建Cache
            self.cache = Cache(database=database, config=cache_config)
            logger.info("Redis Cache连接成功")
            
        except Exception as e:
            logger.error(f"初始化Redis Cache失败: {e}")
            raise
    
    def _init_instrument(self):
        """初始化159506 ETF工具"""
        self.instrument_id = InstrumentId(
            symbol=Symbol("159506"),
            venue=Venue("SZSE")
        )
        logger.info(f"工具ID: {self.instrument_id}")
    
    def get_latest_data(self) -> Dict:
        """获取最新数据"""
        try:
            latest_quote = self.cache.quote_tick(self.instrument_id)
            latest_trade = self.cache.trade_tick(self.instrument_id)
            
            # 创建BarType用于获取bar_count
            bar_spec = BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST)
            bar_type = BarType(self.instrument_id, bar_spec)
            
            return {
                'latest_quote': latest_quote,
                'latest_trade': latest_trade,
                'quote_count': self.cache.quote_tick_count(self.instrument_id),
                'trade_count': self.cache.trade_tick_count(self.instrument_id),
                'bar_count': self.cache.bar_count(bar_type),
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"获取最新数据失败: {e}")
            return {}
    
    def generate_kline_from_ticks(self, bar_type) -> Optional['Bar']:
        """从tick数据生成K线"""
        try:
            # 获取交易tick数据
            trade_ticks = self.cache.trade_ticks(self.instrument_id)
            if len(trade_ticks) < 2:
                return None
            
            # 按时间窗口聚合数据
            window_trades = self._get_window_trades(trade_ticks, bar_type)
            
            if len(window_trades) == 0:
                return None
            
            # 计算OHLC
            trade_prices = [float(tick.price) for tick in window_trades]
            trade_volumes = [int(tick.size) for tick in window_trades]
            
            total_volume = sum(trade_volumes)
            if total_volume > 18_446_744_073:  # Quantity最大值
                total_volume = 18_446_744_073
            
            # 创建K线
            bar = Bar(
                bar_type=bar_type,
                open=Price.from_str(str(trade_prices[0])),
                high=Price.from_str(str(max(trade_prices))),
                low=Price.from_str(str(min(trade_prices))),
                close=Price.from_str(str(trade_prices[-1])),
                volume=Quantity.from_int(total_volume),
                ts_event=window_trades[-1].ts_event,
                ts_init=window_trades[-1].ts_init,
            )
            
            return bar
            
        except Exception as e:
            logger.error(f"生成K线失败: {e}")
            return None
    
    def _get_window_trades(self, trade_ticks, bar_type):
        """获取时间窗口内的交易数据"""
        if len(trade_ticks) == 0:
            return []
        
        # 获取当前时间
        current_time = self.clock.timestamp_ns()
        
        # 根据bar_type确定时间窗口
        if bar_type.spec.aggregation == BarAggregation.MINUTE:
            window_ns = 60 * 1_000_000_000  # 1分钟
        elif bar_type.spec.aggregation == BarAggregation.HOUR:
            window_ns = 60 * 60 * 1_000_000_000  # 1小时
        elif bar_type.spec.aggregation == BarAggregation.DAY:
            window_ns = 24 * 60 * 60 * 1_000_000_000  # 1天
        else:
            window_ns = 60 * 1_000_000_000  # 默认1分钟
        
        # 过滤时间窗口内的数据
        window_start = current_time - window_ns
        window_trades = [tick for tick in trade_ticks if tick.ts_event >= window_start]
        
        return window_trades
    
    def get_kline_data(self, limit: int = 100) -> List[Dict]:
        """获取K线数据"""
        try:
            # 首先尝试从Redis获取数据
            if self.cache:
                # 获取交易tick数据
                trade_ticks = self.cache.trade_ticks(self.instrument_id)
                if len(trade_ticks) > 0:
                    # 转换为K线数据格式
                    kline_data = []
                    for tick in trade_ticks[-limit:]:
                        kline_data.append({
                            'timestamp': pd.to_datetime(tick.ts_event, unit='ns'),
                            'price': float(tick.price),
                            'volume': int(tick.size),
                            'trade_id': str(tick.trade_id)
                        })
                    return kline_data
            
            # 如果Redis没有数据，尝试从catalog文件读取
            return self._get_kline_from_catalog()
            
        except Exception as e:
            logger.error(f"获取K线数据失败: {e}")
            return []
    
    def _get_kline_from_catalog(self) -> List[Dict]:
        """从catalog文件获取K线数据"""
        try:
            if not self.catalog_path.exists():
                logger.warning(f"Catalog路径不存在: {self.catalog_path}")
                return []
            
            # 查找最新的parquet文件
            parquet_files = list(self.catalog_path.glob("*.parquet"))
            if not parquet_files:
                logger.warning(f"Catalog目录中没有parquet文件: {self.catalog_path}")
                return []
            
            # 按修改时间排序，获取最新文件
            latest_file = max(parquet_files, key=lambda x: x.stat().st_mtime)
            logger.info(f"从文件读取数据: {latest_file}")
            
            # 读取数据
            df = pd.read_parquet(latest_file)
            
            # 转换为K线数据格式
            kline_data = []
            for _, row in df.iterrows():
                kline_data.append({
                    'timestamp': row['timestamp'],
                    'price': float(row['price']) if pd.notna(row['price']) else None,
                    'volume': int(row['size']) if pd.notna(row['size']) else 0,
                    'trade_id': str(row.get('trade_id', ''))
                })
            
            return kline_data
            
        except Exception as e:
            logger.error(f"从catalog文件读取数据失败: {e}")
            return []
    
    def get_today_kline_data(self, target_date: datetime.date = None) -> List[Dict]:
        """获取指定日期的K线数据，如果不指定则获取今日数据，如果不是交易日则获取上一个交易日数据"""
        try:
            # 如果没有指定日期，使用当前日期
            if target_date is None:
                current_date = datetime.now().date()
                
                # 判断当前是否为交易日
                if TRADING_TIME_MANAGER_AVAILABLE:
                    trading_manager = TradingTimeManager()
                    if trading_manager.is_trading_day(datetime.now()):
                        target_date = current_date
                        logger.info(f"当前是交易日，获取今日({target_date})数据")
                    else:
                        # 获取上一个交易日
                        target_date = self._get_previous_trading_day(current_date)
                        logger.info(f"当前不是交易日，获取上一个交易日({target_date})数据")
                else:
                    # 简单的备用实现：检查是否为工作日
                    if current_date.weekday() < 5:  # 周一到周五
                        target_date = current_date
                        logger.info(f"当前是工作日，获取今日({target_date})数据")
                    else:
                        # 获取上一个工作日
                        target_date = self._get_previous_trading_day(current_date)
                        logger.info(f"当前不是工作日，获取上一个工作日({target_date})数据")
            else:
                logger.info(f"获取指定日期({target_date})数据")
            
            all_data = []
            
            # 首先尝试从Redis获取数据
            if self.cache:
                try:
                    # 获取交易tick数据
                    trade_ticks = self.cache.trade_ticks(self.instrument_id)
                    if len(trade_ticks) > 0:
                        # 过滤目标日期数据
                        for tick in trade_ticks:
                            tick_time = pd.to_datetime(tick.ts_event, unit='ns')
                            if tick_time.date() == target_date:
                                all_data.append({
                                    'timestamp': tick_time,
                                    'price': float(tick.price),
                                    'volume': int(tick.size),
                                    'trade_id': str(tick.trade_id),
                                    'source': 'redis'
                                })
                        
                        logger.info(f"从Redis获取到 {len(all_data)} 条{target_date}数据")
                except Exception as e:
                    logger.warning(f"从Redis获取数据失败: {e}")
            
            # 从catalog文件补充数据
            catalog_data = self._get_data_from_catalog(target_date)
            if catalog_data:
                # 过滤掉Redis中已有的数据（避免重复）
                redis_times = {item['timestamp'] for item in all_data}
                for item in catalog_data:
                    if item['timestamp'] not in redis_times:
                        item['source'] = 'catalog'
                        all_data.append(item)
                
                logger.info(f"从catalog补充 {len(catalog_data)} 条数据")
            
            # 按时间排序
            all_data.sort(key=lambda x: x['timestamp'])
            
            logger.info(f"总共获取到 {len(all_data)} 条{target_date}数据")
            return all_data
            
        except Exception as e:
            logger.error(f"获取K线数据失败: {e}")
            return []
    
    def _get_previous_trading_day(self, current_date: datetime.date) -> datetime.date:
        """获取上一个交易日"""
        if TRADING_TIME_MANAGER_AVAILABLE:
            trading_manager = TradingTimeManager()
            
            # 从当前日期往前查找，直到找到交易日
            previous_date = current_date - timedelta(days=1)
            while not trading_manager.is_trading_day(datetime.combine(previous_date, datetime.min.time())):
                previous_date -= timedelta(days=1)
            
            return previous_date
        else:
            # 简单的备用实现：跳过周末
            previous_date = current_date - timedelta(days=1)
            
            # 跳过周末（周六=5, 周日=6）
            while previous_date.weekday() >= 5:
                previous_date -= timedelta(days=1)
            
            logger.warning(f"使用简单工作日判断，上一个交易日: {previous_date}")
            return previous_date
    
    def _get_data_from_catalog(self, target_date: datetime.date) -> List[Dict]:
        """从catalog文件获取指定日期的数据"""
        try:
            if not self.catalog_path.exists():
                logger.warning(f"Catalog路径不存在: {self.catalog_path}")
                return []
            
            # 查找所有parquet文件
            parquet_files = list(self.catalog_path.glob("*.parquet"))
            if not parquet_files:
                logger.warning(f"Catalog目录中没有parquet文件: {self.catalog_path}")
                return []
            
            logger.info(f"查找{target_date}的数据文件...")
            
            # 读取所有文件并合并数据
            all_dataframes = []
            target_files = []
            
            for file_path in parquet_files:
                try:
                    # 读取数据
                    df = pd.read_parquet(file_path)
                    
                    # 确保timestamp列存在
                    if 'timestamp' not in df.columns:
                        continue
                    
                    # 转换timestamp
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    
                    # 检查是否需要时区转换
                    if df['timestamp'].dt.tz is None:
                        # 假设数据是UTC时间，转换为北京时间
                        import pytz
                        utc_tz = pytz.UTC
                        beijing_tz = pytz.timezone('Asia/Shanghai')
                        
                        # 添加UTC时区信息
                        df['timestamp'] = df['timestamp'].dt.tz_localize(utc_tz)
                        # 转换为北京时间
                        df['timestamp'] = df['timestamp'].dt.tz_convert(beijing_tz)
                    
                    # 检查是否包含目标日期数据
                    file_dates = df['timestamp'].dt.date.unique()
                    if target_date in file_dates:
                        all_dataframes.append(df)
                        target_files.append(file_path.name)
                        logger.info(f"找到{target_date}数据文件: {file_path.name}")
                
                except Exception as e:
                    logger.warning(f"读取文件 {file_path} 失败: {e}")
                    continue
            
            if not all_dataframes:
                logger.warning(f"没有找到{target_date}的数据文件")
                return []
            
            # 合并所有数据
            combined_df = pd.concat(all_dataframes, ignore_index=True)
            logger.info(f"合并了 {len(target_files)} 个文件的数据")
            
            # 去重（按timestamp和trade_id）
            combined_df = combined_df.drop_duplicates(subset=['timestamp', 'trade_id'], keep='last')
            logger.info(f"去重后数据量: {len(combined_df)} 条")
            
            # 过滤目标日期数据（使用北京时间）
            target_data = combined_df[combined_df['timestamp'].dt.date == target_date]
            
            if target_data.empty:
                logger.warning(f"合并后没有{target_date}的数据")
                return []
            
            logger.info(f"读取到 {len(target_data)} 条{target_date}数据")
            
            # 转换为K线数据格式 - 只处理trade类型的数据
            kline_data = []
            for _, row in target_data.iterrows():
                if row['type'] == 'trade' and pd.notna(row['price']):
                    kline_data.append({
                        'timestamp': row['timestamp'],
                        'price': float(row['price']),
                        'volume': int(row['size']) if pd.notna(row['size']) else 0,
                        'trade_id': str(row.get('trade_id', ''))
                    })
            
            logger.info(f"从文件读取到 {len(kline_data)} 条{target_date}交易数据")
            return kline_data
            
        except Exception as e:
            logger.error(f"从catalog文件读取{target_date}数据失败: {e}")
            return []
    
    def _get_today_data_from_catalog(self) -> List[Dict]:
        """从catalog文件获取今日数据（保持向后兼容）"""
        return self._get_data_from_catalog(datetime.now().date())
    
    def create_realtime_kline_chart(self, save_path: str = None, auto_refresh: bool = True, target_date: datetime.date = None, trade_signals: List[Dict] = None, technical_signals: List[Dict] = None, extremes_data: Dict = None):
        """创建实时K线图"""
        if auto_refresh:
            self._start_realtime_chart(save_path, target_date, trade_signals, technical_signals, extremes_data)
        else:
            self._plot_kline_chart(save_path, target_date, trade_signals, technical_signals, extremes_data)
    
    def create_trade_points_chart(self, save_path: str = None, target_date: datetime.date = None, trade_signals: List[Dict] = None):
        """创建专门的买卖点图表"""
        try:
            # 获取数据
            kline_data = self.get_today_kline_data(target_date)
            
            if not kline_data:
                logger.warning("没有数据可绘制")
                return
            
            # 转换为DataFrame
            df = pd.DataFrame(kline_data)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # 检查是否需要时区转换
            if df['timestamp'].dt.tz is None:
                # 假设是UTC时间，转换为北京时间
                import pytz
                utc_tz = pytz.UTC
                beijing_tz = pytz.timezone('Asia/Shanghai')
                
                # 添加UTC时区信息
                df['timestamp'] = df['timestamp'].dt.tz_localize(utc_tz)
                # 转换为北京时间
                df['timestamp'] = df['timestamp'].dt.tz_convert(beijing_tz)
                logger.info("已将UTC时间转换为北京时间")
            
            df = df.sort_values('timestamp')
            
            # 检查数据时间范围
            start_time = df['timestamp'].min()
            end_time = df['timestamp'].max()
            data_date = start_time.date()
            logger.info(f"数据时间范围: {start_time} 到 {end_time}")
            logger.info(f"数据条数: {len(df)}")
            
            # 确定图表标题
            if target_date:
                chart_title = f'159506 ETF {target_date} 买卖点分析 (北京时间)'
            else:
                chart_title = f'159506 ETF {data_date} 买卖点分析 (北京时间)'
            
            # 过滤交易时间内的数据，正确处理午休时间
            from datetime import time as datetime_time
            morning_start = datetime_time(9, 30)
            morning_end = datetime_time(11, 30)
            afternoon_start = datetime_time(13, 0)
            afternoon_end = datetime_time(15, 0)
            
            # 分别获取上午和下午的数据
            morning_data = df[
                (df['timestamp'].dt.time >= morning_start) & 
                (df['timestamp'].dt.time <= morning_end)
            ]
            
            afternoon_data = df[
                (df['timestamp'].dt.time >= afternoon_start) & 
                (df['timestamp'].dt.time <= afternoon_end)
            ]
            
            # 合并上午和下午数据
            trading_data = pd.concat([morning_data, afternoon_data])
            
            if len(trading_data) == 0:
                logger.warning("没有交易时间内的数据可绘制")
                return
            
            logger.info(f"上午数据: {len(morning_data)} 条")
            logger.info(f"下午数据: {len(afternoon_data)} 条")
            logger.info(f"总交易数据: {len(trading_data)} 条")
            
            # 使用交易数据的时间作为索引
            trading_data = trading_data.set_index('timestamp')
            
            # 确保索引为升序、唯一、无NaN
            complete_df = trading_data.copy()
            complete_df = complete_df.sort_index()
            complete_df = complete_df[~complete_df.index.duplicated(keep='first')]
            complete_df = complete_df[complete_df.index.notnull()]

            # 创建三联图，专门用于买卖点分析
            fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(20, 24), height_ratios=[2, 1, 1])

            # 创建时间轴映射，保持时间连续性但保留原始时间信息
            def create_time_mapping(df):
                """创建时间轴映射，保持图表连续性"""
                new_times = []
                time_mapping = {}
                original_time_mapping = {}  # 保存原始时间到映射时间的对应关系
                
                for idx in df.index:
                    current_time = idx.time()
                    
                    if current_time < datetime_time(11, 30):
                        # 上午时间保持不变
                        new_time = idx
                    elif current_time > datetime_time(13, 0):
                        # 下午时间减去1.5小时（午休时间），保持图表连续
                        new_time = idx - timedelta(hours=1, minutes=30)
                    else:
                        # 午休时间的数据跳过
                        continue
                    
                    new_times.append(new_time)
                    time_mapping[idx] = new_time
                    original_time_mapping[new_time] = idx  # 反向映射
                
                return pd.DatetimeIndex(new_times), time_mapping, original_time_mapping
            
            # 创建时间映射
            new_index, time_mapping, original_time_mapping = create_time_mapping(complete_df)
            
            # 重新索引数据
            mapped_df = complete_df[complete_df.index.isin(time_mapping.keys())].copy()
            mapped_df.index = [time_mapping[idx] for idx in mapped_df.index]
            
            # 设置x轴范围
            x_min = new_index.min()
            x_max = new_index.max()
            
            # 为每个子图设置相同的x轴范围
            for ax in [ax1, ax2, ax3]:
                ax.set_xlim(x_min, x_max)

            # ====== ax1主图（价格走势 + 买卖点） ======
            # 绘制价格走势（使用更清晰的线条，设置较高的zorder确保在买卖点之上）
            ax1.plot(mapped_df.index, mapped_df['price'], linewidth=1.0, color='darkblue', alpha=1.0, label='价格走势', zorder=20)
            
            # 标记关键价格点
            valid_prices = mapped_df['price'].dropna()
            if len(valid_prices) > 0:
                # 开盘价（第一个有效价格）
                open_price = valid_prices.iloc[0]
                open_time = valid_prices.index[0]
                ax1.scatter(open_time, open_price, color='black', s=80, marker='o', label='开盘', zorder=5)
                
                # 当前价（最后一个有效价格）
                current_price = valid_prices.iloc[-1]
                current_time = valid_prices.index[-1]
                ax1.scatter(current_time, current_price, color='black', s=80, marker='o', label='当前', zorder=5)
                
                # 最高价
                high_price = valid_prices.max()
                high_time = valid_prices.idxmax()
                ax1.scatter(high_time, high_price, color='orange', s=60, marker='^', label='最高', zorder=5)
                
                # 最低价
                low_price = valid_prices.min()
                low_time = valid_prices.idxmin()
                ax1.scatter(low_time, low_price, color='purple', s=60, marker='v', label='最低', zorder=5)
                
                # 添加价格信息
                price_info = f'开盘: {open_price:.3f} ({open_time.strftime("%H:%M")})\n'
                price_info += f'当前: {current_price:.3f} ({current_time.strftime("%H:%M")})\n'
                price_info += f'最高: {high_price:.3f} ({high_time.strftime("%H:%M")})\n'
                price_info += f'最低: {low_price:.3f} ({low_time.strftime("%H:%M")})'
                
                ax1.text(0.02, 0.98, price_info, 
                       transform=ax1.transAxes, verticalalignment='top', 
                       bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
            
            ax1.set_title(chart_title, fontsize=16, fontweight='bold')
            ax1.set_ylabel('价格', fontsize=12)
            ax1.grid(True, alpha=0.3)
            
            # 添加买卖点标记
            if trade_signals and len(trade_signals) > 0:
                buy_signals = []
                sell_signals = []
                hold_signals = []
                watch_signals = []
                
                # 添加调试日志
                logger.info(f"开始处理 {len(trade_signals)} 个交易信号...")
                logger.info(f"图表时间范围: {mapped_df.index.min()} 到 {mapped_df.index.max()}")
                
                for i, signal in enumerate(trade_signals):
                    logger.info(f"处理第 {i+1} 个信号: {signal}")
                    
                    # 转换时间戳为pandas datetime
                    if isinstance(signal['timestamp'], str):
                        signal_time = pd.to_datetime(signal['timestamp'])
                    else:
                        signal_time = signal['timestamp']
                    
                    # 确保时间戳有时区信息，与图表数据保持一致
                    if not isinstance(signal_time, pd.Timestamp):
                        signal_time = pd.Timestamp(signal_time)
                    
                    if signal_time.tz is None:
                        # 如果没有时区信息，假设是UTC时间，转换为北京时间
                        import pytz
                        utc_tz = pytz.UTC
                        beijing_tz = pytz.timezone('Asia/Shanghai')
                        signal_time = signal_time.tz_localize(utc_tz).tz_convert(beijing_tz)
                    
                    logger.info(f"信号 {i+1} 原始时间: {signal_time}")
                    
                    # 应用相同的时间映射，保持图表连续性
                    current_time = signal_time.time()
                    if current_time < datetime_time(11, 30):
                        # 上午时间保持不变
                        mapped_signal_time = signal_time
                        logger.info(f"信号 {i+1} 上午时间，映射后时间: {mapped_signal_time}")
                    elif current_time > datetime_time(13, 0):
                        # 下午时间减去1.5小时（午休时间），保持图表连续
                        mapped_signal_time = signal_time - timedelta(hours=1, minutes=30)
                        logger.info(f"信号 {i+1} 下午时间，映射后时间: {mapped_signal_time}")
                    else:
                        # 午休时间的信号跳过
                        logger.warning(f"信号 {i+1} 在午休时间 {current_time}，跳过")
                        continue
                    
                    # 检查映射后的时间是否在图表范围内，如果超出范围则调整到最近的有效时间
                    if mapped_signal_time < mapped_df.index.min():
                        logger.warning(f"信号 {i+1} 映射后时间 {mapped_signal_time} 早于图表开始时间，调整到 {mapped_df.index.min()}")
                        mapped_signal_time = mapped_df.index.min()
                    elif mapped_signal_time > mapped_df.index.max():
                        logger.warning(f"信号 {i+1} 映射后时间 {mapped_signal_time} 晚于图表结束时间，调整到 {mapped_df.index.max()}")
                        mapped_signal_time = mapped_df.index.max()
                    
                    logger.info(f"信号 {i+1} 最终映射时间: {mapped_signal_time}")
                    
                    # 所有信号都添加到对应列表（经过时间调整后）
                    if signal['side'] == 'BUY':
                        buy_signals.append({
                            'timestamp': mapped_signal_time,
                            'price': signal['price'],
                            'original_time': signal_time,  # 保存原始时间
                            'signal_type': signal.get('signal_type', 'unknown')
                        })
                    elif signal['side'] == 'SELL':
                        sell_signals.append({
                            'timestamp': mapped_signal_time,
                            'price': signal['price'],
                            'original_time': signal_time,  # 保存原始时间
                            'signal_type': signal.get('signal_type', 'unknown')
                        })
                    elif signal['side'] == 'HOLD':
                        hold_signals.append({
                            'timestamp': mapped_signal_time,
                            'price': signal['price'],
                            'original_time': signal_time,  # 保存原始时间
                            'signal_type': signal.get('signal_type', 'unknown')
                        })
                    elif signal['side'] == 'WATCH':
                        watch_signals.append({
                            'timestamp': mapped_signal_time,
                            'price': signal['price'],
                            'original_time': signal_time,  # 保存原始时间
                            'signal_type': signal.get('signal_type', 'unknown')
                        })
                
                logger.info(f"信号处理完成: 买入={len(buy_signals)}, 卖出={len(sell_signals)}, 持有={len(hold_signals)}, 观望={len(watch_signals)}")
                
                # 绘制买入点（红色三角形向上，更大更显眼）
                if buy_signals:
                    buy_df = pd.DataFrame(buy_signals)
                    ax1.scatter(buy_df['timestamp'], buy_df['price'], 
                               color='red', marker='^', s=200, label='买入信号', zorder=25, alpha=0.9)
                    # 添加买入点标注
                    for _, row in buy_df.iterrows():
                        # 显示原始时间（北京时间）
                        original_time_str = row['original_time'].strftime('%H:%M')
                        signal_type = row.get('signal_type', 'unknown')
                        ax1.annotate(f'买入\n{row["price"]:.3f}\n{original_time_str}\n{signal_type}', 
                                   xy=(row['timestamp'], row['price']),
                                   xytext=(15, 15), textcoords='offset points',
                                   fontsize=10, color='red', weight='bold',
                                   bbox=dict(boxstyle='round,pad=0.5', facecolor='red', alpha=0.3))
                
                # 绘制卖出点（绿色三角形向下，更大更显眼）
                if sell_signals:
                    sell_df = pd.DataFrame(sell_signals)
                    ax1.scatter(sell_df['timestamp'], sell_df['price'], 
                               color='green', marker='v', s=200, label='卖出信号', zorder=25, alpha=0.9)
                    # 添加卖出点标注
                    for _, row in sell_df.iterrows():
                        # 显示原始时间（北京时间）
                        original_time_str = row['original_time'].strftime('%H:%M')
                        signal_type = row.get('signal_type', 'unknown')
                        ax1.annotate(f'卖出\n{row["price"]:.3f}\n{original_time_str}\n{signal_type}', 
                                   xy=(row['timestamp'], row['price']),
                                   xytext=(15, -25), textcoords='offset points',
                                   fontsize=10, color='green', weight='bold',
                                   bbox=dict(boxstyle='round,pad=0.5', facecolor='green', alpha=0.3))
                
                # 绘制持有点（蓝色圆点）
                if hold_signals:
                    hold_df = pd.DataFrame(hold_signals)
                    ax1.scatter(hold_df['timestamp'], hold_df['price'], 
                               color='blue', marker='o', s=120, label='持有信号', zorder=25, alpha=0.8)
                    # 添加持有点标注
                    for _, row in hold_df.iterrows():
                        original_time_str = row['original_time'].strftime('%H:%M')
                        signal_type = row.get('signal_type', 'unknown')
                        ax1.annotate(f'持有\n{row["price"]:.3f}\n{original_time_str}\n{signal_type}', 
                                   xy=(row['timestamp'], row['price']),
                                   xytext=(15, 0), textcoords='offset points',
                                   fontsize=9, color='blue', weight='bold',
                                   bbox=dict(boxstyle='round,pad=0.4', facecolor='blue', alpha=0.2))
                
                # 绘制观望点（黄色方块）
                if watch_signals:
                    watch_df = pd.DataFrame(watch_signals)
                    ax1.scatter(watch_df['timestamp'], watch_df['price'], 
                               color='orange', marker='s', s=120, label='观望信号', zorder=25, alpha=0.8)
                    # 添加观望点标注
                    for _, row in watch_df.iterrows():
                        original_time_str = row['original_time'].strftime('%H:%M')
                        signal_type = row.get('signal_type', 'unknown')
                        ax1.annotate(f'观望\n{row["price"]:.3f}\n{original_time_str}\n{signal_type}', 
                                   xy=(row['timestamp'], row['price']),
                                   xytext=(15, 0), textcoords='offset points',
                                   fontsize=9, color='orange', weight='bold',
                                   bbox=dict(boxstyle='round,pad=0.4', facecolor='orange', alpha=0.2))
                
                logger.info(f"添加了 {len(buy_signals)} 个买入点、{len(sell_signals)} 个卖出点、{len(hold_signals)} 个持有点、{len(watch_signals)} 个观望点")
            else:
                logger.info("没有交易信号数据")
            
            # 添加信号类型说明
            signal_info = "信号说明:\n"
            signal_info += "▲ 买入信号: 金叉出现，无持仓时买入\n"
            signal_info += "▼ 卖出信号: 死叉出现，有持仓时卖出\n"
            signal_info += "● 持有信号: 金叉出现，已有持仓时持有\n"
            signal_info += "■ 观望信号: 死叉出现，无持仓时观望"
            
            ax1.text(0.02, 0.85, signal_info, 
                   transform=ax1.transAxes, verticalalignment='top', 
                   bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.8),
                   fontsize=10)
            
            ax1.legend(loc='upper right')
            
            # 设置x轴格式 - 显示北京时间
            ax1.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
            ax1.xaxis.set_major_locator(plt.matplotlib.dates.MinuteLocator(interval=10))  # 每10分钟一个刻度
            plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)
            
            # 设置y轴格式 - 价格三位小数
            import matplotlib.ticker as mticker
            ax1.yaxis.set_major_formatter(mticker.FormatStrFormatter('%.3f'))

            # 添加x轴标签（明确标注北京时间）
            ax1.set_xlabel('时间 (北京时间)', fontsize=13)
            ax1.annotate('所有横轴时间均为北京时间', xy=(1, 0), xycoords='axes fraction', fontsize=11, color='gray', ha='right', va='top')
            
            # ====== ax2成交量 ======
            # 跳过第一条（因为是累积成交量）
            vol_df = mapped_df.iloc[1:].copy()
            if len(vol_df) > 0:
                # 计算涨跌颜色
                price_arr = vol_df['price'].values
                prev_price_arr = mapped_df['price'].values[:-1]
                colors = np.where(price_arr > prev_price_arr, 'red', np.where(price_arr < prev_price_arr, 'green', 'gray'))
                
                # 绘制成交量柱状图
                ax2.bar(vol_df.index, vol_df['volume'], alpha=0.6, color=colors, width=0.0005)
                ax2.set_title('成交量', fontsize=12)
                ax2.set_ylabel('成交量', fontsize=10)
                ax2.grid(True, alpha=0.3)
                
                # 设置x轴格式
                ax2.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
                ax2.xaxis.set_major_locator(plt.matplotlib.dates.MinuteLocator(interval=10))
                plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
            
            # ====== ax3 MACD指标 ======
            # 生成1分钟K线收盘价序列，用于技术指标
            minute_close = mapped_df['price'].resample('1min').last().dropna()
            minute_index = minute_close.index
            
            if len(minute_close) > 0:
                ema12 = minute_close.ewm(span=12, adjust=False).mean()
                ema26 = minute_close.ewm(span=26, adjust=False).mean()
                dif = ema12 - ema26  # DIF
                dea = dif.ewm(span=9, adjust=False).mean()  # DEA
                macd_hist = 2 * (dif - dea) # MACD柱子
                macd_colors = np.where(macd_hist > 0, 'red', np.where(macd_hist < 0, 'green', 'gray'))
                
                ax3.bar(minute_index, macd_hist, color=macd_colors, width=0.0005, alpha=0.7, label='MACD柱')
                ax3.plot(minute_index, dif, color='orange', linewidth=1.5, label='DIF线')
                ax3.plot(minute_index, dea, color='deepskyblue', linewidth=1.5, label='DEA线')
                ax3.set_title('MACD指标 (12,26,9)', fontsize=12)
                ax3.set_ylabel('MACD', fontsize=10)
                ax3.set_xlabel('时间 (北京时间)', fontsize=13)
                ax3.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
                ax3.xaxis.set_major_locator(plt.matplotlib.dates.MinuteLocator(interval=10))
                plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45)
                ax3.legend(loc='upper right')
                ax3.grid(True, alpha=0.3)
            
            # 调整布局
            plt.tight_layout()
            
            # 保存图片
            if save_path:
                plt.savefig(save_path, dpi=300, bbox_inches='tight')
                logger.info(f"买卖点图表已保存到: {save_path}")
            
            # 显示图表（非阻塞模式）
            plt.show(block=False)
            
            logger.info("买卖点图表生成完成")
            
        except Exception as e:
            logger.error(f"生成买卖点图表失败: {e}")
            import traceback
            logger.error(f"详细错误: {traceback.format_exc()}")
    
    def _plot_kline_chart(self, save_path: str = None, target_date: datetime.date = None, trade_signals: List[Dict] = None, technical_signals: List[Dict] = None, extremes_data: Dict = None):
        """绘制价格走势图"""
        try:
            # 获取数据
            kline_data = self.get_today_kline_data(target_date)
            
            if not kline_data:
                logger.warning("没有数据可绘制")
                return
            
            # 转换为DataFrame
            df = pd.DataFrame(kline_data)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # 检查是否需要时区转换
            if df['timestamp'].dt.tz is None:
                # 假设是UTC时间，转换为北京时间
                import pytz
                utc_tz = pytz.UTC
                beijing_tz = pytz.timezone('Asia/Shanghai')
                
                # 添加UTC时区信息
                df['timestamp'] = df['timestamp'].dt.tz_localize(utc_tz)
                # 转换为北京时间
                df['timestamp'] = df['timestamp'].dt.tz_convert(beijing_tz)
                logger.info("已将UTC时间转换为北京时间")
            
            df = df.sort_values('timestamp')
            
            # 检查数据时间范围
            start_time = df['timestamp'].min()
            end_time = df['timestamp'].max()
            data_date = start_time.date()
            logger.info(f"数据时间范围: {start_time} 到 {end_time}")
            logger.info(f"数据条数: {len(df)}")
            
            # 确定图表标题
            if target_date:
                chart_title = f'159506 ETF {target_date} 价格走势 (北京时间)'
            else:
                chart_title = f'159506 ETF {data_date} 价格走势 (北京时间)'
            
            # 过滤交易时间内的数据，正确处理午休时间
            from datetime import time as datetime_time
            morning_start = datetime_time(9, 30)
            morning_end = datetime_time(11, 30)
            afternoon_start = datetime_time(13, 0)
            afternoon_end = datetime_time(15, 0)
            
            # 分别获取上午和下午的数据
            morning_data = df[
                (df['timestamp'].dt.time >= morning_start) & 
                (df['timestamp'].dt.time <= morning_end)
            ]
            
            afternoon_data = df[
                (df['timestamp'].dt.time >= afternoon_start) & 
                (df['timestamp'].dt.time <= afternoon_end)
            ]
            
            # 合并上午和下午数据
            trading_data = pd.concat([morning_data, afternoon_data])
            
            if len(trading_data) == 0:
                logger.warning("没有交易时间内的数据可绘制")
                return
            
            logger.info(f"上午数据: {len(morning_data)} 条")
            logger.info(f"下午数据: {len(afternoon_data)} 条")
            logger.info(f"总交易数据: {len(trading_data)} 条")
            
            # 使用交易数据的时间作为索引
            trading_data = trading_data.set_index('timestamp')
            
            # 确保索引为升序、唯一、无NaN
            complete_df = trading_data.copy()
            complete_df = complete_df.sort_index()
            complete_df = complete_df[~complete_df.index.duplicated(keep='first')]
            complete_df = complete_df[complete_df.index.notnull()]

            # 创建五联图，图片高度更大
            fig, (ax1, ax2, ax3, ax4, ax5) = plt.subplots(5, 1, figsize=(20, 40), height_ratios=[3, 1, 1, 1, 1])

            # 创建时间轴映射，保持时间连续性但保留原始时间信息
            def create_time_mapping(df):
                """创建时间轴映射，保持图表连续性"""
                new_times = []
                time_mapping = {}
                original_time_mapping = {}  # 保存原始时间到映射时间的对应关系
                
                for idx in df.index:
                    current_time = idx.time()
                    
                    if current_time < datetime_time(11, 30):
                        # 上午时间保持不变
                        new_time = idx
                    elif current_time > datetime_time(13, 0):
                        # 下午时间减去1.5小时（午休时间），保持图表连续
                        new_time = idx - timedelta(hours=1, minutes=30)
                    else:
                        # 午休时间的数据跳过
                        continue
                    
                    new_times.append(new_time)
                    time_mapping[idx] = new_time
                    original_time_mapping[new_time] = idx  # 反向映射
                
                return pd.DatetimeIndex(new_times), time_mapping, original_time_mapping
            
            # 创建时间映射
            new_index, time_mapping, original_time_mapping = create_time_mapping(complete_df)
            
            # 重新索引数据
            mapped_df = complete_df[complete_df.index.isin(time_mapping.keys())].copy()
            mapped_df.index = [time_mapping[idx] for idx in mapped_df.index]
            
            # 设置x轴范围
            x_min = new_index.min()
            x_max = new_index.max()
            
            # 为每个子图设置相同的x轴范围
            for ax in [ax1, ax2, ax3, ax4, ax5]:
                ax.set_xlim(x_min, x_max)
            


            # ====== ax1主图（价格走势） ======
            # 绘制价格走势
            ax1.plot(mapped_df.index, mapped_df['price'], linewidth=1, color='blue', alpha=0.8, label='成交价')
            
            # 标记关键价格点
            valid_prices = mapped_df['price'].dropna()
            if len(valid_prices) > 0:
                # 开盘价（第一个有效价格）
                open_price = valid_prices.iloc[0]
                open_time = valid_prices.index[0]
                ax1.scatter(open_time, open_price, color='green', s=100, marker='o', label='开盘')
                
                # 当前价（最后一个有效价格）
                current_price = valid_prices.iloc[-1]
                current_time = valid_prices.index[-1]
                ax1.scatter(current_time, current_price, color='red', s=100, marker='o', label='当前')
                
                # 最高价
                high_price = valid_prices.max()
                high_time = valid_prices.idxmax()
                ax1.scatter(high_time, high_price, color='orange', s=80, marker='^', label='最高')
                
                # 最低价
                low_price = valid_prices.min()
                low_time = valid_prices.idxmin()
                ax1.scatter(low_time, low_price, color='purple', s=80, marker='v', label='最低')
                
                # 添加价格信息 - 显示北京时间，精确到三位小数
                price_info = f'开盘: {open_price:.3f} ({open_time.strftime("%H:%M")})\n'
                price_info += f'当前: {current_price:.3f} ({current_time.strftime("%H:%M")})\n'
                price_info += f'最高: {high_price:.3f} ({high_time.strftime("%H:%M")})\n'
                price_info += f'最低: {low_price:.3f} ({low_time.strftime("%H:%M")})'
                
                ax1.text(0.02, 0.98, price_info, 
                       transform=ax1.transAxes, verticalalignment='top', 
                       bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
            
            ax1.set_title(chart_title)
            ax1.set_ylabel('价格')
            ax1.grid(True, alpha=0.3)
            
            # 添加信号类型说明
            signal_info = "信号说明:\n"
            signal_info += "▲ 买入信号: 金叉出现，无持仓时买入\n"
            signal_info += "▼ 卖出信号: 死叉出现，有持仓时卖出\n"
            signal_info += "● 持有信号: 金叉出现，已有持仓时持有\n"
            signal_info += "■ 观望信号: 死叉出现，无持仓时观望\n"
            
            ax1.text(0.02, 0.85, signal_info, 
                   transform=ax1.transAxes, verticalalignment='top', 
                   bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.8),
                   fontsize=9)
            
            ax1.legend(loc='upper right')
            
            # 设置x轴格式 - 显示北京时间
            ax1.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
            ax1.xaxis.set_major_locator(plt.matplotlib.dates.MinuteLocator(interval=10))  # 每10分钟一个刻度
            plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)
            
            # 设置y轴格式 - 价格三位小数
            import matplotlib.ticker as mticker
            ax1.yaxis.set_major_formatter(mticker.FormatStrFormatter('%.3f'))

            # 添加x轴标签（明确标注北京时间）
            ax1.set_xlabel('时间 (北京时间)', fontsize=13)
            ax1.annotate('所有横轴时间均为北京时间', xy=(1, 0), xycoords='axes fraction', fontsize=11, color='gray', ha='right', va='top')
            
            # ====== 添加买卖点标记 ======
            # 初始化信号列表
            buy_signals = []
            sell_signals = []
            hold_signals = []
            watch_signals = []
            
            if trade_signals and len(trade_signals) > 0:
                
                # 添加调试日志
                logger.info(f"开始处理 {len(trade_signals)} 个交易信号...")
                logger.info(f"图表时间范围: {mapped_df.index.min()} 到 {mapped_df.index.max()}")
                
                for i, signal in enumerate(trade_signals):
                    logger.info(f"处理第 {i+1} 个信号: {signal}")
                    
                    # 转换时间戳为pandas datetime
                    if isinstance(signal['timestamp'], str):
                        signal_time = pd.to_datetime(signal['timestamp'])
                    else:
                        signal_time = signal['timestamp']
                    
                    # 确保时间戳有时区信息，与图表数据保持一致
                    if not isinstance(signal_time, pd.Timestamp):
                        signal_time = pd.Timestamp(signal_time)
                    
                    if signal_time.tz is None:
                        # 如果没有时区信息，假设是UTC时间，转换为北京时间
                        import pytz
                        utc_tz = pytz.UTC
                        beijing_tz = pytz.timezone('Asia/Shanghai')
                        signal_time = signal_time.tz_localize(utc_tz).tz_convert(beijing_tz)
                    
                    logger.info(f"信号 {i+1} 原始时间: {signal_time}")
                    
                    # 应用相同的时间映射，保持图表连续性
                    current_time = signal_time.time()
                    if current_time < datetime_time(11, 30):
                        # 上午时间保持不变
                        mapped_signal_time = signal_time
                        logger.info(f"信号 {i+1} 上午时间，映射后时间: {mapped_signal_time}")
                    elif current_time > datetime_time(13, 0):
                        # 下午时间减去1.5小时（午休时间），保持图表连续
                        mapped_signal_time = signal_time - timedelta(hours=1, minutes=30)
                        logger.info(f"信号 {i+1} 下午时间，映射后时间: {mapped_signal_time}")
                    else:
                        # 午休时间的信号跳过
                        logger.warning(f"信号 {i+1} 在午休时间 {current_time}，跳过")
                        continue
                    
                    # 检查映射后的时间是否在图表范围内，如果超出范围则调整到最近的有效时间
                    if mapped_signal_time < mapped_df.index.min():
                        logger.warning(f"信号 {i+1} 映射后时间 {mapped_signal_time} 早于图表开始时间，调整到 {mapped_df.index.min()}")
                        mapped_signal_time = mapped_df.index.min()
                    elif mapped_signal_time > mapped_df.index.max():
                        logger.warning(f"信号 {i+1} 映射后时间 {mapped_signal_time} 晚于图表结束时间，调整到 {mapped_df.index.max()}")
                        mapped_signal_time = mapped_df.index.max()
                    
                    logger.info(f"信号 {i+1} 最终映射时间: {mapped_signal_time}")
                    
                    # 所有信号都添加到对应列表（经过时间调整后）
                    if signal['side'] == 'BUY':
                        buy_signals.append({
                            'timestamp': mapped_signal_time,
                            'price': signal['price'],
                            'original_time': signal_time,  # 保存原始时间
                            'signal_type': signal.get('signal_type', 'unknown')
                        })
                    elif signal['side'] == 'SELL':
                        sell_signals.append({
                            'timestamp': mapped_signal_time,
                            'price': signal['price'],
                            'original_time': signal_time,  # 保存原始时间
                            'signal_type': signal.get('signal_type', 'unknown')
                        })
                    elif signal['side'] == 'HOLD':
                        hold_signals.append({
                            'timestamp': mapped_signal_time,
                            'price': signal['price'],
                            'original_time': signal_time,  # 保存原始时间
                            'signal_type': signal.get('signal_type', 'unknown')
                        })
                    elif signal['side'] == 'WATCH':
                        watch_signals.append({
                            'timestamp': mapped_signal_time,
                            'price': signal['price'],
                            'original_time': signal_time,  # 保存原始时间
                            'signal_type': signal.get('signal_type', 'unknown')
                        })
                
                logger.info(f"信号处理完成: 买入={len(buy_signals)}, 卖出={len(sell_signals)}, 持有={len(hold_signals)}, 观望={len(watch_signals)}")
                logger.info(f"总共 {len(buy_signals) + len(sell_signals) + len(hold_signals) + len(watch_signals)} 个信号被添加到图表")
            
            # ====== 添加技术指标信号标记 ======
            technical_markers = []
            if technical_signals and len(technical_signals) > 0:
                logger.info(f"开始处理 {len(technical_signals)} 个技术指标信号...")
                
                for i, signal in enumerate(technical_signals):
                    logger.info(f"处理第 {i+1} 个技术信号: {signal}")
                    
                    # 转换时间戳为pandas datetime
                    if isinstance(signal['timestamp'], str):
                        signal_time = pd.to_datetime(signal['timestamp'])
                    else:
                        signal_time = signal['timestamp']
                    
                    # 确保时间戳有时区信息，与图表数据保持一致
                    if not isinstance(signal_time, pd.Timestamp):
                        signal_time = pd.Timestamp(signal_time)
                    
                    if signal_time.tz is None:
                        # 如果没有时区信息，假设是UTC时间，转换为北京时间
                        import pytz
                        utc_tz = pytz.UTC
                        beijing_tz = pytz.timezone('Asia/Shanghai')
                        signal_time = signal_time.tz_localize(utc_tz).tz_convert(beijing_tz)
                    
                    # 应用相同的时间映射，保持图表连续性
                    current_time = signal_time.time()
                    if current_time < datetime_time(11, 30):
                        # 上午时间保持不变
                        mapped_signal_time = signal_time
                    elif current_time > datetime_time(13, 0):
                        # 下午时间减去1.5小时（午休时间），保持图表连续
                        mapped_signal_time = signal_time - timedelta(hours=1, minutes=30)
                    else:
                        # 午休时间的信号跳过
                        logger.warning(f"技术信号 {i+1} 在午休时间 {current_time}，跳过")
                        continue
                    
                    # 检查映射后的时间是否在图表范围内
                    if mapped_signal_time < mapped_df.index.min():
                        mapped_signal_time = mapped_df.index.min()
                    elif mapped_signal_time > mapped_df.index.max():
                        mapped_signal_time = mapped_df.index.max()
                    
                    # 根据信号类型设置不同的标记样式
                    signal_type = signal.get('signal_type', 'unknown')
                    if signal_type == 'golden_cross':
                        marker_style = '^'  # 上三角
                        marker_color = 'green'
                        marker_size = 100
                    elif signal_type == 'death_cross':
                        marker_style = 'v'  # 下三角
                        marker_color = 'red'
                        marker_size = 100
                    elif signal_type == 'top_divergence':
                        marker_style = 's'  # 正方形
                        marker_color = 'orange'
                        marker_size = 80
                    elif signal_type == 'bottom_divergence':
                        marker_style = 's'  # 正方形
                        marker_color = 'purple'
                        marker_size = 80
                    else:
                        marker_style = 'o'  # 圆形
                        marker_color = 'gray'
                        marker_size = 60
                    
                    technical_markers.append({
                        'timestamp': mapped_signal_time,
                        'price': signal['price'],
                        'signal_type': signal_type,
                        'marker_style': marker_style,
                        'marker_color': marker_color,
                        'marker_size': marker_size,
                        'signal_value': signal.get('signal_value', 0)
                    })
                
                logger.info(f"技术信号处理完成: {len(technical_markers)} 个技术信号被添加到图表")
                
                # 绘制买入点（红色三角形向上）
                if buy_signals:
                    buy_df = pd.DataFrame(buy_signals)
                    ax1.scatter(buy_df['timestamp'], buy_df['price'], 
                               color='red', marker='^', s=150, label='买入信号', zorder=10, alpha=0.8)
                    # 添加买入点标注
                    for _, row in buy_df.iterrows():
                        # 显示原始时间（北京时间）
                        original_time_str = row['original_time'].strftime('%H:%M')
                        ax1.annotate(f'买入\n{row["price"]:.3f}\n{original_time_str}', 
                                   xy=(row['timestamp'], row['price']),
                                   xytext=(10, 10), textcoords='offset points',
                                   fontsize=8, color='red', weight='bold',
                                   bbox=dict(boxstyle='round,pad=0.3', facecolor='red', alpha=0.2))
                
                # 绘制卖出点（绿色三角形向下）
                if sell_signals:
                    sell_df = pd.DataFrame(sell_signals)
                    ax1.scatter(sell_df['timestamp'], sell_df['price'], 
                               color='green', marker='v', s=150, label='卖出信号', zorder=10, alpha=0.8)
                    # 添加卖出点标注
                    for _, row in sell_df.iterrows():
                        # 显示原始时间（北京时间）
                        original_time_str = row['original_time'].strftime('%H:%M')
                        ax1.annotate(f'卖出\n{row["price"]:.3f}\n{original_time_str}', 
                                   xy=(row['timestamp'], row['price']),
                                   xytext=(10, -20), textcoords='offset points',
                                   fontsize=8, color='green', weight='bold',
                                   bbox=dict(boxstyle='round,pad=0.3', facecolor='green', alpha=0.2))
                
                # 绘制持有点（蓝色圆点）
                if hold_signals:
                    hold_df = pd.DataFrame(hold_signals)
                    ax1.scatter(hold_df['timestamp'], hold_df['price'], 
                               color='blue', marker='o', s=100, label='持有信号', zorder=10, alpha=0.8)
                    # 添加持有点标注
                    for _, row in hold_df.iterrows():
                        original_time_str = row['original_time'].strftime('%H:%M')
                        ax1.annotate(f'持有\n{row["price"]:.3f}\n{original_time_str}', 
                                   xy=(row['timestamp'], row['price']),
                                   xytext=(10, 0), textcoords='offset points',
                                   fontsize=8, color='blue', weight='bold',
                                   bbox=dict(boxstyle='round,pad=0.3', facecolor='blue', alpha=0.2))
                
                # 绘制观望点（黄色方块）
                if watch_signals:
                    watch_df = pd.DataFrame(watch_signals)
                    ax1.scatter(watch_df['timestamp'], watch_df['price'], 
                               color='orange', marker='s', s=100, label='观望信号', zorder=10, alpha=0.8)
                    # 添加观望点标注
                    for _, row in watch_df.iterrows():
                        original_time_str = row['original_time'].strftime('%H:%M')
                        ax1.annotate(f'观望\n{row["price"]:.3f}\n{original_time_str}', 
                                   xy=(row['timestamp'], row['price']),
                                   xytext=(10, 0), textcoords='offset points',
                                   fontsize=8, color='orange', weight='bold',
                                   bbox=dict(boxstyle='round,pad=0.3', facecolor='orange', alpha=0.2))
                
                logger.info(f"添加了 {len(buy_signals)} 个买入点、{len(sell_signals)} 个卖出点、{len(hold_signals)} 个持有点、{len(watch_signals)} 个观望点")
            else:
                logger.info("没有交易信号数据")
            
            # ====== 绘制技术指标信号标记 ======
            if technical_markers:
                technical_df = pd.DataFrame(technical_markers)
                for _, row in technical_df.iterrows():
                    ax1.scatter(row['timestamp'], row['price'], 
                               color=row['marker_color'], marker=row['marker_style'], 
                               s=row['marker_size'], alpha=0.7, zorder=5)
                    
                    # 添加技术信号标注
                    signal_type = row['signal_type']
                    if signal_type == 'golden_cross':
                        label = '金叉'
                        color = 'green'
                    elif signal_type == 'death_cross':
                        label = '死叉'
                        color = 'red'
                    elif signal_type == 'top_divergence':
                        label = '顶背离'
                        color = 'orange'
                    elif signal_type == 'bottom_divergence':
                        label = '底背离'
                        color = 'purple'
                    else:
                        label = signal_type
                        color = 'gray'
                    
                    ax1.annotate(f'{label}\n{row["price"]:.3f}', 
                               xy=(row['timestamp'], row['price']),
                               xytext=(5, 5), textcoords='offset points',
                               fontsize=7, color=color, weight='bold',
                               bbox=dict(boxstyle='round,pad=0.2', facecolor=color, alpha=0.1))
                
                logger.info(f"添加了 {len(technical_markers)} 个技术指标信号标记")
            else:
                logger.info("没有技术指标信号数据")
            
            # ====== 添加极值点标记 ======
            if extremes_data and len(extremes_data) > 0:
                logger.info("开始处理极值点数据...")
                
                # 处理价格极值点
                if 'price_peaks' in extremes_data and extremes_data['price_peaks']:
                    price_peaks = extremes_data['price_peaks']
                    logger.info(f"处理 {len(price_peaks)} 个价格峰值点")
                    
                    for peak in price_peaks:
                        try:
                            # 极值点格式: (timestamp, price, dif_value)
                            peak_timestamp = pd.to_datetime(peak[0], unit='ns')
                            peak_price = peak[1]
                            
                            # 应用时间映射
                            current_time = peak_timestamp.time()
                            if current_time < datetime_time(11, 30):
                                mapped_peak_time = peak_timestamp
                            elif current_time > datetime_time(13, 0):
                                mapped_peak_time = peak_timestamp - timedelta(hours=1, minutes=30)
                            else:
                                continue
                            
                            # 检查时间范围
                            if mapped_peak_time < mapped_df.index.min():
                                mapped_peak_time = mapped_df.index.min()
                            elif mapped_peak_time > mapped_df.index.max():
                                mapped_peak_time = mapped_df.index.max()
                            
                            # 绘制价格峰值点（紫色菱形）
                            ax1.scatter(mapped_peak_time, peak_price, 
                                       color='purple', marker='D', s=80, label='价格峰值', zorder=8, alpha=0.7)
                            
                        except Exception as e:
                            logger.warning(f"处理价格峰值点失败: {e}")
                
                if 'price_troughs' in extremes_data and extremes_data['price_troughs']:
                    price_troughs = extremes_data['price_troughs']
                    logger.info(f"处理 {len(price_troughs)} 个价格谷值点")
                    
                    for trough in price_troughs:
                        try:
                            # 极值点格式: (timestamp, price, dif_value)
                            trough_timestamp = pd.to_datetime(trough[0], unit='ns')
                            trough_price = trough[1]
                            
                            # 应用时间映射
                            current_time = trough_timestamp.time()
                            if current_time < datetime_time(11, 30):
                                mapped_trough_time = trough_timestamp
                            elif current_time > datetime_time(13, 0):
                                mapped_trough_time = trough_timestamp - timedelta(hours=1, minutes=30)
                            else:
                                continue
                            
                            # 检查时间范围
                            if mapped_trough_time < mapped_df.index.min():
                                mapped_trough_time = mapped_df.index.min()
                            elif mapped_trough_time > mapped_df.index.max():
                                mapped_trough_time = mapped_df.index.max()
                            
                            # 绘制价格谷值点（棕色菱形）
                            ax1.scatter(mapped_trough_time, trough_price, 
                                       color='brown', marker='D', s=80, label='价格谷值', zorder=8, alpha=0.7)
                            
                        except Exception as e:
                            logger.warning(f"处理价格谷值点失败: {e}")
                
                logger.info("极值点处理完成")
            else:
                logger.info("没有极值点数据")
            
            # ====== ax2成交量 ======
            # 跳过第一条（因为是累积成交量）
            vol_df = mapped_df.iloc[1:].copy()
            if len(vol_df) == 0:
                logger.warning("成交量数据不足，无法绘制")
                return
            
            # 计算涨跌颜色
            price_arr = vol_df['price'].values
            prev_price_arr = mapped_df['price'].values[:-1]
            colors = np.where(price_arr > prev_price_arr, 'red', np.where(price_arr < prev_price_arr, 'green', 'gray'))
            
            # 绘制成交量柱状图，使用更小的宽度避免重叠
            ax2.bar(vol_df.index, vol_df['volume'], alpha=0.6, color=colors, width=0.0005)
            if target_date:
                ax2.set_title(f'成交量 {target_date} (北京时间)')
            else:
                ax2.set_title(f'成交量 {data_date} (北京时间)')
            ax2.set_ylabel('成交量')
            ax2.set_xlabel('时间 (北京时间)', fontsize=13)
            ax2.annotate('所有横轴时间均为北京时间', xy=(1, 0), xycoords='axes fraction', fontsize=11, color='gray', ha='right', va='top')
            ax2.grid(True, alpha=0.3)
            
            # 设置x轴格式 - 显示北京时间
            ax2.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
            ax2.xaxis.set_major_locator(plt.matplotlib.dates.MinuteLocator(interval=10))  # 每10分钟一个刻度
            plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
            
            # ====== 生成1分钟K线收盘价序列，用于技术指标 ======
            # 使用映射后的数据重新采样，上午和下午数据直接连接
            minute_close = mapped_df['price'].resample('1min').last().dropna()
            minute_index = minute_close.index

            # ====== ax3 MACD副图（用1分钟K线收盘价） ======
            ema12 = minute_close.ewm(span=12, adjust=False).mean()
            ema26 = minute_close.ewm(span=26, adjust=False).mean()
            dif = ema12 - ema26  # DIF
            dea = dif.ewm(span=9, adjust=False).mean()  # DEA
            macd_hist = 2 * (dif - dea) # MACD柱子
            macd_colors = np.where(macd_hist > 0, 'red', np.where(macd_hist < 0, 'green', 'gray'))
            ax3.bar(minute_index, macd_hist, color=macd_colors, width=0.0005, alpha=0.7, label='MACD柱')
            ax3.plot(minute_index, dif, color='orange', label='DIF线')      # DIF橙色
            ax3.plot(minute_index, dea, color='deepskyblue', label='DEA线') # DEA天蓝色
            if target_date:
                ax3.set_title(f'MACD指标 {target_date} (12,26,9)')
            else:
                ax3.set_title(f'MACD指标 {data_date} (12,26,9)')
            ax3.set_ylabel('MACD')
            ax3.set_xlabel('时间 (北京时间)', fontsize=13)
            ax3.annotate('所有横轴时间均为北京时间', xy=(1, 0), xycoords='axes fraction', fontsize=11, color='gray', ha='right', va='top')
            ax3.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
            ax3.xaxis.set_major_locator(plt.matplotlib.dates.MinuteLocator(interval=10))
            plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45)
            ax3.legend(loc='upper right')
            ax3.grid(True, alpha=0.3)

            # ====== 计算RSI(6), RSI(12), RSI(24)（用1分钟K线收盘价） ======
            def calc_rsi(series, period):
                delta = series.diff()
                gain = delta.where(delta > 0, 0.0)
                loss = -delta.where(delta < 0, 0.0)
                avg_gain = gain.rolling(window=period, min_periods=period).mean()
                avg_loss = loss.rolling(window=period, min_periods=period).mean()
                rs = avg_gain / avg_loss
                rsi = 100 - (100 / (1 + rs))
                return rsi
            
            # 先计算RSI(1)，用于填充其他RSI的初始值
            def calc_rsi1(series):
                """计算RSI(1)，第一个值为0"""
                delta = series.diff()
                gain = delta.where(delta > 0, 0.0)
                loss = -delta.where(delta < 0, 0.0)
                # RSI(1)使用当前值，不需要移动平均
                rs = gain / loss
                rsi = 100 - (100 / (1 + rs))
                # 第一个值设为0，其他值保持不变
                rsi.iloc[0] = 0
                return rsi
            
            # 计算基础RSI值
            rsi1 = calc_rsi1(minute_close)
            rsi6_raw = calc_rsi(minute_close, 6)
            rsi12_raw = calc_rsi(minute_close, 12)
            rsi24_raw = calc_rsi(minute_close, 24)
            
            # 使用级联填充逻辑，在数据不足时用更短周期的RSI填充
            # RSI(6)在数据不足时用RSI(1)填充
            rsi6 = rsi6_raw.copy()
            for i in range(len(rsi6)):
                if pd.isna(rsi6.iloc[i]):
                    rsi6.iloc[i] = rsi1.iloc[i]
            
            # RSI(12)在数据不足时用RSI(6)填充
            rsi12 = rsi12_raw.copy()
            for i in range(len(rsi12)):
                if pd.isna(rsi12.iloc[i]):
                    rsi12.iloc[i] = rsi6.iloc[i]
            
            # RSI(24)在数据不足时用RSI(12)填充
            rsi24 = rsi24_raw.copy()
            for i in range(len(rsi24)):
                if pd.isna(rsi24.iloc[i]):
                    rsi24.iloc[i] = rsi12.iloc[i]

            # ====== ax4 RSI副图（6,12,24三线，用1分钟K线收盘价） ======
            ax4.plot(minute_index, rsi6, color='orange', label='RSI(6)')         # 橙色
            ax4.plot(minute_index, rsi12, color='deepskyblue', label='RSI(12)')  # 天蓝色
            ax4.plot(minute_index, rsi24, color='purple', label='RSI(24)')       # 紫色
            ax4.axhline(70, color='red', linestyle='--', linewidth=1, label='超买70')
            ax4.axhline(30, color='green', linestyle='--', linewidth=1, label='超卖30')
            if target_date:
                ax4.set_title(f'RSI指标 {target_date} (6,12,24)')
            else:
                ax4.set_title(f'RSI指标 {data_date} (6,12,24)')
            ax4.set_ylabel('RSI')
            ax4.set_xlabel('时间 (北京时间)', fontsize=13)
            ax4.annotate('所有横轴时间均为北京时间', xy=(1, 0), xycoords='axes fraction', fontsize=11, color='gray', ha='right', va='top')
            ax4.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
            ax4.xaxis.set_major_locator(plt.matplotlib.dates.MinuteLocator(interval=10))
            plt.setp(ax4.xaxis.get_majorticklabels(), rotation=45)
            ax4.set_ylim(0, 100)
            ax4.legend(loc='upper right')
            ax4.grid(True, alpha=0.3)

            # ====== 计算KDJ(9,3,3)（用1分钟K线收盘价） ======
            def calc_kdj(close, n=9, k_period=3, d_period=3):
                # 计算N1周期内的最低价和最高价
                low_list = close.rolling(window=n, min_periods=1).min()
                high_list = close.rolling(window=n, min_periods=1).max()
                
                # 计算RSV：RSV = (CLOSE - LLV(LOW, N1)) / (HHV(HIGH, N1) - LLV(LOW, N1)) * 100
                rsv = (close - low_list) / (high_list - low_list) * 100
                
                # 计算K值：K = MA(RSV, N2) 其中 N2 = 3
                k = rsv.rolling(window=k_period, min_periods=1).mean()
                
                # 计算D值：D = MA(K, N3) 其中 N3 = 3
                d = k.rolling(window=d_period, min_periods=1).mean()
                
                # 计算J值：J = 3*K - 2*D
                j = 3 * k - 2 * d
                
                return k, d, j
            kdj_k, kdj_d, kdj_j = calc_kdj(minute_close, n=9, k_period=3, d_period=3)

            # ====== ax5 KDJ副图（用1分钟K线收盘价） ======
            ax5.plot(minute_index, kdj_k, color='orange', label='K')         # 橙色
            ax5.plot(minute_index, kdj_d, color='deepskyblue', label='D')    # 天蓝色
            ax5.plot(minute_index, kdj_j, color='purple', label='J')         # 紫色
            if target_date:
                ax5.set_title(f'KDJ指标 {target_date} (9,3,3)')
            else:
                ax5.set_title(f'KDJ指标 {data_date} (9,3,3)')
            ax5.set_ylabel('KDJ')
            ax5.set_xlabel('时间 (北京时间)', fontsize=13)
            ax5.annotate('所有横轴时间均为北京时间', xy=(1, 0), xycoords='axes fraction', fontsize=11, color='gray', ha='right', va='top')
            ax5.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
            ax5.xaxis.set_major_locator(plt.matplotlib.dates.MinuteLocator(interval=10))
            plt.setp(ax5.xaxis.get_majorticklabels(), rotation=45)
            ax5.legend(loc='upper right')
            ax5.grid(True, alpha=0.3)

            # 统一x轴格式化，防止内容错乱
            for ax in [ax1, ax2, ax3, ax4, ax5]:
                ax.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
                # 根据数据时间跨度调整刻度间隔
                time_span = x_max - x_min
                if time_span.total_seconds() < 3600:  # 小于1小时
                    interval = 5  # 每5分钟
                elif time_span.total_seconds() < 7200:  # 小于2小时
                    interval = 10  # 每10分钟
                else:
                    interval = 15  # 每15分钟
                ax.xaxis.set_major_locator(plt.matplotlib.dates.MinuteLocator(interval=interval))
                plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

            plt.tight_layout()
            
            # 只生成一张图，且只show一次
            if save_path:
                plt.savefig(save_path, dpi=300, bbox_inches='tight')
                logger.info(f"价格走势图已保存到: {save_path}")
            
            # 显示图表（非阻塞模式）
            plt.show(block=False)
            
        except Exception as e:
            logger.error(f"绘制价格走势图失败: {e}")
            import traceback
            logger.error(f"详细错误: {traceback.format_exc()}")
    
    def _start_realtime_chart(self, save_path: str = None, target_date: datetime.date = None, trade_signals: List[Dict] = None, technical_signals: List[Dict] = None, extremes_data: Dict = None):
        """启动实时图表更新"""
        def update_chart():
            while True:
                try:
                    time.sleep(1)  # 每30秒更新一次
                    self._plot_kline_chart(save_path, target_date, trade_signals, technical_signals, extremes_data)
                    logger.info("实时K线图已更新")
                except Exception as e:
                    logger.error(f"实时图表更新失败: {e}")
                    break
        
        # 启动更新线程
        update_thread = threading.Thread(target=update_chart, daemon=True)
        update_thread.start()
        logger.info("实时K线图更新已启动")

    def create_extremes_chart(self, save_path: str = None, target_date: datetime.date = None, extremes_data: Dict = None):
        """创建专门的极值点图表"""
        try:
            # 获取数据
            kline_data = self.get_today_kline_data(target_date)
            
            if not kline_data:
                logger.warning("没有数据可绘制")
                return
            
            # 转换为DataFrame
            df = pd.DataFrame(kline_data)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # 检查是否需要时区转换
            if df['timestamp'].dt.tz is None:
                # 假设是UTC时间，转换为北京时间
                import pytz
                utc_tz = pytz.UTC
                beijing_tz = pytz.timezone('Asia/Shanghai')
                
                # 添加UTC时区信息
                df['timestamp'] = df['timestamp'].dt.tz_localize(utc_tz)
                # 转换为北京时间
                df['timestamp'] = df['timestamp'].dt.tz_convert(beijing_tz)
                logger.info("已将UTC时间转换为北京时间")
            
            df = df.sort_values('timestamp')
            
            # 检查数据时间范围
            start_time = df['timestamp'].min()
            end_time = df['timestamp'].max()
            data_date = start_time.date()
            logger.info(f"数据时间范围: {start_time} 到 {end_time}")
            logger.info(f"数据条数: {len(df)}")
            
            # 确定图表标题
            if target_date:
                chart_title = f'159506 ETF {target_date} 极值点分析 (北京时间)'
            else:
                chart_title = f'159506 ETF {data_date} 极值点分析 (北京时间)'
            
            # 过滤交易时间内的数据，正确处理午休时间
            from datetime import time as datetime_time
            morning_start = datetime_time(9, 30)
            morning_end = datetime_time(11, 30)
            afternoon_start = datetime_time(13, 0)
            afternoon_end = datetime_time(15, 0)
            
            # 分别获取上午和下午的数据
            morning_data = df[
                (df['timestamp'].dt.time >= morning_start) & 
                (df['timestamp'].dt.time <= morning_end)
            ]
            
            afternoon_data = df[
                (df['timestamp'].dt.time >= afternoon_start) & 
                (df['timestamp'].dt.time <= afternoon_end)
            ]
            
            # 合并上午和下午数据
            trading_data = pd.concat([morning_data, afternoon_data])
            
            if len(trading_data) == 0:
                logger.warning("没有交易时间内的数据可绘制")
                return
            
            logger.info(f"上午数据: {len(morning_data)} 条")
            logger.info(f"下午数据: {len(afternoon_data)} 条")
            logger.info(f"总交易数据: {len(trading_data)} 条")
            
            # 使用交易数据的时间作为索引
            trading_data = trading_data.set_index('timestamp')
            
            # 创建时间映射，处理午休时间
            from datetime import timedelta
            
            # 创建映射后的时间索引
            mapped_times = []
            for timestamp in trading_data.index:
                current_time = timestamp.time()
                if current_time < datetime_time(11, 30):
                    # 上午时间保持不变
                    mapped_time = timestamp
                elif current_time > datetime_time(13, 0):
                    # 下午时间减去1.5小时（午休时间），保持图表连续
                    mapped_time = timestamp - timedelta(hours=1, minutes=30)
                else:
                    # 午休时间的数据跳过
                    continue
                mapped_times.append(mapped_time)
            
            # 创建映射后的DataFrame
            mapped_df = trading_data.copy()
            mapped_df.index = mapped_times
            mapped_df = mapped_df.sort_index()
            
            logger.info(f"映射后数据时间范围: {mapped_df.index.min()} 到 {mapped_df.index.max()}")
            logger.info(f"映射后数据条数: {len(mapped_df)}")
            
            # 创建图表
            fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(16, 12), height_ratios=[3, 1, 2])
            fig.suptitle(chart_title, fontsize=16, fontweight='bold')
            
            # ====== ax1主图（价格走势 + 极值点） ======
            # 绘制价格走势（使用更清晰的线条，设置较高的zorder确保在极值点之上）
            ax1.plot(mapped_df.index, mapped_df['price'], linewidth=1.0, color='darkblue', alpha=1.0, label='价格走势', zorder=20)
            
            # 添加极值点标记
            if extremes_data and len(extremes_data) > 0:
                logger.info("开始处理极值点数据...")
                
                # 处理价格极值点
                if 'price_peaks' in extremes_data and extremes_data['price_peaks']:
                    price_peaks = extremes_data['price_peaks']
                    logger.info(f"处理 {len(price_peaks)} 个价格峰值点")
                    
                    for peak in price_peaks:
                        try:
                            # 极值点格式: (timestamp, price, dif_value)
                            peak_timestamp = pd.to_datetime(peak[0], unit='ns')
                            peak_price = peak[1]
                            
                            # 应用时间映射
                            current_time = peak_timestamp.time()
                            if current_time < datetime_time(11, 30):
                                mapped_peak_time = peak_timestamp
                            elif current_time > datetime_time(13, 0):
                                mapped_peak_time = peak_timestamp - timedelta(hours=1, minutes=30)
                            else:
                                continue
                            
                            # 检查时间范围
                            if mapped_peak_time < mapped_df.index.min():
                                mapped_peak_time = mapped_df.index.min()
                            elif mapped_peak_time > mapped_df.index.max():
                                mapped_peak_time = mapped_df.index.max()
                            
                            # 绘制价格峰值点（紫色菱形）
                            ax1.scatter(mapped_peak_time, peak_price, 
                                       color='purple', marker='D', s=100, label='价格峰值', zorder=25, alpha=0.8)
                            
                            # 添加峰值标注
                            ax1.annotate(f'峰值\n{peak_price:.3f}', 
                                       xy=(mapped_peak_time, peak_price),
                                       xytext=(5, 10), textcoords='offset points',
                                       fontsize=8, color='purple', weight='bold',
                                       bbox=dict(boxstyle='round,pad=0.2', facecolor='purple', alpha=0.1))
                            
                        except Exception as e:
                            logger.warning(f"处理价格峰值点失败: {e}")
                
                if 'price_troughs' in extremes_data and extremes_data['price_troughs']:
                    price_troughs = extremes_data['price_troughs']
                    logger.info(f"处理 {len(price_troughs)} 个价格谷值点")
                    
                    for trough in price_troughs:
                        try:
                            # 极值点格式: (timestamp, price, dif_value)
                            trough_timestamp = pd.to_datetime(trough[0], unit='ns')
                            trough_price = trough[1]
                            
                            # 应用时间映射
                            current_time = trough_timestamp.time()
                            if current_time < datetime_time(11, 30):
                                mapped_trough_time = trough_timestamp
                            elif current_time > datetime_time(13, 0):
                                mapped_trough_time = trough_timestamp - timedelta(hours=1, minutes=30)
                            else:
                                continue
                            
                            # 检查时间范围
                            if mapped_trough_time < mapped_df.index.min():
                                mapped_trough_time = mapped_df.index.min()
                            elif mapped_trough_time > mapped_df.index.max():
                                mapped_trough_time = mapped_df.index.max()
                            
                            # 绘制价格谷值点（棕色菱形）
                            ax1.scatter(mapped_trough_time, trough_price, 
                                       color='brown', marker='D', s=100, label='价格谷值', zorder=25, alpha=0.8)
                            
                            # 添加谷值标注
                            ax1.annotate(f'谷值\n{trough_price:.3f}', 
                                       xy=(mapped_trough_time, trough_price),
                                       xytext=(5, -15), textcoords='offset points',
                                       fontsize=8, color='brown', weight='bold',
                                       bbox=dict(boxstyle='round,pad=0.2', facecolor='brown', alpha=0.1))
                            
                        except Exception as e:
                            logger.warning(f"处理价格谷值点失败: {e}")
                
                logger.info("极值点处理完成")
            else:
                logger.info("没有极值点数据")
            
            # 设置主图属性
            ax1.set_title('价格走势与极值点', fontsize=14)
            ax1.set_ylabel('价格', fontsize=12)
            ax1.grid(True, alpha=0.3)
            ax1.legend(loc='upper left')
            
            # 设置x轴格式
            ax1.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
            ax1.xaxis.set_major_locator(plt.matplotlib.dates.MinuteLocator(interval=10))
            plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)
            
            # ====== ax2成交量 ======
            # 跳过第一条（因为是累积成交量）
            vol_df = mapped_df.iloc[1:].copy()
            if len(vol_df) > 0:
                # 计算涨跌颜色
                price_arr = vol_df['price'].values
                prev_price_arr = mapped_df['price'].values[:-1]
                colors = np.where(price_arr > prev_price_arr, 'red', np.where(price_arr < prev_price_arr, 'green', 'gray'))
                
                # 绘制成交量柱状图
                ax2.bar(vol_df.index, vol_df['volume'], alpha=0.6, color=colors, width=0.0005)
            
            ax2.set_title('成交量', fontsize=12)
            ax2.set_ylabel('成交量', fontsize=10)
            ax2.grid(True, alpha=0.3)
            ax2.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
            ax2.xaxis.set_major_locator(plt.matplotlib.dates.MinuteLocator(interval=10))
            plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
            
            # ====== ax3 MACD副图 ======
            # 生成1分钟K线收盘价序列，用于技术指标
            minute_close = mapped_df['price'].resample('1min').last().dropna()
            minute_index = minute_close.index

            # 计算MACD
            ema12 = minute_close.ewm(span=12, adjust=False).mean()
            ema26 = minute_close.ewm(span=26, adjust=False).mean()
            dif = ema12 - ema26  # DIF
            dea = dif.ewm(span=9, adjust=False).mean()  # DEA
            macd_hist = 2 * (dif - dea) # MACD柱子
            
            # 绘制MACD
            macd_colors = np.where(macd_hist > 0, 'red', np.where(macd_hist < 0, 'green', 'gray'))
            ax3.bar(minute_index, macd_hist, color=macd_colors, width=0.0005, alpha=0.7, label='MACD柱')
            ax3.plot(minute_index, dif, color='orange', label='DIF线')
            ax3.plot(minute_index, dea, color='deepskyblue', label='DEA线')
            
            # 添加DIF极值点
            if extremes_data and len(extremes_data) > 0:
                if 'dif_peaks' in extremes_data and extremes_data['dif_peaks']:
                    dif_peaks = extremes_data['dif_peaks']
                    logger.info(f"处理 {len(dif_peaks)} 个DIF峰值点")
                    
                    for peak in dif_peaks:
                        try:
                            # 极值点格式: (timestamp, dif_value, price_value)
                            peak_timestamp = pd.to_datetime(peak[0], unit='ns')
                            peak_dif = peak[1]
                            
                            # 应用时间映射
                            current_time = peak_timestamp.time()
                            if current_time < datetime_time(11, 30):
                                mapped_peak_time = peak_timestamp
                            elif current_time > datetime_time(13, 0):
                                mapped_peak_time = peak_timestamp - timedelta(hours=1, minutes=30)
                            else:
                                continue
                            
                            # 检查时间范围
                            if mapped_peak_time < minute_index.min():
                                mapped_peak_time = minute_index.min()
                            elif mapped_peak_time > minute_index.max():
                                mapped_peak_time = minute_index.max()
                            
                            # 绘制DIF峰值点（红色三角形）
                            ax3.scatter(mapped_peak_time, peak_dif, 
                                       color='red', marker='^', s=80, label='DIF峰值', zorder=25, alpha=0.8)
                            
                        except Exception as e:
                            logger.warning(f"处理DIF峰值点失败: {e}")
                
                if 'dif_troughs' in extremes_data and extremes_data['dif_troughs']:
                    dif_troughs = extremes_data['dif_troughs']
                    logger.info(f"处理 {len(dif_troughs)} 个DIF谷值点")
                    
                    for trough in dif_troughs:
                        try:
                            # 极值点格式: (timestamp, dif_value, price_value)
                            trough_timestamp = pd.to_datetime(trough[0], unit='ns')
                            trough_dif = trough[1]
                            
                            # 应用时间映射
                            current_time = trough_timestamp.time()
                            if current_time < datetime_time(11, 30):
                                mapped_trough_time = trough_timestamp
                            elif current_time > datetime_time(13, 0):
                                mapped_trough_time = trough_timestamp - timedelta(hours=1, minutes=30)
                            else:
                                continue
                            
                            # 检查时间范围
                            if mapped_trough_time < minute_index.min():
                                mapped_trough_time = minute_index.min()
                            elif mapped_trough_time > minute_index.max():
                                mapped_trough_time = minute_index.max()
                            
                            # 绘制DIF谷值点（绿色三角形）
                            ax3.scatter(mapped_trough_time, trough_dif, 
                                       color='green', marker='v', s=80, label='DIF谷值', zorder=25, alpha=0.8)
                            
                        except Exception as e:
                            logger.warning(f"处理DIF谷值点失败: {e}")
            
            ax3.set_title('MACD指标与DIF极值点', fontsize=12)
            ax3.set_ylabel('MACD', fontsize=10)
            ax3.set_xlabel('时间 (北京时间)', fontsize=12)
            ax3.grid(True, alpha=0.3)
            ax3.legend(loc='upper left')
            ax3.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
            ax3.xaxis.set_major_locator(plt.matplotlib.dates.MinuteLocator(interval=10))
            plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45)
            
            # 调整布局
            plt.tight_layout()
            
            # 保存图片
            if save_path:
                plt.savefig(save_path, dpi=300, bbox_inches='tight')
                logger.info(f"极值点图表已保存到: {save_path}")
            
            # 显示图表（非阻塞模式）
            plt.show(block=False)
            
            logger.info("极值点图表生成完成")
            
        except Exception as e:
            logger.error(f"创建极值点图表失败: {e}")
            import traceback
            logger.error(f"详细错误: {traceback.format_exc()}")

    def create_trade_points_chart(self, save_path: str = None, target_date: datetime.date = None, trade_signals: List[Dict] = None):
        """创建专门的买卖点图表"""
        try:
            # 获取数据
            kline_data = self.get_today_kline_data(target_date)
            
            if not kline_data:
                logger.warning("没有数据可绘制")
                return
            
            # 转换为DataFrame
            df = pd.DataFrame(kline_data)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # 检查是否需要时区转换
            if df['timestamp'].dt.tz is None:
                # 假设是UTC时间，转换为北京时间
                import pytz
                utc_tz = pytz.UTC
                beijing_tz = pytz.timezone('Asia/Shanghai')
                
                # 添加UTC时区信息
                df['timestamp'] = df['timestamp'].dt.tz_localize(utc_tz)
                # 转换为北京时间
                df['timestamp'] = df['timestamp'].dt.tz_convert(beijing_tz)
                logger.info("已将UTC时间转换为北京时间")
            
            df = df.sort_values('timestamp')
            
            # 检查数据时间范围
            start_time = df['timestamp'].min()
            end_time = df['timestamp'].max()
            data_date = start_time.date()
            logger.info(f"数据时间范围: {start_time} 到 {end_time}")
            logger.info(f"数据条数: {len(df)}")
            
            # 确定图表标题
            if target_date:
                chart_title = f'159506 ETF {target_date} 买卖点分析 (北京时间)'
            else:
                chart_title = f'159506 ETF {data_date} 买卖点分析 (北京时间)'
            
            # 过滤交易时间内的数据，正确处理午休时间
            from datetime import time as datetime_time
            morning_start = datetime_time(9, 30)
            morning_end = datetime_time(11, 30)
            afternoon_start = datetime_time(13, 0)
            afternoon_end = datetime_time(15, 0)
            
            # 分别获取上午和下午的数据
            morning_data = df[
                (df['timestamp'].dt.time >= morning_start) & 
                (df['timestamp'].dt.time <= morning_end)
            ]
            
            afternoon_data = df[
                (df['timestamp'].dt.time >= afternoon_start) & 
                (df['timestamp'].dt.time <= afternoon_end)
            ]
            
            # 合并上午和下午数据
            trading_data = pd.concat([morning_data, afternoon_data])
            
            if len(trading_data) == 0:
                logger.warning("没有交易时间内的数据可绘制")
                return
            
            logger.info(f"上午数据: {len(morning_data)} 条")
            logger.info(f"下午数据: {len(afternoon_data)} 条")
            logger.info(f"总交易数据: {len(trading_data)} 条")
            
            # 使用交易数据的时间作为索引
            trading_data = trading_data.set_index('timestamp')
            
            # 确保索引为升序、唯一、无NaN
            complete_df = trading_data.copy()
            complete_df = complete_df.sort_index()
            complete_df = complete_df[~complete_df.index.duplicated(keep='first')]
            complete_df = complete_df[complete_df.index.notnull()]

            # 创建三联图，专门用于买卖点分析
            fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(20, 24), height_ratios=[2, 1, 1])

            # 创建时间轴映射，保持时间连续性但保留原始时间信息
            def create_time_mapping(df):
                """创建时间轴映射，保持图表连续性"""
                new_times = []
                time_mapping = {}
                original_time_mapping = {}  # 保存原始时间到映射时间的对应关系
                
                for idx in df.index:
                    current_time = idx.time()
                    
                    if current_time < datetime_time(11, 30):
                        # 上午时间保持不变
                        new_time = idx
                    elif current_time > datetime_time(13, 0):
                        # 下午时间减去1.5小时（午休时间），保持图表连续
                        new_time = idx - timedelta(hours=1, minutes=30)
                    else:
                        # 午休时间的数据跳过
                        continue
                    
                    new_times.append(new_time)
                    time_mapping[idx] = new_time
                    original_time_mapping[new_time] = idx  # 反向映射
                
                return pd.DatetimeIndex(new_times), time_mapping, original_time_mapping
            
            # 创建时间映射
            new_index, time_mapping, original_time_mapping = create_time_mapping(complete_df)
            
            # 重新索引数据
            mapped_df = complete_df[complete_df.index.isin(time_mapping.keys())].copy()
            mapped_df.index = [time_mapping[idx] for idx in mapped_df.index]
            
            # 设置x轴范围
            x_min = new_index.min()
            x_max = new_index.max()
            
            # 为每个子图设置相同的x轴范围
            for ax in [ax1, ax2, ax3]:
                ax.set_xlim(x_min, x_max)

            # ====== ax1主图（价格走势 + 买卖点） ======
            # 绘制价格走势（使用较细的线条）
            ax1.plot(mapped_df.index, mapped_df['price'], linewidth=0.8, color='lightgray', alpha=0.6, label='价格走势')
            
            # 标记关键价格点
            valid_prices = mapped_df['price'].dropna()
            if len(valid_prices) > 0:
                # 开盘价（第一个有效价格）
                open_price = valid_prices.iloc[0]
                open_time = valid_prices.index[0]
                ax1.scatter(open_time, open_price, color='black', s=80, marker='o', label='开盘', zorder=5)
                
                # 当前价（最后一个有效价格）
                current_price = valid_prices.iloc[-1]
                current_time = valid_prices.index[-1]
                ax1.scatter(current_time, current_price, color='black', s=80, marker='o', label='当前', zorder=5)
                
                # 最高价
                high_price = valid_prices.max()
                high_time = valid_prices.idxmax()
                ax1.scatter(high_time, high_price, color='orange', s=60, marker='^', label='最高', zorder=5)
                
                # 最低价
                low_price = valid_prices.min()
                low_time = valid_prices.idxmin()
                ax1.scatter(low_time, low_price, color='purple', s=60, marker='v', label='最低', zorder=5)
                
                # 添加价格信息
                price_info = f'开盘: {open_price:.3f} ({open_time.strftime("%H:%M")})\n'
                price_info += f'当前: {current_price:.3f} ({current_time.strftime("%H:%M")})\n'
                price_info += f'最高: {high_price:.3f} ({high_time.strftime("%H:%M")})\n'
                price_info += f'最低: {low_price:.3f} ({low_time.strftime("%H:%M")})'
                
                ax1.text(0.02, 0.98, price_info, 
                       transform=ax1.transAxes, verticalalignment='top', 
                       bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
            
            ax1.set_title(chart_title, fontsize=16, fontweight='bold')
            ax1.set_ylabel('价格', fontsize=12)
            ax1.grid(True, alpha=0.3)
            
            # 添加买卖点标记
            if trade_signals and len(trade_signals) > 0:
                buy_signals = []
                sell_signals = []
                hold_signals = []
                watch_signals = []
                
                # 添加调试日志
                logger.info(f"开始处理 {len(trade_signals)} 个交易信号...")
                logger.info(f"图表时间范围: {mapped_df.index.min()} 到 {mapped_df.index.max()}")
                
                for i, signal in enumerate(trade_signals):
                    logger.info(f"处理第 {i+1} 个信号: {signal}")
                    
                    # 转换时间戳为pandas datetime
                    if isinstance(signal['timestamp'], str):
                        signal_time = pd.to_datetime(signal['timestamp'])
                    else:
                        signal_time = signal['timestamp']
                    
                    # 确保时间戳有时区信息，与图表数据保持一致
                    if not isinstance(signal_time, pd.Timestamp):
                        signal_time = pd.Timestamp(signal_time)
                    
                    if signal_time.tz is None:
                        # 如果没有时区信息，假设是UTC时间，转换为北京时间
                        import pytz
                        utc_tz = pytz.UTC
                        beijing_tz = pytz.timezone('Asia/Shanghai')
                        signal_time = signal_time.tz_localize(utc_tz).tz_convert(beijing_tz)
                    
                    logger.info(f"信号 {i+1} 原始时间: {signal_time}")
                    
                    # 应用相同的时间映射，保持图表连续性
                    current_time = signal_time.time()
                    if current_time < datetime_time(11, 30):
                        # 上午时间保持不变
                        mapped_signal_time = signal_time
                        logger.info(f"信号 {i+1} 上午时间，映射后时间: {mapped_signal_time}")
                    elif current_time > datetime_time(13, 0):
                        # 下午时间减去1.5小时（午休时间），保持图表连续
                        mapped_signal_time = signal_time - timedelta(hours=1, minutes=30)
                        logger.info(f"信号 {i+1} 下午时间，映射后时间: {mapped_signal_time}")
                    else:
                        # 午休时间的信号跳过
                        logger.warning(f"信号 {i+1} 在午休时间 {current_time}，跳过")
                        continue
                    
                    # 检查映射后的时间是否在图表范围内，如果超出范围则调整到最近的有效时间
                    if mapped_signal_time < mapped_df.index.min():
                        logger.warning(f"信号 {i+1} 映射后时间 {mapped_signal_time} 早于图表开始时间，调整到 {mapped_df.index.min()}")
                        mapped_signal_time = mapped_df.index.min()
                    elif mapped_signal_time > mapped_df.index.max():
                        logger.warning(f"信号 {i+1} 映射后时间 {mapped_signal_time} 晚于图表结束时间，调整到 {mapped_df.index.max()}")
                        mapped_signal_time = mapped_df.index.max()
                    
                    logger.info(f"信号 {i+1} 最终映射时间: {mapped_signal_time}")
                    
                    # 所有信号都添加到对应列表（经过时间调整后）
                    if signal['side'] == 'BUY':
                        buy_signals.append({
                            'timestamp': mapped_signal_time,
                            'price': signal['price'],
                            'original_time': signal_time,  # 保存原始时间
                            'signal_type': signal.get('signal_type', 'unknown')
                        })
                    elif signal['side'] == 'SELL':
                        sell_signals.append({
                            'timestamp': mapped_signal_time,
                            'price': signal['price'],
                            'original_time': signal_time,  # 保存原始时间
                            'signal_type': signal.get('signal_type', 'unknown')
                        })
                    elif signal['side'] == 'HOLD':
                        hold_signals.append({
                            'timestamp': mapped_signal_time,
                            'price': signal['price'],
                            'original_time': signal_time,  # 保存原始时间
                            'signal_type': signal.get('signal_type', 'unknown')
                        })
                    elif signal['side'] == 'WATCH':
                        watch_signals.append({
                            'timestamp': mapped_signal_time,
                            'price': signal['price'],
                            'original_time': signal_time,  # 保存原始时间
                            'signal_type': signal.get('signal_type', 'unknown')
                        })
                
                logger.info(f"信号处理完成: 买入={len(buy_signals)}, 卖出={len(sell_signals)}, 持有={len(hold_signals)}, 观望={len(watch_signals)}")
                
                # 绘制买入点（红色三角形向上，更大更显眼）
                if buy_signals:
                    buy_df = pd.DataFrame(buy_signals)
                    ax1.scatter(buy_df['timestamp'], buy_df['price'], 
                               color='red', marker='^', s=200, label='买入信号', zorder=15, alpha=0.9)
                    # 添加买入点标注
                    for _, row in buy_df.iterrows():
                        # 显示原始时间（北京时间）
                        original_time_str = row['original_time'].strftime('%H:%M')
                        signal_type = row.get('signal_type', 'unknown')
                        ax1.annotate(f'买入\n{row["price"]:.3f}\n{original_time_str}\n{signal_type}', 
                                   xy=(row['timestamp'], row['price']),
                                   xytext=(15, 15), textcoords='offset points',
                                   fontsize=10, color='red', weight='bold',
                                   bbox=dict(boxstyle='round,pad=0.5', facecolor='red', alpha=0.3))
                
                # 绘制卖出点（绿色三角形向下，更大更显眼）
                if sell_signals:
                    sell_df = pd.DataFrame(sell_signals)
                    ax1.scatter(sell_df['timestamp'], sell_df['price'], 
                               color='green', marker='v', s=200, label='卖出信号', zorder=15, alpha=0.9)
                    # 添加卖出点标注
                    for _, row in sell_df.iterrows():
                        # 显示原始时间（北京时间）
                        original_time_str = row['original_time'].strftime('%H:%M')
                        signal_type = row.get('signal_type', 'unknown')
                        ax1.annotate(f'卖出\n{row["price"]:.3f}\n{original_time_str}\n{signal_type}', 
                                   xy=(row['timestamp'], row['price']),
                                   xytext=(15, -25), textcoords='offset points',
                                   fontsize=10, color='green', weight='bold',
                                   bbox=dict(boxstyle='round,pad=0.5', facecolor='green', alpha=0.3))
                
                # 绘制持有点（蓝色圆点）
                if hold_signals:
                    hold_df = pd.DataFrame(hold_signals)
                    ax1.scatter(hold_df['timestamp'], hold_df['price'], 
                               color='blue', marker='o', s=120, label='持有信号', zorder=15, alpha=0.8)
                    # 添加持有点标注
                    for _, row in hold_df.iterrows():
                        original_time_str = row['original_time'].strftime('%H:%M')
                        signal_type = row.get('signal_type', 'unknown')
                        ax1.annotate(f'持有\n{row["price"]:.3f}\n{original_time_str}\n{signal_type}', 
                                   xy=(row['timestamp'], row['price']),
                                   xytext=(15, 0), textcoords='offset points',
                                   fontsize=9, color='blue', weight='bold',
                                   bbox=dict(boxstyle='round,pad=0.4', facecolor='blue', alpha=0.2))
                
                # 绘制观望点（黄色方块）
                if watch_signals:
                    watch_df = pd.DataFrame(watch_signals)
                    ax1.scatter(watch_df['timestamp'], watch_df['price'], 
                               color='orange', marker='s', s=120, label='观望信号', zorder=15, alpha=0.8)
                    # 添加观望点标注
                    for _, row in watch_df.iterrows():
                        original_time_str = row['original_time'].strftime('%H:%M')
                        signal_type = row.get('signal_type', 'unknown')
                        ax1.annotate(f'观望\n{row["price"]:.3f}\n{original_time_str}\n{signal_type}', 
                                   xy=(row['timestamp'], row['price']),
                                   xytext=(15, 0), textcoords='offset points',
                                   fontsize=9, color='orange', weight='bold',
                                   bbox=dict(boxstyle='round,pad=0.4', facecolor='orange', alpha=0.2))
                
                logger.info(f"添加了 {len(buy_signals)} 个买入点、{len(sell_signals)} 个卖出点、{len(hold_signals)} 个持有点、{len(watch_signals)} 个观望点")
            else:
                logger.info("没有交易信号数据")
            
            # 添加信号类型说明
            signal_info = "信号说明:\n"
            signal_info += "▲ 买入信号: 金叉出现，无持仓时买入\n"
            signal_info += "▼ 卖出信号: 死叉出现，有持仓时卖出\n"
            signal_info += "● 持有信号: 金叉出现，已有持仓时持有\n"
            signal_info += "■ 观望信号: 死叉出现，无持仓时观望"
            
            ax1.text(0.02, 0.85, signal_info, 
                   transform=ax1.transAxes, verticalalignment='top', 
                   bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.8),
                   fontsize=10)
            
            ax1.legend(loc='upper right')
            
            # 设置x轴格式 - 显示北京时间
            ax1.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
            ax1.xaxis.set_major_locator(plt.matplotlib.dates.MinuteLocator(interval=10))  # 每10分钟一个刻度
            plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)
            
            # 设置y轴格式 - 价格三位小数
            import matplotlib.ticker as mticker
            ax1.yaxis.set_major_formatter(mticker.FormatStrFormatter('%.3f'))

            # 添加x轴标签（明确标注北京时间）
            ax1.set_xlabel('时间 (北京时间)', fontsize=13)
            ax1.annotate('所有横轴时间均为北京时间', xy=(1, 0), xycoords='axes fraction', fontsize=11, color='gray', ha='right', va='top')
            
            # ====== ax2成交量 ======
            # 跳过第一条（因为是累积成交量）
            vol_df = mapped_df.iloc[1:].copy()
            if len(vol_df) > 0:
                # 计算涨跌颜色
                price_arr = vol_df['price'].values
                prev_price_arr = mapped_df['price'].values[:-1]
                colors = np.where(price_arr > prev_price_arr, 'red', np.where(price_arr < prev_price_arr, 'green', 'gray'))
                
                # 绘制成交量柱状图
                ax2.bar(vol_df.index, vol_df['volume'], alpha=0.6, color=colors, width=0.0005)
                ax2.set_title('成交量', fontsize=12)
                ax2.set_ylabel('成交量', fontsize=10)
                ax2.grid(True, alpha=0.3)
                
                # 设置x轴格式
                ax2.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
                ax2.xaxis.set_major_locator(plt.matplotlib.dates.MinuteLocator(interval=10))
                plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
            
            # ====== ax3 MACD指标 ======
            # 生成1分钟K线收盘价序列，用于技术指标
            minute_close = mapped_df['price'].resample('1min').last().dropna()
            minute_index = minute_close.index
            
            if len(minute_close) > 0:
                ema12 = minute_close.ewm(span=12, adjust=False).mean()
                ema26 = minute_close.ewm(span=26, adjust=False).mean()
                dif = ema12 - ema26  # DIF
                dea = dif.ewm(span=9, adjust=False).mean()  # DEA
                macd_hist = 2 * (dif - dea) # MACD柱子
                macd_colors = np.where(macd_hist > 0, 'red', np.where(macd_hist < 0, 'green', 'gray'))
                
                ax3.bar(minute_index, macd_hist, color=macd_colors, width=0.0005, alpha=0.7, label='MACD柱')
                ax3.plot(minute_index, dif, color='orange', linewidth=1.5, label='DIF线')
                ax3.plot(minute_index, dea, color='deepskyblue', linewidth=1.5, label='DEA线')
                ax3.set_title('MACD指标 (12,26,9)', fontsize=12)
                ax3.set_ylabel('MACD', fontsize=10)
                ax3.set_xlabel('时间 (北京时间)', fontsize=13)
                ax3.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
                ax3.xaxis.set_major_locator(plt.matplotlib.dates.MinuteLocator(interval=10))
                plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45)
                ax3.legend(loc='upper right')
                ax3.grid(True, alpha=0.3)
            
            # 调整布局
            plt.tight_layout()
            
            # 保存图片
            if save_path:
                plt.savefig(save_path, dpi=300, bbox_inches='tight')
                logger.info(f"买卖点图表已保存到: {save_path}")
            
            # 显示图表
            plt.show()
            
            logger.info("买卖点图表生成完成")
            
        except Exception as e:
            logger.error(f"生成买卖点图表失败: {e}")
            import traceback
            logger.error(f"详细错误: {traceback.format_exc()}")


def main():
    """主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(
        description="159506 ETF K线图表生成器",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python etf_159506_catalog_loader.py                    # 生成今日K线图
  python etf_159506_catalog_loader.py --date 2024-01-15  # 生成指定日期K线图
  python etf_159506_catalog_loader.py -d 2024-01-15      # 简写形式
  python etf_159506_catalog_loader.py --output my_chart.png  # 指定输出文件名
  python etf_159506_catalog_loader.py --date 2024-01-15 --output 2024-01-15_chart.png
        """
    )
    
    parser.add_argument(
        '--date', '-d',
        type=str,
        help='指定日期 (格式: YYYY-MM-DD，例如: 2024-01-15)'
    )
    
    parser.add_argument(
        '--output', '-o',
        type=str,
        help='输出文件名 (默认: etf_159506_today_kline.png)'
    )
    
    parser.add_argument(
        '--realtime', '-r',
        action='store_true',
        help='启用实时更新模式 (每30秒自动更新)'
    )
    
    args = parser.parse_args()
    
    # 解析日期参数
    target_date = None
    if args.date:
        try:
            target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
            print(f"📅 指定日期: {target_date}")
        except ValueError:
            print(f"❌ 日期格式错误: {args.date}")
            print("   请使用 YYYY-MM-DD 格式，例如: 2024-01-15")
            return
    else:
        # 交互式输入日期
        print("\n📅 请选择操作:")
        print("1. 生成今日/最近交易日的K线图")
        print("2. 生成指定日期的K线图")
        
        while True:
            try:
                choice = input("\n请输入选择 (1 或 2): ").strip()
                if choice == "1":
                    print("✅ 将生成今日/最近交易日的K线图")
                    break
                elif choice == "2":
                    while True:
                        date_input = input("请输入日期 (格式: YYYY-MM-DD，例如: 2024-01-15): ").strip()
                        if not date_input:
                            print("❌ 日期不能为空，请重新输入")
                            continue
                        
                        try:
                            target_date = datetime.strptime(date_input, '%Y-%m-%d').date()
                            print(f"✅ 指定日期: {target_date}")
                            break
                        except ValueError:
                            print(f"❌ 日期格式错误: {date_input}")
                            print("   请使用 YYYY-MM-DD 格式，例如: 2024-01-15")
                    break
                else:
                    print("❌ 无效选择，请输入 1 或 2")
            except KeyboardInterrupt:
                print("\n\n👋 用户取消操作")
                return
            except EOFError:
                print("\n\n👋 用户取消操作")
                return
    
    # 处理输出文件名
    output_filename = args.output
    if not output_filename:
        # 交互式输入输出文件名
        if target_date:
            default_filename = f"etf_159506_{target_date}_kline.png"
        else:
            default_filename = "etf_159506_today_kline.png"
        
        print(f"\n💾 输出文件名设置:")
        print(f"默认文件名: {default_filename}")
        
        while True:
            try:
                filename_input = input(f"请输入输出文件名 (直接回车使用默认): ").strip()
                if not filename_input:
                    output_filename = default_filename
                    print(f"✅ 使用默认文件名: {output_filename}")
                    break
                else:
                    # 确保文件名有.png后缀
                    if not filename_input.lower().endswith('.png'):
                        filename_input += '.png'
                    output_filename = filename_input
                    print(f"✅ 输出文件名: {output_filename}")
                    break
            except KeyboardInterrupt:
                print("\n\n👋 用户取消操作")
                return
            except EOFError:
                print("\n\n👋 用户取消操作")
                return
    
    print("=" * 60)
    print("159506 ETF K线图表生成器")
    if target_date:
        print(f"目标日期: {target_date}")
    else:
        print("目标日期: 今日/最近交易日")
    print("=" * 60)
    
    try:
        # 创建K线生成器
        kline_generator = ETF159506RedisKlineGenerator()
        
        # 获取最新数据状态
        latest_data = kline_generator.get_latest_data()
        if latest_data:
            print(f"✅ Redis连接成功")
            print(f"   报价数据: {latest_data.get('quote_count', 0)} 条")
            print(f"   交易数据: {latest_data.get('trade_count', 0)} 条")
            print(f"   K线数据: {latest_data.get('bar_count', 0)} 条")
            
            # 显示最新价格
            latest_trade = latest_data.get('latest_trade')
            if latest_trade:
                print(f"   最新成交价: {float(latest_trade.price):.4f}")
        else:
            print("⚠️  Redis中没有数据，将使用catalog文件数据")
        
        # 生成K线图
        if target_date:
            print(f"\n正在生成 {target_date} 的K线图...")
        else:
            print("\n正在生成今日K线图...")
        
        kline_generator.create_realtime_kline_chart(
            save_path=output_filename,
            auto_refresh=args.realtime,
            target_date=target_date
        )
        
    except Exception as e:
        print(f"❌ 运行失败: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 60)


if __name__ == "__main__":
    main() 

    #7*25  8-18  8-19 8-20 8-21 8-22