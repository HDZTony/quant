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
    
    def get_today_kline_data(self) -> List[Dict]:
        """获取今日K线数据，如果不是交易日则获取上一个交易日数据"""
        try:
            # 创建交易时间管理器
            trading_manager = TradingTimeManager()
            current_date = datetime.now().date()
            
            # 判断当前是否为交易日
            if TRADING_TIME_MANAGER_AVAILABLE:
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
    
    def create_realtime_kline_chart(self, save_path: str = None, auto_refresh: bool = True):
        """创建实时K线图"""
        if auto_refresh:
            self._start_realtime_chart(save_path)
        else:
            self._plot_kline_chart(save_path)
    
    def _plot_kline_chart(self, save_path: str = None):
        """绘制价格走势图"""
        try:
            # 获取数据
            kline_data = self.get_today_kline_data()
            
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
            
            # 过滤交易时间内的数据（9:30-15:00）
            from datetime import time as datetime_time
            trading_start = datetime_time(9, 30)
            trading_end = datetime_time(15, 0)
            
            trading_data = df[
                (df['timestamp'].dt.time >= trading_start) & 
                (df['timestamp'].dt.time <= trading_end)
            ]
            
            if len(trading_data) == 0:
                logger.warning("没有交易时间内的数据可绘制")
                return
            
            logger.info(f"交易时间内的数据: {len(trading_data)} 条")
            
            # 使用交易数据的时间作为索引
            trading_data = trading_data.set_index('timestamp')
            
            # 确保索引为升序、唯一、无NaN
            complete_df = trading_data.copy()
            complete_df = complete_df.sort_index()
            complete_df = complete_df[~complete_df.index.duplicated(keep='first')]
            complete_df = complete_df[complete_df.index.notnull()]

            # 创建五联图，图片高度更大
            fig, (ax1, ax2, ax3, ax4, ax5) = plt.subplots(5, 1, figsize=(20, 40), height_ratios=[3, 1, 1, 1, 1])

            # ====== ax1主图（价格走势） ======
            # 绘制价格走势
            ax1.plot(complete_df.index, complete_df['price'], linewidth=1, color='blue', alpha=0.8, label='成交价')
            
            # 标记关键价格点
            valid_prices = complete_df['price'].dropna()
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
            
            ax1.set_title('价格走势 (北京时间)')
            ax1.set_ylabel('价格')
            ax1.grid(True, alpha=0.3)
            ax1.legend()
            
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
            vol_df = complete_df.iloc[1:].copy()
            if len(vol_df) == 0:
                logger.warning("成交量数据不足，无法绘制")
                return
            
            # 计算涨跌颜色
            price_arr = vol_df['price'].values
            prev_price_arr = complete_df['price'].values[:-1]
            colors = np.where(price_arr > prev_price_arr, 'red', np.where(price_arr < prev_price_arr, 'green', 'gray'))
            
            # 绘制成交量柱状图
            ax2.bar(vol_df.index, vol_df['volume'], alpha=0.6, color=colors, width=1/1440)
            ax2.set_title('成交量 (北京时间)')
            ax2.set_ylabel('成交量')
            ax2.set_xlabel('时间 (北京时间)', fontsize=13)
            ax2.annotate('所有横轴时间均为北京时间', xy=(1, 0), xycoords='axes fraction', fontsize=11, color='gray', ha='right', va='top')
            ax2.grid(True, alpha=0.3)
            
            # 设置x轴格式 - 显示北京时间
            ax2.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
            ax2.xaxis.set_major_locator(plt.matplotlib.dates.MinuteLocator(interval=10))  # 每10分钟一个刻度
            plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
            
            # ====== 生成1分钟K线收盘价序列，用于技术指标 ======
            minute_close = complete_df['price'].resample('1min').last().dropna()
            minute_index = minute_close.index

            # ====== ax3 MACD副图（用1分钟K线收盘价） ======
            ema12 = minute_close.ewm(span=12, adjust=False).mean()
            ema26 = minute_close.ewm(span=26, adjust=False).mean()
            dif = ema12 - ema26  # DIF
            dea = dif.ewm(span=9, adjust=False).mean()  # DEA
            macd_hist = 2 * (dif - dea)  # MACD柱子
            macd_colors = np.where(macd_hist > 0, 'red', np.where(macd_hist < 0, 'green', 'gray'))
            ax3.bar(minute_index, macd_hist, color=macd_colors, width=1/1440, alpha=0.7, label='MACD柱')
            ax3.plot(minute_index, dif, color='orange', label='DIF线')      # DIF橙色
            ax3.plot(minute_index, dea, color='deepskyblue', label='DEA线') # DEA天蓝色
            ax3.set_title('MACD指标 (12,26,9)')
            ax3.set_ylabel('MACD')
            ax3.set_xlabel('时间 (北京时间)', fontsize=13)
            ax3.annotate('所有横轴时间均为北京时间', xy=(1, 0), xycoords='axes fraction', fontsize=11, color='gray', ha='right', va='top')
            ax3.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
            ax3.xaxis.set_major_locator(plt.matplotlib.dates.MinuteLocator(interval=10))
            plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45)
            ax3.legend()
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
            rsi6 = calc_rsi(minute_close, 6)
            rsi12 = calc_rsi(minute_close, 12)
            rsi24 = calc_rsi(minute_close, 24)

            # ====== ax4 RSI副图（6,12,24三线，用1分钟K线收盘价） ======
            ax4.plot(minute_index, rsi6, color='orange', label='RSI(6)')         # 橙色
            ax4.plot(minute_index, rsi12, color='deepskyblue', label='RSI(12)')  # 天蓝色
            ax4.plot(minute_index, rsi24, color='purple', label='RSI(24)')       # 紫色
            ax4.axhline(70, color='red', linestyle='--', linewidth=1, label='超买70')
            ax4.axhline(30, color='green', linestyle='--', linewidth=1, label='超卖30')
            ax4.set_title('RSI指标 (6,12,24)')
            ax4.set_ylabel('RSI')
            ax4.set_xlabel('时间 (北京时间)', fontsize=13)
            ax4.annotate('所有横轴时间均为北京时间', xy=(1, 0), xycoords='axes fraction', fontsize=11, color='gray', ha='right', va='top')
            ax4.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
            ax4.xaxis.set_major_locator(plt.matplotlib.dates.MinuteLocator(interval=10))
            plt.setp(ax4.xaxis.get_majorticklabels(), rotation=45)
            ax4.set_ylim(0, 100)
            ax4.legend()
            ax4.grid(True, alpha=0.3)

            # ====== 计算KDJ(9,3,3)（用1分钟K线收盘价） ======
            def calc_kdj(close, n=9, k_period=3, d_period=3):
                low_list = close.rolling(window=n, min_periods=1).min()
                high_list = close.rolling(window=n, min_periods=1).max()
                rsv = (close - low_list) / (high_list - low_list) * 100
                k = rsv.ewm(com=(k_period-1), adjust=False).mean()
                d = k.ewm(com=(d_period-1), adjust=False).mean()
                j = 3 * k - 2 * d
                return k, d, j
            kdj_k, kdj_d, kdj_j = calc_kdj(minute_close, n=9, k_period=3, d_period=3)

            # ====== ax5 KDJ副图（用1分钟K线收盘价） ======
            ax5.plot(minute_index, kdj_k, color='orange', label='K')         # 橙色
            ax5.plot(minute_index, kdj_d, color='deepskyblue', label='D')    # 天蓝色
            ax5.plot(minute_index, kdj_j, color='purple', label='J')         # 紫色
            ax5.set_title('KDJ指标 (9,3,3)')
            ax5.set_ylabel('KDJ')
            ax5.set_xlabel('时间 (北京时间)', fontsize=13)
            ax5.annotate('所有横轴时间均为北京时间', xy=(1, 0), xycoords='axes fraction', fontsize=11, color='gray', ha='right', va='top')
            ax5.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
            ax5.xaxis.set_major_locator(plt.matplotlib.dates.MinuteLocator(interval=10))
            plt.setp(ax5.xaxis.get_majorticklabels(), rotation=45)
            ax5.legend()
            ax5.grid(True, alpha=0.3)

            # 统一x轴格式化，防止内容错乱
            for ax in [ax1, ax2, ax3, ax4, ax5]:
                ax.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
                ax.xaxis.set_major_locator(plt.matplotlib.dates.MinuteLocator(interval=10))
                plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

            plt.tight_layout()
            
            # 只生成一张图，且只show一次
            if save_path:
                plt.savefig(save_path, dpi=300, bbox_inches='tight')
                logger.info(f"价格走势图已保存到: {save_path}")
            
            plt.show()
            
        except Exception as e:
            logger.error(f"绘制价格走势图失败: {e}")
            import traceback
            logger.error(f"详细错误: {traceback.format_exc()}")
    
    def _start_realtime_chart(self, save_path: str = None):
        """启动实时图表更新"""
        def update_chart():
            while True:
                try:
                    time.sleep(30)  # 每30秒更新一次
                    self._plot_kline_chart(save_path)
                    logger.info("实时K线图已更新")
                except Exception as e:
                    logger.error(f"实时图表更新失败: {e}")
                    break
        
        # 启动更新线程
        update_thread = threading.Thread(target=update_chart, daemon=True)
        update_thread.start()
        logger.info("实时K线图更新已启动")


def main():
    """主函数"""
    print("=" * 60)
    print("159506 ETF K线图表生成器")
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
        
        # 直接生成K线图
        print("\n正在生成今日K线图...")
        kline_generator.create_realtime_kline_chart("etf_159506_today_kline.png", auto_refresh=False)
        
    except Exception as e:
        print(f"❌ 运行失败: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 60)


if __name__ == "__main__":
    main() 