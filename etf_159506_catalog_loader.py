#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
159506 ETF Catalog数据加载器
专门用于加载和分析159506 ETF的catalog数据，支持回测和数据分析
支持从Redis读取实时数据并生成K线图
"""

import pandas as pd
import numpy as np
from pathlib import Path
import json
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import logging
import time
import threading

# Redis连接检测
REDIS_AVAILABLE = False
try:
    import redis
    REDIS_AVAILABLE = True
    print("[Redis] redis-py已安装，支持Redis数据读取")
except ImportError:
    print("[Redis] 未安装redis-py，将使用文件数据模式")

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    print("[NautilusTrader] 将使用文件数据模式")


class ETF159506RedisKlineGenerator:
    """159506 ETF Redis K线生成器"""
    
    def __init__(self, redis_host: str = "localhost", redis_port: int = 6379):
        if not REDIS_AVAILABLE or not NAUTILUS_AVAILABLE:
            raise RuntimeError("Redis或NautilusTrader不可用")
        
        self.redis_host = redis_host
        self.redis_port = redis_port
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
            
            return {
                'latest_quote': latest_quote,
                'latest_trade': latest_trade,
                'quote_count': self.cache.quote_tick_count(self.instrument_id),
                'trade_count': self.cache.trade_tick_count(self.instrument_id),
                'bar_count': self.cache.bar_count(self.instrument_id),
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"获取最新数据失败: {e}")
            return {}
    
    def generate_kline_from_ticks(self, bar_type: BarType) -> Optional[Bar]:
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
        
        # 根据bar_type的时间窗口来聚合数据
        # 对于1分钟K线，获取最近1分钟的数据
        current_time = trade_ticks[-1].ts_event
        window_start = current_time - (60 * 1_000_000_000)  # 1分钟 = 60秒 * 10^9纳秒
        
        # 过滤时间窗口内的数据
        window_trades = [tick for tick in trade_ticks if tick.ts_event >= window_start]
        
        # 如果数据量过大，进一步限制
        max_records = 100
        if len(window_trades) > max_records:
            window_trades = window_trades[-max_records:]
        
        return window_trades
    
    def get_kline_data(self, limit: int = 100) -> List[Dict]:
        """获取K线数据"""
        try:
            bars = self.cache.bars(self.instrument_id)[-limit:]
            
            kline_data = []
            for bar in bars:
                kline_data.append({
                    'timestamp': pd.to_datetime(bar.ts_event, unit='ns'),
                    'open': float(bar.open),
                    'high': float(bar.high),
                    'low': float(bar.low),
                    'close': float(bar.close),
                    'volume': int(bar.volume),
                    'bar_type': str(bar.bar_type)
                })
            
            return kline_data
            
        except Exception as e:
            logger.error(f"获取K线数据失败: {e}")
            return []
    
    def create_realtime_kline_chart(self, save_path: str = None, auto_refresh: bool = True):
        """创建实时K线图"""
        if not auto_refresh:
            self._plot_kline_chart(save_path)
            return
        
        # 实时更新模式
        self._start_realtime_chart(save_path)
    
    def _plot_kline_chart(self, save_path: str = None):
        """绘制K线图"""
        try:
            kline_data = self.get_kline_data(100)  # 获取最近100根K线
            
            if not kline_data:
                logger.warning("没有K线数据可绘制")
                return
            
            df = pd.DataFrame(kline_data)
            
            # 创建K线图
            fig, axes = plt.subplots(2, 1, figsize=(15, 10))
            fig.suptitle('159506 ETF 实时K线图', fontsize=16)
            
            # K线图
            ax1 = axes[0]
            self._plot_candlestick(ax1, df)
            ax1.set_title('K线图')
            ax1.set_ylabel('价格')
            ax1.grid(True, alpha=0.3)
            
            # 成交量图
            ax2 = axes[1]
            ax2.bar(df['timestamp'], df['volume'], alpha=0.6, width=0.001)
            ax2.set_title('成交量')
            ax2.set_ylabel('成交量')
            ax2.set_xlabel('时间')
            ax2.grid(True, alpha=0.3)
            
            plt.tight_layout()
            
            if save_path:
                plt.savefig(save_path, dpi=300, bbox_inches='tight')
                logger.info(f"K线图已保存到: {save_path}")
            
            plt.show()
            
        except Exception as e:
            logger.error(f"绘制K线图失败: {e}")
    
    def _plot_candlestick(self, ax, df):
        """绘制蜡烛图"""
        for i, row in df.iterrows():
            timestamp = row['timestamp']
            open_price = row['open']
            high_price = row['high']
            low_price = row['low']
            close_price = row['close']
            
            # 判断涨跌
            if close_price >= open_price:
                color = 'red'  # 上涨
                body_bottom = open_price
                body_top = close_price
            else:
                color = 'green'  # 下跌
                body_bottom = close_price
                body_top = open_price
            
            # 绘制实体
            ax.bar(timestamp, body_top - body_bottom, bottom=body_bottom, 
                   color=color, alpha=0.7, width=0.0005)
            
            # 绘制影线
            ax.plot([timestamp, timestamp], [low_price, high_price], 
                   color='black', linewidth=1)
    
    def _start_realtime_chart(self, save_path: str = None):
        """启动实时图表更新"""
        def update_chart():
            while True:
                try:
                    # 生成新的K线
                    bar_spec = BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST)
                    bar_type = BarType(self.instrument_id, bar_spec)
                    
                    new_bar = self.generate_kline_from_ticks(bar_type)
                    if new_bar:
                        self.cache.add_bar(new_bar)
                        logger.info(f"生成新K线: {float(new_bar.close):.4f}")
                    
                    # 更新图表
                    self._plot_kline_chart(save_path)
                    
                    # 等待1分钟
                    time.sleep(60)
                    
                except KeyboardInterrupt:
                    logger.info("实时图表更新已停止")
                    break
                except Exception as e:
                    logger.error(f"实时图表更新失败: {e}")
                    time.sleep(10)
        
        # 在后台线程中运行
        chart_thread = threading.Thread(target=update_chart, daemon=True)
        chart_thread.start()
        logger.info("实时K线图更新已启动")


class ETF159506CatalogLoader:
    """159506 ETF Catalog数据加载器"""
    
    def __init__(self, catalog_path: str = "catalog/etf_159506"):
        self.catalog_path = Path(catalog_path)
        self.metadata = None
        self.data_files = []
        
        logger.info(f"初始化159506 ETF Catalog加载器: {self.catalog_path}")
        
        # 检查目录是否存在
        if not self.catalog_path.exists():
            logger.warning(f"Catalog目录不存在: {self.catalog_path}")
            return
        
        # 加载元数据
        self._load_metadata()
        
        # 扫描数据文件
        self._scan_data_files()
    
    def _load_metadata(self):
        """加载元数据"""
        metadata_file = self.catalog_path / 'metadata.json'
        if metadata_file.exists():
            try:
                with open(metadata_file, 'r', encoding='utf-8') as f:
                    self.metadata = json.load(f)
                logger.info(f"加载元数据成功: {self.metadata}")
            except Exception as e:
                logger.error(f"加载元数据失败: {e}")
        else:
            logger.warning("元数据文件不存在")
    
    def _scan_data_files(self):
        """扫描数据文件"""
        try:
            # 查找所有parquet文件
            parquet_files = list(self.catalog_path.glob("*.parquet"))
            self.data_files = sorted(parquet_files)
            
            logger.info(f"找到 {len(self.data_files)} 个数据文件")
            for file in self.data_files:
                logger.info(f"  - {file.name}")
                
        except Exception as e:
            logger.error(f"扫描数据文件失败: {e}")
    
    def get_data_files_info(self) -> List[Dict]:
        """获取数据文件信息"""
        file_info = []
        
        for file in self.data_files:
            try:
                # 读取文件基本信息
                df = pd.read_parquet(file)
                
                info = {
                    'filename': file.name,
                    'filepath': str(file),
                    'size_mb': file.stat().st_size / (1024 * 1024),
                    'records': len(df),
                    'start_time': df['timestamp'].min() if 'timestamp' in df.columns else None,
                    'end_time': df['timestamp'].max() if 'timestamp' in df.columns else None,
                    'columns': list(df.columns)
                }
                file_info.append(info)
                
            except Exception as e:
                logger.error(f"读取文件信息失败 {file.name}: {e}")
        
        return file_info
    
    def load_all_data(self) -> pd.DataFrame:
        """加载所有数据"""
        all_data = []
        
        for file in self.data_files:
            try:
                logger.info(f"加载数据文件: {file.name}")
                df = pd.read_parquet(file)
                all_data.append(df)
                
            except Exception as e:
                logger.error(f"加载文件失败 {file.name}: {e}")
        
        if all_data:
            # 合并所有数据
            combined_df = pd.concat(all_data, ignore_index=True)
            
            # 去重和排序
            combined_df = combined_df.drop_duplicates(subset=['timestamp', 'stock_code'])
            combined_df = combined_df.sort_values('timestamp')
            
            logger.info(f"数据加载完成: {len(combined_df)} 条记录")
            return combined_df
        else:
            logger.warning("没有数据可加载")
            return pd.DataFrame()
    
    def load_data_by_date_range(self, start_date: str, end_date: str) -> pd.DataFrame:
        """按日期范围加载数据"""
        try:
            start_dt = pd.to_datetime(start_date)
            end_dt = pd.to_datetime(end_date)
            
            all_data = []
            
            for file in self.data_files:
                try:
                    df = pd.read_parquet(file)
                    
                    # 过滤日期范围
                    if 'timestamp' in df.columns:
                        df['timestamp'] = pd.to_datetime(df['timestamp'])
                        mask = (df['timestamp'] >= start_dt) & (df['timestamp'] <= end_dt)
                        df_filtered = df[mask]
                        
                        if len(df_filtered) > 0:
                            all_data.append(df_filtered)
                            
                except Exception as e:
                    logger.error(f"加载文件失败 {file.name}: {e}")
            
            if all_data:
                combined_df = pd.concat(all_data, ignore_index=True)
                combined_df = combined_df.drop_duplicates(subset=['timestamp', 'stock_code'])
                combined_df = combined_df.sort_values('timestamp')
                
                logger.info(f"按日期范围加载数据完成: {len(combined_df)} 条记录")
                return combined_df
            else:
                logger.warning(f"在指定日期范围内没有找到数据: {start_date} 到 {end_date}")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"按日期范围加载数据失败: {e}")
            return pd.DataFrame()
    
    def analyze_data(self, df: pd.DataFrame) -> Dict:
        """分析数据"""
        if df.empty:
            return {}
        
        try:
            analysis = {
                'basic_info': {
                    'total_records': len(df),
                    'date_range': {
                        'start': df['timestamp'].min().isoformat(),
                        'end': df['timestamp'].max().isoformat()
                    },
                    'unique_days': df['timestamp'].dt.date.nunique(),
                    'columns': list(df.columns)
                },
                'price_analysis': {
                    'price_range': {
                        'min': float(df['price'].min()),
                        'max': float(df['price'].max()),
                        'mean': float(df['price'].mean()),
                        'std': float(df['price'].std())
                    },
                    'volume_analysis': {
                        'total_volume': float(df['volume'].sum()),
                        'avg_volume': float(df['volume'].mean()),
                        'max_volume': float(df['volume'].max())
                    }
                },
                'time_analysis': {
                    'trading_hours': df['timestamp'].dt.hour.value_counts().to_dict(),
                    'weekday_distribution': df['timestamp'].dt.dayofweek.value_counts().to_dict()
                }
            }
            
            # 计算价格变化统计
            if len(df) > 1:
                df_sorted = df.sort_values('timestamp')
                price_changes = df_sorted['price'].diff().dropna()
                
                analysis['price_analysis']['price_changes'] = {
                    'positive_changes': int((price_changes > 0).sum()),
                    'negative_changes': int((price_changes < 0).sum()),
                    'no_changes': int((price_changes == 0).sum()),
                    'max_change': float(price_changes.max()),
                    'min_change': float(price_changes.min()),
                    'avg_change': float(price_changes.mean())
                }
            
            return analysis
            
        except Exception as e:
            logger.error(f"数据分析失败: {e}")
            return {}
    
    def generate_summary_report(self) -> str:
        """生成摘要报告"""
        try:
            # 获取文件信息
            file_info = self.get_data_files_info()
            
            # 加载所有数据
            df = self.load_all_data()
            
            if df.empty:
                return "没有数据可分析"
            
            # 分析数据
            analysis = self.analyze_data(df)
            
            # 生成报告
            report = []
            report.append("=" * 60)
            report.append("159506 ETF Catalog数据摘要报告")
            report.append("=" * 60)
            
            # 文件信息
            report.append(f"\n📁 数据文件信息:")
            report.append(f"   目录: {self.catalog_path}")
            report.append(f"   文件数量: {len(file_info)}")
            
            total_size = sum(info['size_mb'] for info in file_info)
            total_records = sum(info['records'] for info in file_info)
            report.append(f"   总大小: {total_size:.2f} MB")
            report.append(f"   总记录数: {total_records}")
            
            # 数据文件详情
            for info in file_info:
                report.append(f"   - {info['filename']}: {info['records']} 条记录, {info['size_mb']:.2f} MB")
            
            # 基本统计
            if analysis:
                basic_info = analysis['basic_info']
                report.append(f"\n📊 基本统计:")
                report.append(f"   总记录数: {basic_info['total_records']}")
                report.append(f"   时间范围: {basic_info['date_range']['start']} 到 {basic_info['date_range']['end']}")
                report.append(f"   交易日数: {basic_info['unique_days']}")
                
                # 价格分析
                price_analysis = analysis['price_analysis']
                report.append(f"\n💰 价格分析:")
                report.append(f"   价格范围: {price_analysis['price_range']['min']:.4f} - {price_analysis['price_range']['max']:.4f}")
                report.append(f"   平均价格: {price_analysis['price_range']['mean']:.4f}")
                report.append(f"   价格标准差: {price_analysis['price_range']['std']:.4f}")
                
                # 成交量分析
                volume_analysis = price_analysis['volume_analysis']
                report.append(f"\n📈 成交量分析:")
                report.append(f"   总成交量: {volume_analysis['total_volume']:,.0f}")
                report.append(f"   平均成交量: {volume_analysis['avg_volume']:,.0f}")
                report.append(f"   最大成交量: {volume_analysis['max_volume']:,.0f}")
                
                # 价格变化分析
                if 'price_changes' in price_analysis:
                    changes = price_analysis['price_changes']
                    report.append(f"\n📉 价格变化分析:")
                    report.append(f"   上涨次数: {changes['positive_changes']}")
                    report.append(f"   下跌次数: {changes['negative_changes']}")
                    report.append(f"   平盘次数: {changes['no_changes']}")
                    report.append(f"   最大涨幅: {changes['max_change']:.4f}")
                    report.append(f"   最大跌幅: {changes['min_change']:.4f}")
                    report.append(f"   平均变化: {changes['avg_change']:.4f}")
            
            report.append("\n" + "=" * 60)
            
            return "\n".join(report)
            
        except Exception as e:
            logger.error(f"生成摘要报告失败: {e}")
            return f"生成报告失败: {e}"
    
    def plot_price_chart(self, df: pd.DataFrame, save_path: str = None):
        """绘制价格图表"""
        try:
            if df.empty:
                logger.warning("没有数据可绘制")
                return
            
            # 准备数据
            df_plot = df.copy()
            df_plot['timestamp'] = pd.to_datetime(df_plot['timestamp'])
            df_plot = df_plot.sort_values('timestamp')
            
            # 创建图表
            fig, axes = plt.subplots(2, 1, figsize=(15, 10))
            fig.suptitle('159506 ETF 价格和成交量分析', fontsize=16)
            
            # 价格图
            ax1 = axes[0]
            ax1.plot(df_plot['timestamp'], df_plot['price'], linewidth=1, alpha=0.8)
            ax1.set_title('价格走势')
            ax1.set_ylabel('价格')
            ax1.grid(True, alpha=0.3)
            
            # 成交量图
            ax2 = axes[1]
            ax2.bar(df_plot['timestamp'], df_plot['volume'], alpha=0.6, width=0.001)
            ax2.set_title('成交量')
            ax2.set_ylabel('成交量')
            ax2.set_xlabel('时间')
            ax2.grid(True, alpha=0.3)
            
            plt.tight_layout()
            
            if save_path:
                plt.savefig(save_path, dpi=300, bbox_inches='tight')
                logger.info(f"图表已保存到: {save_path}")
            
            plt.show()
            
        except Exception as e:
            logger.error(f"绘制价格图表失败: {e}")
    
    def export_to_csv(self, df: pd.DataFrame, output_path: str):
        """导出数据到CSV"""
        try:
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
            logger.info(f"数据已导出到: {output_path}")
        except Exception as e:
            logger.error(f"导出CSV失败: {e}")


def main():
    """主函数"""
    print("=" * 60)
    print("159506 ETF Catalog数据加载器 & Redis K线生成器")
    print("=" * 60)
    
    # 检查是否支持Redis模式
    if REDIS_AVAILABLE and NAUTILUS_AVAILABLE:
        print("✅ 支持Redis实时K线生成模式")
        
        # 询问用户选择模式
        print("\n请选择运行模式:")
        print("1. Redis实时K线生成模式")
        print("2. 文件数据分析模式")
        
        try:
            choice = input("请输入选择 (1 或 2): ").strip()
            
            if choice == "1":
                run_redis_kline_mode()
            elif choice == "2":
                run_file_analysis_mode()
            else:
                print("无效选择，默认使用文件分析模式")
                run_file_analysis_mode()
                
        except KeyboardInterrupt:
            print("\n用户中断，退出程序")
        except Exception as e:
            print(f"选择模式失败: {e}")
            run_file_analysis_mode()
    else:
        print("⚠️  Redis或NautilusTrader不可用，使用文件分析模式")
        run_file_analysis_mode()
    
    print("\n" + "=" * 60)


def run_redis_kline_mode():
    """运行Redis实时K线生成模式"""
    print("\n" + "=" * 60)
    print("Redis实时K线生成模式")
    print("=" * 60)
    
    try:
        # 创建Redis K线生成器
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
            print("⚠️  Redis中没有数据，请先运行数据采集器")
            return
        
        # 询问用户选择
        print("\n请选择操作:")
        print("1. 生成单次K线图")
        print("2. 启动实时K线图更新")
        print("3. 查看K线数据")
        
        choice = input("请输入选择 (1, 2, 或 3): ").strip()
        
        if choice == "1":
            # 生成单次K线图
            print("正在生成K线图...")
            kline_generator.create_realtime_kline_chart("etf_159506_realtime_kline.png", auto_refresh=False)
            
        elif choice == "2":
            # 启动实时更新
            print("启动实时K线图更新...")
            print("按Ctrl+C停止更新")
            kline_generator.create_realtime_kline_chart("etf_159506_realtime_kline.png", auto_refresh=True)
            
            # 保持程序运行
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                print("\n实时更新已停止")
                
        elif choice == "3":
            # 查看K线数据
            kline_data = kline_generator.get_kline_data(20)  # 获取最近20根K线
            if kline_data:
                df = pd.DataFrame(kline_data)
                print(f"\n最近{len(df)}根K线数据:")
                print(df.to_string(index=False))
            else:
                print("没有K线数据")
        else:
            print("无效选择")
            
    except Exception as e:
        print(f"❌ Redis模式运行失败: {e}")


def run_file_analysis_mode():
    """运行文件数据分析模式"""
    print("\n" + "=" * 60)
    print("文件数据分析模式")
    print("=" * 60)
    
    # 创建加载器
    loader = ETF159506CatalogLoader()
    
    # 生成摘要报告
    report = loader.generate_summary_report()
    print(report)
    
    # 加载所有数据
    print("\n正在加载数据...")
    df = loader.load_all_data()
    
    if not df.empty:
        print(f"✅ 数据加载成功: {len(df)} 条记录")
        
        # 显示前几行数据
        print("\n前5行数据:")
        print(df.head())
        
        # 显示数据列信息
        print(f"\n数据列: {list(df.columns)}")
        
        # 绘制价格图表
        print("\n正在生成价格图表...")
        loader.plot_price_chart(df, "etf_159506_price_chart.png")
        
        # 导出数据
        print("\n正在导出数据...")
        loader.export_to_csv(df, "etf_159506_data.csv")
        
    else:
        print("❌ 没有数据可加载")


if __name__ == "__main__":
    main() 