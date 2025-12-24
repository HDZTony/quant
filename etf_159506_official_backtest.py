#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
159506 ETF 官方 NautilusTrader 回测系统
使用官方架构进行生产级回测
"""

import sys
from pathlib import Path
from decimal import Decimal
from datetime import datetime, date
import logging
import pandas as pd
import matplotlib.pyplot as plt
import mplfinance as mpf
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

# NautilusTrader imports
from nautilus_trader.backtest.node import BacktestNode
from nautilus_trader.backtest.node import BacktestDataConfig
from nautilus_trader.backtest.node import BacktestEngineConfig
from nautilus_trader.backtest.node import BacktestRunConfig
from nautilus_trader.backtest.node import BacktestVenueConfig
from nautilus_trader.backtest.results import BacktestResult
from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import ImportableStrategyConfig
from nautilus_trader.model.identifiers import TraderId, Venue, InstrumentId, Symbol
from nautilus_trader.model.enums import AccountType, OmsType
from nautilus_trader.model.currencies import CNY
from nautilus_trader.model.objects import Money
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.model.data import BarType, TradeTick, Bar
from nautilus_trader.serialization.arrow.serializer import ArrowSerializer
from nautilus_trader.persistence.wranglers import BarDataWrangler

# 导入策略和工具
from etf_159506_realtime_strategy import ETF159506Strategy
from etf_159506_strategy_config import ETF159506Config
from etf_159506_instrument import create_etf_159506_default, create_etf_159506_bar_type
from etf_159506_catalog_loader import ETF159506RedisKlineGenerator

# 配置日志
import os

# 配置日志 - 输出到根目录，与cache_collector保持一致
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etf_159506_backtest.log', mode='w', encoding='utf-8'),  # 根目录
        logging.StreamHandler(sys.stdout)
    ],
    force=True  # 强制重新配置日志
)
logger = logging.getLogger(__name__)

# 设置其他模块的日志级别
logging.getLogger('nautilus_trader').setLevel(logging.INFO)
logging.getLogger('BACKTESTER').setLevel(logging.INFO)


class ETF159506OfficialBacktest:
    """159506 ETF 官方回测系统"""
    
    def __init__(self, catalog_path: str = "data_catalog"):
        self.catalog_path = Path(catalog_path)
        self.catalog = None
        
        # 使用 etf_159506_instrument.py 中的工具定义
        self.instrument = create_etf_159506_default()
        self.instrument_id = self.instrument.id
        
        # 创建BarType
        bar_type_str = create_etf_159506_bar_type()
        self.bar_type = BarType.from_str(bar_type_str)
        
        # 交易信号存储
        self.trade_signals = []
        self.technical_signals = []  # 添加技术指标信号列表
        
        # 极值点数据存储
        self.extremes_data = {}
        
        # 初始化 catalog
        self._init_catalog()
        
    def _init_catalog(self):
        """初始化数据 catalog"""
        try:
            # 如果路径不存在，尝试创建目录
            if not self.catalog_path.exists():
                logger.warning(f"Catalog 路径不存在: {self.catalog_path}，尝试创建...")
                self.catalog_path.mkdir(parents=True, exist_ok=True)
                logger.info(f"已创建 Catalog 路径: {self.catalog_path}")
            
            self.catalog = ParquetDataCatalog(self.catalog_path)
            logger.info(f"Catalog 初始化成功: {self.catalog_path}")
            
            # 检查数据
            instruments = self.catalog.instruments()
            logger.info(f"Catalog 中的工具数量: {len(instruments)}")
            
            quote_ticks = self.catalog.quote_ticks()
            trade_ticks = self.catalog.trade_ticks()
            logger.info(f"Quote ticks: {len(quote_ticks)}, Trade ticks: {len(trade_ticks)}")
            
            # 确保工具有效
            if not instruments:
                logger.info("Catalog 中没有工具，添加工具...")
                self.catalog.write_data([self.instrument])
                logger.info("已添加工具到 catalog")
            
            # 强制重新加载数据以确保精度一致
            logger.info("强制重新加载数据以确保精度一致...")
            # 移除硬编码的数据加载，改为在回测时动态加载
            
        except Exception as e:
            logger.error(f"初始化 Catalog 失败: {e}")
            raise
    
    def _load_data_from_parquet_file(self, parquet_file_path: str):
        """
        从指定的 parquet 文件加载 TradeTick 数据
        
        根据 NautilusTrader 文档，此方法：
        1. 读取外部 parquet 文件（NautilusTrader 标准格式）
        2. 使用 ArrowSerializer 反序列化为 TradeTick 对象
        3. 写入当前 catalog 以供回测使用
        
        参考: https://nautilustrader.io/docs/latest/concepts/data
        
        Parameters
        ----------
        parquet_file_path : str
            Parquet 文件的完整路径，应为 NautilusTrader 标准格式的 TradeTick 数据
        """
        try:
            parquet_path = Path(parquet_file_path)
            
            if not parquet_path.exists():
                raise FileNotFoundError(f"Parquet 文件不存在: {parquet_file_path}")
            
            logger.info(f"正在从 parquet 文件加载 TradeTick 数据: {parquet_file_path}")
            
            # 读取 parquet 文件为 pyarrow Table
            # 根据文档，NautilusTrader 使用 PyArrow 作为底层存储格式
            table = pq.read_table(parquet_file_path)
            logger.info(f"从 parquet 文件读取到 {len(table)} 条记录")
            
            # 使用 ArrowSerializer 的公共 API 将 Table 转换为 TradeTick 对象
            # 这是 NautilusTrader 推荐的反序列化方法
            # 参考: https://nautilustrader.io/docs/latest/concepts/data#reading-data
            trade_ticks = ArrowSerializer.deserialize(
                data_cls=TradeTick,
                batch=table
            )
            
            logger.info(f"成功转换 {len(trade_ticks)} 个 TradeTick 对象")
            
            # 写入 catalog
            # 根据文档，write_data 方法会自动处理数据组织
            # 参考: https://nautilustrader.io/docs/latest/concepts/data#writing-data
            if trade_ticks:
                # 确保工具有效（如果 catalog 中没有）
                instruments = self.catalog.instruments()
                if not instruments or self.instrument_id not in [inst.id for inst in instruments]:
                    logger.info("添加工具到 catalog...")
                    self.catalog.write_data([self.instrument])
                
                # 写入 TradeTick 数据
                # 注意：如果数据重叠，可能需要使用 skip_disjoint_check=True
                self.catalog.write_data(trade_ticks, skip_disjoint_check=True)
                logger.info(f"已写入 {len(trade_ticks)} 条 TradeTick 数据到 catalog: {self.catalog_path}")
                
                # 从 TradeTick 数据生成 Bar 数据（1分钟K线）
                logger.info("正在从 TradeTick 数据生成 Bar 数据...")
                bars = self._generate_bars_from_ticks(trade_ticks)
                
                if bars:
                    self.catalog.write_data(bars, skip_disjoint_check=True)
                    logger.info(f"已写入 {len(bars)} 条 Bar 数据到 catalog")
                else:
                    logger.warning("未能生成 Bar 数据")
                
                # 验证数据写入
                written_ticks = self.catalog.query(
                    data_cls=TradeTick,
                    identifiers=[str(self.instrument_id)]
                )
                logger.info(f"Catalog 中现有 TradeTick 数据: {len(written_ticks)} 条")
                
                written_bars = self.catalog.query(
                    data_cls=Bar,
                    identifiers=[str(self.instrument_id)],
                    bar_types=[self.bar_type]
                )
                logger.info(f"Catalog 中现有 Bar 数据: {len(written_bars)} 条")
            else:
                logger.warning("没有成功转换任何 TradeTick 数据")
                
        except Exception as e:
            logger.error(f"从 parquet 文件加载数据失败: {e}")
            import traceback
            logger.error(f"详细错误: {traceback.format_exc()}")
            raise
    
    def _generate_bars_from_ticks(self, trade_ticks: list[TradeTick]) -> list[Bar]:
        """
        从 TradeTick 数据生成 Bar 数据（1分钟K线）
        
        根据 NautilusTrader 文档，Trade-to-bar aggregation 会从 TradeTick 自动生成 Bar
        参考: https://nautilustrader.io/docs/latest/concepts/data#types-of-aggregation
        """
        try:
            if not trade_ticks:
                logger.warning("没有 TradeTick 数据，无法生成 Bar")
                return []
            
            # 将 TradeTick 转换为 DataFrame 进行聚合
            tick_data = []
            for tick in trade_ticks:
                # 将纳秒时间戳转换为 datetime
                ts_event_ns = tick.ts_event
                timestamp = pd.to_datetime(ts_event_ns, unit='ns')
                
                tick_data.append({
                    'timestamp': timestamp,
                    'price': float(tick.price),
                    'volume': int(tick.size),  # TradeTick.size 是单笔成交量
                    'ts_event': ts_event_ns
                })
            
            df = pd.DataFrame(tick_data)
            df.set_index('timestamp', inplace=True)
            
            # 按1分钟时间窗口聚合数据
            resampled = df.resample('1min').agg({
                'price': ['first', 'max', 'min', 'last'],
                'volume': 'sum'  # 累加得到该分钟的总成交量
            }).dropna()
            
            # 重命名列
            resampled.columns = ['open', 'high', 'low', 'close', 'volume']
            
            # 使用 BarDataWrangler 生成 Bar 对象
            wrangler = BarDataWrangler(bar_type=self.bar_type, instrument=self.instrument)
            bars = wrangler.process(resampled)
            
            logger.info(f"成功生成 {len(bars)} 条 Bar 数据")
            return bars
            
        except Exception as e:
            logger.error(f"从 TradeTick 生成 Bar 数据失败: {e}")
            import traceback
            logger.error(f"详细错误: {traceback.format_exc()}")
            return []
    
    def _load_data_for_date(self, target_date: date):
        """
        加载指定日期的所有 TradeTick 数据文件
        
        Parameters
        ----------
        target_date : date
            要加载数据的日期
        """
        try:
            # 构建数据目录路径
            trade_tick_dir = Path(self.catalog_path) / "data" / "trade_tick" / "159506.SZSE"
            
            if not trade_tick_dir.exists():
                raise FileNotFoundError(f"TradeTick 数据目录不存在: {trade_tick_dir}")
            
            # 查找指定日期的所有 parquet 文件
            date_str = target_date.strftime("%Y-%m-%d")
            pattern = f"{date_str}T*.parquet"
            
            parquet_files = list(trade_tick_dir.glob(pattern))
            
            if not parquet_files:
                logger.warning(f"未找到 {target_date} 的 TradeTick 数据文件")
                return
            
            logger.info(f"找到 {len(parquet_files)} 个 {target_date} 的 TradeTick 数据文件")
            
            # 按文件名排序，确保按时间顺序加载
            parquet_files.sort()
            
            # 加载所有文件
            total_ticks = 0
            total_bars = 0
            
            for parquet_file in parquet_files:
                logger.info(f"正在加载文件: {parquet_file.name}")
                
                # 读取并转换数据
                table = pq.read_table(parquet_file)
                trade_ticks = ArrowSerializer.deserialize(
                    data_cls=TradeTick,
                    batch=table
                )
                
                if trade_ticks:
                    # 确保工具有效
                    instruments = self.catalog.instruments()
                    if not instruments or self.instrument_id not in [inst.id for inst in instruments]:
                        self.catalog.write_data([self.instrument])
                    
                    # 写入 TradeTick 数据
                    self.catalog.write_data(trade_ticks, skip_disjoint_check=True)
                    total_ticks += len(trade_ticks)
                    
                    # 生成 Bar 数据
                    bars = self._generate_bars_from_ticks(trade_ticks)
                    if bars:
                        self.catalog.write_data(bars, skip_disjoint_check=True)
                        total_bars += len(bars)
            
            logger.info(f"总共加载 {total_ticks} 条 TradeTick 数据，生成 {total_bars} 条 Bar 数据")
            
            # 验证数据
            written_ticks = self.catalog.query(
                data_cls=TradeTick,
                identifiers=[str(self.instrument_id)],
                start=datetime.combine(target_date, datetime.min.time()),
                end=datetime.combine(target_date, datetime.max.time())
            )
            logger.info(f"Catalog 中 {target_date} 的 TradeTick 数据: {len(written_ticks)} 条")
            
            written_bars = self.catalog.query(
                data_cls=Bar,
                identifiers=[str(self.instrument_id)],
                bar_types=[self.bar_type],
                start=datetime.combine(target_date, datetime.min.time()),
                end=datetime.combine(target_date, datetime.max.time())
            )
            logger.info(f"Catalog 中 {target_date} 的 Bar 数据: {len(written_bars)} 条")
            
        except Exception as e:
            logger.error(f"加载 {target_date} 的数据失败: {e}")
            import traceback
            logger.error(f"详细错误: {traceback.format_exc()}")
            raise
    
    def _load_data_with_catalog_loader(self, target_date=None):
        """使用 catalog loader 加载数据（已废弃，保留用于兼容性）"""
        try:
            # 创建 catalog loader
            catalog_loader = ETF159506RedisKlineGenerator(catalog_path=str(self.catalog_path))
            
            # 如果没有指定日期，默认加载 2025-07-25 的数据
            if target_date is None:
                target_date = datetime(2025, 7, 25).date()
            
            logger.info(f"正在加载 {target_date} 的数据...")
            
            # 获取数据
            kline_data = catalog_loader.get_today_kline_data(target_date)
            logger.info(f"从 catalog loader 获取到 {len(kline_data)} 条数据")
            
            if len(kline_data) > 0:
                # 将数据转换为 NautilusTrader 格式并写入 catalog
                self._convert_and_write_data(kline_data)
            else:
                logger.warning(f"没有找到 {target_date} 的数据")
                
        except Exception as e:
            logger.error(f"使用 catalog loader 加载数据失败: {e}")
    
    def _convert_and_write_data(self, kline_data):
        """将数据转换为 NautilusTrader 格式并写入 catalog"""
        try:
            from nautilus_trader.model.data import Bar
            from nautilus_trader.model.objects import Price, Quantity
            from nautilus_trader.model.enums import BarAggregation, PriceType
            
            logger.info("正在转换数据为1分钟K线格式...")
            
            # 检查数据类型
            if isinstance(kline_data, list):
                # 如果是列表，转换为 DataFrame
                df = pd.DataFrame(kline_data)
            else:
                df = kline_data
            
            # 按1分钟时间窗口聚合数据
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)
            
            # 创建1分钟K线数据
            bars = []
            
            # 按1分钟重采样
            resampled = df.resample('1T').agg({
                'price': ['first', 'max', 'min', 'last'],
                'volume': 'sum'
            }).dropna()
            
            # 重命名列
            resampled.columns = ['open', 'high', 'low', 'close', 'volume']
            
            for timestamp, row in resampled.iterrows():
                try:
                    # 创建Bar对象
                    bar = Bar(
                        bar_type=self.bar_type,
                        open=Price.from_str(f"{row['open']:.3f}"),
                        high=Price.from_str(f"{row['high']:.3f}"),
                        low=Price.from_str(f"{row['low']:.3f}"),
                        close=Price.from_str(f"{row['close']:.3f}"),
                        volume=Quantity.from_int(int(row['volume'])),
                        ts_event=int(timestamp.timestamp() * 1_000_000_000),  # 纳秒
                        ts_init=int(timestamp.timestamp() * 1_000_000_000),
                    )
                    bars.append(bar)
                    
                except Exception as e:
                    logger.warning(f"转换K线数据失败: {e}, 数据: {row}")
                    continue
            
            # 写入K线数据
            if bars:
                self.catalog.write_data(bars)
                logger.info(f"已写入 {len(bars)} 条1分钟K线数据")
            else:
                logger.warning("没有成功转换任何K线数据")
            
        except Exception as e:
            logger.error(f"转换数据失败: {e}")
            raise
    
    def collect_trade_signals(self, result: BacktestResult, backtest_node: BacktestNode = None):
        """收集交易信号数据"""
        try:
            logger.info("开始收集交易信号...")
            
            # 方法1: 从 BacktestNode 中获取引擎实例
            if backtest_node:
                try:
                    logger.info("从 BacktestNode 获取引擎实例...")
                    
                    # 获取所有引擎
                    engines = backtest_node.get_engines()
                    logger.info(f"找到 {len(engines)} 个引擎")
                    
                    for engine in engines:
                        logger.info(f"引擎类型: {type(engine)}")
                        
                        if hasattr(engine, 'trader'):
                            logger.info(f"找到trader属性: {engine.trader}")
                            strategies = engine.trader.strategies()
                            logger.info(f"策略列表: {strategies}")
                            
                            for strategy in strategies:
                                logger.info(f"策略ID: {strategy.id}")
                                logger.info(f"策略属性: {dir(strategy)}")
                                
                                # 只从策略实例的 _saved_trade_signals 获取（策略停止时保存的）
                                if hasattr(strategy, '_saved_trade_signals') and strategy._saved_trade_signals:
                                    logger.info(f"从策略 {strategy.id} 的保存信号中获取到 {len(strategy._saved_trade_signals)} 个交易信号")
                                    self.trade_signals.extend(strategy._saved_trade_signals)
                                else:
                                    logger.warning(f"策略 {strategy.id} 没有保存的交易信号")
                                
                                # 从策略实例的 _saved_technical_signals 获取技术指标信号
                                if hasattr(strategy, '_saved_technical_signals') and strategy._saved_technical_signals:
                                    logger.info(f"从策略 {strategy.id} 的保存信号中获取到 {len(strategy._saved_technical_signals)} 个技术指标信号")
                                    self.technical_signals.extend(strategy._saved_technical_signals)
                                else:
                                    logger.warning(f"策略 {strategy.id} 没有保存的技术指标信号")
                                
                                # 获取极值点数据
                                if hasattr(strategy, '_saved_extremes') and strategy._saved_extremes:
                                    logger.info(f"从策略 {strategy.id} 的保存极值点中获取到极值点数据")
                                    self.extremes_data = strategy._saved_extremes
                                else:
                                    logger.warning(f"策略 {strategy.id} 没有保存的极值点数据")
                        else:
                            logger.warning("引擎没有trader属性")
                            
                except Exception as e:
                    logger.warning(f"从 BacktestNode 获取策略交易信号失败: {e}")
                    import traceback
                    logger.warning(f"详细错误: {traceback.format_exc()}")
            
            
            logger.info(f"最终收集到 {len(self.trade_signals)} 个交易信号, {len(self.technical_signals)} 个技术指标信号")
            
            # 添加详细的信号统计信息
            if self.trade_signals:
                buy_count = sum(1 for s in self.trade_signals if s.get('side') == 'BUY')
                sell_count = sum(1 for s in self.trade_signals if s.get('side') == 'SELL')
                
                logger.info("=" * 60)
                logger.info("交易信号详细统计")
                logger.info("=" * 60)
                logger.info(f"买入交易: {buy_count} 个")
                logger.info(f"卖出交易: {sell_count} 个")
                logger.info(f"总计: {len(self.trade_signals)} 个")
                
                # 显示前几个交易信号的详细信息
                logger.info("\n前5个交易信号详情:")
                for i, signal in enumerate(self.trade_signals[:5]):
                    logger.info(f"交易信号 {i+1}: {signal}")
                
                if len(self.trade_signals) > 5:
                    logger.info(f"... 还有 {len(self.trade_signals) - 5} 个交易信号")
                logger.info("=" * 60)
            
            # 添加技术指标信号统计信息
            if self.technical_signals:
                golden_cross_count = sum(1 for s in self.technical_signals if s.get('signal_type') == 'golden_cross')
                death_cross_count = sum(1 for s in self.technical_signals if s.get('signal_type') == 'death_cross')
                top_divergence_count = sum(1 for s in self.technical_signals if s.get('signal_type') == 'top_divergence')
                bottom_divergence_count = sum(1 for s in self.technical_signals if s.get('signal_type') == 'bottom_divergence')
                
                logger.info("=" * 60)
                logger.info("技术指标信号详细统计")
                logger.info("=" * 60)
                logger.info(f"金叉信号: {golden_cross_count} 个")
                logger.info(f"死叉信号: {death_cross_count} 个")
                logger.info(f"顶背离信号: {top_divergence_count} 个")
                logger.info(f"底背离信号: {bottom_divergence_count} 个")
                logger.info(f"总计: {len(self.technical_signals)} 个")
                
                # 显示前几个技术信号的详细信息
                logger.info("\n前5个技术指标信号详情:")
                for i, signal in enumerate(self.technical_signals[:5]):
                    logger.info(f"技术信号 {i+1}: {signal}")
                
                if len(self.technical_signals) > 5:
                    logger.info(f"... 还有 {len(self.technical_signals) - 5} 个技术指标信号")
                logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"收集交易信号失败: {e}")
            import traceback
            logger.error(f"详细错误: {traceback.format_exc()}")

    def display_backtest_chart(self, start_date: date, end_date: date):
        """显示回测结果K线图"""
        try:
            # 创建 catalog loader
            catalog_loader = ETF159506RedisKlineGenerator(catalog_path=str(self.catalog_path))
            
            # 显示包含买卖点的K线图
            # 生成图片文件名
            image_filename = f"etf_159506_backtest_{start_date.strftime('%Y%m%d')}.png"
            catalog_loader.create_realtime_kline_chart(
                save_path=image_filename,  # 保存图片文件
                auto_refresh=False,  # 不自动刷新
                target_date=start_date,
                trade_signals=self.trade_signals,
                technical_signals=self.technical_signals,
            )
            
            logger.info("回测结果K线图已显示")
            
            # 显示专门的买卖点图表
            trade_points_filename = f"etf_159506_trade_points_{start_date.strftime('%Y%m%d')}.png"
            catalog_loader.create_trade_points_chart(
                save_path=trade_points_filename,  # 保存图片文件
                target_date=start_date,
                trade_signals=self.trade_signals
            )
            
            logger.info("买卖点分析图表已显示")
            
            # 显示专门的极值点图表
            extremes_filename = f"etf_159506_extremes_{start_date.strftime('%Y%m%d')}.png"
            catalog_loader.create_extremes_chart(
                save_path=extremes_filename,  # 保存图片文件
                target_date=start_date,
                extremes_data=self.extremes_data
            )
            
            logger.info("极值点分析图表已显示")
            
            # 等待用户查看图表
            import matplotlib.pyplot as plt
            plt.show(block=True)
            
        except Exception as e:
            logger.error(f"显示回测图表失败: {e}")

    def create_backtest_config(self, start_date: date, end_date: date) -> BacktestRunConfig:
        """创建回测配置"""
        try:
            # 使用已创建的 bar_type 实例
            bar_spec = self.bar_type
            
            # 数据配置 - 使用1分钟K线数据
            data_config = BacktestDataConfig(
                catalog_path=str(self.catalog_path),
                data_cls="nautilus_trader.model.data:Bar",
                instrument_id=self.instrument_id,
                start_time=datetime.combine(start_date, datetime.min.time()),
                end_time=datetime.combine(end_date, datetime.max.time()),
                bar_spec=bar_spec,  # 指定使用1分钟K线
            )
            
            # 交易场所配置
            venue_config = BacktestVenueConfig(
                name="SZSE",
                oms_type="NETTING",
                account_type="MARGIN",
                base_currency="CNY",
                starting_balances=["230000 CNY"]  # 恢复初始资金，策略逻辑处理满仓状态
            )
            
            # 引擎配置
            engine_config = BacktestEngineConfig(
                strategies=[
                    ImportableStrategyConfig(
                        strategy_path="etf_159506_realtime_strategy:ETF159506Strategy",
                        config_path="etf_159506_strategy_config:ETF159506Config",
                        config={
                            "instrument_id": str(self.instrument_id),
                            "bar_type": str(bar_spec),
                            "venue": "SZSE",
                            "trade_size": 0,  # 设置为0表示满仓交易
                            "fast_ema_period": 12,
                            "slow_ema_period": 26,
                            "volume_threshold": 500000,
                            "stop_loss_pct": 0.02,  # 修复：2%止损
                            "take_profit_pct": 0.25,  # 修复：25%止盈
                            "max_daily_trades": 100,
                            "lookback_period": 2,  # 修复：需要至少2个数据点
                            "price_threshold": 0.001,
                            "emulation_trigger": "NO_TRIGGER",
                            "initial_position_quantity": 0,  # 添加：初始持仓数量
                            # 背离检测参数
                            "dea_trend_period": 3,
                            "advance_trading_bars": 1,
                            "confirmation_bars": 1,
                        },
                    )
                ],
                logging=LoggingConfig(
                    log_level="INFO",
                    log_file_name="etf_159506_backtest.log",  # 根目录
                    log_file_format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                    log_file_max_size=10485760,  # 10MB
                    log_file_max_backup_count=5,
                    log_colors=True,
                    bypass_logging=False,
                    print_config=True,  # 打印配置信息
                ),
            )
            
            # 回测运行配置
            run_config = BacktestRunConfig(
                engine=engine_config,
                venues=[venue_config],
                data=[data_config],
                dispose_on_completion=False,  # 不销毁引擎，以便获取策略实例
            )
            
            return run_config
            
        except Exception as e:
            logger.error(f"创建回测配置失败: {e}")
            raise
    
    def run_backtest(self, start_date: date, end_date: date) -> tuple[BacktestResult, BacktestNode]:
        """运行回测"""
        try:
            logger.info(f"开始回测: {start_date} 到 {end_date}")
            
            # 加载指定日期的所有 TradeTick 数据文件
            self._load_data_for_date(start_date)
            
            # 创建回测配置
            logger.info("创建回测配置...")
            run_config = self.create_backtest_config(start_date, end_date)
            logger.info("回测配置创建完成")
            
            # 创建回测节点
            logger.info("创建回测节点...")
            backtest_node = BacktestNode(configs=[run_config])
            logger.info("回测节点创建完成")
            
            # 运行回测
            logger.info("开始运行回测...")
            results = backtest_node.run()
            logger.info("回测运行完成")
            
            if not results:
                logger.error("回测没有返回结果")
                raise RuntimeError("回测没有返回结果")
            
            result = results[0]
            logger.info(f"回测完成: {result.run_id}")
            
            # 输出回测统计信息
            logger.info("=" * 60)
            logger.info("回测统计信息")
            logger.info("=" * 60)
            logger.info(f"回测ID: {result.run_id}")
            logger.info(f"开始时间: {result.backtest_start}")
            logger.info(f"结束时间: {result.backtest_end}")
            logger.info(f"运行时间: {result.elapsed_time:.2f} 秒")
            logger.info(f"总事件数: {result.total_events}")
            logger.info(f"总订单数: {result.total_orders}")
            logger.info(f"总持仓数: {result.total_positions}")
            logger.info("=" * 60)
            
            return result, backtest_node
            
        except Exception as e:
            logger.error(f"运行回测失败: {e}")
            import traceback
            logger.error(f"详细错误: {traceback.format_exc()}")
            raise
    
    def analyze_results(self, result: BacktestResult):
        """分析回测结果"""
        try:
            logger.info("=" * 60)
            logger.info("回测结果分析")
            logger.info("=" * 60)
            
            # 基本信息
            logger.info(f"回测ID: {result.run_id}")
            logger.info(f"开始时间: {result.backtest_start}")
            logger.info(f"结束时间: {result.backtest_end}")
            logger.info(f"运行时间: {result.elapsed_time:.2f} 秒")
            logger.info(f"总事件数: {result.total_events}")
            logger.info(f"总订单数: {result.total_orders}")
            logger.info(f"总持仓数: {result.total_positions}")
            
            # 性能指标
            if hasattr(result, 'stats_pnls') and result.stats_pnls:
                for venue, pnl_stats in result.stats_pnls.items():
                    logger.info(f"\n{venue} 性能指标:")
                    for metric, value in pnl_stats.items():
                        if isinstance(value, (int, float)):
                            logger.info(f"  {metric}: {value:.4f}")
                        else:
                            logger.info(f"  {metric}: {value}")
            else:
                logger.info("未找到PnL统计数据")
            
            if hasattr(result, 'stats_returns') and result.stats_returns:
                logger.info(f"\n收益率统计:")
                for metric, value in result.stats_returns.items():
                    if isinstance(value, (int, float)):
                        logger.info(f"  {metric}: {value:.4f}")
                    else:
                        logger.info(f"  {metric}: {value}")
            else:
                logger.info("未找到收益率统计数据")
            
            # 输出账户信息
            if hasattr(result, 'account') and result.account:
                logger.info(f"\n账户信息:")
                logger.info(f"  账户ID: {result.account.id}")
                logger.info(f"  账户类型: {result.account.type}")
                logger.info(f"  基础货币: {result.account.base_currency}")
                if hasattr(result.account, 'balances') and result.account.balances:
                    for balance in result.account.balances:
                        logger.info(f"  余额: {balance}")
            
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"分析结果失败: {e}")
            import traceback
            logger.error(f"详细错误: {traceback.format_exc()}")
    
    def run_july_25_backtest(self):
        """运行 7-25 日的回测"""
        try:
            # 设置回测日期
            start_date = date(2025, 12, 24)
            end_date = date(2025, 12, 24)
            
            logger.info(f"开始 7-25 日回测...")
            
            # 运行回测
            result, backtest_node = self.run_backtest(start_date, end_date)
            
            # 收集交易信号
            self.collect_trade_signals(result, backtest_node)
            
            # 分析结果
            self.analyze_results(result)
            
            # 注意：图表已由策略在 on_stop 时绘制（带日期后缀）
            # 回测系统不再重复绘制，避免从 catalog 读取数据的问题
            logger.info("图表已由策略在回测结束时自动绘制（带日期后缀）")
            logger.info(f"请查看以下文件：")
            date_str = start_date.strftime('%Y%m%d')
            logger.info(f"  - etf_159506_backtest_kline_{date_str}.png")
            logger.info(f"  - etf_159506_backtest_trade_points_{date_str}.png")
            logger.info(f"  - etf_159506_backtest_extremes_{date_str}.png")
            
            return result
            
        except Exception as e:
            logger.error(f"7-25 日回测失败: {e}")
            raise


def main():
    """主函数"""
    try:
        logger.info("159506 ETF 官方回测系统启动")
        
        # 创建回测系统
        backtest_system = ETF159506OfficialBacktest()
        
        # 运行 7-25 日回测
        result = backtest_system.run_july_25_backtest()
        
        logger.info("回测完成")
        
    except Exception as e:
        logger.error(f"回测系统运行失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main() 