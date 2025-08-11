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
from nautilus_trader.model.data import BarType

# 导入策略和工具
from etf_159506_strategy import ETF159506Strategy
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
    
    def __init__(self, catalog_path: str = "catalog/etf_159506_cache"):
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
        
        # 初始化 catalog
        self._init_catalog()
        
    def _init_catalog(self):
        """初始化数据 catalog"""
        try:
            if not self.catalog_path.exists():
                raise FileNotFoundError(f"Catalog 路径不存在: {self.catalog_path}")
            
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
            self._load_data_with_catalog_loader()
            
        except Exception as e:
            logger.error(f"初始化 Catalog 失败: {e}")
            raise
    
    def _load_data_with_catalog_loader(self):
        """使用 catalog loader 加载数据"""
        try:
            # 创建 catalog loader
            catalog_loader = ETF159506RedisKlineGenerator(catalog_path=str(self.catalog_path))
            
            # 加载 2025-07-25 的数据
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
                        else:
                            logger.warning("引擎没有trader属性")
                            
                except Exception as e:
                    logger.warning(f"从 BacktestNode 获取策略交易信号失败: {e}")
                    import traceback
                    logger.warning(f"详细错误: {traceback.format_exc()}")
            
            
            logger.info(f"最终收集到 {len(self.trade_signals)} 个交易信号")
            
            # 添加详细的信号统计信息
            if self.trade_signals:
                buy_count = sum(1 for s in self.trade_signals if s.get('side') == 'BUY')
                sell_count = sum(1 for s in self.trade_signals if s.get('side') == 'SELL')
                hold_count = sum(1 for s in self.trade_signals if s.get('side') == 'HOLD')
                watch_count = sum(1 for s in self.trade_signals if s.get('side') == 'WATCH')
                
                logger.info("=" * 60)
                logger.info("交易信号详细统计")
                logger.info("=" * 60)
                logger.info(f"买入信号: {buy_count} 个")
                logger.info(f"卖出信号: {sell_count} 个")
                logger.info(f"持有信号: {hold_count} 个")
                logger.info(f"观望信号: {watch_count} 个")
                logger.info(f"总计: {len(self.trade_signals)} 个")
                
                # 显示前几个信号的详细信息
                logger.info("\n前5个交易信号详情:")
                for i, signal in enumerate(self.trade_signals[:5]):
                    logger.info(f"信号 {i+1}: {signal}")
                
                if len(self.trade_signals) > 5:
                    logger.info(f"... 还有 {len(self.trade_signals) - 5} 个信号")
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
                trade_signals=self.trade_signals
            )
            
            logger.info("回测结果K线图已显示")
            
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
                        strategy_path="etf_159506_strategy:ETF159506Strategy",
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
                            "take_profit_pct": 0.05,  # 修复：5%止盈
                            "max_daily_trades": 100,
                            "lookback_period": 2,  # 修复：需要至少2个数据点
                            "price_threshold": 0.001,
                            "emulation_trigger": "NO_TRIGGER",
                            "initial_position_quantity": 0,  # 添加：初始持仓数量
                            # 背离检测参数
                            "dea_trend_period": 3,
                            "divergence_threshold": 0.0001,
                            "advance_trading_bars": 1,
                            "confirmation_bars": 1,
                            "max_divergence_duration": 8,
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
            start_date = date(2025, 7, 25)
            end_date = date(2025, 7, 25)
            
            logger.info(f"开始 7-25 日回测...")
            
            # 运行回测
            result, backtest_node = self.run_backtest(start_date, end_date)
            
            # 收集交易信号
            self.collect_trade_signals(result, backtest_node)
            
            # 分析结果
            self.analyze_results(result)
            
            # 显示回测结果K线图
            self.display_backtest_chart(start_date, end_date)
            
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