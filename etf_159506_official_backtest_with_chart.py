#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
159506 ETF 官方 NautilusTrader 回测系统（带K线图显示）
使用官方架构进行生产级回测，并在回测完成后显示买卖点标记的K线图
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
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etf_159506_backtest.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False


class ETF159506OfficialBacktestWithChart:
    """159506 ETF 官方回测系统（带K线图显示）"""
    
    def __init__(self, catalog_path: str = "catalog/etf_159506_cache"):
        self.catalog_path = Path(catalog_path)
        self.catalog = None
        
        # 使用 etf_159506_instrument.py 中的工具定义
        self.instrument = create_etf_159506_default()
        self.instrument_id = self.instrument.id
        
        # 创建BarType
        bar_type_str = create_etf_159506_bar_type()
        self.bar_type = BarType.from_str(bar_type_str)
        
        # 交易信号数据存储
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
            # 使用 catalog loader 获取数据
            kline_generator = ETF159506RedisKlineGenerator(catalog_path=str(self.catalog_path))
            
            # 获取今天的数据
            today_data = kline_generator.get_today_kline_data()
            logger.info(f"从 catalog loader 获取到 {len(today_data)} 条K线数据")
            
            if today_data:
                # 转换并写入 catalog
                self._convert_and_write_data(today_data)
                logger.info("数据已写入 catalog")
            else:
                logger.warning("没有获取到数据")
                
        except Exception as e:
            logger.error(f"使用 catalog loader 加载数据失败: {e}")
            raise

    def _convert_and_write_data(self, kline_data):
        """转换K线数据并写入 catalog"""
        try:
            from nautilus_trader.model.data import Bar
            from nautilus_trader.model.objects import Price, Quantity
            from nautilus_trader.core.uuid import UUID4
            
            bars = []
            
            for kline in kline_data:
                try:
                    # 解析时间戳
                    if isinstance(kline['timestamp'], str):
                        ts = pd.to_datetime(kline['timestamp'])
                    else:
                        ts = kline['timestamp']
                    
                    # 转换为纳秒时间戳
                    ts_ns = int(ts.timestamp() * 1_000_000_000)
                    
                    # 创建 Bar 对象
                    bar = Bar(
                        bar_type=self.bar_type,
                        open=Price.from_str(str(kline['open'])),
                        high=Price.from_str(str(kline['high'])),
                        low=Price.from_str(str(kline['low'])),
                        close=Price.from_str(str(kline['close'])),
                        volume=Quantity.from_int(int(kline['volume'])),
                        ts_event=ts_ns,
                        ts_init=ts_ns,
                    )
                    
                    bars.append(bar)
                    
                except Exception as e:
                    logger.warning(f"转换K线数据失败: {e}, 数据: {kline}")
                    continue
            
            if bars:
                # 写入 catalog
                self.catalog.write_data(bars)
                logger.info(f"成功写入 {len(bars)} 条K线数据到 catalog")
            
        except Exception as e:
            logger.error(f"转换和写入数据失败: {e}")
            raise

    def create_backtest_config(self, start_date: date, end_date: date) -> BacktestRunConfig:
        """创建回测配置"""
        try:
            # 创建数据配置
            data_config = BacktestDataConfig(
                catalog_path=str(self.catalog_path),
                data_clients={
                    "SZSE": BacktestVenueConfig(
                        venue=Venue("SZSE"),
                        oms_type=OmsType.NETTING,
                        account_type=AccountType.MARGIN,
                        starting_balances=[Money(230_000, CNY)],
                    )
                }
            )
            
            # 创建引擎配置
            engine_config = BacktestEngineConfig(
                trader_id=TraderId("BACKTESTER-001"),
                log_level="INFO",
            )
            
            # 创建策略配置
            strategy_config = ETF159506Config(
                instrument_id=self.instrument_id,
                bar_type=self.bar_type,
                venue=Venue("SZSE"),
                trade_size=Decimal("1000"),
                fast_ema_period=10,
                slow_ema_period=20,
                stop_loss_pct=0.02,
                take_profit_pct=0.015,
                confirmation_bars=1,  # 减少确认K线数，提高灵敏度
            )
            
            # 创建回测运行配置
            run_config = BacktestRunConfig(
                engine_config=engine_config,
                data_config=data_config,
                strategies=[ImportableStrategyConfig(
                    strategy_path="etf_159506_strategy:ETF159506Strategy",
                    config_path="etf_159506_strategy_config:ETF159506Config",
                    config=dict(strategy_config),
                )],
                start=start_date,
                end=end_date,
            )
            
            logger.info(f"回测配置创建成功: {start_date} 到 {end_date}")
            return run_config
            
        except Exception as e:
            logger.error(f"创建回测配置失败: {e}")
            raise

    def run_backtest(self, start_date: date, end_date: date) -> BacktestResult:
        """运行回测"""
        try:
            # 创建回测配置
            config = self.create_backtest_config(start_date, end_date)
            
            # 创建回测节点
            node = BacktestNode(configs=[config])
            
            # 运行回测
            logger.info("开始运行回测...")
            result = node.run()
            
            if result is None:
                raise RuntimeError("回测没有返回结果")
            
            logger.info("回测完成")
            return result
            
        except Exception as e:
            logger.error(f"运行回测失败: {e}")
            raise

    def collect_trade_signals(self, result: BacktestResult):
        """收集交易信号数据"""
        try:
            # 从回测结果中提取交易信号
            for order in result.orders:
                if order.status.name == "FILLED":
                    signal = {
                        'timestamp': pd.to_datetime(order.ts_init, unit='ns'),
                        'price': float(order.last_px),
                        'side': order.side.name,  # BUY or SELL
                        'quantity': float(order.last_qty),
                        'order_id': str(order.client_order_id)
                    }
                    self.trade_signals.append(signal)
            
            logger.info(f"收集到 {len(self.trade_signals)} 个交易信号")
            
        except Exception as e:
            logger.error(f"收集交易信号失败: {e}")

    def plot_backtest_results(self, start_date: date, end_date: date):
        """绘制回测结果K线图"""
        try:
            # 获取K线数据
            kline_generator = ETF159506RedisKlineGenerator(catalog_path=str(self.catalog_path))
            kline_data = kline_generator.get_kline_data(limit=1000)
            
            if not kline_data:
                logger.warning("没有获取到K线数据")
                return
            
            # 转换为DataFrame
            df = pd.DataFrame(kline_data)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.set_index('timestamp')
            
            # 过滤日期范围
            start_dt = pd.Timestamp(start_date)
            end_dt = pd.Timestamp(end_date) + pd.Timedelta(days=1)
            df = df[(df.index >= start_dt) & (df.index < end_dt)]
            
            if df.empty:
                logger.warning("指定日期范围内没有K线数据")
                return
            
            # 准备买卖点数据
            buy_signals = []
            sell_signals = []
            
            for signal in self.trade_signals:
                if signal['side'] == 'BUY':
                    buy_signals.append({
                        'timestamp': signal['timestamp'],
                        'price': signal['price']
                    })
                elif signal['side'] == 'SELL':
                    sell_signals.append({
                        'timestamp': signal['timestamp'],
                        'price': signal['price']
                    })
            
            # 创建K线图
            fig, axes = plt.subplots(2, 1, figsize=(15, 10), height_ratios=[3, 1])
            
            # 主图：K线 + 买卖点
            ax1 = axes[0]
            
            # 绘制K线图
            mpf.plot(df, type='candle', style='charles', ax=ax1, volume=False, 
                    title=f'159506 ETF 回测结果 ({start_date} 到 {end_date})')
            
            # 添加买卖点标记
            if buy_signals:
                buy_df = pd.DataFrame(buy_signals)
                ax1.scatter(buy_df['timestamp'], buy_df['price'], 
                           color='red', marker='^', s=100, label='买入信号', zorder=5)
            
            if sell_signals:
                sell_df = pd.DataFrame(sell_signals)
                ax1.scatter(sell_df['timestamp'], sell_df['price'], 
                           color='green', marker='v', s=100, label='卖出信号', zorder=5)
            
            ax1.legend()
            ax1.grid(True, alpha=0.3)
            
            # 副图：成交量
            ax2 = axes[1]
            ax2.bar(df.index, df['volume'], color='blue', alpha=0.7)
            ax2.set_title('成交量')
            ax2.grid(True, alpha=0.3)
            
            # 调整布局
            plt.tight_layout()
            
            # 保存图片
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"etf_159506_backtest_{start_date}_{end_date}_{timestamp}.png"
            plt.savefig(filename, dpi=300, bbox_inches='tight')
            logger.info(f"回测结果图已保存: {filename}")
            
            # 显示图片
            plt.show()
            
        except Exception as e:
            logger.error(f"绘制回测结果失败: {e}")

    def analyze_results(self, result: BacktestResult):
        """分析回测结果"""
        try:
            logger.info("============================================================")
            logger.info("回测结果分析")
            logger.info("============================================================")
            
            # 基本信息
            logger.info(f"回测ID: {result.run_id}")
            logger.info(f"开始时间: {result.start}")
            logger.info(f"结束时间: {result.end}")
            logger.info(f"运行时间: {result.duration_ns / 1_000_000_000:.2f} 秒")
            logger.info(f"总事件数: {result.total_events}")
            logger.info(f"总订单数: {result.total_orders}")
            logger.info(f"总持仓数: {result.total_positions}")
            
            # 性能指标
            if result.portfolio_stats:
                logger.info("\nCNY 性能指标:")
                for key, value in result.portfolio_stats.items():
                    if isinstance(value, float):
                        logger.info(f"  {key}: {value:.4f}")
                    else:
                        logger.info(f"  {key}: {value}")
            
            # 收益率统计
            if result.returns_stats:
                logger.info("\n收益率统计:")
                for key, value in result.returns_stats.items():
                    if isinstance(value, float):
                        logger.info(f"  {key}: {value:.4f}")
                    else:
                        logger.info(f"  {key}: {value}")
            
            logger.info("============================================================")
            
        except Exception as e:
            logger.error(f"分析回测结果失败: {e}")

    def run_july_25_backtest(self):
        """运行7月25日的回测"""
        try:
            # 设置回测日期
            start_date = date(2025, 7, 25)
            end_date = date(2025, 7, 25)
            
            logger.info(f"开始运行 {start_date} 的回测...")
            
            # 运行回测
            result = self.run_backtest(start_date, end_date)
            
            # 收集交易信号
            self.collect_trade_signals(result)
            
            # 分析结果
            self.analyze_results(result)
            
            # 绘制回测结果图
            self.plot_backtest_results(start_date, end_date)
            
            logger.info("回测完成")
            
        except Exception as e:
            logger.error(f"运行7月25日回测失败: {e}")
            raise


def main():
    """主函数"""
    try:
        # 创建回测系统
        backtest = ETF159506OfficialBacktestWithChart()
        
        # 运行回测
        backtest.run_july_25_backtest()
        
    except Exception as e:
        logger.error(f"运行回测失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 