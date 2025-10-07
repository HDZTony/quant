#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
159506 ETF 实时交易主程序
基于NautilusTrader TradingNode架构
"""

import asyncio
import logging
import signal
import sys
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from pathlib import Path

from nautilus_trader.live.node import TradingNode
from nautilus_trader.config import LoggingConfig, TradingNodeConfig
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.config import ImportableStrategyConfig

# 导入配置和适配器
from etf_159506_live_config import ETF159506LiveConfig, create_default_live_config
from etf_159506_adapter import ETF159506Adapter

# 导入策略
from etf_159506_realtime_strategy import ETF159506Strategy
from etf_159506_strategy_config import ETF159506Config

# 导入数据保存器
from nautilus_trader.model.identifiers import InstrumentId, Venue
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import Bar
from nautilus_trader.model.objects import Price, Quantity
from decimal import Decimal
from collections import deque

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etf_159506_live_trading.log', mode='w', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ],
    force=True
)
logger = logging.getLogger(__name__)


class ETF159506LiveTradingSystem:
    """159506 ETF 实时交易系统"""
    
    def __init__(
        self,
        config: Optional[ETF159506LiveConfig] = None,
        adapter_config: Optional[Dict[str, Any]] = None,
    ):
        """
        初始化实时交易系统
        
        Parameters
        ----------
        config : Optional[ETF159506LiveConfig]
            主配置
        adapter_config : Optional[Dict[str, Any]]
            适配器配置（包含token等）
        """
        self.config = config or create_default_live_config()
        self.adapter_config = adapter_config or {
            'token': 'd0c519adcd47d266f1c96750d4e80aa6',  # 使用您adapter中的token
            'stock_code': '159506'
        }
        
        # 创建适配器
        self.adapter = ETF159506Adapter(self.adapter_config)
        
        # 创建TradingNode配置
        self.trading_node_config = self._create_trading_node_config()
        
        # K线数据生成器
        self.current_bar = None
        self.bar_start_time = None
        self.bar_duration = 60  # 1分钟K线
        self.bar_count = 0
        
        # 交易节点
        self.trading_node: Optional[TradingNode] = None
        
     
        
        # 运行状态
        self.is_running = False
        self.shutdown_event = asyncio.Event()
        
        logger.info(f"ETF159506实时交易系统初始化: trader_id={self.config.trader_id}")
    
    def _create_trading_node_config(self) -> TradingNodeConfig:
        """创建TradingNode配置 - 参考官方回测配置"""
        try:
            # 使用与回测相同的工具和BarType创建方式
            from etf_159506_instrument import create_etf_159506_default, create_etf_159506_bar_type
            from nautilus_trader.model.data import BarType
            
            # 创建工具和BarType（与回测保持一致）
            instrument = create_etf_159506_default()
            bar_type_str = create_etf_159506_bar_type()
            self.bar_type = BarType.from_str(bar_type_str)
            
            # 创建策略配置（与回测保持相同的参数）
            strategy_config = ImportableStrategyConfig(
                strategy_path="etf_159506_realtime_strategy:ETF159506Strategy",
                config_path="etf_159506_strategy_config:ETF159506Config",
                config={
                    "instrument_id": str(instrument.id),
                    "bar_type": str(self.bar_type),
                    "venue": "SZSE",
                    "trade_size": 10000,  # 每次交易10000股
                    "fast_ema_period": 12,
                    "slow_ema_period": 26,
                    "volume_threshold": 500000,
                    "stop_loss_pct": 0.02,
                    "take_profit_pct": 0.25,
                    "max_daily_trades": 100,
                    "lookback_period": 2,
                    "price_threshold": 0.001,
                    "emulation_trigger": "NO_TRIGGER",
                    "initial_position_quantity": 0,
                    # 背离检测参数
                    "dea_trend_period": 3,
                    "advance_trading_bars": 1,
                    "confirmation_bars": 1,
                }
            )
            
            # 创建数据客户端配置
            from etf_159506_data_client_config import create_etf_159506_data_client_config, create_etf_159506_exec_client_config
            from nautilus_trader.persistence.config import DataCatalogConfig
            from nautilus_trader.config import CacheConfig, DatabaseConfig
            from nautilus_trader.config import DatabaseConfig

            cache_config = CacheConfig(
                database=DatabaseConfig(
                type="redis", # Database type
                host="localhost", # Database host
                port=6379, # Database port
                timeout=2, # Connection timeout (seconds)
                )
            )
             
            # Configure catalog for live system
            catalog_config = DataCatalogConfig(
                path="data_catalog",
                fs_protocol="file",
                name="historical_data"
            )
            
            data_client_config = create_etf_159506_data_client_config()
            exec_client_config = create_etf_159506_exec_client_config()
            # 创建TradingNode配置
            config = TradingNodeConfig(
                trader_id=TraderId("ETF159506-LIVE-001"),
                cache=cache_config,
                strategies=[strategy_config],
                data_clients={"ETF159506": data_client_config},
                exec_clients={"ETF159506": exec_client_config},
                catalogs=[catalog_config],
                logging=LoggingConfig(
                    log_level="INFO",
                    log_file_name="etf_159506_live_trading.log",
                    log_file_format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                    log_file_max_size=10485760,  # 10MB
                    log_file_max_backup_count=5,
                    log_colors=True,
                    bypass_logging=False,
                    print_config=True,
                ),
            )
            
            logger.info("TradingNode配置创建成功（使用与回测相同的配置）")
            return config
            
        except Exception as e:
            logger.error(f"创建TradingNode配置失败: {e}")
            raise
    
    async def start(self) -> None:
        """启动实时交易系统"""
        try:
            logger.info("启动ETF159506实时交易系统...")
            
            # 验证配置
            if not self.config.validate_config():
                raise ValueError("配置验证失败")
            
            # 连接适配器
            logger.info("连接ETF159506适配器...")
            if not await self.adapter.connect():
                raise RuntimeError("适配器连接失败")
            
            logger.info("适配器连接成功")
            
            # 获取适配器状态
            status = await self.adapter.get_status()
            logger.info(f"适配器状态: {status}")
            
            # 设置全局适配器实例（在创建TradingNode之前设置）
            logger.info("设置全局适配器实例...")
            from etf_159506_adapter import set_global_adapter, get_global_adapter
            set_global_adapter(self.adapter)
            # 验证全局适配器是否正确设置
            test_adapter = get_global_adapter()
            logger.info(f"全局适配器设置完成，验证: {test_adapter is not None}, 连接状态: {test_adapter.is_connected if test_adapter else 'N/A'}")
            
            # 创建TradingNode
            logger.info("创建TradingNode...")
            self.trading_node = TradingNode(config=self.trading_node_config)
            
            # 注册数据客户端工厂
            logger.info("注册数据客户端工厂...")
            from etf_159506_adapter import ETF159506LiveDataClientFactory
            self.trading_node.add_data_client_factory("ETF159506", ETF159506LiveDataClientFactory)
            
            # 注册执行客户端工厂
            logger.info("注册执行客户端工厂...")
            from etf_159506_adapter import ETF159506LiveExecClientFactory
            self.trading_node.add_exec_client_factory("ETF159506", ETF159506LiveExecClientFactory)
            
            # 构建TradingNode（必须先调用build()）
            logger.info("构建TradingNode...")
            self.trading_node.build()
            
            # 启动TradingNode（这会自动启动策略）
            logger.info("启动TradingNode...")
            # TradingNode使用run()方法启动，这是一个阻塞调用
            # 我们需要在单独的线程中运行它
            import threading
            self.trading_node_thread = threading.Thread(target=self.trading_node.run)
            self.trading_node_thread.start()
            
            self.is_running = True
            logger.info("ETF159506实时交易系统启动成功")
            
            # 等待关闭信号
            await self.shutdown_event.wait()
                    
        except Exception as e:
            logger.error(f"启动ETF159506实时交易系统失败: {e}")
            raise
    
    # 注意：TradingNode会自动处理策略的启动、数据订阅和执行
    # 我们不需要手动处理这些，因为TradingNode会管理整个策略生命周期
    
    async def stop(self) -> None:
        """停止实时交易系统"""
        try:
            logger.info("停止ETF159506实时交易系统...")
            
            self.is_running = False
            
            # 停止TradingNode
            if self.trading_node:
                logger.info("停止TradingNode...")
                self.trading_node.stop()
                # 等待线程结束
                if hasattr(self, 'trading_node_thread'):
                    self.trading_node_thread.join(timeout=10)
            
            # 断开适配器连接
            if self.adapter:
                await self.adapter.disconnect()
            
            # 设置关闭事件
            self.shutdown_event.set()
            
            logger.info("ETF159506实时交易系统停止成功")
                
        except Exception as e:
            logger.error(f"停止ETF159506实时交易系统失败: {e}")
    
    async def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态"""
        try:
            status = {
                "is_running": self.is_running,
                "trader_id": self.config.trader_id,
                "instrument_id": str(self.config.instrument_id),
                "bar_type": str(self.config.bar_type),
                "testnet": self.config.testnet,
                "starting_balance": self.config.starting_balance,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            
            # 添加适配器状态
            if self.adapter:
                adapter_status = await self.adapter.get_status()
                status.update(adapter_status)
            
            # 添加TradingNode状态
            if self.trading_node:
                status['trading_node_running'] = True
            
            return status
            
        except Exception as e:
            logger.error(f"获取系统状态失败: {e}")
            return {"error": str(e)}
    
    def print_system_info(self) -> None:
        """打印系统信息"""
        try:
            logger.info("=" * 80)
            logger.info("ETF159506 实时交易系统信息")
            logger.info("=" * 80)
            
            # 基本配置信息
            logger.info(f"交易者ID: {self.config.trader_id}")
            logger.info(f"工具ID: {self.config.instrument_id}")
            logger.info(f"K线类型: {self.config.bar_type}")
            logger.info(f"测试网络: {self.config.testnet}")
            logger.info(f"初始资金: {self.config.starting_balance}")
            
            # Redis配置信息
            logger.info(f"Redis主机: {self.config.redis_host}:{self.config.redis_port}")
            logger.info(f"Redis数据库: {self.config.redis_db}")
            
            # 适配器配置信息
            logger.info(f"适配器Token: {self.adapter_config.get('token', 'N/A')}")
            logger.info(f"股票代码: {self.adapter_config.get('stock_code', 'N/A')}")
            
            # 工具信息
            instrument_info = self.config.get_instrument_info()
            logger.info("工具详细信息:")
            for key, value in instrument_info.items():
                logger.info(f"  {key}: {value}")
            
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"打印系统信息失败: {e}")


class LiveTradingManager:
    """实时交易管理器"""
    
    def __init__(self):
        self.trading_system: Optional[ETF159506LiveTradingSystem] = None
        self.shutdown_signals = [signal.SIGINT, signal.SIGTERM]
        self._setup_signal_handlers()
    
    def _setup_signal_handlers(self) -> None:
        """设置信号处理器"""
        def signal_handler(signum, frame):
            logger.info(f"收到信号 {signum}，开始关闭系统...")
            if self.trading_system:
                asyncio.create_task(self.trading_system.stop())
        
        for sig in self.shutdown_signals:
            signal.signal(sig, signal_handler)
    
    async def run(
        self,
        config: Optional[ETF159506LiveConfig] = None,
        adapter_config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """运行实时交易系统"""
        try:
            logger.info("启动实时交易管理器...")
            
            # 创建交易系统
            self.trading_system = ETF159506LiveTradingSystem(
                config=config,
                adapter_config=adapter_config,
            )
            
            # 打印系统信息
            self.trading_system.print_system_info()
            
            # 启动系统
            await self.trading_system.start()
            
        except KeyboardInterrupt:
            logger.info("收到键盘中断信号，正在关闭系统...")
        except Exception as e:
            logger.error(f"运行实时交易系统失败: {e}")
            raise
        finally:
            if self.trading_system:
                await self.trading_system.stop()


def create_testnet_system() -> ETF159506LiveTradingSystem:
    """创建测试网络交易系统"""
    from etf_159506_live_config import create_testnet_live_config
    
    config = create_testnet_live_config()
    adapter_config = {
        'token': 'd0c519adcd47d266f1c96750d4e80aa6',
        'stock_code': '159506'
    }
    
    return ETF159506LiveTradingSystem(
        config=config,
        adapter_config=adapter_config,
    )


def create_production_system() -> ETF159506LiveTradingSystem:
    """创建生产环境交易系统"""
    from etf_159506_live_config import create_production_live_config
    
    config = create_production_live_config()
    adapter_config = {
        'token': 'd0c519adcd47d266f1c96750d4e80aa6',  # 生产环境token
        'stock_code': '159506'
    }
    
    return ETF159506LiveTradingSystem(
        config=config,
        adapter_config=adapter_config,
    )


async def main():
    """主函数"""
    try:
        logger.info("ETF159506 实时交易系统启动")
        logger.info("=" * 60)
        
        # 创建交易管理器
        manager = LiveTradingManager()
        
        # 选择运行模式
        import argparse
        parser = argparse.ArgumentParser(description='ETF159506 实时交易系统')
        parser.add_argument('--mode', choices=['testnet', 'production'], default='testnet',
                          help='运行模式: testnet (测试网络) 或 production (生产环境)')
        parser.add_argument('--config', type=str, help='配置文件路径')
        
        args = parser.parse_args()
        
        logger.info(f"运行模式: {args.mode}")
        
        # 根据模式创建系统
        if args.mode == 'testnet':
            logger.info("创建测试网络交易系统...")
            config = None
            adapter_config = {
                'token': 'd0c519adcd47d266f1c96750d4e80aa6',
                'stock_code': '159506'
            }
        else:
            logger.info("创建生产环境交易系统...")
            config = None
            adapter_config = {
                'token': 'd0c519adcd47d266f1c96750d4e80aa6',  # 生产环境token
                'stock_code': '159506'
            }
        
        # 运行系统
        await manager.run(
            config=config,
            adapter_config=adapter_config,
        )
        
        logger.info("ETF159506 实时交易系统已关闭")
        
    except Exception as e:
        logger.error(f"主函数执行失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    # 运行主函数
    asyncio.run(main())