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
from nautilus_trader.config import LoggingConfig, TradingNodeConfig, LiveExecEngineConfig
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.config import ImportableStrategyConfig

# 导入配置和适配器
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
        adapter_config: Optional[Dict[str, Any]] = None,
    ):
        """
        初始化实时交易系统
        
        Parameters
        ----------
        adapter_config : Optional[Dict[str, Any]]
            适配器配置（包含token等）
        """
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
        
        logger.info("ETF159506实时交易系统初始化")
    
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
                    "trade_size": 0,  # 0表示使用全部可用资金买入（全仓）
                    
                    # === 对账配置（官方推荐） ===
                    "external_order_claims": [str(instrument.id)],  # ✅ 认领对账发现的外部订单
                    
                    # === 技术指标参数 ===
                    "fast_ema_period": 12,
                    "slow_ema_period": 26,
                    "volume_threshold": 500000,
                    
                    # === 风险参数 ===
                    "stop_loss_pct": 0.02,
                    "take_profit_pct": 0.25,
                    "max_daily_trades": 100,
                    "lookback_period": 2,
                    "price_threshold": 0.001,
                    "emulation_trigger": "NO_TRIGGER",
                    "initial_position_quantity": 0,
                    
                    # === 背离检测参数 ===
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
            
            # ✅ 创建执行引擎配置（启用对账功能 - 官方推荐配置）
            # 参考: https://nautilustrader.io/docs/latest/concepts/live/#execution-reconciliation
            exec_engine_config = LiveExecEngineConfig(
                # === 启动对账配置 ===
                reconciliation=True,  # 启用对账，启动时同步交易所状态到缓存
                reconciliation_lookback_mins=1440,  # 对账回溯24小时（官方建议不设置None使用最大历史，但24小时更安全）
                reconciliation_instrument_ids=[instrument.id],  # ✅ 关键：指定需要对账的工具
                reconciliation_startup_delay_secs=5.0,  # 启动对账完成后等待5秒再开始连续对账
                
                # === 订单过滤配置 ===
                filter_unclaimed_external_orders=False,  # False: 允许外部订单（如果其他系统也在交易）
                generate_missing_orders=True,  # ✅ 生成缺失订单以对齐持仓差异（默认True，显式设置）
                
                # === 连续对账配置 ===
                # 订单检查
                open_check_interval_secs=5.0,  # ✅ 每5秒检查交易所未成交订单
                open_check_open_only=False,  # False: 检查所有订单历史（不仅限于未成交）
                open_check_lookback_mins=60,  # 检查最近60分钟的订单
                open_check_threshold_ms=5000,  # 订单最后更新超过5秒才检查（防止竞争条件）
                open_check_missing_retries=5,  # ✅ 订单丢失时的重试次数（防止快速解决导致的竞争条件）
                
                # 持仓检查
                position_check_interval_secs=30.0,  # ✅ 每30秒检查持仓差异
                position_check_lookback_mins=60,  # 持仓差异时查询最近60分钟的成交
                position_check_threshold_ms=5000,  # 持仓最后活动超过5秒才检查（防止竞争条件）
                
                # === 订单飞行中检查 ===
                inflight_check_interval_ms=2000,  # 每2秒检查飞行中订单
                inflight_check_threshold_ms=5000,  # 订单飞行超过5秒触发检查
                inflight_check_retries=5,  # 检查失败重试5次
                
                # === 订单簿审计（可选但推荐） ===
                # own_books_audit_interval_secs=300.0,  # 每5分钟审计订单簿（可选，用于检测订单状态不一致）
            )
            
            # 创建TradingNode配置
            config = TradingNodeConfig(
                trader_id=TraderId("ETF159506-LIVE-001"),
                cache=cache_config,
                exec_engine=exec_engine_config,  # ✅ 关键：启用对账功能
                strategies=[strategy_config],
                data_clients={"SZSE": data_client_config},
                exec_clients={"SZSE": exec_client_config},
                catalogs=[catalog_config],
                
                # === 超时配置（官方推荐） ===
                timeout_connection=30.0,  # 连接超时（秒）
                timeout_reconciliation=60.0,  # ✅ 对账超时（秒）- 对账可能需要较长时间
                timeout_portfolio=10.0,  # 投资组合初始化超时（秒）
                timeout_disconnection=10.0,  # 断开连接超时（秒）
                timeout_post_stop=5.0,  # 停止后清理超时（秒）
                
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
    
    async def _connect_adapter_async(self) -> None:
        """异步连接适配器（在 TradingNode 的事件循环中运行）"""
        logger.info("连接ETF159506适配器...")
        if not await self.adapter.connect():
            raise RuntimeError("适配器连接失败")
        logger.info("适配器连接成功")
        
        # 获取适配器状态
        status = await self.adapter.get_status()
        logger.info(f"适配器状态: {status}")
    
    def start_sync(self) -> None:
        """
        同步启动实时交易系统
        
        注意：此方法是阻塞的，会一直运行直到收到停止信号。
        """
        try:
            logger.info("启动ETF159506实时交易系统...")
            
            # 设置全局适配器实例（在创建TradingNode之前设置）
            logger.info("设置全局适配器实例...")
            from etf_159506_adapter import set_global_adapter, get_global_adapter
            set_global_adapter(self.adapter)
            # 验证全局适配器是否正确设置
            test_adapter = get_global_adapter()
            logger.info(f"全局适配器设置完成，验证: {test_adapter is not None}")
            
            # 创建TradingNode
            logger.info("创建TradingNode...")
            self.trading_node = TradingNode(config=self.trading_node_config)
            
            # 注册数据客户端工厂
            logger.info("注册数据客户端工厂...")
            from etf_159506_adapter import ETF159506LiveDataClientFactory
            self.trading_node.add_data_client_factory("SZSE", ETF159506LiveDataClientFactory)
            
            # 注册执行客户端工厂
            logger.info("注册执行客户端工厂...")
            from etf_159506_adapter import ETF159506LiveExecClientFactory
            self.trading_node.add_exec_client_factory("SZSE", ETF159506LiveExecClientFactory)
            
            # 构建TradingNode（必须先调用build()）
            logger.info("构建TradingNode...")
            self.trading_node.build()
            
            # 在 TradingNode 的事件循环中连接适配器
            logger.info("在TradingNode事件循环中连接适配器...")
            self.trading_node.kernel.loop.run_until_complete(self._connect_adapter_async())
            
            self.is_running = True
            logger.info("ETF159506实时交易系统启动成功")
            
            # 启动TradingNode（这是一个阻塞调用，会一直运行直到停止）
            logger.info("运行TradingNode（阻塞调用）...")
            self.trading_node.run()
            
            logger.info("TradingNode已停止运行")
                    
        except Exception as e:
            logger.error(f"启动ETF159506实时交易系统失败: {e}")
            raise
    
    # 注意：TradingNode会自动处理策略的启动、数据订阅和执行
    # 我们不需要手动处理这些，因为TradingNode会管理整个策略生命周期
    
    async def _disconnect_adapter_async(self) -> None:
        """异步断开适配器"""
        logger.info("断开适配器连接...")
        await self.adapter.disconnect()
        logger.info("适配器已断开")
    
    def stop_sync(self) -> None:
        """
        同步停止实时交易系统
        
        此方法被信号处理器调用，必须是同步的。
        """
        try:
            logger.info("停止ETF159506实时交易系统...")
            
            # 立即标记为非运行状态
            self.is_running = False
            
            # 停止TradingNode（这会触发优雅关闭）
            if self.trading_node:
                logger.info("停止TradingNode...")
                try:
                    self.trading_node.stop()
                    logger.info("TradingNode已停止")
                except Exception as e:
                    logger.error(f"停止TradingNode失败: {e}")
            
            # 断开适配器
            if self.adapter:
                try:
                    # 使用 TradingNode 的事件循环（如果还可用）
                    if self.trading_node and hasattr(self.trading_node, 'kernel'):
                        loop = self.trading_node.kernel.loop
                        if not loop.is_closed():
                            loop.run_until_complete(self._disconnect_adapter_async())
                        else:
                            # 事件循环已关闭，直接同步断开
                            logger.info("事件循环已关闭，尝试同步断开适配器...")
                            import asyncio
                            new_loop = asyncio.new_event_loop()
                            try:
                                new_loop.run_until_complete(self._disconnect_adapter_async())
                            finally:
                                new_loop.close()
                    else:
                        logger.info("TradingNode不可用，跳过适配器断开")
                except Exception as e:
                    logger.error(f"断开适配器失败: {e}")
            
            logger.info("ETF159506实时交易系统停止成功")
                
        except Exception as e:
            logger.error(f"停止ETF159506实时交易系统失败: {e}")
            import traceback
            traceback.print_exc()
    
    async def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态（包含内存监控信息）"""
        try:
            status = {
                "is_running": self.is_running,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            
            # 添加配置信息（从 trading_node_config）
            if self.trading_node_config:
                status["trader_id"] = str(self.trading_node_config.trader_id)
            
            # 添加适配器状态（包含内存监控信息）
            if self.adapter:
                adapter_status = await self.adapter.get_status()
                status.update(adapter_status)
                
                # ✅ 显示内存统计摘要
                if 'memory_stats' in adapter_status:
                    mem = adapter_status['memory_stats']
                    logger.info(f"💾 内存使用: {mem['current_mb']:.1f} MB (峰值: {mem['peak_mb']:.1f} MB)")
            
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
            if self.trading_node_config:
                logger.info(f"交易者ID: {self.trading_node_config.trader_id}")
            
            # 适配器配置信息
            logger.info(f"适配器Token: {self.adapter_config.get('token', 'N/A')}")
            logger.info(f"股票代码: {self.adapter_config.get('stock_code', 'N/A')}")
            
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"打印系统信息失败: {e}")


class LiveTradingManager:
    """
    实时交易管理器
    
    按照官方 Windows 信号处理建议实现：
    https://nautilustrader.io/docs/latest/concepts/live#windows-signal-handling
    """
    
    def __init__(self):
        self.trading_system: Optional[ETF159506LiveTradingSystem] = None
        self._signal_received = False
    
    def _setup_signal_handler(self) -> None:
        """
        设置信号处理器
        
        关键：在事件循环中调度异步停止任务
        """
        def signal_handler(signum, frame):
            if self._signal_received:
                logger.warning(f"再次收到信号 {signum}，强制退出...")
                sys.exit(1)
            
            self._signal_received = True
            logger.info(f"收到信号 {signum}（Ctrl+C），开始优雅关闭...")
            
            if self.trading_system and self.trading_system.trading_node:
                try:
                    # 获取 TradingNode 的事件循环
                    loop = self.trading_system.trading_node.kernel.loop
                    
                    # 创建停止任务并调度到事件循环
                    async def stop_async():
                        logger.info("在事件循环中停止 TradingNode...")
                        await self.trading_system.trading_node.stop_async()
                        logger.info("TradingNode 异步停止完成")
                    
                    # 在事件循环中调度停止任务
                    if loop and not loop.is_closed():
                        logger.info("调度 TradingNode 停止任务到事件循环...")
                        asyncio.run_coroutine_threadsafe(stop_async(), loop)
                    else:
                        logger.error("事件循环不可用，无法优雅关闭")
                        sys.exit(1)
                        
                except Exception as e:
                    logger.error(f"停止 TradingNode 失败: {e}")
                    import traceback
                    traceback.print_exc()
                    sys.exit(1)
        
        # Windows 只支持 SIGINT
        signal.signal(signal.SIGINT, signal_handler)
        
        # Unix-like 系统也支持 SIGTERM
        if sys.platform != 'win32':
            signal.signal(signal.SIGTERM, signal_handler)
            logger.info("已设置信号处理器 (SIGINT, SIGTERM)")
        else:
            logger.info("已设置信号处理器 (SIGINT) - 按 Ctrl+C 停止")
    
    def run(
        self,
        adapter_config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        运行实时交易系统（同步方法）
        
        使用信号处理器 + TradingNode.run() 的组合方式：
        1. 注册信号处理器，在收到 SIGINT 时调用 node.stop()
        2. TradingNode.run() 会阻塞直到 stop() 被调用
        3. finally 块确保清理
        """
        try:
            logger.info("启动实时交易管理器...")
            
            # 创建交易系统
            self.trading_system = ETF159506LiveTradingSystem(
                adapter_config=adapter_config,
            )
            
            # 打印系统信息
            self.trading_system.print_system_info()
            
            # 设置信号处理器（在 TradingNode 创建后）
            self._setup_signal_handler()
            
            # 启动系统（阻塞调用，直到收到停止信号）
            logger.info("启动系统（按 Ctrl+C 停止）...")
            self.trading_system.start_sync()
            
            logger.info("系统已停止运行")
            
        except KeyboardInterrupt:
            # 如果 KeyboardInterrupt 传播到这里（备用处理）
            logger.info("收到键盘中断异常（KeyboardInterrupt）...")
        except Exception as e:
            logger.error(f"运行实时交易系统失败: {e}")
            import traceback
            traceback.print_exc()
            raise
        finally:
            # 确保系统正确停止和清理
            if self.trading_system:
                logger.info("执行最终清理...")
                try:
                    # 只有在信号处理器未被触发时才需要手动停止
                    if not self._signal_received:
                        logger.info("手动停止系统...")
                        self.trading_system.stop_sync()
                    else:
                        logger.info("系统已通过信号停止，执行清理...")
                        # 断开适配器
                        if self.trading_system.adapter:
                            try:
                                import asyncio
                                if self.trading_system.trading_node and hasattr(self.trading_system.trading_node, 'kernel'):
                                    loop = self.trading_system.trading_node.kernel.loop
                                    if not loop.is_closed():
                                        loop.run_until_complete(self.trading_system._disconnect_adapter_async())
                            except Exception as e:
                                logger.error(f"清理适配器失败: {e}")
                except Exception as e:
                    logger.error(f"停止系统时出错: {e}")
                finally:
                    # 如果 TradingNode 有 dispose() 方法，在这里调用
                    if self.trading_system.trading_node and hasattr(self.trading_system.trading_node, 'dispose'):
                        try:
                            self.trading_system.trading_node.dispose()
                            logger.info("TradingNode已释放资源")
                        except Exception as e:
                            logger.error(f"释放TradingNode资源时出错: {e}")


def create_testnet_system() -> ETF159506LiveTradingSystem:
    """创建测试网络交易系统"""
    adapter_config = {
        'token': 'd0c519adcd47d266f1c96750d4e80aa6',
        'stock_code': '159506'
    }
    
    return ETF159506LiveTradingSystem(
        adapter_config=adapter_config,
    )


def create_production_system() -> ETF159506LiveTradingSystem:
    """创建生产环境交易系统"""
    adapter_config = {
        'token': 'd0c519adcd47d266f1c96750d4e80aa6',  # 生产环境token
        'stock_code': '159506'
    }
    
    return ETF159506LiveTradingSystem(
        adapter_config=adapter_config,
    )


def main():
    """
    主函数（同步版本）
    
    按照官方 Windows 信号处理建议实现
    """
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
            adapter_config = {
                'token': 'd0c519adcd47d266f1c96750d4e80aa6',
                'stock_code': '159506',
                
                # 邮件通知配置
                'email_notification': {
                    'enabled': True,                               # 启用邮件通知
                    'sender_email': '954504788@qq.com',            # ✅ 你的QQ邮箱（发件人）
                    'sender_password': 'fopwaoxwqpqmbedg',         # ✅ 你的授权码
                    'receiver_email': 'he.d.z@outlook.com',        # ✅ Outlook邮箱（收件人）
                }
            }
        else:
            logger.info("创建生产环境交易系统...")
            adapter_config = {
                'token': 'd0c519adcd47d266f1c96750d4e80aa6',  # 生产环境token
                'stock_code': '159506',
                
                # 邮件通知配置
                'email_notification': {
                    'enabled': True,                               # 启用邮件通知
                    'sender_email': '954504788@qq.com',            # ✅ 你的QQ邮箱（发件人）
                    'sender_password': 'fopwaoxwqpqmbedg',         # ✅ 你的授权码
                    'receiver_email': 'he.d.z@outlook.com',        # ✅ Outlook邮箱（收件人）
                }
            }
        
        # 运行系统（同步调用）
        manager.run(
            adapter_config=adapter_config,
        )
        
        logger.info("ETF159506 实时交易系统已关闭")
        
    except Exception as e:
        logger.error(f"主函数执行失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    # Windows 平台提示
    if sys.platform == 'win32':
        logger.info("Windows 平台：使用 Ctrl+C 停止系统")
        logger.info("官方文档: https://nautilustrader.io/docs/latest/concepts/live#windows-signal-handling")
    
    # 运行主函数（同步方式，按照官方建议）
    main()