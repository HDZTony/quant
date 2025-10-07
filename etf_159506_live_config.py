#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
159506 ETF 实时交易配置
基于NautilusTrader TradingNodeConfig架构
"""

import logging
from decimal import Decimal
from typing import Dict, Any, Optional

from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.config import CacheConfig
from nautilus_trader.config import DatabaseConfig
from nautilus_trader.config import MessageBusConfig
from nautilus_trader.config import LiveDataEngineConfig
from nautilus_trader.config import LiveRiskEngineConfig
from nautilus_trader.config import LiveExecEngineConfig
from nautilus_trader.config import PortfolioConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import ImportableStrategyConfig
from nautilus_trader.model.identifiers import TraderId, Venue, InstrumentId, Symbol
from nautilus_trader.model.enums import AccountType, OmsType
from nautilus_trader.model.currencies import CNY
from nautilus_trader.model.objects import Money
from nautilus_trader.model.data import BarType

# 导入策略和工具配置
from etf_159506_strategy_config import ETF159506Config
from etf_159506_instrument import create_etf_159506_default, create_etf_159506_bar_type

logger = logging.getLogger(__name__)


class ETF159506LiveConfig:
    """159506 ETF 实时交易配置类"""
    
    def __init__(
        self,
        trader_id: str = "ETF159506-LIVE-001",
        instance_id: Optional[str] = None,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_password: Optional[str] = None,
        redis_db: int = 0,
        testnet: bool = True,  # 默认使用测试网络
        starting_balance: str = "100000 CNY",  # 初始资金10万
    ):
        """
        初始化实时交易配置
        
        Parameters
        ----------
        trader_id : str
            交易者ID
        instance_id : Optional[str]
            实例ID
        redis_host : str
            Redis主机地址
        redis_port : int
            Redis端口
        redis_password : Optional[str]
            Redis密码
        redis_db : int
            Redis数据库编号
        testnet : bool
            是否使用测试网络
        starting_balance : str
            初始资金
        """
        self.trader_id = trader_id
        self.instance_id = instance_id
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_password = redis_password
        self.redis_db = redis_db
        self.testnet = testnet
        self.starting_balance = starting_balance
        
        # 创建工具和BarType
        self.instrument = create_etf_159506_default()
        self.instrument_id = self.instrument.id
        self.bar_type_str = create_etf_159506_bar_type()
        self.bar_type = BarType.from_str(self.bar_type_str)
        
        logger.info(f"初始化实时交易配置: trader_id={trader_id}, instrument_id={self.instrument_id}")
    
    def create_trading_node_config(self) -> TradingNodeConfig:
        """创建TradingNodeConfig配置"""
        try:
            logger.info("创建TradingNodeConfig配置...")
            
            # 缓存配置
            cache_config = self._create_cache_config()
            
            # 消息总线配置
            message_bus_config = self._create_message_bus_config()
            
            # 数据引擎配置
            data_engine_config = self._create_data_engine_config()
            
            # 风险引擎配置
            risk_engine_config = self._create_risk_engine_config()
            
            # 执行引擎配置
            exec_engine_config = self._create_exec_engine_config()
            
            # 投资组合配置
            portfolio_config = self._create_portfolio_config()
            
            # 数据客户端配置
            data_clients = self._create_data_clients()
            
            # 执行客户端配置
            exec_clients = self._create_exec_clients()
            
            # 策略配置
            strategies = self._create_strategy_configs()
            
            # 创建TradingNodeConfig
            config = TradingNodeConfig(
                trader_id=TraderId(self.trader_id),
                instance_id=self.instance_id,
                
                # 核心配置
                cache=cache_config,
                message_bus=message_bus_config,
                data_engine=data_engine_config,
                risk_engine=risk_engine_config,
                exec_engine=exec_engine_config,
                portfolio=portfolio_config,
                
                # 客户端配置
                data_clients=data_clients,
                exec_clients=exec_clients,
                
                # 策略配置
                strategies=strategies,
                
                # 超时配置
                timeout_connection=30.0,
                timeout_reconciliation=10.0,
                timeout_portfolio=10.0,
                timeout_disconnection=10.0,
                timeout_post_stop=5.0,
            )
            
            logger.info("TradingNodeConfig配置创建成功")
            return config
            
        except Exception as e:
            logger.error(f"创建TradingNodeConfig配置失败: {e}")
            raise
    
    def _create_cache_config(self) -> CacheConfig:
        """创建缓存配置"""
        logger.info("创建缓存配置...")
        
        # Redis数据库配置
        database_config = DatabaseConfig(
            host=self.redis_host,
            port=self.redis_port,
            password=self.redis_password,
            database=self.redis_db,
            timeout=2.0,
        )
        
        cache_config = CacheConfig(
            database=database_config,
            encoding="msgpack",  # 使用msgpack编码，性能更好
            timestamps_as_iso8601=True,
            buffer_interval_ms=100,  # 100ms缓冲间隔
            flush_on_start=False,  # 启动时不刷新缓存
        )
        
        logger.info(f"缓存配置创建成功: Redis={self.redis_host}:{self.redis_port}")
        return cache_config
    
    def _create_message_bus_config(self) -> MessageBusConfig:
        """创建消息总线配置"""
        logger.info("创建消息总线配置...")
        
        # Redis数据库配置
        database_config = DatabaseConfig(
            host=self.redis_host,
            port=self.redis_port,
            password=self.redis_password,
            database=self.redis_db,
            timeout=2.0,
        )
        
        message_bus_config = MessageBusConfig(
            database=database_config,
            timestamps_as_iso8601=True,
            use_instance_id=False,
            types_filter=None,  # 不过滤消息类型
            stream_per_topic=False,
            autotrim_mins=30,  # 30分钟自动清理
            heartbeat_interval_secs=1,  # 1秒心跳间隔
        )
        
        logger.info("消息总线配置创建成功")
        return message_bus_config
    
    def _create_data_engine_config(self) -> LiveDataEngineConfig:
        """创建数据引擎配置"""
        logger.info("创建数据引擎配置...")
        
        data_engine_config = LiveDataEngineConfig(
            qsize=10000,  # 队列大小
        )
        
        logger.info("数据引擎配置创建成功")
        return data_engine_config
    
    def _create_risk_engine_config(self) -> LiveRiskEngineConfig:
        """创建风险引擎配置"""
        logger.info("创建风险引擎配置...")
        
        risk_engine_config = LiveRiskEngineConfig(
            bypass=False,  # 不绕过风险检查
        )
        
        logger.info("风险引擎配置创建成功")
        return risk_engine_config
    
    def _create_exec_engine_config(self) -> LiveExecEngineConfig:
        """创建执行引擎配置"""
        logger.info("创建执行引擎配置...")
        
        exec_engine_config = LiveExecEngineConfig(
            qsize=10000,  # 队列大小
            
            # 对账配置
            reconciliation=True,  # 启用对账
            reconciliation_lookback_mins=None,  # 使用最大历史
            reconciliation_instrument_ids=None,  # 对所有工具对账
            filtered_client_order_ids=None,  # 不过滤订单
            
            # 订单过滤
            filter_unclaimed_external_orders=False,  # 不过滤外部订单
            filter_position_reports=False,  # 不过滤持仓报告
            
            # 连续对账
            inflight_check_interval_ms=2000,  # 2秒检查间隔
            inflight_check_threshold_ms=5000,  # 5秒超时阈值
            inflight_check_retries=5,  # 5次重试
            open_check_interval_secs=10,  # 10秒检查开放订单
            open_check_open_only=True,  # 只检查开放订单
            own_books_audit_interval_secs=30,  # 30秒审计间隔
            
            # 其他选项
            generate_missing_orders=True,  # 生成缺失订单
        )
        
        logger.info("执行引擎配置创建成功")
        return exec_engine_config
    
    def _create_portfolio_config(self) -> PortfolioConfig:
        """创建投资组合配置"""
        logger.info("创建投资组合配置...")
        
        portfolio_config = PortfolioConfig(
            base_currency=CNY,
        )
        
        logger.info("投资组合配置创建成功")
        return portfolio_config
    
    def _create_data_clients(self) -> Dict[str, Any]:
        """创建数据客户端配置"""
        logger.info("创建数据客户端配置...")
        
        # 注意：这里需要根据实际的数据提供商进行配置
        # 目前使用模拟数据客户端作为示例
        data_clients = {}
        
        # 如果有实际的数据提供商，可以添加配置
        # 例如：
        # data_clients["BINANCE"] = BinanceDataClientConfig(
        #     account_type=BinanceAccountType.SPOT,
        #     testnet=self.testnet,
        # )
        
        logger.info(f"数据客户端配置创建成功: {len(data_clients)} 个客户端")
        return data_clients
    
    def _create_exec_clients(self) -> Dict[str, Any]:
        """创建执行客户端配置"""
        logger.info("创建执行客户端配置...")
        
        # 注意：这里需要根据实际的交易提供商进行配置
        # 目前使用模拟执行客户端作为示例
        exec_clients = {}
        
        # 如果有实际的交易提供商，可以添加配置
        # 例如：
        # exec_clients["BINANCE"] = BinanceExecClientConfig(
        #     account_type=BinanceAccountType.SPOT,
        #     testnet=self.testnet,
        # )
        
        logger.info(f"执行客户端配置创建成功: {len(exec_clients)} 个客户端")
        return exec_clients
    
    def _create_strategy_configs(self) -> list[ImportableStrategyConfig]:
        """创建策略配置"""
        logger.info("创建策略配置...")
        
        # 创建策略配置
        strategy_config = ImportableStrategyConfig(
            strategy_path="etf_159506_strategy:ETF159506Strategy",
            config_path="etf_159506_strategy_config:ETF159506Config",
            config={
                "instrument_id": str(self.instrument_id),
                "bar_type": self.bar_type,  # 直接传递BarType对象，不要转换为字符串
                "venue": "SZSE",
                "trade_size": 0,  # 0表示满仓交易
                "fast_ema_period": 12,
                "slow_ema_period": 26,
                "volume_threshold": 500000,
                "stop_loss_pct": 0.02,  # 2%止损
                "take_profit_pct": 0.25,  # 25%止盈
                "max_daily_trades": 100,
                "lookback_period": 2,
                "price_threshold": 0.001,
                "emulation_trigger": "NO_TRIGGER",
                "initial_position_quantity": 0,  # 空仓开始
                # 背离检测参数
                "dea_trend_period": 3,
                "advance_trading_bars": 1,
                "confirmation_bars": 1,
                "divergence_threshold": 0.0005,
                "max_extremes": 200,
            },
        )
        
        strategies = [strategy_config]
        
        logger.info(f"策略配置创建成功: {len(strategies)} 个策略")
        return strategies
    
    def create_logging_config(self) -> LoggingConfig:
        """创建日志配置"""
        logger.info("创建日志配置...")
        
        logging_config = LoggingConfig(
            log_level="INFO",
            log_file_name="etf_159506_live_trading.log",
            log_file_format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            log_file_max_size=10485760,  # 10MB
            log_file_max_backup_count=5,
            log_colors=True,
            bypass_logging=False,
            print_config=True,  # 打印配置信息
        )
        
        logger.info("日志配置创建成功")
        return logging_config
    
    def get_instrument_info(self) -> Dict[str, Any]:
        """获取工具信息"""
        return {
            "instrument_id": str(self.instrument_id),
            "bar_type": str(self.bar_type),
            "symbol": self.instrument.symbol.value,
            "venue": self.instrument.venue.value,
            "currency": self.instrument.quote_currency.code,
            "price_precision": self.instrument.price_precision,
            "price_increment": str(self.instrument.price_increment),
            "lot_size": int(self.instrument.lot_size),
        }
    
    def validate_config(self) -> bool:
        """验证配置"""
        try:
            logger.info("验证配置...")
            
            # 验证必要参数
            if not self.trader_id:
                raise ValueError("trader_id不能为空")
            
            if not self.instrument_id:
                raise ValueError("instrument_id不能为空")
            
            if not self.bar_type:
                raise ValueError("bar_type不能为空")
            
            # 验证Redis连接参数
            if not self.redis_host:
                raise ValueError("redis_host不能为空")
            
            if self.redis_port <= 0 or self.redis_port > 65535:
                raise ValueError("redis_port必须在1-65535范围内")
            
            logger.info("配置验证成功")
            return True
            
        except Exception as e:
            logger.error(f"配置验证失败: {e}")
            return False


def create_default_live_config() -> ETF159506LiveConfig:
    """创建默认的实时交易配置"""
    return ETF159506LiveConfig()


def create_testnet_live_config() -> ETF159506LiveConfig:
    """创建测试网络的实时交易配置"""
    return ETF159506LiveConfig(
        trader_id="ETF159506-TEST-001",
        testnet=True,
        starting_balance="10000 CNY",  # 测试资金1万
    )


def create_production_live_config() -> ETF159506LiveConfig:
    """创建生产环境的实时交易配置"""
    return ETF159506LiveConfig(
        trader_id="ETF159506-PROD-001",
        testnet=False,
        starting_balance="100000 CNY",  # 生产资金10万
    )


# 使用示例
if __name__ == "__main__":
    print("159506 ETF 实时交易配置示例")
    print("=" * 60)
    
    # 1. 默认配置
    print("\n1. 默认配置:")
    config = create_default_live_config()
    print(f"   交易者ID: {config.trader_id}")
    print(f"   工具ID: {config.instrument_id}")
    print(f"   K线类型: {config.bar_type}")
    print(f"   测试网络: {config.testnet}")
    print(f"   初始资金: {config.starting_balance}")
    
    # 2. 验证配置
    print("\n2. 配置验证:")
    is_valid = config.validate_config()
    print(f"   配置有效: {is_valid}")
    
    # 3. 获取工具信息
    print("\n3. 工具信息:")
    instrument_info = config.get_instrument_info()
    for key, value in instrument_info.items():
        print(f"   {key}: {value}")
    
    # 4. 创建TradingNodeConfig
    print("\n4. 创建TradingNodeConfig:")
    try:
        trading_config = config.create_trading_node_config()
        print(f"   TradingNodeConfig创建成功")
        print(f"   交易者ID: {trading_config.trader_id}")
        print(f"   策略数量: {len(trading_config.strategies)}")
        print(f"   数据客户端数量: {len(trading_config.data_clients)}")
        print(f"   执行客户端数量: {len(trading_config.exec_clients)}")
    except Exception as e:
        print(f"   TradingNodeConfig创建失败: {e}")
    
    print("\n✅ 实时交易配置示例完成！")
