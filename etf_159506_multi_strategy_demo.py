#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
159506 ETF多策略共享Cache数据演示
展示如何让不同策略访问相同的数据进行协同分析
"""

import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
import json

# NautilusTrader imports
from nautilus_trader.config import CacheConfig, DatabaseConfig
from nautilus_trader.cache.cache import Cache
from nautilus_trader.model.data import QuoteTick, TradeTick
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.common.clock import LiveClock
from nautilus_trader.common.logging import Logger

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etf_159506_multi_strategy.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class BaseStrategy:
    """基础策略类"""
    
    def __init__(self, name: str, cache: Cache, instrument_id: InstrumentId):
        self.name = name
        self.cache = cache
        self.instrument_id = instrument_id
        self.is_running = False
        self.analysis_count = 0
        self.start_time = datetime.now()
        
        logger.info(f"策略 {name} 初始化完成")
    
    def start(self):
        """启动策略"""
        self.is_running = True
        logger.info(f"策略 {self.name} 已启动")
    
    def stop(self):
        """停止策略"""
        self.is_running = False
        logger.info(f"策略 {self.name} 已停止")
    
    def analyze(self):
        """分析数据（子类实现）"""
        raise NotImplementedError
    
    def get_status(self) -> Dict:
        """获取策略状态"""
        runtime = datetime.now() - self.start_time
        return {
            'name': self.name,
            'is_running': self.is_running,
            'analysis_count': self.analysis_count,
            'runtime': str(runtime)
        }


class MomentumStrategy(BaseStrategy):
    """动量策略 - 基于价格动量"""
    
    def __init__(self, cache: Cache, instrument_id: InstrumentId):
        super().__init__("Momentum策略", cache, instrument_id)
        self.lookback_period = 10
        self.momentum_threshold = 0.005  # 0.5%
    
    def analyze(self):
        """分析价格动量"""
        try:
            quote_ticks = self.cache.quote_ticks(self.instrument_id)
            
            if len(quote_ticks) < self.lookback_period:
                return
            
            # 计算价格动量
            recent_ticks = quote_ticks[-self.lookback_period:]
            prices = [float(tick.bid_price) for tick in recent_ticks]
            
            # 计算动量
            momentum = (prices[-1] - prices[0]) / prices[0]
            
            # 判断信号
            if momentum > self.momentum_threshold:
                signal = "BUY"
                strength = min(momentum * 100, 100)
            elif momentum < -self.momentum_threshold:
                signal = "SELL"
                strength = min(abs(momentum) * 100, 100)
            else:
                signal = "HOLD"
                strength = 0
            
            # 存储分析结果到Cache
            analysis_result = {
                'strategy': self.name,
                'timestamp': datetime.now().isoformat(),
                'signal': signal,
                'strength': strength,
                'momentum': momentum,
                'current_price': prices[-1],
                'lookback_prices': prices
            }
            
            # 将分析结果存储到Cache中供其他策略使用
            self.cache.add(f"strategy_analysis_{self.name}", json.dumps(analysis_result).encode())
            
            self.analysis_count += 1
            
            logger.info(f"{self.name}: 动量={momentum:.4f}, 信号={signal}, 强度={strength:.1f}")
            
        except Exception as e:
            logger.error(f"{self.name} 分析失败: {e}")


class VolumeStrategy(BaseStrategy):
    """成交量策略 - 基于成交量异常"""
    
    def __init__(self, cache: Cache, instrument_id: InstrumentId):
        super().__init__("Volume策略", cache, instrument_id)
        self.lookback_period = 20
        self.volume_threshold = 1.5  # 成交量放大1.5倍
    
    def analyze(self):
        """分析成交量异常"""
        try:
            trade_ticks = self.cache.trade_ticks(self.instrument_id)
            
            if len(trade_ticks) < self.lookback_period:
                return
            
            # 计算成交量统计
            recent_trades = trade_ticks[-self.lookback_period:]
            volumes = [int(tick.size) for tick in recent_trades]
            
            avg_volume = sum(volumes) / len(volumes)
            recent_avg_volume = sum(volumes[-5:]) / 5  # 最近5期平均
            
            # 计算成交量比率
            volume_ratio = recent_avg_volume / avg_volume if avg_volume > 0 else 1
            
            # 判断信号
            if volume_ratio > self.volume_threshold:
                signal = "BUY"
                strength = min((volume_ratio - 1) * 50, 100)
            elif volume_ratio < 1 / self.volume_threshold:
                signal = "SELL"
                strength = min((1 / volume_ratio - 1) * 50, 100)
            else:
                signal = "HOLD"
                strength = 0
            
            # 存储分析结果
            analysis_result = {
                'strategy': self.name,
                'timestamp': datetime.now().isoformat(),
                'signal': signal,
                'strength': strength,
                'volume_ratio': volume_ratio,
                'recent_avg_volume': recent_avg_volume,
                'historical_avg_volume': avg_volume
            }
            
            self.cache.add(f"strategy_analysis_{self.name}", json.dumps(analysis_result).encode())
            
            self.analysis_count += 1
            
            logger.info(f"{self.name}: 成交量比率={volume_ratio:.2f}, 信号={signal}, 强度={strength:.1f}")
            
        except Exception as e:
            logger.error(f"{self.name} 分析失败: {e}")


class SpreadStrategy(BaseStrategy):
    """价差策略 - 基于买卖价差"""
    
    def __init__(self, cache: Cache, instrument_id: InstrumentId):
        super().__init__("Spread策略", cache, instrument_id)
        self.lookback_period = 15
        self.spread_threshold = 0.002  # 0.2%
    
    def analyze(self):
        """分析买卖价差"""
        try:
            quote_ticks = self.cache.quote_ticks(self.instrument_id)
            
            if len(quote_ticks) < self.lookback_period:
                return
            
            # 计算价差统计
            recent_quotes = quote_ticks[-self.lookback_period:]
            spreads = [float(tick.ask_price) - float(tick.bid_price) for tick in recent_quotes]
            spread_ratios = [spread / float(tick.bid_price) for spread, tick in zip(spreads, recent_quotes)]
            
            avg_spread_ratio = sum(spread_ratios) / len(spread_ratios)
            current_spread_ratio = spread_ratios[-1]
            
            # 判断信号
            if current_spread_ratio < avg_spread_ratio * 0.8:  # 价差收窄
                signal = "BUY"
                strength = min((avg_spread_ratio / current_spread_ratio - 1) * 50, 100)
            elif current_spread_ratio > avg_spread_ratio * 1.2:  # 价差扩大
                signal = "SELL"
                strength = min((current_spread_ratio / avg_spread_ratio - 1) * 50, 100)
            else:
                signal = "HOLD"
                strength = 0
            
            # 存储分析结果
            analysis_result = {
                'strategy': self.name,
                'timestamp': datetime.now().isoformat(),
                'signal': signal,
                'strength': strength,
                'current_spread_ratio': current_spread_ratio,
                'avg_spread_ratio': avg_spread_ratio,
                'spread_tightening': current_spread_ratio < avg_spread_ratio
            }
            
            self.cache.add(f"strategy_analysis_{self.name}", json.dumps(analysis_result).encode())
            
            self.analysis_count += 1
            
            logger.info(f"{self.name}: 当前价差比率={current_spread_ratio:.4f}, 信号={signal}, 强度={strength:.1f}")
            
        except Exception as e:
            logger.error(f"{self.name} 分析失败: {e}")


class ConsensusStrategy(BaseStrategy):
    """共识策略 - 综合其他策略的分析结果"""
    
    def __init__(self, cache: Cache, instrument_id: InstrumentId, strategy_names: List[str]):
        super().__init__("Consensus策略", cache, instrument_id)
        self.strategy_names = strategy_names
        self.consensus_threshold = 2  # 至少2个策略同意
    
    def analyze(self):
        """综合其他策略的分析结果"""
        try:
            # 获取其他策略的分析结果
            strategy_results = {}
            
            for strategy_name in self.strategy_names:
                analysis_data = self.cache.get(f"strategy_analysis_{strategy_name}")
                if analysis_data:
                    try:
                        result = json.loads(analysis_data.decode())
                        strategy_results[strategy_name] = result
                    except:
                        continue
            
            if len(strategy_results) < 2:
                logger.warning(f"{self.name}: 策略结果不足，需要至少2个策略")
                return
            
            # 统计信号
            buy_count = 0
            sell_count = 0
            hold_count = 0
            total_strength = 0
            
            for strategy_name, result in strategy_results.items():
                signal = result.get('signal', 'HOLD')
                strength = result.get('strength', 0)
                
                if signal == 'BUY':
                    buy_count += 1
                    total_strength += strength
                elif signal == 'SELL':
                    sell_count += 1
                    total_strength += strength
                else:
                    hold_count += 1
            
            # 确定共识信号
            if buy_count >= self.consensus_threshold:
                consensus_signal = "BUY"
                consensus_strength = total_strength / buy_count
                reason = f"看涨共识 ({buy_count}/{len(strategy_results)} 策略)"
            elif sell_count >= self.consensus_threshold:
                consensus_signal = "SELL"
                consensus_strength = total_strength / sell_count
                reason = f"看跌共识 ({sell_count}/{len(strategy_results)} 策略)"
            else:
                consensus_signal = "HOLD"
                consensus_strength = 0
                reason = f"策略分歧 (买{buy_count}, 卖{sell_count}, 持有{hold_count})"
            
            # 存储共识结果
            consensus_result = {
                'strategy': self.name,
                'timestamp': datetime.now().isoformat(),
                'signal': consensus_signal,
                'strength': consensus_strength,
                'reason': reason,
                'strategy_results': strategy_results,
                'signal_distribution': {
                    'buy': buy_count,
                    'sell': sell_count,
                    'hold': hold_count
                }
            }
            
            self.cache.add(f"strategy_analysis_{self.name}", json.dumps(consensus_result).encode())
            
            self.analysis_count += 1
            
            logger.info(f"{self.name}: {reason}, 信号={consensus_signal}, 强度={consensus_strength:.1f}")
            
            # 如果有强烈共识，输出警告
            if consensus_strength > 70:
                if consensus_signal == "BUY":
                    logger.warning(f"🚀 强烈看涨共识！强度: {consensus_strength:.1f}")
                elif consensus_signal == "SELL":
                    logger.warning(f"📉 强烈看跌共识！强度: {consensus_strength:.1f}")
            
        except Exception as e:
            logger.error(f"{self.name} 分析失败: {e}")


class MultiStrategyManager:
    """多策略管理器"""
    
    def __init__(self, use_redis: bool = True, redis_host: str = "localhost", redis_port: int = 6379):
        self.use_redis = use_redis
        self.redis_host = redis_host
        self.redis_port = redis_port
        
        # 创建Cache配置
        if use_redis:
            cache_config = CacheConfig(
                database=DatabaseConfig(
                    type="redis",
                    host=redis_host,
                    port=redis_port,
                    timeout=2,
                ),
                tick_capacity=100_000,
                bar_capacity=50_000,
                encoding="msgpack",
                timestamps_as_iso8601=True,
                use_trader_prefix=True,
                use_instance_id=False,
                flush_on_start=False,
                drop_instruments_on_reset=True,
            )
        else:
            cache_config = CacheConfig(
                tick_capacity=100_000,
                bar_capacity=50_000,
                encoding="msgpack",
                timestamps_as_iso8601=True,
            )
        
        # 创建Cache实例
        self.clock = LiveClock()
        self.logger = Logger(clock=self.clock)
        self.cache = Cache(config=cache_config, clock=self.clock, logger=self.logger)
        
        # 初始化工具ID
        self.instrument_id = InstrumentId(
            symbol=Symbol("159506"),
            venue=Venue("SZSE")
        )
        
        # 创建策略实例
        self.strategies = {}
        self._create_strategies()
        
        # 监控线程
        self.monitor_thread = None
        self.stop_monitor = False
        self.start_time = datetime.now()
        
        logger.info(f"多策略管理器初始化完成 - Redis: {use_redis}")
    
    def _create_strategies(self):
        """创建策略实例"""
        # 创建基础策略
        self.strategies['momentum'] = MomentumStrategy(self.cache, self.instrument_id)
        self.strategies['volume'] = VolumeStrategy(self.cache, self.instrument_id)
        self.strategies['spread'] = SpreadStrategy(self.cache, self.instrument_id)
        
        # 创建共识策略
        strategy_names = ['momentum', 'volume', 'spread']
        self.strategies['consensus'] = ConsensusStrategy(self.cache, self.instrument_id, strategy_names)
        
        logger.info(f"创建了 {len(self.strategies)} 个策略")
    
    def start_all_strategies(self):
        """启动所有策略"""
        for name, strategy in self.strategies.items():
            strategy.start()
        logger.info("所有策略已启动")
    
    def stop_all_strategies(self):
        """停止所有策略"""
        for name, strategy in self.strategies.items():
            strategy.stop()
        logger.info("所有策略已停止")
    
    def start_monitoring(self):
        """启动策略监控"""
        self.monitor_thread = threading.Thread(target=self.monitor_loop)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        logger.info("策略监控线程已启动")
    
    def stop_monitoring(self):
        """停止策略监控"""
        self.stop_monitor = True
        if self.monitor_thread:
            self.monitor_thread.join(timeout=2)
        logger.info("策略监控线程已停止")
    
    def monitor_loop(self):
        """策略监控循环"""
        while not self.stop_monitor:
            try:
                time.sleep(15)  # 每15秒运行一次所有策略
                
                if not self.stop_monitor:
                    self.run_all_strategies()
                    
            except Exception as e:
                logger.error(f"策略监控循环错误: {e}")
                break
    
    def run_all_strategies(self):
        """运行所有策略"""
        try:
            # 检查是否有足够的数据
            quote_count = self.cache.quote_tick_count(self.instrument_id)
            trade_count = self.cache.trade_tick_count(self.instrument_id)
            
            if quote_count < 20 or trade_count < 20:
                logger.warning(f"数据不足，报价{quote_count}条，交易{trade_count}条")
                return
            
            # 运行所有策略
            for name, strategy in self.strategies.items():
                if strategy.is_running:
                    try:
                        strategy.analyze()
                    except Exception as e:
                        logger.error(f"策略 {name} 运行失败: {e}")
            
        except Exception as e:
            logger.error(f"运行所有策略失败: {e}")
    
    def get_all_strategies_status(self) -> Dict:
        """获取所有策略状态"""
        try:
            runtime = datetime.now() - self.start_time
            
            # 获取Cache状态
            quote_count = self.cache.quote_tick_count(self.instrument_id)
            trade_count = self.cache.trade_tick_count(self.instrument_id)
            
            # 获取各策略状态
            strategy_statuses = {}
            for name, strategy in self.strategies.items():
                strategy_statuses[name] = strategy.get_status()
            
            return {
                'manager_runtime': str(runtime),
                'cache_status': {
                    'quote_count': quote_count,
                    'trade_count': trade_count,
                    'total_data': quote_count + trade_count
                },
                'strategies': strategy_statuses
            }
        except Exception as e:
            logger.error(f"获取策略状态失败: {e}")
            return {}
    
    def get_consensus_analysis(self) -> Dict:
        """获取共识分析结果"""
        try:
            consensus_data = self.cache.get("strategy_analysis_Consensus策略")
            if consensus_data:
                return json.loads(consensus_data.decode())
            else:
                return {}
        except Exception as e:
            logger.error(f"获取共识分析失败: {e}")
            return {}


def main():
    """主函数"""
    print("=" * 60)
    print("159506 ETF多策略共享Cache数据演示")
    print("=" * 60)
    print("策略列表:")
    print("1. Momentum策略 - 基于价格动量")
    print("2. Volume策略 - 基于成交量异常")
    print("3. Spread策略 - 基于买卖价差")
    print("4. Consensus策略 - 综合其他策略结果")
    print("=" * 60)
    
    # 创建多策略管理器
    manager = MultiStrategyManager(use_redis=True)
    
    try:
        # 启动所有策略
        manager.start_all_strategies()
        
        # 启动监控
        manager.start_monitoring()
        
        print("多策略系统已启动，按Ctrl+C退出")
        print("每15秒运行一次所有策略")
        
        # 主循环
        while True:
            time.sleep(60)  # 每分钟显示一次状态
            
            # 显示系统状态
            status = manager.get_all_strategies_status()
            cache_status = status.get('cache_status', {})
            
            print(f"系统状态: 运行{status.get('manager_runtime', 'N/A')}, "
                  f"数据量={cache_status.get('total_data', 0)}")
            
            # 显示各策略状态
            strategies = status.get('strategies', {})
            for name, strategy_status in strategies.items():
                print(f"  {name}: 分析{strategy_status.get('analysis_count', 0)}次")
            
            # 显示最新共识
            consensus = manager.get_consensus_analysis()
            if consensus:
                signal = consensus.get('signal', 'N/A')
                strength = consensus.get('strength', 0)
                reason = consensus.get('reason', 'N/A')
                print(f"  最新共识: {signal} (强度{strength:.1f}) - {reason}")
            
    except KeyboardInterrupt:
        print("\n用户中断，正在停止系统...")
        manager.stop_all_strategies()
        manager.stop_monitoring()
        
        # 显示最终状态
        final_status = manager.get_all_strategies_status()
        print(f"\n最终系统状态:")
        print(f"运行时间: {final_status.get('manager_runtime', 'N/A')}")
        print(f"缓存数据量: {final_status.get('cache_status', {}).get('total_data', 0)}")
        
        strategies = final_status.get('strategies', {})
        for name, strategy_status in strategies.items():
            print(f"{name}: 总分析{strategy_status.get('analysis_count', 0)}次")
        
        print("系统已停止")
    
    except Exception as e:
        logger.error(f"系统运行错误: {e}")
        manager.stop_all_strategies()
        manager.stop_monitoring()


if __name__ == "__main__":
    main() 