#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
159506 ETF基于Cache的交易策略示例
展示如何访问Cache中的历史数据和实时数据进行策略决策
"""

import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging

# NautilusTrader imports
from nautilus_trader.config import CacheConfig, DatabaseConfig
from nautilus_trader.cache.cache import Cache
from nautilus_trader.model.data import Bar, BarType, BarSpecification
from nautilus_trader.model.data import QuoteTick, TradeTick
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue
from nautilus_trader.model.identifiers import BarAggregation, PriceType
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.model.enums import BarAggregation, PriceType
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.currency import Currency
from nautilus_trader.common.clock import LiveClock
from nautilus_trader.common.logging import Logger

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etf_159506_cache_strategy.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ETF159506CacheStrategy:
    """159506 ETF基于Cache的交易策略"""
    
    def __init__(self, use_redis: bool = True, redis_host: str = "localhost", redis_port: int = 6379):
        self.use_redis = use_redis
        self.redis_host = redis_host
        self.redis_port = redis_port
        
        # 创建Cache配置（与数据采集器相同的配置）
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
        
        # 策略参数
        self.lookback_period = 20  # 回看周期
        self.price_threshold = 0.01  # 价格变化阈值
        self.volume_threshold = 1000000  # 成交量阈值
        
        # 策略状态
        self.last_analysis_time = None
        self.signal_count = 0
        self.start_time = datetime.now()
        
        # 监控线程
        self.monitor_thread = None
        self.stop_monitor = False
        
        logger.info(f"Cache策略初始化完成 - Redis: {use_redis}")
    
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
                time.sleep(10)  # 每10秒分析一次
                
                if not self.stop_monitor:
                    self.analyze_market_data()
                    
            except Exception as e:
                logger.error(f"策略监控循环错误: {e}")
                break
    
    def analyze_market_data(self):
        """分析市场数据"""
        try:
            # 获取最新数据
            latest_quote = self.cache.quote_tick(self.instrument_id)
            latest_trade = self.cache.trade_tick(self.instrument_id)
            
            if not latest_quote:
                logger.warning("没有可用的报价数据")
                return
            
            # 获取历史数据
            quote_ticks = self.cache.quote_ticks(self.instrument_id)
            trade_ticks = self.cache.trade_ticks(self.instrument_id)
            
            if len(quote_ticks) < self.lookback_period:
                logger.warning(f"历史数据不足，需要{self.lookback_period}条，当前{len(quote_ticks)}条")
                return
            
            # 分析价格趋势
            price_signal = self._analyze_price_trend(quote_ticks)
            
            # 分析成交量
            volume_signal = self._analyze_volume_trend(trade_ticks)
            
            # 分析买卖盘压力
            order_book_signal = self._analyze_order_book_pressure(quote_ticks)
            
            # 综合信号
            combined_signal = self._combine_signals(price_signal, volume_signal, order_book_signal)
            
            # 记录分析结果
            self._log_analysis_result(latest_quote, combined_signal)
            
            # 更新分析时间
            self.last_analysis_time = datetime.now()
            
        except Exception as e:
            logger.error(f"分析市场数据失败: {e}")
    
    def _analyze_price_trend(self, quote_ticks: List[QuoteTick]) -> Dict:
        """分析价格趋势"""
        try:
            if len(quote_ticks) < self.lookback_period:
                return {'signal': 'neutral', 'strength': 0, 'reason': '数据不足'}
            
            # 获取最近的价格数据
            recent_ticks = quote_ticks[-self.lookback_period:]
            prices = [float(tick.bid_price) for tick in recent_ticks]
            
            # 计算价格变化
            price_change = prices[-1] - prices[0]
            price_change_pct = (price_change / prices[0]) * 100
            
            # 计算移动平均
            ma_short = sum(prices[-5:]) / 5  # 5期移动平均
            ma_long = sum(prices) / len(prices)  # 长期移动平均
            
            # 判断趋势
            if ma_short > ma_long and price_change_pct > self.price_threshold:
                signal = 'bullish'
                strength = min(abs(price_change_pct) * 10, 100)
                reason = f"价格上升趋势，短期均线{ma_short:.3f} > 长期均线{ma_long:.3f}"
            elif ma_short < ma_long and price_change_pct < -self.price_threshold:
                signal = 'bearish'
                strength = min(abs(price_change_pct) * 10, 100)
                reason = f"价格下降趋势，短期均线{ma_short:.3f} < 长期均线{ma_long:.3f}"
            else:
                signal = 'neutral'
                strength = 0
                reason = f"价格震荡，变化{price_change_pct:.2f}%"
            
            return {
                'signal': signal,
                'strength': strength,
                'reason': reason,
                'price_change_pct': price_change_pct,
                'ma_short': ma_short,
                'ma_long': ma_long
            }
            
        except Exception as e:
            logger.error(f"分析价格趋势失败: {e}")
            return {'signal': 'neutral', 'strength': 0, 'reason': f'分析失败: {e}'}
    
    def _analyze_volume_trend(self, trade_ticks: List[TradeTick]) -> Dict:
        """分析成交量趋势"""
        try:
            if len(trade_ticks) < self.lookback_period:
                return {'signal': 'neutral', 'strength': 0, 'reason': '数据不足'}
            
            # 获取最近的成交量数据
            recent_trades = trade_ticks[-self.lookback_period:]
            volumes = [int(tick.size) for tick in recent_trades]
            
            # 计算成交量变化
            avg_volume = sum(volumes) / len(volumes)
            recent_volume = sum(volumes[-5:]) / 5  # 最近5期平均成交量
            
            # 判断成交量趋势
            if recent_volume > avg_volume * 1.5 and recent_volume > self.volume_threshold:
                signal = 'bullish'
                strength = min((recent_volume / avg_volume - 1) * 50, 100)
                reason = f"成交量放大，最近{recent_volume:.0f} > 平均{avg_volume:.0f}"
            elif recent_volume < avg_volume * 0.5:
                signal = 'bearish'
                strength = min((1 - recent_volume / avg_volume) * 50, 100)
                reason = f"成交量萎缩，最近{recent_volume:.0f} < 平均{avg_volume:.0f}"
            else:
                signal = 'neutral'
                strength = 0
                reason = f"成交量正常，最近{recent_volume:.0f} ≈ 平均{avg_volume:.0f}"
            
            return {
                'signal': signal,
                'strength': strength,
                'reason': reason,
                'recent_volume': recent_volume,
                'avg_volume': avg_volume
            }
            
        except Exception as e:
            logger.error(f"分析成交量趋势失败: {e}")
            return {'signal': 'neutral', 'strength': 0, 'reason': f'分析失败: {e}'}
    
    def _analyze_order_book_pressure(self, quote_ticks: List[QuoteTick]) -> Dict:
        """分析买卖盘压力"""
        try:
            if len(quote_ticks) < 10:
                return {'signal': 'neutral', 'strength': 0, 'reason': '数据不足'}
            
            # 获取最近的报价数据
            recent_quotes = quote_ticks[-10:]
            
            # 计算买卖盘压力
            bid_pressure = sum(int(tick.bid_size) for tick in recent_quotes)
            ask_pressure = sum(int(tick.ask_size) for tick in recent_quotes)
            
            # 计算买卖价差
            spreads = [float(tick.ask_price) - float(tick.bid_price) for tick in recent_quotes]
            avg_spread = sum(spreads) / len(spreads)
            
            # 判断买卖盘压力
            if bid_pressure > ask_pressure * 1.2:
                signal = 'bullish'
                strength = min((bid_pressure / ask_pressure - 1) * 50, 100)
                reason = f"买盘压力大，买盘{bid_pressure} > 卖盘{ask_pressure}"
            elif ask_pressure > bid_pressure * 1.2:
                signal = 'bearish'
                strength = min((ask_pressure / bid_pressure - 1) * 50, 100)
                reason = f"卖盘压力大，卖盘{ask_pressure} > 买盘{bid_pressure}"
            else:
                signal = 'neutral'
                strength = 0
                reason = f"买卖盘平衡，买盘{bid_pressure} ≈ 卖盘{ask_pressure}"
            
            return {
                'signal': signal,
                'strength': strength,
                'reason': reason,
                'bid_pressure': bid_pressure,
                'ask_pressure': ask_pressure,
                'avg_spread': avg_spread
            }
            
        except Exception as e:
            logger.error(f"分析买卖盘压力失败: {e}")
            return {'signal': 'neutral', 'strength': 0, 'reason': f'分析失败: {e}'}
    
    def _combine_signals(self, price_signal: Dict, volume_signal: Dict, order_book_signal: Dict) -> Dict:
        """综合多个信号"""
        try:
            # 统计信号
            signals = [price_signal['signal'], volume_signal['signal'], order_book_signal['signal']]
            bullish_count = signals.count('bullish')
            bearish_count = signals.count('bearish')
            neutral_count = signals.count('neutral')
            
            # 计算综合强度
            total_strength = (price_signal['strength'] + volume_signal['strength'] + order_book_signal['strength']) / 3
            
            # 确定综合信号
            if bullish_count >= 2:
                combined_signal = 'bullish'
                reason = f"看涨信号占优 ({bullish_count}/3)"
            elif bearish_count >= 2:
                combined_signal = 'bearish'
                reason = f"看跌信号占优 ({bearish_count}/3)"
            else:
                combined_signal = 'neutral'
                reason = f"信号分歧 ({bullish_count}看涨, {bearish_count}看跌, {neutral_count}中性)"
            
            return {
                'signal': combined_signal,
                'strength': total_strength,
                'reason': reason,
                'price_signal': price_signal,
                'volume_signal': volume_signal,
                'order_book_signal': order_book_signal,
                'signal_distribution': {
                    'bullish': bullish_count,
                    'bearish': bearish_count,
                    'neutral': neutral_count
                }
            }
            
        except Exception as e:
            logger.error(f"综合信号失败: {e}")
            return {'signal': 'neutral', 'strength': 0, 'reason': f'综合失败: {e}'}
    
    def _log_analysis_result(self, latest_quote: QuoteTick, combined_signal: Dict):
        """记录分析结果"""
        try:
            self.signal_count += 1
            
            # 获取信号详情
            price_signal = combined_signal.get('price_signal', {})
            volume_signal = combined_signal.get('volume_signal', {})
            order_book_signal = combined_signal.get('order_book_signal', {})
            
            # 记录分析结果
            analysis_log = f"""
=== 策略分析结果 (第{self.signal_count}次) ===
时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
最新价格: 买{float(latest_quote.bid_price):.3f} / 卖{float(latest_quote.ask_price):.3f}
综合信号: {combined_signal['signal'].upper()} (强度: {combined_signal['strength']:.1f})
综合理由: {combined_signal['reason']}

详细分析:
1. 价格趋势: {price_signal.get('signal', 'N/A')} - {price_signal.get('reason', 'N/A')}
2. 成交量: {volume_signal.get('signal', 'N/A')} - {volume_signal.get('reason', 'N/A')}
3. 买卖盘: {order_book_signal.get('signal', 'N/A')} - {order_book_signal.get('reason', 'N/A')}

信号分布: {combined_signal.get('signal_distribution', {})}
===============================
"""
            logger.info(analysis_log)
            
            # 如果有强烈信号，输出警告
            if combined_signal['strength'] > 70:
                if combined_signal['signal'] == 'bullish':
                    logger.warning(f"🚀 强烈看涨信号！强度: {combined_signal['strength']:.1f}")
                elif combined_signal['signal'] == 'bearish':
                    logger.warning(f"📉 强烈看跌信号！强度: {combined_signal['strength']:.1f}")
            
        except Exception as e:
            logger.error(f"记录分析结果失败: {e}")
    
    def get_strategy_status(self) -> Dict:
        """获取策略状态"""
        try:
            runtime = datetime.now() - self.start_time
            
            # 获取Cache状态
            quote_count = self.cache.quote_tick_count(self.instrument_id)
            trade_count = self.cache.trade_tick_count(self.instrument_id)
            
            return {
                'strategy_name': 'ETF159506 Cache Strategy',
                'runtime': str(runtime),
                'signal_count': self.signal_count,
                'last_analysis_time': self.last_analysis_time.isoformat() if self.last_analysis_time else None,
                'cache_status': {
                    'quote_count': quote_count,
                    'trade_count': trade_count,
                    'total_data': quote_count + trade_count
                },
                'parameters': {
                    'lookback_period': self.lookback_period,
                    'price_threshold': self.price_threshold,
                    'volume_threshold': self.volume_threshold
                }
            }
        except Exception as e:
            logger.error(f"获取策略状态失败: {e}")
            return {}
    
    def get_historical_analysis(self, limit: int = 100) -> List[Dict]:
        """获取历史分析结果（这里简化处理，实际应该存储分析历史）"""
        try:
            # 获取历史数据
            quote_ticks = self.cache.quote_ticks(self.instrument_id)[-limit:]
            trade_ticks = self.cache.trade_ticks(self.instrument_id)[-limit:]
            
            analysis_history = []
            
            # 简化分析历史数据
            for i, quote in enumerate(quote_ticks[-10:]):  # 只分析最近10条
                analysis = {
                    'timestamp': pd.to_datetime(quote.ts_event, unit='ns'),
                    'bid_price': float(quote.bid_price),
                    'ask_price': float(quote.ask_price),
                    'bid_size': int(quote.bid_size),
                    'ask_size': int(quote.ask_size),
                    'analysis_index': i
                }
                analysis_history.append(analysis)
            
            return analysis_history
            
        except Exception as e:
            logger.error(f"获取历史分析失败: {e}")
            return []


def main():
    """主函数"""
    print("=" * 60)
    print("159506 ETF基于Cache的交易策略")
    print("=" * 60)
    print("功能:")
    print("1. 实时分析Cache中的市场数据")
    print("2. 价格趋势分析")
    print("3. 成交量分析")
    print("4. 买卖盘压力分析")
    print("5. 综合信号生成")
    print("=" * 60)
    
    # 创建策略实例
    strategy = ETF159506CacheStrategy(use_redis=True)
    
    try:
        # 启动策略监控
        strategy.start_monitoring()
        
        print("策略监控已启动，按Ctrl+C退出")
        print("每10秒分析一次市场数据")
        
        # 主循环
        while True:
            time.sleep(60)  # 每分钟显示一次状态
            
            # 显示策略状态
            status = strategy.get_strategy_status()
            print(f"策略状态: 运行{status.get('runtime', 'N/A')}, "
                  f"分析次数={status.get('signal_count', 0)}, "
                  f"数据量={status.get('cache_status', {}).get('total_data', 0)}")
            
    except KeyboardInterrupt:
        print("\n用户中断，正在停止策略...")
        strategy.stop_monitoring()
        
        # 显示最终状态
        final_status = strategy.get_strategy_status()
        print(f"\n最终策略状态:")
        print(f"总分析次数: {final_status.get('signal_count', 0)}")
        print(f"运行时间: {final_status.get('runtime', 'N/A')}")
        print(f"缓存数据量: {final_status.get('cache_status', {}).get('total_data', 0)}")
        
        print("策略已停止")
    
    except Exception as e:
        logger.error(f"策略运行错误: {e}")
        strategy.stop_monitoring()


if __name__ == "__main__":
    main() 