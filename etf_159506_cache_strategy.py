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
        
        # 策略参数 - 日内交易优化
        self.lookback_period = 10  # 回看周期（缩短为10，适合日内交易）
        self.price_threshold = 0.003  # 价格变化阈值（降低到0.3%，更敏感）
        self.volume_threshold = 500000  # 成交量阈值（降低到50万股）
        
        # 日内交易参数
        self.trading_hours = {
            'start': '09:30',
            'end': '15:00'
        }
        self.max_daily_trades = 10  # 每日最大交易次数
        self.stop_loss_pct = 0.02  # 止损比例 2%
        self.take_profit_pct = 0.015  # 止盈比例 1.5%
        self.position_size = 0.1  # 仓位比例 10%
        
        # 策略状态
        self.last_analysis_time = None
        self.signal_count = 0
        self.start_time = datetime.now()
        
        # 日内交易状态
        self.daily_trades = 0
        self.current_position = 0  # 0: 无仓位, 1: 多头, -1: 空头
        self.entry_price = 0.0
        self.entry_time = None
        self.daily_pnl = 0.0
        self.today_date = datetime.now().date()
        
        # 监控线程
        self.monitor_thread = None
        self.stop_monitor = False
        
        logger.info(f"Cache策略初始化完成 - Redis: {use_redis}")
        logger.info("实时分析模式已启用：有新数据时立即触发分析")
    
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
        """策略监控循环 - 实时分析"""
        last_quote_count = 0
        last_trade_count = 0
        
        while not self.stop_monitor:
            try:
                # 检查是否有新数据
                current_quote_count = self.cache.quote_tick_count(self.instrument_id)
                current_trade_count = self.cache.trade_tick_count(self.instrument_id)
                
                # 如果有新数据，立即分析
                if (current_quote_count > last_quote_count or 
                    current_trade_count > last_trade_count):
                    
                    if not self.stop_monitor:
                        self.analyze_market_data()
                    
                    # 更新计数
                    last_quote_count = current_quote_count
                    last_trade_count = current_trade_count
                
                # 短暂休眠以避免过度占用CPU
                time.sleep(0.1)  # 100毫秒检查一次
                    
            except Exception as e:
                logger.error(f"策略监控循环错误: {e}")
                break
    
    def analyze_market_data(self):
        """分析市场数据 - 日内交易模式"""
        try:
            # 检查交易时间
            if not self._is_trading_time():
                return
            
            # 检查每日交易次数限制
            if self.daily_trades >= self.max_daily_trades:
                logger.info(f"今日交易次数已达上限: {self.max_daily_trades}")
                return
            
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
            
            # 检查数据是否真的更新了
            if (self.last_analysis_time and 
                latest_quote.ts_event <= self.last_analysis_time.timestamp() * 1e9):
                return  # 数据没有更新，跳过分析
            
            # 检查止损止盈
            if self.current_position != 0:
                self._check_stop_loss_take_profit(latest_quote)
            
            # 分析价格趋势
            price_signal = self._analyze_price_trend(quote_ticks)
            
            # 分析成交量
            volume_signal = self._analyze_volume_trend(trade_ticks)
            
            # 分析买卖盘压力
            order_book_signal = self._analyze_order_book_pressure(quote_ticks)
            
            # 综合信号
            combined_signal = self._combine_signals(price_signal, volume_signal, order_book_signal)
            
            # 执行交易决策
            self._execute_trading_decision(latest_quote, combined_signal)
            
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
                'strategy_name': 'ETF159506 Intraday Strategy',
                'runtime': str(runtime),
                'signal_count': self.signal_count,
                'last_analysis_time': self.last_analysis_time.isoformat() if self.last_analysis_time else None,
                'analysis_mode': 'intraday-real-time',
                'trading_status': {
                    'current_position': self.current_position,
                    'entry_price': self.entry_price,
                    'entry_time': self.entry_time.isoformat() if self.entry_time else None,
                    'daily_trades': self.daily_trades,
                    'daily_pnl': self.daily_pnl,
                    'is_trading_time': self._is_trading_time()
                },
                'cache_status': {
                    'quote_count': quote_count,
                    'trade_count': trade_count,
                    'total_data': quote_count + trade_count
                },
                'parameters': {
                    'lookback_period': self.lookback_period,
                    'price_threshold': self.price_threshold,
                    'volume_threshold': self.volume_threshold,
                    'stop_loss_pct': self.stop_loss_pct,
                    'take_profit_pct': self.take_profit_pct,
                    'max_daily_trades': self.max_daily_trades
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
    
    def _is_trading_time(self) -> bool:
        """检查是否在交易时间内"""
        try:
            now = datetime.now()
            current_time = now.strftime('%H:%M')
            
            # 检查是否是新的一天
            if now.date() != self.today_date:
                self._reset_daily_stats()
            
            return self.trading_hours['start'] <= current_time <= self.trading_hours['end']
        except Exception as e:
            logger.error(f"检查交易时间失败: {e}")
            return False
    
    def _reset_daily_stats(self):
        """重置每日统计数据"""
        self.daily_trades = 0
        self.daily_pnl = 0.0
        self.today_date = datetime.now().date()
        self.current_position = 0
        self.entry_price = 0.0
        self.entry_time = None
        logger.info("每日统计数据已重置")
    
    def _check_stop_loss_take_profit(self, latest_quote: QuoteTick):
        """检查止损止盈"""
        try:
            current_price = float(latest_quote.bid_price)  # 用买价作为当前价格
            
            if self.current_position == 1:  # 多头仓位
                # 计算收益率
                pnl_pct = (current_price - self.entry_price) / self.entry_price
                
                # 止损检查
                if pnl_pct <= -self.stop_loss_pct:
                    self._close_position(latest_quote, "止损")
                    return
                
                # 止盈检查
                if pnl_pct >= self.take_profit_pct:
                    self._close_position(latest_quote, "止盈")
                    return
                    
            elif self.current_position == -1:  # 空头仓位
                # 计算收益率
                pnl_pct = (self.entry_price - current_price) / self.entry_price
                
                # 止损检查
                if pnl_pct <= -self.stop_loss_pct:
                    self._close_position(latest_quote, "止损")
                    return
                
                # 止盈检查
                if pnl_pct >= self.take_profit_pct:
                    self._close_position(latest_quote, "止盈")
                    return
                    
        except Exception as e:
            logger.error(f"检查止损止盈失败: {e}")
    
    def _execute_trading_decision(self, latest_quote: QuoteTick, combined_signal: Dict):
        """执行交易决策"""
        try:
            signal = combined_signal.get('signal', 'neutral')
            strength = combined_signal.get('strength', 0)
            
            # 只有信号强度大于50才考虑交易
            if strength < 50:
                return
            
            current_price = float(latest_quote.bid_price)
            
            if signal == 'bullish' and self.current_position == 0:
                # 开多头仓位
                self._open_long_position(latest_quote, combined_signal)
                
            elif signal == 'bearish' and self.current_position == 0:
                # 开空头仓位
                self._open_short_position(latest_quote, combined_signal)
                
            elif signal == 'bearish' and self.current_position == 1:
                # 多头平仓
                self._close_position(latest_quote, "信号反转")
                
            elif signal == 'bullish' and self.current_position == -1:
                # 空头平仓
                self._close_position(latest_quote, "信号反转")
                
        except Exception as e:
            logger.error(f"执行交易决策失败: {e}")
    
    def _open_long_position(self, latest_quote: QuoteTick, signal: Dict):
        """开多头仓位"""
        try:
            self.current_position = 1
            self.entry_price = float(latest_quote.ask_price)  # 用卖价买入
            self.entry_time = datetime.now()
            self.daily_trades += 1
            
            logger.warning(f"🚀 开多头仓位 - 价格: {self.entry_price:.3f}, "
                          f"信号强度: {signal.get('strength', 0):.1f}, "
                          f"今日交易次数: {self.daily_trades}")
            
        except Exception as e:
            logger.error(f"开多头仓位失败: {e}")
    
    def _open_short_position(self, latest_quote: QuoteTick, signal: Dict):
        """开空头仓位"""
        try:
            self.current_position = -1
            self.entry_price = float(latest_quote.bid_price)  # 用买价卖出
            self.entry_time = datetime.now()
            self.daily_trades += 1
            
            logger.warning(f"📉 开空头仓位 - 价格: {self.entry_price:.3f}, "
                          f"信号强度: {signal.get('strength', 0):.1f}, "
                          f"今日交易次数: {self.daily_trades}")
            
        except Exception as e:
            logger.error(f"开空头仓位失败: {e}")
    
    def _close_position(self, latest_quote: QuoteTick, reason: str):
        """平仓"""
        try:
            current_price = float(latest_quote.bid_price)
            
            if self.current_position == 1:  # 平多头
                exit_price = current_price
                pnl_pct = (exit_price - self.entry_price) / self.entry_price
                pnl_amount = pnl_pct * 10000  # 假设1万元仓位
                
            elif self.current_position == -1:  # 平空头
                exit_price = current_price
                pnl_pct = (self.entry_price - exit_price) / self.entry_price
                pnl_amount = pnl_pct * 10000  # 假设1万元仓位
                
            else:
                return
            
            # 更新每日盈亏
            self.daily_pnl += pnl_amount
            
            # 记录平仓信息
            logger.warning(f"💰 平仓 - {reason} - 入场价: {self.entry_price:.3f}, "
                          f"出场价: {exit_price:.3f}, 收益率: {pnl_pct*100:.2f}%, "
                          f"盈亏: {pnl_amount:.2f}元, 今日总盈亏: {self.daily_pnl:.2f}元")
            
            # 重置仓位状态
            self.current_position = 0
            self.entry_price = 0.0
            self.entry_time = None
            
        except Exception as e:
            logger.error(f"平仓失败: {e}")


def main():
    """主函数"""
    print("=" * 60)
    print("159506 ETF日内交易策略")
    print("=" * 60)
    print("功能:")
    print("1. 实时分析Cache中的市场数据")
    print("2. 价格趋势分析")
    print("3. 成交量分析")
    print("4. 买卖盘压力分析")
    print("5. 综合信号生成")
    print("6. 自动交易执行")
    print("7. 止损止盈管理")
    print("8. 日内交易统计")
    print("=" * 60)
    
    # 创建策略实例
    strategy = ETF159506CacheStrategy(use_redis=True)
    
    try:
        # 启动策略监控
        strategy.start_monitoring()
        
        print("策略监控已启动，按Ctrl+C退出")
        print("日内交易模式：实时分析 + 自动交易执行")
        print(f"交易时间: {strategy.trading_hours['start']} - {strategy.trading_hours['end']}")
        print(f"止损: {strategy.stop_loss_pct*100}%, 止盈: {strategy.take_profit_pct*100}%")
        print(f"每日最大交易次数: {strategy.max_daily_trades}")
        
        # 主循环
        while True:
            time.sleep(30)  # 每30秒显示一次状态（更频繁的状态更新）
            
            # 显示策略状态
            status = strategy.get_strategy_status()
            trading_status = status.get('trading_status', {})
            print(f"策略状态: 运行{status.get('runtime', 'N/A')}, "
                  f"分析次数={status.get('signal_count', 0)}, "
                  f"数据量={status.get('cache_status', {}).get('total_data', 0)}")
            print(f"交易状态: 仓位={trading_status.get('current_position', 0)}, "
                  f"今日交易={trading_status.get('daily_trades', 0)}次, "
                  f"今日盈亏={trading_status.get('daily_pnl', 0):.2f}元")
            print(f"日内交易模式: 实时分析 + 自动交易执行")
            
    except KeyboardInterrupt:
        print("\n用户中断，正在停止策略...")
        strategy.stop_monitoring()
        
        # 显示最终状态
        final_status = strategy.get_strategy_status()
        final_trading = final_status.get('trading_status', {})
        print(f"\n最终策略状态:")
        print(f"总分析次数: {final_status.get('signal_count', 0)}")
        print(f"运行时间: {final_status.get('runtime', 'N/A')}")
        print(f"缓存数据量: {final_status.get('cache_status', {}).get('total_data', 0)}")
        print(f"今日交易次数: {final_trading.get('daily_trades', 0)}")
        print(f"今日总盈亏: {final_trading.get('daily_pnl', 0):.2f}元")
        
        print("策略已停止")
    
    except Exception as e:
        logger.error(f"策略运行错误: {e}")
        strategy.stop_monitoring()


if __name__ == "__main__":
    main() 