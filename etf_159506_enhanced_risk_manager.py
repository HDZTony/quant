#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
159506 ETF增强风险管理模块
包含动态止损、仓位管理、风险预算等功能
"""

import logging
import math
from datetime import datetime, time, timedelta
from typing import Dict, List, Optional, Tuple
from decimal import Decimal
import numpy as np

logger = logging.getLogger(__name__)


class EnhancedRiskManager:
    """增强风险管理器"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.logger = logging.getLogger("EnhancedRiskManager")
        
        # 基础风险参数
        self.max_position_size = config.get('max_position_size', Decimal("10000"))
        self.max_daily_loss = config.get('max_daily_loss', Decimal("1000"))
        self.max_drawdown = config.get('max_drawdown', 0.1)  # 10%
        self.risk_per_trade = config.get('risk_per_trade', 0.02)  # 2%
        
        # 动态止损参数
        self.trailing_stop = config.get('trailing_stop', True)
        self.trailing_stop_pct = config.get('trailing_stop_pct', 0.015)  # 1.5%
        self.atr_multiplier = config.get('atr_multiplier', 2.0)
        
        # 仓位管理
        self.position_sizing_method = config.get('position_sizing_method', 'kelly')
        self.kelly_fraction = config.get('kelly_fraction', 0.25)  # 使用25%的凯利公式
        
        # 风险预算
        self.risk_budget = config.get('risk_budget', Decimal("10000"))
        self.used_risk = Decimal("0")
        
        # 交易状态
        self.current_position = 0
        self.entry_price = 0.0
        self.entry_time = None
        self.highest_price = 0.0
        self.lowest_price = float('inf')
        self.daily_pnl = Decimal("0")
        self.total_pnl = Decimal("0")
        self.max_equity = Decimal("0")
        self.current_equity = Decimal("0")
        
        # 历史数据
        self.trade_history = []
        self.price_history = []
        self.volatility_history = []
        
        # 风险指标
        self.current_drawdown = 0.0
        self.sharpe_ratio = 0.0
        self.max_drawdown_ever = 0.0
        
    def calculate_position_size(self, price: float, stop_loss_pct: float, 
                              win_rate: float = 0.5, avg_win: float = 0.02, 
                              avg_loss: float = 0.01) -> Decimal:
        """计算仓位大小"""
        try:
            if self.position_sizing_method == 'fixed':
                return self.max_position_size
                
            elif self.position_sizing_method == 'kelly':
                return self._kelly_position_size(price, win_rate, avg_win, avg_loss)
                
            elif self.position_sizing_method == 'risk_based':
                return self._risk_based_position_size(price, stop_loss_pct)
                
            elif self.position_sizing_method == 'volatility':
                return self._volatility_based_position_size(price)
                
            else:
                return self.max_position_size
                
        except Exception as e:
            self.logger.error(f"计算仓位大小失败: {e}")
            return Decimal("1000")  # 默认最小仓位
    
    def _kelly_position_size(self, price: float, win_rate: float, 
                           avg_win: float, avg_loss: float) -> Decimal:
        """凯利公式计算仓位"""
        try:
            # 凯利公式: f = (bp - q) / b
            # 其中 b = 平均盈利/平均亏损, p = 胜率, q = 败率
            if avg_loss <= 0:
                return Decimal("1000")
            
            # 转换为float进行计算
            b = float(avg_win) / float(avg_loss)
            p = float(win_rate)
            q = 1.0 - float(win_rate)
            
            kelly_fraction = (b * p - q) / b
            
            # 使用部分凯利公式
            kelly_fraction *= float(self.kelly_fraction)
            
            if kelly_fraction <= 0:
                return Decimal("1000")
            
            # 计算仓位大小
            position_value = float(self.current_equity) * kelly_fraction
            position_size = position_value / price
            
            return min(Decimal(str(position_size)), self.max_position_size)
            
        except Exception as e:
            self.logger.error(f"凯利公式计算失败: {e}")
            return Decimal("1000")
    
    def _risk_based_position_size(self, price: float, stop_loss_pct: float) -> Decimal:
        """基于风险计算仓位"""
        try:
            risk_amount = self.current_equity * self.risk_per_trade
            position_size = risk_amount / (price * stop_loss_pct)
            
            return min(position_size, self.max_position_size)
            
        except Exception as e:
            self.logger.error(f"风险计算仓位失败: {e}")
            return Decimal("1000")
    
    def _volatility_based_position_size(self, price: float) -> Decimal:
        """基于波动率计算仓位"""
        try:
            if len(self.volatility_history) < 20:
                return Decimal("1000")
            
            # 计算历史波动率
            recent_volatility = np.mean(self.volatility_history[-20:])
            
            # 波动率越高，仓位越小
            volatility_factor = 1.0 / (1.0 + recent_volatility * 10)
            position_size = self.max_position_size * volatility_factor
            
            return max(position_size, Decimal("1000"))
            
        except Exception as e:
            self.logger.error(f"波动率计算仓位失败: {e}")
            return Decimal("1000")
    
    def calculate_dynamic_stop_loss(self, current_price: float, 
                                  atr: float = None) -> float:
        """计算动态止损价格"""
        try:
            if self.current_position == 0:
                return 0.0
            
            if self.trailing_stop and self.current_position == 1:
                # 多头动态止损
                if current_price > self.highest_price:
                    self.highest_price = current_price
                
                # 基于最高价的动态止损
                trailing_stop_price = self.highest_price * (1 - self.trailing_stop_pct)
                
                # 基于ATR的动态止损
                if atr:
                    atr_stop_price = current_price - (atr * self.atr_multiplier)
                    return max(trailing_stop_price, atr_stop_price)
                
                return trailing_stop_price
                
            elif self.trailing_stop and self.current_position == -1:
                # 空头动态止损
                if current_price < self.lowest_price:
                    self.lowest_price = current_price
                
                # 基于最低价的动态止损
                trailing_stop_price = self.lowest_price * (1 + self.trailing_stop_pct)
                
                # 基于ATR的动态止损
                if atr:
                    atr_stop_price = current_price + (atr * self.atr_multiplier)
                    return min(trailing_stop_price, atr_stop_price)
                
                return trailing_stop_price
            
            else:
                # 固定止损
                if self.current_position == 1:
                    return self.entry_price * (1 - self.config.get('stop_loss_pct', 0.02))
                else:
                    return self.entry_price * (1 + self.config.get('stop_loss_pct', 0.02))
                    
        except Exception as e:
            self.logger.error(f"计算动态止损失败: {e}")
            return 0.0
    
    def update_risk_metrics(self, current_price: float) -> Dict:
        """更新风险指标"""
        try:
            # 更新当前权益
            if self.current_position == 1:
                self.current_equity = self.total_pnl + (current_price - self.entry_price) * self.current_position
            elif self.current_position == -1:
                self.current_equity = self.total_pnl + (self.entry_price - current_price) * abs(self.current_position)
            else:
                self.current_equity = self.total_pnl
            
            # 更新最大权益
            if self.current_equity > self.max_equity:
                self.max_equity = self.current_equity
            
            # 计算回撤
            if self.max_equity > 0:
                self.current_drawdown = (self.max_equity - self.current_equity) / self.max_equity
                if self.current_drawdown > self.max_drawdown_ever:
                    self.max_drawdown_ever = self.current_drawdown
            
            # 计算夏普比率
            if len(self.trade_history) > 10:
                returns = [trade['return'] for trade in self.trade_history[-20:]]
                if returns:
                    avg_return = np.mean(returns)
                    std_return = np.std(returns)
                    if std_return > 0:
                        self.sharpe_ratio = avg_return / std_return * math.sqrt(252)  # 年化
            
            return {
                'current_equity': float(self.current_equity),
                'max_equity': float(self.max_equity),
                'current_drawdown': self.current_drawdown,
                'max_drawdown_ever': self.max_drawdown_ever,
                'sharpe_ratio': self.sharpe_ratio,
                'daily_pnl': float(self.daily_pnl),
                'total_pnl': float(self.total_pnl)
            }
            
        except Exception as e:
            self.logger.error(f"更新风险指标失败: {e}")
            return {}
    
    def check_risk_limits(self) -> Tuple[bool, str]:
        """检查风险限制"""
        try:
            # 检查日亏损限制
            if self.daily_pnl < -self.max_daily_loss:
                return False, f"日亏损超限: {self.daily_pnl}"
            
            # 检查回撤限制
            if self.current_drawdown > self.max_drawdown:
                return False, f"回撤超限: {self.current_drawdown:.2%}"
            
            # 检查风险预算
            if self.used_risk > self.risk_budget:
                return False, f"风险预算超限: {self.used_risk}"
            
            return True, "风险检查通过"
            
        except Exception as e:
            self.logger.error(f"风险限制检查失败: {e}")
            return False, f"检查失败: {e}"
    
    def open_position(self, price: float, side: int, stop_loss_pct: float = 0.02) -> bool:
        """开仓"""
        try:
            # 检查风险限制
            can_trade, reason = self.check_risk_limits()
            if not can_trade:
                self.logger.warning(f"风险限制阻止开仓: {reason}")
                return False
            
            # 计算仓位大小
            position_size = self.calculate_position_size(price, stop_loss_pct)
            
            # 更新状态
            self.current_position = side
            self.entry_price = price
            self.entry_time = datetime.now()
            self.highest_price = price if side == 1 else float('inf')
            self.lowest_price = price if side == -1 else 0.0
            
            # 记录交易
            trade_record = {
                'timestamp': self.entry_time,
                'side': side,
                'price': price,
                'size': float(position_size),
                'type': 'open'
            }
            self.trade_history.append(trade_record)
            
            self.logger.info(f"开仓成功 - 方向: {'多头' if side == 1 else '空头'}, "
                           f"价格: {price:.3f}, 大小: {position_size}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"开仓失败: {e}")
            return False
    
    def close_position(self, price: float, reason: str) -> bool:
        """平仓"""
        try:
            if self.current_position == 0:
                return False
            
            # 计算盈亏
            if self.current_position == 1:
                pnl = (price - self.entry_price) * abs(self.current_position)
            else:
                pnl = (self.entry_price - price) * abs(self.current_position)
            
            # 更新统计
            self.daily_pnl += pnl
            self.total_pnl += pnl
            
            # 记录交易
            trade_record = {
                'timestamp': datetime.now(),
                'side': -self.current_position,
                'price': price,
                'size': abs(self.current_position),
                'type': 'close',
                'pnl': float(pnl),
                'return': float(pnl / self.entry_price) if self.entry_price > 0 else 0
            }
            self.trade_history.append(trade_record)
            
            # 重置状态
            self.current_position = 0
            self.entry_price = 0.0
            self.entry_time = None
            self.highest_price = 0.0
            self.lowest_price = float('inf')
            
            self.logger.info(f"平仓成功 - 原因: {reason}, 盈亏: {pnl:.2f}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"平仓失败: {e}")
            return False
    
    def update_price_data(self, price: float, volume: int = 0) -> None:
        """更新价格数据"""
        try:
            self.price_history.append({
                'timestamp': datetime.now(),
                'price': price,
                'volume': volume
            })
            
            # 保持最近1000个数据点
            if len(self.price_history) > 1000:
                self.price_history.pop(0)
            
            # 计算波动率
            if len(self.price_history) > 20:
                prices = [d['price'] for d in self.price_history[-20:]]
                returns = [(prices[i] - prices[i-1]) / prices[i-1] for i in range(1, len(prices))]
                volatility = np.std(returns)
                self.volatility_history.append(volatility)
                
                # 保持最近100个波动率数据
                if len(self.volatility_history) > 100:
                    self.volatility_history.pop(0)
                    
        except Exception as e:
            self.logger.error(f"更新价格数据失败: {e}")
    
    def get_risk_report(self) -> Dict:
        """获取风险报告"""
        try:
            return {
                'position_status': {
                    'current_position': self.current_position,
                    'entry_price': self.entry_price,
                    'entry_time': self.entry_time.isoformat() if self.entry_time else None,
                    'highest_price': self.highest_price,
                    'lowest_price': self.lowest_price
                },
                'pnl_status': {
                    'daily_pnl': float(self.daily_pnl),
                    'total_pnl': float(self.total_pnl),
                    'current_equity': float(self.current_equity),
                    'max_equity': float(self.max_equity)
                },
                'risk_metrics': {
                    'current_drawdown': self.current_drawdown,
                    'max_drawdown_ever': self.max_drawdown_ever,
                    'sharpe_ratio': self.sharpe_ratio,
                    'volatility': np.mean(self.volatility_history[-20:]) if self.volatility_history else 0
                },
                'risk_limits': {
                    'max_daily_loss': float(self.max_daily_loss),
                    'max_drawdown': self.max_drawdown,
                    'risk_budget': float(self.risk_budget),
                    'used_risk': float(self.used_risk)
                },
                'trade_statistics': {
                    'total_trades': len(self.trade_history),
                    'win_rate': self._calculate_win_rate(),
                    'avg_win': self._calculate_avg_win(),
                    'avg_loss': self._calculate_avg_loss()
                }
            }
            
        except Exception as e:
            self.logger.error(f"生成风险报告失败: {e}")
            return {}
    
    def _calculate_win_rate(self) -> float:
        """计算胜率"""
        try:
            closed_trades = [t for t in self.trade_history if t['type'] == 'close']
            if not closed_trades:
                return 0.0
            
            winning_trades = [t for t in closed_trades if t.get('pnl', 0) > 0]
            return len(winning_trades) / len(closed_trades)
            
        except Exception as e:
            self.logger.error(f"计算胜率失败: {e}")
            return 0.0
    
    def _calculate_avg_win(self) -> float:
        """计算平均盈利"""
        try:
            closed_trades = [t for t in self.trade_history if t['type'] == 'close' and t.get('pnl', 0) > 0]
            if not closed_trades:
                return 0.0
            
            return np.mean([t['pnl'] for t in closed_trades])
            
        except Exception as e:
            self.logger.error(f"计算平均盈利失败: {e}")
            return 0.0
    
    def _calculate_avg_loss(self) -> float:
        """计算平均亏损"""
        try:
            closed_trades = [t for t in self.trade_history if t['type'] == 'close' and t.get('pnl', 0) < 0]
            if not closed_trades:
                return 0.0
            
            return abs(np.mean([t['pnl'] for t in closed_trades]))
            
        except Exception as e:
            self.logger.error(f"计算平均亏损失败: {e}")
            return 0.0
    
    def reset_daily_stats(self) -> None:
        """重置每日统计"""
        self.daily_pnl = Decimal("0")
        self.logger.info("每日统计数据已重置") 