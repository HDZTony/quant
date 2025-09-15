#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
159506 ETF 风险管理和监控系统
基于NautilusTrader风险管理架构
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Callable
from decimal import Decimal
from dataclasses import dataclass
from enum import Enum

import pandas as pd
import numpy as np
from nautilus_trader.model.identifiers import InstrumentId, OrderId, PositionId
from nautilus_trader.model.objects import Money, Price, Quantity
from nautilus_trader.model.enums import OrderSide, PositionSide
from nautilus_trader.model.events import OrderEvent, PositionEvent, AccountState
from nautilus_trader.model.position import Position
from nautilus_trader.model.orders import Order

logger = logging.getLogger(__name__)


class RiskLevel(Enum):
    """风险等级"""
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


@dataclass
class RiskMetrics:
    """风险指标"""
    timestamp: datetime
    instrument_id: InstrumentId
    
    # 持仓风险
    position_size: int
    position_value: float
    unrealized_pnl: float
    unrealized_pnl_pct: float
    
    # 账户风险
    account_balance: float
    account_equity: float
    margin_used: float
    margin_available: float
    margin_ratio: float
    
    # 交易风险
    daily_pnl: float
    daily_trades: int
    daily_volume: float
    max_drawdown: float
    max_drawdown_pct: float
    
    # 市场风险
    price_volatility: float
    volume_ratio: float
    
    # 风险等级
    risk_level: RiskLevel
    risk_score: float


@dataclass
class RiskAlert:
    """风险警报"""
    timestamp: datetime
    alert_type: str
    severity: RiskLevel
    message: str
    instrument_id: Optional[InstrumentId] = None
    order_id: Optional[OrderId] = None
    position_id: Optional[PositionId] = None
    data: Optional[Dict[str, Any]] = None


class RiskManager:
    """风险管理器"""
    
    def __init__(
        self,
        max_position_size: int = 100000,
        max_daily_loss: float = 5000.0,
        max_drawdown_pct: float = 0.10,
        max_margin_ratio: float = 0.80,
        position_size_limit_pct: float = 0.20,
        volatility_threshold: float = 0.05,
    ):
        """
        初始化风险管理器
        
        Parameters
        ----------
        max_position_size : int
            最大持仓数量
        max_daily_loss : float
            最大日亏损
        max_drawdown_pct : float
            最大回撤百分比
        max_margin_ratio : float
            最大保证金比例
        position_size_limit_pct : float
            持仓限制百分比
        volatility_threshold : float
            波动率阈值
        """
        self.max_position_size = max_position_size
        self.max_daily_loss = max_daily_loss
        self.max_drawdown_pct = max_drawdown_pct
        self.max_margin_ratio = max_margin_ratio
        self.position_size_limit_pct = position_size_limit_pct
        self.volatility_threshold = volatility_threshold
        
        # 风险数据存储
        self.risk_metrics_history: List[RiskMetrics] = []
        self.risk_alerts: List[RiskAlert] = []
        self.daily_stats: Dict[str, Any] = {}
        
        # 回调函数
        self.alert_callbacks: List[Callable[[RiskAlert], None]] = []
        
        logger.info(f"风险管理器初始化: max_position_size={max_position_size}, max_daily_loss={max_daily_loss}")
    
    def add_alert_callback(self, callback: Callable[[RiskAlert], None]) -> None:
        """添加警报回调函数"""
        self.alert_callbacks.append(callback)
    
    def calculate_risk_metrics(
        self,
        instrument_id: InstrumentId,
        position: Optional[Position],
        account_state: Optional[AccountState],
        current_price: float,
        price_history: List[float],
        volume_history: List[float],
    ) -> RiskMetrics:
        """计算风险指标"""
        try:
            timestamp = datetime.now(timezone.utc)
            
            # 持仓风险
            position_size = position.quantity.as_int() if position else 0
            position_value = position_size * current_price
            unrealized_pnl = 0.0
            unrealized_pnl_pct = 0.0
            
            if position and position.quantity.as_int() != 0:
                entry_price = position.avg_px_open.as_double()
                unrealized_pnl = (current_price - entry_price) * position_size
                unrealized_pnl_pct = unrealized_pnl / (entry_price * position_size) if entry_price != 0 else 0
            
            # 账户风险
            account_balance = account_state.balance_total().as_double() if account_state else 0.0
            account_equity = account_balance + unrealized_pnl
            margin_used = position_value * 0.1  # 假设10%保证金
            margin_available = account_balance - margin_used
            margin_ratio = margin_used / account_balance if account_balance > 0 else 0
            
            # 交易风险
            daily_pnl = self.daily_stats.get('pnl', 0.0)
            daily_trades = self.daily_stats.get('trades', 0)
            daily_volume = self.daily_stats.get('volume', 0.0)
            
            # 计算最大回撤
            max_drawdown, max_drawdown_pct = self._calculate_max_drawdown(account_balance)
            
            # 市场风险
            price_volatility = self._calculate_volatility(price_history)
            volume_ratio = self._calculate_volume_ratio(volume_history)
            
            # 计算风险等级和分数
            risk_level, risk_score = self._calculate_risk_level(
                position_size, position_value, unrealized_pnl_pct,
                account_balance, margin_ratio, daily_pnl,
                max_drawdown_pct, price_volatility
            )
            
            metrics = RiskMetrics(
                timestamp=timestamp,
                instrument_id=instrument_id,
                position_size=position_size,
                position_value=position_value,
                unrealized_pnl=unrealized_pnl,
                unrealized_pnl_pct=unrealized_pnl_pct,
                account_balance=account_balance,
                account_equity=account_equity,
                margin_used=margin_used,
                margin_available=margin_available,
                margin_ratio=margin_ratio,
                daily_pnl=daily_pnl,
                daily_trades=daily_trades,
                daily_volume=daily_volume,
                max_drawdown=max_drawdown,
                max_drawdown_pct=max_drawdown_pct,
                price_volatility=price_volatility,
                volume_ratio=volume_ratio,
                risk_level=risk_level,
                risk_score=risk_score,
            )
            
            # 添加到历史记录
            self.risk_metrics_history.append(metrics)
            
            # 保持历史记录在合理范围内
            if len(self.risk_metrics_history) > 1000:
                self.risk_metrics_history = self.risk_metrics_history[-1000:]
            
            return metrics
            
        except Exception as e:
            logger.error(f"计算风险指标失败: {e}")
            raise
    
    def _calculate_max_drawdown(self, current_balance: float) -> tuple[float, float]:
        """计算最大回撤"""
        try:
            if not self.risk_metrics_history:
                return 0.0, 0.0
            
            # 获取历史余额
            balances = [m.account_balance for m in self.risk_metrics_history]
            balances.append(current_balance)
            
            # 计算峰值和回撤
            peak = balances[0]
            max_drawdown = 0.0
            
            for balance in balances:
                if balance > peak:
                    peak = balance
                drawdown = peak - balance
                if drawdown > max_drawdown:
                    max_drawdown = drawdown
            
            max_drawdown_pct = max_drawdown / peak if peak > 0 else 0
            
            return max_drawdown, max_drawdown_pct
            
        except Exception as e:
            logger.error(f"计算最大回撤失败: {e}")
            return 0.0, 0.0
    
    def _calculate_volatility(self, price_history: List[float]) -> float:
        """计算价格波动率"""
        try:
            if len(price_history) < 2:
                return 0.0
            
            # 计算收益率
            returns = []
            for i in range(1, len(price_history)):
                if price_history[i-1] != 0:
                    ret = (price_history[i] - price_history[i-1]) / price_history[i-1]
                    returns.append(ret)
            
            if not returns:
                return 0.0
            
            # 计算标准差
            volatility = np.std(returns)
            return volatility
            
        except Exception as e:
            logger.error(f"计算波动率失败: {e}")
            return 0.0
    
    def _calculate_volume_ratio(self, volume_history: List[float]) -> float:
        """计算成交量比率"""
        try:
            if len(volume_history) < 2:
                return 1.0
            
            # 计算平均成交量
            avg_volume = np.mean(volume_history[:-1])
            current_volume = volume_history[-1]
            
            if avg_volume == 0:
                return 1.0
            
            return current_volume / avg_volume
            
        except Exception as e:
            logger.error(f"计算成交量比率失败: {e}")
            return 1.0
    
    def _calculate_risk_level(
        self,
        position_size: int,
        position_value: float,
        unrealized_pnl_pct: float,
        account_balance: float,
        margin_ratio: float,
        daily_pnl: float,
        max_drawdown_pct: float,
        price_volatility: float,
    ) -> tuple[RiskLevel, float]:
        """计算风险等级和分数"""
        try:
            risk_score = 0.0
            
            # 持仓风险 (0-25分)
            position_risk = min(abs(position_size) / self.max_position_size, 1.0) * 25
            risk_score += position_risk
            
            # 盈亏风险 (0-25分)
            pnl_risk = min(abs(unrealized_pnl_pct) / 0.1, 1.0) * 25  # 10%为满分
            risk_score += pnl_risk
            
            # 保证金风险 (0-25分)
            margin_risk = min(margin_ratio / self.max_margin_ratio, 1.0) * 25
            risk_score += margin_risk
            
            # 回撤风险 (0-25分)
            drawdown_risk = min(max_drawdown_pct / self.max_drawdown_pct, 1.0) * 25
            risk_score += drawdown_risk
            
            # 确定风险等级
            if risk_score >= 75:
                risk_level = RiskLevel.CRITICAL
            elif risk_score >= 50:
                risk_level = RiskLevel.HIGH
            elif risk_score >= 25:
                risk_level = RiskLevel.MEDIUM
            else:
                risk_level = RiskLevel.LOW
            
            return risk_level, risk_score
            
        except Exception as e:
            logger.error(f"计算风险等级失败: {e}")
            return RiskLevel.LOW, 0.0
    
    def check_order_risk(self, order: Order, current_position: Optional[Position]) -> bool:
        """检查订单风险"""
        try:
            # 检查持仓大小限制
            if order.side == OrderSide.BUY:
                new_position_size = (current_position.quantity.as_int() if current_position else 0) + order.quantity.as_int()
            else:
                new_position_size = (current_position.quantity.as_int() if current_position else 0) - order.quantity.as_int()
            
            if abs(new_position_size) > self.max_position_size:
                self._create_alert(
                    alert_type="POSITION_SIZE_LIMIT",
                    severity=RiskLevel.HIGH,
                    message=f"订单将导致持仓超过限制: {new_position_size} > {self.max_position_size}",
                    order_id=order.order_id,
                    data={"new_position_size": new_position_size, "limit": self.max_position_size}
                )
                return False
            
            # 检查日亏损限制
            daily_pnl = self.daily_stats.get('pnl', 0.0)
            if daily_pnl < -self.max_daily_loss:
                self._create_alert(
                    alert_type="DAILY_LOSS_LIMIT",
                    severity=RiskLevel.HIGH,
                    message=f"日亏损超过限制: {daily_pnl} < -{self.max_daily_loss}",
                    order_id=order.order_id,
                    data={"daily_pnl": daily_pnl, "limit": -self.max_daily_loss}
                )
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"检查订单风险失败: {e}")
            return False
    
    def check_position_risk(self, position: Position, current_price: float) -> List[RiskAlert]:
        """检查持仓风险"""
        alerts = []
        
        try:
            # 检查持仓大小
            position_size = abs(position.quantity.as_int())
            if position_size > self.max_position_size:
                alerts.append(self._create_alert(
                    alert_type="POSITION_SIZE_EXCEEDED",
                    severity=RiskLevel.HIGH,
                    message=f"持仓大小超过限制: {position_size} > {self.max_position_size}",
                    position_id=position.id,
                    data={"position_size": position_size, "limit": self.max_position_size}
                ))
            
            # 检查盈亏
            entry_price = position.avg_px_open.as_double()
            unrealized_pnl_pct = (current_price - entry_price) / entry_price if entry_price != 0 else 0
            
            if abs(unrealized_pnl_pct) > 0.05:  # 5%盈亏阈值
                alerts.append(self._create_alert(
                    alert_type="UNREALIZED_PNL_WARNING",
                    severity=RiskLevel.MEDIUM,
                    message=f"未实现盈亏较大: {unrealized_pnl_pct:.2%}",
                    position_id=position.id,
                    data={"unrealized_pnl_pct": unrealized_pnl_pct}
                ))
            
            return alerts
            
        except Exception as e:
            logger.error(f"检查持仓风险失败: {e}")
            return []
    
    def _create_alert(
        self,
        alert_type: str,
        severity: RiskLevel,
        message: str,
        instrument_id: Optional[InstrumentId] = None,
        order_id: Optional[OrderId] = None,
        position_id: Optional[PositionId] = None,
        data: Optional[Dict[str, Any]] = None,
    ) -> RiskAlert:
        """创建风险警报"""
        alert = RiskAlert(
            timestamp=datetime.now(timezone.utc),
            alert_type=alert_type,
            severity=severity,
            message=message,
            instrument_id=instrument_id,
            order_id=order_id,
            position_id=position_id,
            data=data,
        )
        
        # 添加到警报列表
        self.risk_alerts.append(alert)
        
        # 保持警报列表在合理范围内
        if len(self.risk_alerts) > 500:
            self.risk_alerts = self.risk_alerts[-500:]
        
        # 调用回调函数
        for callback in self.alert_callbacks:
            try:
                callback(alert)
            except Exception as e:
                logger.error(f"风险警报回调失败: {e}")
        
        logger.warning(f"风险警报: {severity.value} - {message}")
        
        return alert
    
    def update_daily_stats(self, stats: Dict[str, Any]) -> None:
        """更新每日统计"""
        self.daily_stats.update(stats)
    
    def reset_daily_stats(self) -> None:
        """重置每日统计"""
        self.daily_stats = {
            'pnl': 0.0,
            'trades': 0,
            'volume': 0.0,
            'start_balance': 0.0,
        }
    
    def get_risk_summary(self) -> Dict[str, Any]:
        """获取风险摘要"""
        try:
            if not self.risk_metrics_history:
                return {"error": "没有风险数据"}
            
            latest_metrics = self.risk_metrics_history[-1]
            
            # 统计警报
            alert_counts = {}
            for alert in self.risk_alerts:
                alert_type = alert.alert_type
                if alert_type not in alert_counts:
                    alert_counts[alert_type] = 0
                alert_counts[alert_type] += 1
            
            return {
                "timestamp": latest_metrics.timestamp.isoformat(),
                "risk_level": latest_metrics.risk_level.value,
                "risk_score": latest_metrics.risk_score,
                "position_size": latest_metrics.position_size,
                "position_value": latest_metrics.position_value,
                "unrealized_pnl": latest_metrics.unrealized_pnl,
                "unrealized_pnl_pct": latest_metrics.unrealized_pnl_pct,
                "account_balance": latest_metrics.account_balance,
                "margin_ratio": latest_metrics.margin_ratio,
                "daily_pnl": latest_metrics.daily_pnl,
                "max_drawdown_pct": latest_metrics.max_drawdown_pct,
                "price_volatility": latest_metrics.price_volatility,
                "volume_ratio": latest_metrics.volume_ratio,
                "alert_counts": alert_counts,
                "total_alerts": len(self.risk_alerts),
            }
            
        except Exception as e:
            logger.error(f"获取风险摘要失败: {e}")
            return {"error": str(e)}


class RiskMonitor:
    """风险监控器"""
    
    def __init__(self, risk_manager: RiskManager):
        self.risk_manager = risk_manager
        self.monitoring_task: Optional[asyncio.Task] = None
        self.is_monitoring = False
    
    async def start_monitoring(self, interval_seconds: int = 60) -> None:
        """开始风险监控"""
        try:
            logger.info(f"开始风险监控，间隔: {interval_seconds}秒")
            
            self.is_monitoring = True
            self.monitoring_task = asyncio.create_task(
                self._monitoring_loop(interval_seconds)
            )
            
        except Exception as e:
            logger.error(f"启动风险监控失败: {e}")
            raise
    
    async def stop_monitoring(self) -> None:
        """停止风险监控"""
        try:
            logger.info("停止风险监控...")
            
            self.is_monitoring = False
            
            if self.monitoring_task and not self.monitoring_task.done():
                self.monitoring_task.cancel()
                try:
                    await self.monitoring_task
                except asyncio.CancelledError:
                    pass
            
            logger.info("风险监控已停止")
            
        except Exception as e:
            logger.error(f"停止风险监控失败: {e}")
    
    async def _monitoring_loop(self, interval_seconds: int) -> None:
        """监控循环"""
        try:
            while self.is_monitoring:
                try:
                    # 执行风险检查
                    await self._perform_risk_checks()
                    
                    # 等待下次检查
                    await asyncio.sleep(interval_seconds)
                    
                except asyncio.CancelledError:
                    logger.info("风险监控循环被取消")
                    break
                except Exception as e:
                    logger.error(f"风险监控循环发生错误: {e}")
                    await asyncio.sleep(interval_seconds)
            
        except Exception as e:
            logger.error(f"风险监控循环失败: {e}")
    
    async def _perform_risk_checks(self) -> None:
        """执行风险检查"""
        try:
            # 这里可以添加定期风险检查逻辑
            # 例如检查市场条件、系统状态等
            
            logger.debug("执行定期风险检查...")
            
        except Exception as e:
            logger.error(f"执行风险检查失败: {e}")


def create_default_risk_manager() -> RiskManager:
    """创建默认风险管理器"""
    return RiskManager()


def create_conservative_risk_manager() -> RiskManager:
    """创建保守风险管理器"""
    return RiskManager(
        max_position_size=50000,
        max_daily_loss=2000.0,
        max_drawdown_pct=0.05,
        max_margin_ratio=0.60,
        position_size_limit_pct=0.10,
        volatility_threshold=0.03,
    )


def create_aggressive_risk_manager() -> RiskManager:
    """创建激进风险管理器"""
    return RiskManager(
        max_position_size=200000,
        max_daily_loss=10000.0,
        max_drawdown_pct=0.20,
        max_margin_ratio=0.90,
        position_size_limit_pct=0.30,
        volatility_threshold=0.08,
    )


# 使用示例
if __name__ == "__main__":
    print("159506 ETF 风险管理和监控系统示例")
    print("=" * 60)
    
    # 1. 创建风险管理器
    print("\n1. 创建风险管理器:")
    risk_manager = create_default_risk_manager()
    print(f"   最大持仓数量: {risk_manager.max_position_size}")
    print(f"   最大日亏损: {risk_manager.max_daily_loss}")
    print(f"   最大回撤: {risk_manager.max_drawdown_pct:.1%}")
    print(f"   最大保证金比例: {risk_manager.max_margin_ratio:.1%}")
    
    # 2. 测试风险指标计算
    print("\n2. 测试风险指标计算:")
    from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue
    
    instrument_id = InstrumentId(Symbol("159506"), Venue("SZSE"))
    price_history = [1.0, 1.01, 0.99, 1.02, 1.0]
    volume_history = [1000, 1200, 800, 1500, 1000]
    
    metrics = risk_manager.calculate_risk_metrics(
        instrument_id=instrument_id,
        position=None,
        account_state=None,
        current_price=1.0,
        price_history=price_history,
        volume_history=volume_history,
    )
    
    print(f"   风险等级: {metrics.risk_level.value}")
    print(f"   风险分数: {metrics.risk_score:.2f}")
    print(f"   价格波动率: {metrics.price_volatility:.4f}")
    print(f"   成交量比率: {metrics.volume_ratio:.2f}")
    
    # 3. 测试风险警报
    print("\n3. 测试风险警报:")
    def alert_callback(alert: RiskAlert):
        print(f"   收到警报: {alert.severity.value} - {alert.message}")
    
    risk_manager.add_alert_callback(alert_callback)
    
    # 模拟高风险情况
    risk_manager._create_alert(
        alert_type="TEST_ALERT",
        severity=RiskLevel.HIGH,
        message="测试高风险警报",
        instrument_id=instrument_id,
    )
    
    # 4. 获取风险摘要
    print("\n4. 风险摘要:")
    summary = risk_manager.get_risk_summary()
    print(f"   风险等级: {summary['risk_level']}")
    print(f"   风险分数: {summary['risk_score']:.2f}")
    print(f"   总警报数: {summary['total_alerts']}")
    
    print("\n✅ 风险管理和监控系统示例完成！")
