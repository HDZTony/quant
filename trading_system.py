#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
jvQuant交易系统
支持登录柜台、交易委托、撤销委托、查询交易、查询持仓等功能
"""

import requests
import json
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import threading


class JVQuantTradingClient:
    """jvQuant交易客户端"""
    
    def __init__(self, token: str):
        self.token = token
        self.trade_server = None
        self.ticket = None
        self.ticket_expire = None
        self.is_logged_in = False
        
        # 获取交易服务器地址
        self._get_trade_server()
    
    def _get_trade_server(self) -> bool:
        """获取交易服务器地址"""
        try:
            url = f"http://jvQuant.com/query/server?market=ab&type=trade&token={self.token}"
            response = requests.get(url, timeout=10)
            data = response.json()
            
            if data.get("code") == "0":
                self.trade_server = data.get("server")
                print(f"✅ 获取交易服务器成功: {self.trade_server}")
                return True
            else:
                print(f"❌ 获取交易服务器失败: {data}")
                return False
                
        except Exception as e:
            print(f"❌ 获取交易服务器异常: {e}")
            return False
    
    def login(self, account: str, password: str) -> bool:
        """
        登录交易柜台
        
        Parameters
        ----------
        account : str
            资金账号
        password : str
            资金密码
        
        Returns
        -------
        bool
            登录是否成功
        """
        if not self.trade_server:
            print("❌ 交易服务器地址未获取")
            return False
        
        try:
            url = f"http://{self.trade_server}/login"
            params = {
                'token': self.token,
                'acc': account, #541460031518
                'pass': password #882200
            }
            
            print(f"正在登录交易柜台: {account}")
            response = requests.get(url, params=params, timeout=10)
            data = response.json()
            
            if data.get("code") == "0":
                self.ticket = data.get("ticket")
                self.ticket_expire = data.get("expire")
                self.is_logged_in = True
                
                print(f"✅ 登录成功!")
                print(f"交易凭证: {self.ticket}")
                print(f"有效期: {self.ticket_expire}秒")
                return True
            else:
                print(f"❌ 登录失败: {data}")
                return False
                
        except Exception as e:
            print(f"❌ 登录异常: {e}")
            return False
    
    def check_login_status(self) -> bool:
        """检查登录状态"""
        if not self.is_logged_in or not self.ticket:
            return False
        
        # 检查ticket是否过期
        if self.ticket_expire and time.time() > self.ticket_expire:
            print("⚠️ 交易凭证已过期，需要重新登录")
            self.is_logged_in = False
            self.ticket = None
            return False
        
        return True
    
    def buy_stock(self, code: str, name: str, price: float, volume: int) -> Optional[str]:
        """
        买入股票
        
        Parameters
        ----------
        code : str
            证券代码
        name : str
            证券名称
        price : float
            委托价格
        volume : int
            委托数量
        
        Returns
        -------
        Optional[str]
            委托编号，失败返回None
        """
        if not self.check_login_status():
            print("❌ 未登录或登录已过期")
            return None
        
        try:
            url = f"http://{self.trade_server}/buy"
            params = {
                'token': self.token,
                'ticket': self.ticket,
                'code': code,
                'name': name,
                'price': str(price),
                'volume': str(volume)
            }
            
            print(f"正在买入: {code} {name}, 价格: {price}, 数量: {volume}")
            response = requests.get(url, params=params, timeout=10)
            data = response.json()
            
            if data.get("code") == "0":
                order_id = data.get("order_id")
                print(f"✅ 买入委托成功! 委托编号: {order_id}")
                return order_id
            else:
                print(f"❌ 买入委托失败: {data}")
                return None
                
        except Exception as e:
            print(f"❌ 买入委托异常: {e}")
            return None
    
    def sell_stock(self, code: str, name: str, price: float, volume: int) -> Optional[str]:
        """
        卖出股票
        
        Parameters
        ----------
        code : str
            证券代码
        name : str
            证券名称
        price : float
            委托价格
        volume : int
            委托数量
        
        Returns
        -------
        Optional[str]
            委托编号，失败返回None
        """
        if not self.check_login_status():
            print("❌ 未登录或登录已过期")
            return None
        
        try:
            url = f"http://{self.trade_server}/sale"
            params = {
                'token': self.token,
                'ticket': self.ticket,
                'code': code,
                'name': name,
                'price': str(price),
                'volume': str(volume)
            }
            
            print(f"正在卖出: {code} {name}, 价格: {price}, 数量: {volume}")
            response = requests.get(url, params=params, timeout=10)
            data = response.json()
            
            if data.get("code") == "0":
                order_id = data.get("order_id")
                print(f"✅ 卖出委托成功! 委托编号: {order_id}")
                return order_id
            else:
                print(f"❌ 卖出委托失败: {data}")
                return None
                
        except Exception as e:
            print(f"❌ 卖出委托异常: {e}")
            return None
    
    def cancel_order(self, order_id: str) -> bool:
        """
        撤销委托
        
        Parameters
        ----------
        order_id : str
            委托编号
        
        Returns
        -------
        bool
            撤单是否成功
        """
        if not self.check_login_status():
            print("❌ 未登录或登录已过期")
            return False
        
        try:
            url = f"http://{self.trade_server}/cancel"
            params = {
                'token': self.token,
                'ticket': self.ticket,
                'order_id': order_id
            }
            
            print(f"正在撤销委托: {order_id}")
            response = requests.get(url, params=params, timeout=10)
            data = response.json()
            
            if data.get("code") == "0":
                print(f"✅ 撤单成功! {data.get('message', '')}")
                return True
            else:
                print(f"❌ 撤单失败: {data}")
                return False
                
        except Exception as e:
            print(f"❌ 撤单异常: {e}")
            return False
    
    def query_orders(self) -> List[Dict]:
        """
        查询交易记录
        
        Returns
        -------
        List[Dict]
            交易记录列表
        """
        if not self.check_login_status():
            print("❌ 未登录或登录已过期")
            return []
        
        try:
            url = f"http://{self.trade_server}/check_order"
            params = {
                'token': self.token,
                'ticket': self.ticket
            }
            
            print("正在查询交易记录...")
            response = requests.get(url, params=params, timeout=10)
            data = response.json()
            
            if data.get("code") == "0":
                orders = data.get("list", [])
                print(f"✅ 查询成功! 共{len(orders)}条交易记录")
                return orders
            else:
                print(f"❌ 查询交易记录失败: {data}")
                return []
                
        except Exception as e:
            print(f"❌ 查询交易记录异常: {e}")
            return []
    
    def query_positions(self) -> List[Dict]:
        """
        查询持仓
        
        Returns
        -------
        List[Dict]
            持仓列表
        """
        if not self.check_login_status():
            print("❌ 未登录或登录已过期")
            return []
        
        try:
            url = f"http://{self.trade_server}/check_position"
            params = {
                'token': self.token,
                'ticket': self.ticket
            }
            
            print("正在查询持仓...")
            response = requests.get(url, params=params, timeout=10)
            data = response.json()
            
            if data.get("code") == "0":
                positions = data.get("list", [])
                print(f"✅ 查询成功! 共{len(positions)}个持仓")
                return positions
            else:
                print(f"❌ 查询持仓失败: {data}")
                return []
                
        except Exception as e:
            print(f"❌ 查询持仓异常: {e}")
            return []
    
    def print_orders(self, orders: List[Dict]):
        """打印交易记录"""
        if not orders:
            print("暂无交易记录")
            return
        
        print("\n" + "="*100)
        print("交易记录")
        print("="*100)
        print(f"{'委托编号':<12} {'日期':<10} {'时间':<8} {'代码':<10} {'名称':<15} {'类型':<8} {'状态':<6} {'委托价':<8} {'委托量':<8} {'成交价':<8} {'成交量':<8}")
        print("-"*100)
        
        for order in orders:
            print(f"{order.get('order_id', ''):<12} "
                  f"{order.get('day', ''):<10} "
                  f"{order.get('time', ''):<8} "
                  f"{order.get('code', ''):<10} "
                  f"{order.get('name', ''):<15} "
                  f"{order.get('type', ''):<8} "
                  f"{order.get('status', ''):<6} "
                  f"{order.get('order_price', ''):<8} "
                  f"{order.get('order_volume', ''):<8} "
                  f"{order.get('deal_price', ''):<8} "
                  f"{order.get('deal_volume', ''):<8}")
        print("="*100)
    
    def print_positions(self, positions: List[Dict]):
        """打印持仓信息"""
        if not positions:
            print("暂无持仓")
            return
        
        print("\n" + "="*80)
        print("持仓信息")
        print("="*80)
        print(f"{'代码':<10} {'名称':<15} {'持仓量':<8} {'可用量':<8} {'持仓盈亏':<10} {'当日盈亏':<10}")
        print("-"*80)
        
        for position in positions:
            print(f"{position.get('code', ''):<10} "
                  f"{position.get('name', ''):<15} "
                  f"{position.get('hold_vol', ''):<8} "
                  f"{position.get('usable_vol', ''):<8} "
                  f"{position.get('hold_earn', ''):<10} "
                  f"{position.get('day_earn', ''):<10}")
        print("="*80)
    
    def logout(self):
        """登出"""
        self.is_logged_in = False
        self.ticket = None
        self.ticket_expire = None
        print("已登出交易系统")


class TradingStrategy:
    """交易策略基类"""
    
    def __init__(self, trading_client: JVQuantTradingClient):
        self.trading_client = trading_client
        self.is_running = False
        self.strategy_thread = None
    
    def start(self):
        """启动策略"""
        if self.is_running:
            print("策略已在运行中")
            return
        
        self.is_running = True
        self.strategy_thread = threading.Thread(target=self._run_strategy)
        self.strategy_thread.daemon = True
        self.strategy_thread.start()
        print("策略已启动")
    
    def stop(self):
        """停止策略"""
        self.is_running = False
        if self.strategy_thread:
            self.strategy_thread.join()
        print("策略已停止")
    
    def _run_strategy(self):
        """策略运行逻辑（子类实现）"""
        raise NotImplementedError("子类必须实现_run_strategy方法")


class SimpleMACDStrategy(TradingStrategy):
    """简单的MACD策略"""
    
    def __init__(self, trading_client: JVQuantTradingClient, stock_code: str, stock_name: str):
        super().__init__(trading_client)
        self.stock_code = stock_code
        self.stock_name = stock_name
        self.last_signal = None  # 上次信号
    
    def _run_strategy(self):
        """MACD策略逻辑"""
        while self.is_running:
            try:
                # 这里应该从实时数据中获取MACD信号
                # 暂时使用模拟信号
                current_signal = self._get_macd_signal()
                
                if current_signal != self.last_signal:
                    if current_signal == "BUY":
                        # 买入信号
                        self.trading_client.buy_stock(
                            self.stock_code, 
                            self.stock_name, 
                            price=1.0,  # 市价买入
                            volume=100
                        )
                    elif current_signal == "SELL":
                        # 卖出信号
                        self.trading_client.sell_stock(
                            self.stock_code, 
                            self.stock_name, 
                            price=1.0,  # 市价卖出
                            volume=100
                        )
                    
                    self.last_signal = current_signal
                
                # 每30秒检查一次
                time.sleep(30)
                
            except Exception as e:
                print(f"策略运行异常: {e}")
                time.sleep(30)
    
    def _get_macd_signal(self) -> str:
        """获取MACD信号（模拟）"""
        # 这里应该基于实际的MACD指标计算
        # 暂时返回随机信号用于演示
        import random
        signals = ["HOLD", "BUY", "SELL"]
        return random.choice(signals)


def main():
    """主函数 - 交易系统演示"""
    TOKEN = "d0c519adcd47d266f1c96750d4e80aa6"
    
    print("jvQuant交易系统演示")
    print("="*60)
    print(f"Token: {TOKEN}")
    print("="*60)
    
    # 创建交易客户端
    trading_client = JVQuantTradingClient(TOKEN)
    
    try:
        # 登录（需要提供真实的账户信息）
        print("\n请输入交易账户信息:")
        account = input("资金账号: ").strip()
        password = input("资金密码: ").strip()
        
        if not account or not password:
            print("账户信息不能为空")
            return
        
        # 登录
        if not trading_client.login(account, password):
            print("登录失败，退出系统")
            return
        
        # 主菜单
        while True:
            print("\n" + "="*60)
            print("交易系统菜单")
            print("="*60)
            print("1. 查询持仓")
            print("2. 查询交易记录")
            print("3. 买入股票")
            print("4. 卖出股票")
            print("5. 撤销委托")
            print("6. 启动自动交易策略")
            print("7. 停止自动交易策略")
            print("0. 退出系统")
            print("="*60)
            
            choice = input("请选择操作 (0-7): ").strip()
            
            if choice == "0":
                break
            elif choice == "1":
                positions = trading_client.query_positions()
                trading_client.print_positions(positions)
            elif choice == "2":
                orders = trading_client.query_orders()
                trading_client.print_orders(orders)
            elif choice == "3":
                code = input("证券代码: ").strip()
                name = input("证券名称: ").strip()
                price = float(input("委托价格: ").strip())
                volume = int(input("委托数量: ").strip())
                trading_client.buy_stock(code, name, price, volume)
            elif choice == "4":
                code = input("证券代码: ").strip()
                name = input("证券名称: ").strip()
                price = float(input("委托价格: ").strip())
                volume = int(input("委托数量: ").strip())
                trading_client.sell_stock(code, name, price, volume)
            elif choice == "5":
                order_id = input("委托编号: ").strip()
                trading_client.cancel_order(order_id)
            elif choice == "6":
                stock_code = input("股票代码: ").strip()
                stock_name = input("股票名称: ").strip()
                strategy = SimpleMACDStrategy(trading_client, stock_code, stock_name)
                strategy.start()
            elif choice == "7":
                print("停止策略功能待实现")
            else:
                print("无效选择，请重新输入")
    
    except KeyboardInterrupt:
        print("\n用户中断，正在退出...")
    except Exception as e:
        print(f"系统异常: {e}")
    finally:
        trading_client.logout()
        print("交易系统已退出")


if __name__ == "__main__":
    main() 