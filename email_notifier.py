#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
邮件通知模块
支持订单提交、成交、撤销等交易事件的邮件通知
"""

import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from typing import Optional, Dict, Any
from datetime import datetime

logger = logging.getLogger("EmailNotifier")


class EmailNotifier:
    """
    邮件通知器
    
    支持常见邮箱服务：
    - QQ邮箱
    - 163邮箱
    - Gmail
    - Outlook
    """
    
    # 常见邮箱SMTP服务器配置
    SMTP_SERVERS = {
        'qq': {'host': 'smtp.qq.com', 'port': 587, 'use_tls': True},
        '163': {'host': 'smtp.163.com', 'port': 465, 'use_tls': False},
        'gmail': {'host': 'smtp.gmail.com', 'port': 587, 'use_tls': True},
        'outlook': {'host': 'smtp.office365.com', 'port': 587, 'use_tls': True},
    }
    
    def __init__(self, config: Dict[str, Any]):
        """
        初始化邮件通知器
        
        Parameters
        ----------
        config : Dict[str, Any]
            邮箱配置
            - sender_email: 发件人邮箱
            - sender_password: 邮箱授权码（不是登录密码！）
            - receiver_email: 收件人邮箱（可选，默认发给自己）
            - email_type: 邮箱类型（'qq', '163', 'gmail', 'outlook'，可选）
            - smtp_host: SMTP服务器地址（可选，自动根据邮箱类型判断）
            - smtp_port: SMTP端口（可选）
            - enabled: 是否启用邮件通知（默认True）
        """
        self.sender_email = config.get('sender_email')
        self.sender_password = config.get('sender_password')
        self.receiver_email = config.get('receiver_email', self.sender_email)
        self.enabled = config.get('enabled', True)
        
        # 自动检测邮箱类型
        if not self.sender_email:
            logger.warning("未配置发件人邮箱，邮件通知已禁用")
            self.enabled = False
            return
        
        # 根据邮箱地址自动选择SMTP服务器
        email_domain = self.sender_email.split('@')[-1].lower()
        email_type = config.get('email_type')
        
        if not email_type:
            # 自动检测邮箱类型
            if 'qq.com' in email_domain:
                email_type = 'qq'
            elif '163.com' in email_domain:
                email_type = '163'
            elif 'gmail.com' in email_domain:
                email_type = 'gmail'
            elif 'outlook.com' in email_domain or 'hotmail.com' in email_domain:
                email_type = 'outlook'
            else:
                logger.warning(f"未知邮箱类型: {email_domain}，请手动配置 smtp_host 和 smtp_port")
        
        # 获取SMTP配置
        if email_type and email_type in self.SMTP_SERVERS:
            smtp_config = self.SMTP_SERVERS[email_type]
            self.smtp_host = config.get('smtp_host', smtp_config['host'])
            self.smtp_port = config.get('smtp_port', smtp_config['port'])
            self.use_tls = smtp_config['use_tls']
            logger.info(f"使用 {email_type} 邮箱配置: {self.smtp_host}:{self.smtp_port}")
        else:
            self.smtp_host = config.get('smtp_host')
            self.smtp_port = config.get('smtp_port', 587)
            self.use_tls = config.get('use_tls', True)
        
        if not self.smtp_host:
            logger.warning("未配置SMTP服务器，邮件通知已禁用")
            self.enabled = False
    
    def send_order_notification(
        self,
        order_info: Dict[str, Any],
        notification_type: str = "submit"
    ) -> bool:
        """
        发送订单通知邮件
        
        Parameters
        ----------
        order_info : Dict[str, Any]
            订单信息
            - code: 证券代码
            - name: 证券名称
            - type: 报单类别（'buy' 或 'sale'）
            - price: 委托价格
            - volume: 委托数量
            - order_id: 委托编号（可选）
        notification_type : str
            通知类型：'submit'(提交), 'filled'(成交), 'cancelled'(撤销)
        
        Returns
        -------
        bool
            True if 发送成功, False otherwise
        """
        if not self.enabled:
            return False
        
        try:
            # 构建邮件标题
            type_map = {
                'submit': '订单提交通知',
                'filled': '订单成交通知',
                'cancelled': '订单撤销通知'
            }
            subject = f"[交易通知] {type_map.get(notification_type, '订单通知')}"
            
            # 构建邮件内容
            order_type_cn = '买入' if order_info.get('type') == 'buy' else '卖出'
            
            content = f"""
<html>
<head>
    <style>
        body {{ font-family: Arial, sans-serif; }}
        .container {{ padding: 20px; background-color: #f5f5f5; }}
        .header {{ background-color: #4CAF50; color: white; padding: 15px; text-align: center; }}
        .content {{ background-color: white; padding: 20px; margin-top: 10px; }}
        .info-row {{ padding: 8px; border-bottom: 1px solid #eee; }}
        .label {{ font-weight: bold; color: #333; }}
        .value {{ color: #666; }}
        .footer {{ margin-top: 20px; font-size: 12px; color: #999; text-align: center; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h2>{type_map.get(notification_type, '订单通知')}</h2>
        </div>
        <div class="content">
            <div class="info-row">
                <span class="label">通知时间：</span>
                <span class="value">{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</span>
            </div>
            <div class="info-row">
                <span class="label">操作类型：</span>
                <span class="value" style="color: {'#4CAF50' if order_info.get('type') == 'buy' else '#f44336'}; font-weight: bold;">
                    {order_type_cn}
                </span>
            </div>
            <div class="info-row">
                <span class="label">证券代码：</span>
                <span class="value">{order_info.get('code')}</span>
            </div>
            <div class="info-row">
                <span class="label">证券名称：</span>
                <span class="value">{order_info.get('name')}</span>
            </div>
            <div class="info-row">
                <span class="label">委托价格：</span>
                <span class="value">{order_info.get('price')}</span>
            </div>
            <div class="info-row">
                <span class="label">委托数量：</span>
                <span class="value">{order_info.get('volume')}</span>
            </div>
"""
            
            # 添加委托编号（如果有）
            if order_info.get('order_id'):
                content += f"""
            <div class="info-row">
                <span class="label">委托编号：</span>
                <span class="value">{order_info.get('order_id')}</span>
            </div>
"""
            
            # 添加成交信息（如果是成交通知）
            if notification_type == 'filled':
                content += f"""
            <div class="info-row">
                <span class="label">成交价格：</span>
                <span class="value">{order_info.get('deal_price', 'N/A')}</span>
            </div>
            <div class="info-row">
                <span class="label">成交数量：</span>
                <span class="value">{order_info.get('deal_volume', 'N/A')}</span>
            </div>
"""
            
            content += """
        </div>
        <div class="footer">
            <p>此邮件由 ETF159506 实时交易系统自动发送，请勿回复</p>
        </div>
    </div>
</body>
</html>
"""
            
            # 发送邮件
            return self._send_email(subject, content)
            
        except Exception as e:
            logger.error(f"发送订单通知邮件失败: {e}")
            return False
    
    def _send_email(self, subject: str, content: str) -> bool:
        """
        发送邮件
        
        Parameters
        ----------
        subject : str
            邮件标题
        content : str
            邮件内容（HTML格式）
        
        Returns
        -------
        bool
            True if 发送成功, False otherwise
        """
        if not self.enabled:
            return False
        
        try:
            # 创建邮件对象
            message = MIMEMultipart('alternative')
            # QQ 邮箱要求 From 头部符合 RFC5322 标准，不能包含中文
            message['From'] = self.sender_email  # 直接使用邮箱地址
            message['To'] = self.receiver_email
            message['Subject'] = Header(subject, 'utf-8')
            
            # 添加HTML内容
            html_part = MIMEText(content, 'html', 'utf-8')
            message.attach(html_part)
            
            # 连接SMTP服务器
            if self.use_tls:
                # 使用 TLS（587端口）
                server = smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=10)
                server.starttls()
            else:
                # 使用 SSL（465端口）
                server = smtplib.SMTP_SSL(self.smtp_host, self.smtp_port, timeout=10)
            
            # 登录
            server.login(self.sender_email, self.sender_password)
            
            # 发送邮件
            server.sendmail(self.sender_email, self.receiver_email, message.as_string())
            server.quit()
            
            logger.info(f"邮件发送成功: {subject} -> {self.receiver_email}")
            return True
            
        except smtplib.SMTPAuthenticationError:
            logger.error("SMTP认证失败：请检查邮箱地址和授权码是否正确")
            logger.error("提示：需要使用邮箱的'授权码'而不是登录密码")
            return False
        except smtplib.SMTPException as e:
            logger.error(f"SMTP错误: {e}")
            return False
        except Exception as e:
            logger.error(f"发送邮件失败: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def send_account_summary(self, account_info: Dict[str, Any]) -> bool:
        """
        发送账户汇总邮件
        
        Parameters
        ----------
        account_info : Dict[str, Any]
            账户信息
            - total: 总资产
            - usable: 可用资金
            - day_earn: 当日盈亏
            - hold_earn: 持仓盈亏
            - hold_list: 持仓列表
        
        Returns
        -------
        bool
            True if 发送成功, False otherwise
        """
        if not self.enabled:
            return False
        
        try:
            subject = f"[账户汇总] {datetime.now().strftime('%Y-%m-%d')}"
            
            # 构建持仓列表HTML
            positions_html = ""
            hold_list = account_info.get('hold_list', [])
            if hold_list:
                for i, pos in enumerate(hold_list, 1):
                    if int(pos.get('hold_vol', 0)) > 0:  # 只显示有持仓的
                        earn_color = '#4CAF50' if float(pos.get('hold_earn', 0)) >= 0 else '#f44336'
                        positions_html += f"""
            <div class="info-row">
                <span class="label">{i}. {pos.get('code')} {pos.get('name')}:</span>
                <span class="value">
                    持仓 {pos.get('hold_vol')}, 
                    盈亏 <span style="color: {earn_color}; font-weight: bold;">
                        {pos.get('hold_earn')}
                    </span>
                </span>
            </div>
"""
            else:
                positions_html = """
            <div class="info-row">
                <span class="value">无持仓</span>
            </div>
"""
            
            # 盈亏颜色
            day_earn_color = '#4CAF50' if float(account_info.get('day_earn', 0)) >= 0 else '#f44336'
            hold_earn_color = '#4CAF50' if float(account_info.get('hold_earn', 0)) >= 0 else '#f44336'
            
            content = f"""
<html>
<head>
    <style>
        body {{ font-family: Arial, sans-serif; }}
        .container {{ padding: 20px; background-color: #f5f5f5; }}
        .header {{ background-color: #2196F3; color: white; padding: 15px; text-align: center; }}
        .content {{ background-color: white; padding: 20px; margin-top: 10px; }}
        .info-row {{ padding: 8px; border-bottom: 1px solid #eee; }}
        .label {{ font-weight: bold; color: #333; }}
        .value {{ color: #666; }}
        .section-title {{ font-size: 16px; font-weight: bold; color: #333; margin-top: 15px; margin-bottom: 10px; }}
        .footer {{ margin-top: 20px; font-size: 12px; color: #999; text-align: center; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h2>账户汇总报告</h2>
            <p>{datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}</p>
        </div>
        <div class="content">
            <div class="section-title">💰 资金信息</div>
            <div class="info-row">
                <span class="label">总资产：</span>
                <span class="value" style="font-size: 18px; font-weight: bold; color: #2196F3;">
                    ¥{account_info.get('total', 0)}
                </span>
            </div>
            <div class="info-row">
                <span class="label">可用资金：</span>
                <span class="value">¥{account_info.get('usable', 0)}</span>
            </div>
            <div class="info-row">
                <span class="label">当日盈亏：</span>
                <span class="value" style="color: {day_earn_color}; font-weight: bold;">
                    ¥{account_info.get('day_earn', 0)}
                </span>
            </div>
            <div class="info-row">
                <span class="label">持仓盈亏：</span>
                <span class="value" style="color: {hold_earn_color}; font-weight: bold;">
                    ¥{account_info.get('hold_earn', 0)}
                </span>
            </div>
            
            <div class="section-title">📦 持仓明细</div>
{positions_html}
        </div>
        <div class="footer">
            <p>此邮件由 ETF159506 实时交易系统自动发送，请勿回复</p>
        </div>
    </div>
</body>
</html>
"""
            
            return self._send_email(subject, content)
            
        except Exception as e:
            logger.error(f"发送账户汇总邮件失败: {e}")
            return False
    
    def send_order_with_account_notification(
        self,
        order_info: Dict[str, Any],
        account_info: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        发送订单和账户综合通知邮件（合并版本）
        
        Parameters
        ----------
        order_info : Dict[str, Any]
            订单信息
        account_info : Optional[Dict[str, Any]]
            账户信息（可选）
        
        Returns
        -------
        bool
            True if 发送成功, False otherwise
        """
        if not self.enabled:
            return False
        
        try:
            # 构建邮件标题
            order_type_cn = '买入' if order_info.get('type') == 'buy' else '卖出'
            subject = f"[交易通知] {order_type_cn} {order_info.get('code')} - 订单已提交"
            
            # 订单信息部分
            order_type_color = '#4CAF50' if order_info.get('type') == 'buy' else '#f44336'
            
            content = f"""
<html>
<head>
    <style>
        body {{ font-family: Arial, sans-serif; }}
        .container {{ padding: 20px; background-color: #f5f5f5; }}
        .header {{ background-color: {order_type_color}; color: white; padding: 15px; text-align: center; }}
        .section {{ background-color: white; padding: 20px; margin-top: 10px; border-radius: 5px; }}
        .section-title {{ font-size: 18px; font-weight: bold; color: #333; margin-bottom: 15px; padding-bottom: 10px; border-bottom: 2px solid #eee; }}
        .info-row {{ padding: 8px; border-bottom: 1px solid #eee; }}
        .label {{ font-weight: bold; color: #333; display: inline-block; width: 120px; }}
        .value {{ color: #666; }}
        .highlight {{ font-size: 18px; font-weight: bold; color: {order_type_color}; }}
        .footer {{ margin-top: 20px; font-size: 12px; color: #999; text-align: center; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h2>💰 交易通知</h2>
            <p style="font-size: 14px; margin-top: 5px;">{datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}</p>
        </div>
        
        <!-- 订单信息部分 -->
        <div class="section">
            <div class="section-title">📋 订单信息</div>
            <div class="info-row">
                <span class="label">操作类型：</span>
                <span class="highlight">{order_type_cn}</span>
            </div>
            <div class="info-row">
                <span class="label">证券代码：</span>
                <span class="value">{order_info.get('code')}</span>
            </div>
            <div class="info-row">
                <span class="label">证券名称：</span>
                <span class="value">{order_info.get('name')}</span>
            </div>
            <div class="info-row">
                <span class="label">委托价格：</span>
                <span class="value">{order_info.get('price')}</span>
            </div>
            <div class="info-row">
                <span class="label">委托数量：</span>
                <span class="value">{order_info.get('volume')}</span>
            </div>
"""
            
            # 添加委托编号（如果有）
            if order_info.get('order_id'):
                content += f"""
            <div class="info-row">
                <span class="label">委托编号：</span>
                <span class="value">{order_info.get('order_id')}</span>
            </div>
"""
            
            content += """
        </div>
"""
            
            # 账户信息部分（如果提供）
            if account_info:
                day_earn = float(account_info.get('day_earn', 0))
                hold_earn = float(account_info.get('hold_earn', 0))
                day_earn_color = '#4CAF50' if day_earn >= 0 else '#f44336'
                hold_earn_color = '#4CAF50' if hold_earn >= 0 else '#f44336'
                
                content += f"""
        <div class="section">
            <div class="section-title">💰 账户信息</div>
            <div class="info-row">
                <span class="label">总资产：</span>
                <span class="value" style="font-size: 16px; font-weight: bold; color: #2196F3;">¥{account_info.get('total', 0)}</span>
            </div>
            <div class="info-row">
                <span class="label">可用资金：</span>
                <span class="value">¥{account_info.get('usable', 0)}</span>
            </div>
            <div class="info-row">
                <span class="label">当日盈亏：</span>
                <span class="value" style="color: {day_earn_color}; font-weight: bold;">¥{day_earn}</span>
            </div>
            <div class="info-row">
                <span class="label">持仓盈亏：</span>
                <span class="value" style="color: {hold_earn_color}; font-weight: bold;">¥{hold_earn}</span>
            </div>
        </div>
"""
                
                # 持仓列表
                hold_list = account_info.get('hold_list', [])
                if hold_list:
                    # 只显示有持仓的
                    active_positions = [p for p in hold_list if int(p.get('hold_vol', 0)) > 0]
                    if active_positions:
                        content += """
        <div class="section">
            <div class="section-title">📦 持仓明细</div>
"""
                        for i, pos in enumerate(active_positions, 1):
                            pos_earn = float(pos.get('hold_earn', 0))
                            pos_earn_color = '#4CAF50' if pos_earn >= 0 else '#f44336'
                            content += f"""
            <div class="info-row">
                <span class="label">{i}. {pos.get('code')} {pos.get('name')}：</span>
                <span class="value">
                    持仓 {pos.get('hold_vol')}, 
                    盈亏 <span style="color: {pos_earn_color}; font-weight: bold;">¥{pos.get('hold_earn')}</span>
                </span>
            </div>
"""
                        content += """
        </div>
"""
            
            content += """
        <div class="footer">
            <p>此邮件由 ETF159506 实时交易系统自动发送</p>
            <p style="font-size: 10px; margin-top: 5px;">发件人: 954504788@qq.com</p>
        </div>
    </div>
</body>
</html>
"""
            
            # 发送邮件
            return self._send_email(subject, content)
            
        except Exception as e:
            logger.error(f"发送综合通知邮件失败: {e}")
            return False


# 测试函数
async def test_email_notifier():
    """测试邮件通知功能"""
    
    # 配置邮箱（请替换为你的邮箱信息）
    config = {
        'sender_email': 'your_email@qq.com',           # 发件人邮箱
        'sender_password': 'your_authorization_code',   # 邮箱授权码（不是登录密码！）
        'receiver_email': 'your_email@qq.com',         # 收件人邮箱（可选）
        'enabled': True
    }
    
    notifier = EmailNotifier(config)
    
    # 测试订单通知
    print("测试订单提交通知...")
    order_info = {
        'code': '159506',
        'name': '恒生医疗',
        'type': 'buy',
        'price': 1.603,
        'volume': 100,
        'order_id': '202501120001'
    }
    success = notifier.send_order_notification(order_info, 'submit')
    print(f"订单通知发送{'成功' if success else '失败'}")
    
    # 测试账户汇总
    print("\n测试账户汇总通知...")
    account_info = {
        'total': 501527.77,
        'usable': 422977.27,
        'day_earn': 16325.27,
        'hold_earn': 18273.22,
        'hold_list': [
            {
                'code': '159506',
                'name': '恒生医疗',
                'hold_vol': '1000',
                'usable_vol': '1000',
                'hold_earn': '1250.50',
                'day_earn': '320.15'
            }
        ]
    }
    success = notifier.send_account_summary(account_info)
    print(f"账户汇总发送{'成功' if success else '失败'}")


if __name__ == "__main__":
    print("=" * 80)
    print("邮件通知功能测试")
    print("=" * 80)
    print("\n⚠️  使用前请先配置邮箱信息：")
    print("1. 修改 config 中的 sender_email 和 receiver_email")
    print("2. 获取邮箱授权码（不是登录密码！）：")
    print("   - QQ邮箱: 设置 -> 账户 -> 开启SMTP服务 -> 生成授权码")
    print("   - 163邮箱: 设置 -> POP3/SMTP/IMAP -> 开启SMTP服务 -> 授权码管理")
    print("3. 将授权码填入 sender_password")
    print("\n按回车开始测试...")
    input()
    
    asyncio.run(test_email_notifier())

