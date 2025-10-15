#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
测试邮件功能 - 发送到 he.d.z@outlook.com
"""

import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

from email_notifier import EmailNotifier

def test_email():
    """测试邮件发送"""
    print("=" * 80)
    print("📧 测试邮件通知功能")
    print("=" * 80)
    print()
    
    # 邮箱配置
    config = {
        'enabled': True,
        'sender_email': '954504788@qq.com',        # 发件人（QQ邮箱）
        'sender_password': 'fopwaoxwqpqmbedg',     # QQ邮箱授权码
        'receiver_email': 'he.d.z@outlook.com',    # 收件人（Outlook邮箱）
    }
    
    print(f"发件人: {config['sender_email']}")
    print(f"收件人: {config['receiver_email']}")
    print(f"授权码: {config['sender_password']}")
    print()
    
    # 创建邮件通知器
    notifier = EmailNotifier(config)
    
    # 测试：发送订单和账户综合通知（合并版）
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("测试：发送订单和账户综合通知（1封邮件包含所有信息）")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    
    order_info = {
        'code': '159506',
        'name': '恒生医疗',
        'type': 'buy',               # 买入
        'price': 1.603,
        'volume': 100,
        'order_id': 'TEST_ORDER_001'
    }
    
    # 准备账户信息
    account_info = {
        'total': 501527.77,
        'usable': 422977.27,
        'day_earn': 1632.50,
        'hold_earn': 1827.30,
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
    
    print(f"模拟订单: {order_info['type']} {order_info['code']} {order_info['name']}")
    print(f"  价格: {order_info['price']}, 数量: {order_info['volume']}")
    print()
    print(f"模拟账户: 总资产={account_info['total']}, 可用={account_info['usable']}")
    print(f"  当日盈亏={account_info['day_earn']}, 持仓盈亏={account_info['hold_earn']}")
    print()
    print("正在发送综合邮件（订单 + 账户信息）...")
    
    success = notifier.send_order_with_account_notification(order_info, account_info)
    
    if success:
        print("✅ 邮件发送成功！")
        print()
        print(f"   📬 请检查 he.d.z@outlook.com 的收件箱")
        print(f"   📧 邮件主题: [交易通知] {order_info['type']} {order_info['code']} - 订单已提交")
        print()
        print("   📋 邮件内容包括：")
        print("      ✅ 订单信息（证券、价格、数量、委托编号）")
        print("      ✅ 账户信息（总资产、可用资金、盈亏）")
        print("      ✅ 持仓明细")
    else:
        print("❌ 邮件发送失败")
        print("   请检查:")
        print("   1. 授权码是否正确")
        print("   2. 网络是否正常")
        print("   3. 查看上面的错误日志")
    
    print()
    print("=" * 80)
    print("✅ 测试完成")
    print("=" * 80)
    print()
    if success:
        print("📬 现在去 he.d.z@outlook.com 检查邮箱！")
        print("   应该收到 1 封综合通知邮件")
        print("   包含：订单信息 + 账户信息 + 持仓明细")
    print()


if __name__ == "__main__":
    try:
        test_email()
    except KeyboardInterrupt:
        print("\n测试被中断")
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

