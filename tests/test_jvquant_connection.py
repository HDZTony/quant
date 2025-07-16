#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
jvQuant连接测试脚本
用于验证Token和服务器连接是否正常
"""

import requests
import json
import time


def test_server_allocation(token: str):
    """测试服务器分配功能"""
    print("=" * 50)
    print("测试jvQuant服务器分配")
    print("=" * 50)
    
    base_url = "http://jvQuant.com/query/server"
    
    # 测试沪深WebSocket服务器分配
    print("\n1. 测试沪深WebSocket服务器分配...")
    url = f"{base_url}?market=ab&type=websocket&token={token}"
    
    try:
        response = requests.get(url, timeout=10)
        print(f"请求URL: {url}")
        print(f"响应状态码: {response.status_code}")
        print(f"响应内容: {response.text}")
        
        if response.status_code == 200:
            data = response.json()
            if data.get("code") == "0":
                server = data.get("server")
                print(f"✅ 成功获取服务器地址: {server}")
                return server
            else:
                print(f"❌ 获取服务器失败: {data}")
                return None
        else:
            print(f"❌ HTTP请求失败: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"❌ 请求异常: {e}")
        return None


def test_token_validity(token: str):
    """测试Token有效性"""
    print("\n" + "=" * 50)
    print("测试Token有效性")
    print("=" * 50)
    
    # 测试不同市场的服务器分配
    markets = [
        ("ab", "websocket", "沪深WebSocket"),
        ("ab", "sql", "沪深数据库"),
        ("hk", "websocket", "港股WebSocket"),
        ("us", "websocket", "美股WebSocket")
    ]
    
    base_url = "http://jvQuant.com/query/server"
    
    for market, type_code, description in markets:
        print(f"\n测试{description}...")
        url = f"{base_url}?market={market}&type={type_code}&token={token}"
        
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get("code") == "0":
                    server = data.get("server")
                    print(f"✅ {description}: {server}")
                else:
                    print(f"❌ {description}: {data}")
            else:
                print(f"❌ {description}: HTTP {response.status_code}")
                
        except Exception as e:
            print(f"❌ {description}: {e}")
        
        time.sleep(1)  # 避免请求过于频繁


def test_websocket_connection(token: str, server: str):
    """测试WebSocket连接"""
    print("\n" + "=" * 50)
    print("测试WebSocket连接")
    print("=" * 50)
    
    try:
        import websocket
        
        ws_url = f"ws://{server}/?token={token}"
        print(f"WebSocket URL: {ws_url}")
        
        def on_open(ws):
            print("✅ WebSocket连接已建立")
            # 发送测试订阅
            test_subscription = "all=lv1_159506"
            ws.send(test_subscription)
            print(f"发送订阅: {test_subscription}")
        
        def on_message(ws, message, type, flag):
            print(f"收到消息 - 类型: {type}, 标志: {flag}")
            if type == websocket.ABNF.OPCODE_TEXT:
                print(f"文本消息: {message}")
            elif type == websocket.ABNF.OPCODE_BINARY:
                print(f"二进制消息长度: {len(message)}")
        
        def on_error(ws, error):
            print(f"❌ WebSocket错误: {error}")
        
        def on_close(ws, code, msg):
            print(f"WebSocket连接关闭: {code} - {msg}")
        
        # 创建WebSocket连接
        ws = websocket.WebSocketApp(
            ws_url,
            on_open=on_open,
            on_data=on_message,
            on_error=on_error,
            on_close=on_close
        )
        
        print("正在连接WebSocket...")
        # 设置超时时间
        ws.run_forever(timeout=10)
        
    except ImportError:
        print("❌ 缺少websocket-client库，请安装: pip install websocket-client")
    except Exception as e:
        print(f"❌ WebSocket连接测试失败: {e}")


def main():
    """主函数"""
    # 你的Token
    TOKEN = "d0c519adcd47d266f1c96750d4e80aa6"
    
    print("jvQuant连接测试工具")
    print("=" * 50)
    print(f"Token: {TOKEN}")
    print("=" * 50)
    
    # 1. 测试Token有效性
    test_token_validity(TOKEN)
    
    # 2. 测试服务器分配
    server = test_server_allocation(TOKEN)
    
    # 3. 如果获取到服务器，测试WebSocket连接
    if server:
        test_websocket_connection(TOKEN, server)
    
    print("\n" + "=" * 50)
    print("测试完成")
    print("=" * 50)


if __name__ == "__main__":
    main() 