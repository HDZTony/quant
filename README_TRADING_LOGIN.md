# 交易柜台登录配置说明

## 问题说明

`ETF159506NautilusExecClient` 实现了 `login()` 方法，但之前在 `_connect()` 中未被调用，导致：
- ❌ 执行客户端连接成功，但没有交易凭证（ticket）
- ❌ 所有交易操作（买入/卖出/撤单）会失败

## 已修复

现在 `_connect()` 方法会自动从配置读取账户信息并登录柜台。

## 配置方式

### 方式1：在适配器配置中添加账户信息（推荐）

```python
# 创建适配器配置
adapter_config = {
    'token': 'd0c519adcd47d266f1c96750d4e80aa6',
    'stock_code': '159506',
    'catalog_path': './data_catalog',
    
    # 【新增】交易账户信息
    'trade_account': '541460031518',    # 资金账号
    'trade_password': '882200'           # 资金密码
}

# 创建适配器
adapter = ETF159506Adapter(adapter_config)

# 连接时会自动登录
await adapter.connect()  # ✅ 内部会调用 login()
```

### 方式2：手动调用login（不推荐）

如果不在配置中添加账户信息，可以手动登录：

```python
# 创建执行客户端（通过TradingNode）
# ...

# 获取执行客户端实例
exec_client = trading_node.get_execution_client("ETF159506")

# 手动登录
await exec_client.login(
    account="541460031518",
    password="882200"
)
```

## 修改后的连接流程

```python
async def _connect(self) -> None:
    """连接到执行源"""
    # 1. 获取交易服务器地址
    await self._get_trade_server()
    
    # 2. 连接HTTP客户端
    await self.http_client.connect()
    
    # 3. 【新增】自动登录柜台（如果配置了账户）
    if self.trade_account and self.trade_password:
        login_success = await self.login(
            self.trade_account, 
            self.trade_password
        )
        if login_success:
            # ✅ 获得ticket，可以进行交易操作
            logger.info(f"交易凭证: {self.ticket}")
        else:
            # ❌ 登录失败，执行客户端不可用
            logger.error("登录失败")
            return
    else:
        # ⚠️ 未配置账户，执行客户端连接但无法交易
        logger.warning("未配置交易账户，无法进行交易操作")
```

## 日志输出示例

### 成功登录
```
INFO - 连接ETF159506合并执行客户端...
INFO - 获取交易服务器成功: trade.jvquant.com:8080
INFO - 尝试自动登录交易柜台: 541460031518
INFO - 登录成功! 交易凭证: abc123xyz...
INFO - ✅ ETF159506执行客户端连接并登录成功
INFO -    交易凭证: abc123xyz...
INFO -    凭证有效期: 7200秒
```

### 未配置账户
```
WARNING - ⚠️ 未配置交易账户信息，执行客户端已连接但未登录
WARNING -    交易操作将会失败！请在config中添加:
WARNING -    - trade_account: 资金账号
WARNING -    - trade_password: 资金密码
INFO - ETF159506合并执行客户端连接成功（未登录）
```

### 登录失败
```
ERROR - ❌ 交易柜台登录失败，执行客户端无法使用
WARNING - 提示: 请检查trade_account和trade_password配置是否正确
```

## 安全建议

### 1. 不要硬编码密码
```python
# ❌ 不要这样做
adapter_config = {
    'trade_password': '882200'  # 硬编码密码
}

# ✅ 推荐从环境变量读取
import os
adapter_config = {
    'trade_account': os.getenv('TRADE_ACCOUNT'),
    'trade_password': os.getenv('TRADE_PASSWORD')
}
```

### 2. 使用配置文件（不要提交到git）
```python
# config.json（添加到 .gitignore）
{
    "token": "your_token",
    "trade_account": "your_account",
    "trade_password": "your_password"
}

# 代码中读取
import json
with open('config.json') as f:
    adapter_config = json.load(f)
```

## 相关文件

- 适配器实现: `etf_159506_adapter.py`
  - `ETF159506NautilusExecClient.login()` (第2111-2142行)
  - `ETF159506NautilusExecClient._connect()` (第2330-2375行)
- 官方文档: https://jvquant.com/wiki/券商交易接口/登录券商柜台.html
- 独立交易系统示例: `trading_system.py` (第489行调用login)

## 测试验证

```python
# 测试登录是否成功
async def test_login():
    config = {
        'token': 'your_token',
        'trade_account': 'your_account',
        'trade_password': 'your_password'
    }
    
    adapter = ETF159506Adapter(config)
    
    # 连接（会自动登录）
    if await adapter.connect():
        print("✅ 连接并登录成功")
        
        # 检查状态
        status = await adapter.get_status()
        print(f"适配器状态: {status}")
    else:
        print("❌ 连接失败")

# 运行测试
asyncio.run(test_login())
```

## 常见问题

### Q: 为什么不在_connect中直接要求传入账户密码？
A: NautilusTrader的_connect方法签名是固定的，无法添加参数。因此从config读取是最Pythonic的方式。

### Q: ticket过期后会自动重新登录吗？
A: 目前不会。需要手动检测过期并重新登录。未来可以添加自动续期功能。

### Q: 可以不登录只获取行情吗？
A: 可以。数据客户端（`ETF159506NautilusDataClient`）不需要登录，只有执行客户端需要。

