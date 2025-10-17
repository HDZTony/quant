# Cache中Account数据的来源和流向

## 📚 官方文档参考

根据[Nautilus Trader Accounting API](https://nautilustrader.io/docs/latest/api_reference/accounting/)，账户数据通过`AccountState`事件在系统中流转。

## 🔄 完整数据流

### 1. **数据源头：ExecutionClient（你的Adapter）**

```python
# 在你的 ETF159506NautilusExecClient 中
class ETF159506NautilusExecClient(LiveExecutionClient):
    
    async def _check_positions(self):
        """查询持仓信息（从JVQuant API）"""
        # 1. 从券商API获取账户信息
        account_info = {
            'total': 10000.0,     # 总资产
            'usable': 9500.0,     # ✅ 可用资金（这个很关键！）
            'day_earn': 100.0,
            'hold_earn': 200.0,
        }
        
        # 2. 将券商数据转换为Nautilus格式
        balances = [
            AccountBalance(
                total=Money(account_info['total'], CNY),
                locked=Money(account_info['total'] - account_info['usable'], CNY),
                free=Money(account_info['usable'], CNY),  # ✅ 这就是 balance_free
            )
        ]
        
        # 3. 📤 发送AccountState事件到系统
        self.generate_account_state(
            balances=balances,
            margins=[],
            reported=True,  # 表示这是从交易所直接报告的
            ts_event=self._clock.timestamp_ns(),
        )
```

**关键代码位置**：
- `nautilus_trader/execution/client.pyx` Line 295-334
- 方法：`generate_account_state()`

---

### 2. **事件发送：通过MessageBus**

```python
# ExecutionClient.generate_account_state() 内部实现
def generate_account_state(self, balances, margins, reported, ts_event):
    # 创建AccountState事件
    account_state = AccountState(
        account_id=self.account_id,
        account_type=self.account_type,  # CASH 或 MARGIN
        base_currency=self.base_currency,
        reported=reported,
        balances=balances,  # ✅ 包含 total/free/locked
        margins=margins,
        info={},
        event_id=UUID4(),
        ts_event=ts_event,
        ts_init=self._clock.timestamp_ns(),
    )
    
    # 📨 发送到Portfolio
    self._msgbus.send(
        endpoint="Portfolio.update_account",  # ← 注意这个endpoint
        msg=account_state,
    )
```

**关键代码位置**：
- `nautilus_trader/execution/client.pyx` Line 817-821
- 方法：`_send_account_state()`

---

### 3. **事件处理：Portfolio接收并更新Cache**

```python
# Portfolio注册了消息端点
class Portfolio:
    def __init__(self):
        # 注册endpoint，接收account state更新
        self._msgbus.register(
            endpoint="Portfolio.update_account",  # ← 对应上面的endpoint
            handler=self.update_account
        )
    
    def update_account(self, event: AccountState):
        """处理账户状态更新"""
        # 调用内部方法
        self._update_account(event)
        
        # 发布事件给其他订阅者
        self._msgbus.publish_c(
            topic=f"events.account.{event.account_id}",
            msg=event,
        )
    
    def _update_account(self, event: AccountState):
        """更新Cache中的账户"""
        # 1. 从Cache获取账户对象（如果不存在则创建）
        account = self._cache.account(event.account_id)
        
        if account is None:
            # 首次创建账户（使用AccountFactory）
            account = AccountFactory.create_c(event)
            self._cache.add_account(account)  # ✅ 添加到Cache
        else:
            # 应用新的状态到账户对象
            account.apply(event)
            self._cache.update_account(account)  # ✅ 更新Cache
```

**关键代码位置**：
- `nautilus_trader/portfolio/portfolio.pyx` Line 182, 434-451, 1475-1484

---

### 4. **数据存储：Cache**

```python
# Cache内部存储
class Cache:
    def __init__(self):
        self._accounts = {}  # {AccountId: Account}
    
    def add_account(self, account: Account):
        """添加账户到cache"""
        self._accounts[account.id] = account
    
    def update_account(self, account: Account):
        """更新cache中的账户"""
        self._accounts[account.id] = account
    
    def account(self, account_id: AccountId) -> Account:
        """根据ID获取账户"""
        return self._accounts.get(account_id)
    
    def account_for_venue(self, venue: Venue) -> Account:
        """根据venue获取账户"""
        for account in self._accounts.values():
            # 匹配venue对应的账户
            if account.id.issuer == venue.value:
                return account
        return None
```

---

### 5. **数据使用：Strategy访问**

```python
# 在你的策略中
class ETF159506Strategy(Strategy):
    def execute_scheduled_buy(self, bar: Bar):
        # ✅ 从Cache获取账户（数据来自上述流程）
        account = self.cache.account_for_venue(self.config.venue)
        
        # ✅ 获取可用余额
        free_balance = account.balance_free(CNY)
        available_balance = free_balance.as_double()
        
        # 使用可用余额计算交易数量
        quantity = int(available_balance / current_price)
```

---

## 🎯 完整数据流图

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. JVQuant API (券商接口)                                        │
│    - GET /check_hold                                            │
│    - 返回: {total: 10000, usable: 9500, ...}                   │
└────────────────┬────────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────────┐
│ 2. ETF159506NautilusExecClient (你的Adapter)                    │
│    - _check_positions()                                         │
│    - 转换为 AccountBalance(total/free/locked)                   │
│    - generate_account_state(balances, ...)                     │
└────────────────┬────────────────────────────────────────────────┘
                 │ AccountState Event
                 ▼
┌─────────────────────────────────────────────────────────────────┐
│ 3. MessageBus                                                   │
│    - endpoint: "Portfolio.update_account"                       │
│    - msg: AccountState(balances=[...])                         │
└────────────────┬────────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────────┐
│ 4. Portfolio                                                    │
│    - update_account(event)                                      │
│    - _update_account(event)                                     │
│      ├─ AccountFactory.create_c(event) [首次]                   │
│      └─ account.apply(event) [更新]                             │
└────────────────┬────────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────────┐
│ 5. Cache                                                        │
│    - add_account(account) 或 update_account(account)            │
│    - 存储: _accounts[account_id] = account                      │
└────────────────┬────────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────────┐
│ 6. Strategy (你的策略)                                          │
│    - account = cache.account_for_venue(venue)                   │
│    - balance_free = account.balance_free(CNY)                   │
│    - ✅ 使用可用余额进行交易决策                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🔑 关键点总结

### AccountBalance对象的三个属性

根据[Nautilus Accounting API](https://nautilustrader.io/docs/latest/api_reference/accounting/)：

```python
class AccountBalance:
    total: Money   # 总余额 = free + locked
    free: Money    # ✅ 可用余额（可用于交易）
    locked: Money  # 冻结余额（挂单占用）
```

### Account对象的余额方法

```python
# ❌ 错误 - 可能包括持仓市值或其他
balance_total(currency) → Money

# ✅ 正确 - 真正可用于交易的现金
balance_free(currency) → Money

# 📊 监控 - 挂单冻结的资金
balance_locked(currency) → Money
```

---

## 🔍 你的Adapter需要做什么？

根据你的`etf_159506_adapter.py`（Line 2500-2507），你已经正确获取了数据：

```python
async def _check_positions(self):
    account_info = {
        'total': float(data.get('total', 0)),      # 总资产
        'usable': float(data.get('usable', 0)),    # ✅ 可用资金
        'day_earn': float(data.get('day_earn', 0)),
        'hold_earn': float(data.get('hold_earn', 0)),
    }
```

**下一步：你需要定期调用`generate_account_state`**

```python
# 在你的ExecutionClient中添加
async def _update_account_state(self):
    """定期更新账户状态"""
    # 1. 查询账户信息
    account_info = await self._check_positions()
    if not account_info:
        return
    
    # 2. 创建AccountBalance
    balances = [
        AccountBalance(
            total=Money(account_info['total'], CNY),
            locked=Money(account_info['total'] - account_info['usable'], CNY),
            free=Money(account_info['usable'], CNY),  # ← 这个是关键！
        )
    ]
    
    # 3. 发送AccountState事件
    self.generate_account_state(
        balances=balances,
        margins=[],
        reported=True,
        ts_event=self._clock.timestamp_ns(),
    )
```

---

## 📖 相关文档

- [Nautilus Accounting API](https://nautilustrader.io/docs/latest/api_reference/accounting/)
- [Portfolio API](https://nautilustrader.io/docs/latest/concepts/portfolio/)
- [Cache API](https://nautilustrader.io/docs/latest/concepts/cache/)

## 🎓 源码位置

| 组件 | 文件 | 关键方法/行号 |
|------|------|--------------|
| ExecutionClient | `nautilus_trader/execution/client.pyx` | `generate_account_state()` Line 295<br>`_send_account_state()` Line 817 |
| Portfolio | `nautilus_trader/portfolio/portfolio.pyx` | `update_account()` Line 434<br>`_update_account()` Line 1475 |
| AccountsManager | `nautilus_trader/accounting/manager.pyx` | `generate_account_state()` Line 68<br>`update_balances()` Line 97 |
| Binance示例 | `nautilus_trader/adapters/binance/spot/execution.py` | `_update_account_state()` Line 138 |

