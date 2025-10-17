# LiveExecutionClient账户状态更新实现指南

## 📚 官方Adapter实现分析

我分析了Nautilus Trader官方adapter的实现，总结了三种主要的账户状态更新模式。

---

## 🔍 模式1：WebSocket实时推送（Binance）

### 实现机制

Binance通过**WebSocket实时推送**账户余额变化，无需定期轮询。

### 关键代码

```python
# 文件: nautilus_trader/adapters/binance/spot/execution.py

class BinanceSpotExecutionClient(BinanceCommonExecutionClient):
    def __init__(self, ...):
        # 1. 注册WebSocket事件处理器
        self._spot_user_ws_handlers = {
            BinanceSpotEventType.outboundAccountPosition: self._handle_account_update,  # ← 账户更新
            BinanceSpotEventType.balanceUpdate: self._handle_balance_update,            # ← 余额更新
            BinanceSpotEventType.executionReport: self._handle_execution_report,
        }
    
    async def _connect(self) -> None:
        """连接时初始化"""
        # 2. 连接时主动查询一次账户状态
        await self._update_account_state()
        
        # 3. 订阅WebSocket（自动接收后续更新）
        await self._ws_client.subscribe_listen_key(self._listen_key)
    
    async def _update_account_state(self) -> None:
        """主动查询账户信息（REST API）"""
        # 4. 调用REST API获取账户信息
        account_info = await self._spot_http_account.query_spot_account_info()
        
        # 5. 解析为AccountBalance对象
        balances = account_info.parse_to_account_balances()
        
        # 6. ✅ 调用generate_account_state更新Cache
        self.generate_account_state(
            balances=balances,
            margins=[],
            reported=True,
            ts_event=millis_to_nanos(account_info.updateTime),
        )
    
    def _handle_balance_update(self, raw: bytes) -> None:
        """WebSocket推送余额变化时触发"""
        # 7. 收到WebSocket消息，重新查询账户状态
        self.create_task(self._update_account_state())
    
    def _handle_account_update(self, raw: bytes) -> None:
        """WebSocket推送账户更新时触发"""
        # 8. 解析WebSocket消息
        account_msg = self._decoder_spot_account_update_wrapper.decode(raw)
        # 9. 直接从消息解析并更新
        account_msg.data.handle_account_update(self)
```

### AccountBalance构建

```python
# 文件: nautilus_trader/adapters/binance/spot/schemas/user.py

class BinanceSpotBalance(msgspec.Struct, frozen=True):
    a: str  # Asset (币种)
    f: str  # Free (可用)
    l: str  # Locked (冻结)
    
    def parse_to_account_balance(self) -> AccountBalance:
        currency = Currency.from_str(self.a)
        free = Decimal(self.f)
        locked = Decimal(self.l)
        total = free + locked
        
        # ✅ 关键：构建AccountBalance对象
        return AccountBalance(
            total=Money(total, currency),   # 总余额 = 可用 + 冻结
            locked=Money(locked, currency), # 冻结余额
            free=Money(free, currency),     # ✅ 可用余额（用于交易）
        )

# WebSocket消息处理
class BinanceSpotAccountUpdateMsg(msgspec.Struct):
    B: list[BinanceSpotBalance]  # 余额列表
    
    def handle_account_update(self, exec_client):
        """直接调用generate_account_state"""
        exec_client.generate_account_state(
            balances=self.parse_to_account_balances(),
            margins=[],
            reported=True,
            ts_event=millis_to_nanos(self.u),
        )
```

---

## 🔍 模式2：WebSocket消息推送（Bitmex）

### 实现机制

Bitmex通过**WebSocket直接推送完整的AccountState对象**。

### 关键代码

```python
# 文件: nautilus_trader/adapters/bitmex/execution.py

class BitmexExecutionClient(LiveExecutionClient):
    async def _connect(self) -> None:
        """连接时初始化"""
        # 1. 订阅账户相关的WebSocket频道
        await self._ws_client.subscribe_wallet()   # 钱包更新
        await self._ws_client.subscribe_margin()   # 保证金更新
        
        # 2. 主动查询一次初始状态
        await self._update_account_state()
    
    async def _update_account_state(self) -> None:
        """主动查询账户状态"""
        # 3. 从HTTP API获取完整的AccountState
        pyo3_account_state = await self._http_client.request_account_state(
            self.pyo3_account_id
        )
        account_state = AccountState.from_dict(pyo3_account_state.to_dict())
        
        # 4. ✅ 更新Cache
        self.generate_account_state(
            balances=account_state.balances,
            margins=account_state.margins,
            reported=True,
            ts_event=self._clock.timestamp_ns(),
        )
    
    def _handle_msg(self, msg: Any) -> None:
        """WebSocket消息分发"""
        if isinstance(msg, nautilus_pyo3.AccountState):
            # 5. WebSocket推送AccountState
            self._handle_account_state(msg)
    
    def _handle_account_state(self, msg: nautilus_pyo3.AccountState) -> None:
        """处理WebSocket推送的账户状态"""
        account_state = AccountState.from_dict(msg.to_dict())
        
        # 6. ✅ 直接更新Cache
        self.generate_account_state(
            balances=account_state.balances,
            margins=account_state.margins,
            reported=account_state.is_reported,
            ts_event=account_state.ts_event,
        )
```

---

## 🔍 模式3：回调事件驱动（Interactive Brokers）

### 实现机制

Interactive Brokers通过**API回调**接收账户更新。

### 关键代码

```python
# 文件: nautilus_trader/adapters/interactive_brokers/execution.py

class InteractiveBrokersExecutionClient(LiveExecutionClient):
    def _handle_account_summary_update(self, currency: str) -> None:
        """IB API回调：账户摘要更新"""
        # 从账户摘要中提取余额信息
        total = self._account_summary[currency]["NetLiquidation"]
        locked = self._account_summary[currency]["InitMarginReq"]
        free = total - locked
        
        # ✅ 构建AccountBalance
        account_balance = AccountBalance(
            total=Money(total, Currency.from_str(currency)),
            free=Money(free, Currency.from_str(currency)),   # ← 可用余额
            locked=Money(locked, Currency.from_str(currency)),
        )
        
        # 保证金信息
        margin_balance = MarginBalance(
            initial=Money(
                self._account_summary[currency]["FullInitMarginReq"],
                currency=Currency.from_str(currency),
            ),
            maintenance=Money(
                self._account_summary[currency]["FullMaintMarginReq"],
                currency=Currency.from_str(currency),
            ),
        )
        
        # ✅ 更新Cache
        self.generate_account_state(
            balances=[account_balance],
            margins=[margin_balance],
            reported=True,
            ts_event=self._clock.timestamp_ns(),
        )
```

---

## 🎯 你的Adapter实现方案

根据JVQuant API的特点，建议采用**混合模式**：

### 方案：主动查询 + 事件触发

```python
# 文件: etf_159506_adapter.py

class ETF159506NautilusExecClient(LiveExecutionClient):
    def __init__(self, ...):
        super().__init__(...)
        
        # 定期更新账户状态的任务
        self._account_update_task = None
        self._account_update_interval = 10  # 每10秒更新一次
    
    async def _connect(self) -> None:
        """连接时初始化"""
        # 1. 获取交易服务器
        await self._get_trade_server()
        
        # 2. 登录
        success = await self.login(self.trade_account, self.trade_password)
        if not success:
            self._log.error("登录失败")
            return
        
        # 3. ✅ 初始化账户状态
        await self._update_account_state()
        
        # 4. ✅ 启动定期更新任务
        self._account_update_task = self.create_task(
            self._periodic_account_update()
        )
        
        self._set_connected(True)
    
    async def _update_account_state(self) -> None:
        """查询并更新账户状态"""
        try:
            # 5. 调用JVQuant API查询持仓
            account_info = await self._check_positions()
            if not account_info:
                self._log.warning("无法获取账户信息")
                return
            
            # 6. ✅ 构建AccountBalance（关键步骤）
            total = account_info['total']
            usable = account_info['usable']
            locked = total - usable
            
            balances = [
                AccountBalance(
                    total=Money(total, CNY),        # 总资产
                    locked=Money(locked, CNY),      # 冻结资金
                    free=Money(usable, CNY),        # ✅ 可用资金（重要！）
                )
            ]
            
            # 7. ✅ 调用generate_account_state更新Cache
            self.generate_account_state(
                balances=balances,
                margins=[],
                reported=True,  # 表示数据来自券商
                ts_event=self._clock.timestamp_ns(),
            )
            
            self._log.info(
                f"账户状态已更新: 总资产={total:.2f}, "
                f"可用={usable:.2f}, 冻结={locked:.2f}"
            )
            
        except Exception as e:
            self._log.error(f"更新账户状态失败: {e}")
    
    async def _periodic_account_update(self) -> None:
        """定期更新账户状态"""
        try:
            while self.is_connected:
                await asyncio.sleep(self._account_update_interval)
                
                # 8. ✅ 定期调用更新
                await self._update_account_state()
                
        except asyncio.CancelledError:
            self._log.info("账户更新任务已取消")
        except Exception as e:
            self._log.exception("账户更新任务异常", e)
    
    async def _disconnect(self) -> None:
        """断开连接"""
        # 9. 取消定期更新任务
        if self._account_update_task:
            self._account_update_task.cancel()
            try:
                await self._account_update_task
            except asyncio.CancelledError:
                pass
        
        self._set_connected(False)
    
    # ========== 事件触发更新 ==========
    
    async def submit_order(self, command: SubmitOrder) -> None:
        """提交订单后更新账户"""
        # 执行下单
        result = await self._submit_order_impl(command)
        
        if result:
            # 10. ✅ 订单成功后立即更新账户状态
            await self._update_account_state()
    
    async def cancel_order(self, command: CancelOrder) -> None:
        """撤单后更新账户"""
        result = await self._cancel_order_impl(command)
        
        if result:
            # 11. ✅ 撤单成功后立即更新账户状态
            await self._update_account_state()
```

---

## 🎨 完整实现示例

### 第一步：修改`_check_positions`方法

```python
async def _check_positions(self) -> Optional[Dict]:
    """查询持仓信息"""
    try:
        if not self._check_login_status():
            logger.error("未登录或登录已过期")
            return None
        
        url = f"{self.trade_server}/check_hold"
        params = {
            'token': self.token,
            'ticket': self.ticket,
        }
        
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        
        if data.get("code") == "0":
            # ✅ 返回账户信息
            account_info = {
                'total': float(data.get('total', 0)),      # 总资产
                'usable': float(data.get('usable', 0)),    # ✅ 可用资金
                'day_earn': float(data.get('day_earn', 0)),
                'hold_earn': float(data.get('hold_earn', 0)),
                'hold_list': data.get('hold_list', [])
            }
            
            logger.info(f"账户信息: 总资产={account_info['total']:.2f}, "
                       f"可用={account_info['usable']:.2f}")
            
            return account_info
        else:
            logger.error(f"查询持仓失败: {data.get('message', '')}")
            return None
            
    except Exception as e:
        logger.error(f"查询持仓异常: {e}")
        return None
```

### 第二步：在`_connect`中调用

```python
async def _connect(self) -> None:
    """连接到JVQuant"""
    # 获取交易服务器
    if not await self._get_trade_server():
        return
    
    # 登录
    if not await self.login(self.trade_account, self.trade_password):
        return
    
    # ✅ 更新账户状态
    await self._update_account_state()
    
    # ✅ 启动定期更新任务
    self._account_update_task = self.create_task(
        self._periodic_account_update()
    )
    
    self._set_connected(True)
    self._log.info("JVQuant execution client connected", LogColor.GREEN)
```

---

## ⚙️ 配置选项

可以根据需要调整更新频率：

```python
class JVQuantConfig:
    account_update_interval: int = 10    # 每10秒更新一次
    update_on_order: bool = True         # 下单后立即更新
    update_on_cancel: bool = True        # 撤单后立即更新
    update_on_fill: bool = True          # 成交后立即更新
```

---

## 🔑 关键要点总结

### 1. **必须调用`generate_account_state()`**
   
所有adapter都必须调用此方法来更新Cache：

```python
self.generate_account_state(
    balances=[...],      # AccountBalance对象列表
    margins=[],          # 保证金账户才需要
    reported=True,       # True表示数据来自券商
    ts_event=ts_now,     # 事件时间戳
)
```

### 2. **正确构建AccountBalance**

```python
AccountBalance(
    total=Money(total_amount, currency),    # 总余额
    locked=Money(locked_amount, currency),  # 冻结余额
    free=Money(free_amount, currency),      # ✅ 可用余额（最重要！）
)
```

**映射关系**：
- JVQuant `total` → `AccountBalance.total`
- JVQuant `usable` → `AccountBalance.free` ✅
- `total - usable` → `AccountBalance.locked`

### 3. **更新时机**

建议在以下时机更新：
- ✅ **连接时**：`_connect()` 中首次查询
- ✅ **定期轮询**：每10-30秒查询一次
- ✅ **交易后**：下单、撤单、成交后立即更新
- ⚠️ **避免过于频繁**：防止API限流

### 4. **异步任务管理**

```python
# 启动任务
self._task = self.create_task(self._periodic_update())

# 取消任务（断开连接时）
if self._task:
    self._task.cancel()
    try:
        await self._task
    except asyncio.CancelledError:
        pass
```

---

## 📊 对比表格

| Adapter | 更新方式 | 更新频率 | 优点 | 缺点 |
|---------|---------|---------|------|------|
| **Binance** | WebSocket推送 | 实时 | 延迟低，无轮询 | 需要WebSocket支持 |
| **Bitmex** | WebSocket推送 | 实时 | 延迟低，数据完整 | 需要WebSocket支持 |
| **IB** | API回调 | 实时 | 原生集成 | 依赖特定API |
| **JVQuant** | 定期轮询+事件触发 | 10-30秒 | 实现简单 | 有轮询延迟 |

---

## 🎓 参考文档

- [Nautilus Accounting API](https://nautilustrader.io/docs/latest/api_reference/accounting/)
- [LiveExecutionClient源码](file:///c:/Users/hedz/Downloads/nautilus_trader-develop/nautilus_trader/live/execution_client.py)
- Binance Adapter: `nautilus_trader/adapters/binance/spot/execution.py`
- Bitmex Adapter: `nautilus_trader/adapters/bitmex/execution.py`
- IB Adapter: `nautilus_trader/adapters/interactive_brokers/execution.py`

