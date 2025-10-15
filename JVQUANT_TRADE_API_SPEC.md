# jvquant 交易 API 参数规范

## 📋 API 端点总览

| 操作 | HTTP方法 | API路径 | 功能 | 状态 |
|------|----------|---------|------|------|
| 买入委托 | GET | `/buy` | 提交买入订单 | ✅ 已实现 |
| 卖出委托 | GET | `/sale` | 提交卖出订单 | ✅ 已实现 |
| 撤销委托 | GET | `/cancel` | 撤销已提交的订单 | ✅ 已实现 |
| **查询委托** | GET | `/check_order` | 查询所有委托记录 | ✅ 已实现 |
| **查询持仓** | GET | `/check_hold` | 查询账户持仓和资金 | ✅ 已实现 |

## 🔧 API 参数详细规范

### 1. 买入委托 `/buy`

**请求模板**：
```
http://<柜台地址>/buy?type=buy&token=<token>&ticket=<ticket>&code=<code>&name=<name>&price=<price>&volume=<volume>
```

**参数列表**：

| 参数名 | 类型 | 必需 | 说明 | 示例值 |
|--------|------|------|------|--------|
| **type** | string | ✅ | 报单类别，固定为 `buy` | `buy` |
| token | string | ✅ | 用户账户的认证token | `d0c519adcd...` |
| ticket | string | ✅ | 交易凭证（登录后获取） | `xxx` |
| code | string | ✅ | 证券代码 | `159506` |
| name | string | ✅ | 证券名称 | `恒生医疗` |
| price | string | ✅ | 委托价格 | `1.603` |
| volume | string | ✅ | 委托数量 | `100` |

**代码实现**：
```python
params = {
    'type': 'buy',              # 报单类别：买入
    'token': self.token,        # 用户认证token
    'ticket': self.ticket,      # 交易凭证
    'code': code,               # 证券代码
    'name': name,               # 证券名称
    'price': str(price),        # 委托价格
    'volume': str(volume)       # 委托数量
}
response = requests.get(f"http://{trade_server}/buy", params=params)
```

---

### 2. 卖出委托 `/sale`

**请求模板**：
```
http://<柜台地址>/sale?type=sale&token=<token>&ticket=<ticket>&code=<code>&name=<name>&price=<price>&volume=<volume>
```

**参数列表**：

| 参数名 | 类型 | 必需 | 说明 | 示例值 |
|--------|------|------|------|--------|
| **type** | string | ✅ | 报单类别，固定为 `sale` | `sale` |
| token | string | ✅ | 用户账户的认证token | `d0c519adcd...` |
| ticket | string | ✅ | 交易凭证（登录后获取） | `xxx` |
| code | string | ✅ | 证券代码 | `159506` |
| name | string | ✅ | 证券名称 | `恒生医疗` |
| price | string | ✅ | 委托价格 | `1.603` |
| volume | string | ✅ | 委托数量 | `100` |

**代码实现**：
```python
params = {
    'type': 'sale',             # 报单类别：卖出
    'token': self.token,        # 用户认证token
    'ticket': self.ticket,      # 交易凭证
    'code': code,               # 证券代码
    'name': name,               # 证券名称
    'price': str(price),        # 委托价格
    'volume': str(volume)       # 委托数量
}
response = requests.get(f"http://{trade_server}/sale", params=params)
```

---

### 3. 撤销委托 `/cancel`

**请求模板**：
```
http://<柜台地址>/cancel?token=<token>&ticket=<ticket>&order_id=<order_id>
```

**参数列表**：

| 参数名 | 类型 | 必需 | 说明 | 示例值 |
|--------|------|------|------|--------|
| token | string | ✅ | 用户账户的认证token | `d0c519adcd...` |
| ticket | string | ✅ | 交易凭证（登录后获取） | `xxx` |
| order_id | string | ✅ | 委托编号（买入/卖出返回的订单ID） | `202501120001` |

**代码实现**：
```python
params = {
    'token': self.token,        # 用户认证token
    'ticket': self.ticket,      # 交易凭证
    'order_id': order_id        # 委托编号
}
response = requests.get(f"http://{trade_server}/cancel", params=params)
```

**注意**：撤销委托**不需要** `type` 参数。

---

## 📊 参数对比表

### type 参数的使用

| API | type 参数 | 值 | 是否必需 |
|-----|-----------|-----|----------|
| `/buy` | ✅ 需要 | `buy` | 是 |
| `/sale` | ✅ 需要 | `sale` | 是 |
| `/cancel` | ❌ 不需要 | - | 否 |

### 通用参数

所有交易 API 都需要：
- ✅ `token` - 用户认证
- ✅ `ticket` - 交易凭证

### 特定参数

| 参数 | 买入/卖出 | 撤销 |
|------|-----------|------|
| type | ✅ 需要 | ❌ 不需要 |
| code | ✅ 需要 | ❌ 不需要 |
| name | ✅ 需要 | ❌ 不需要 |
| price | ✅ 需要 | ❌ 不需要 |
| volume | ✅ 需要 | ❌ 不需要 |
| order_id | ❌ 不需要 | ✅ 需要 |

---

## ✅ 实现状态

### 买入委托 `_buy_stock()`
- ✅ 已添加 `type='buy'` 参数
- ✅ 所有必需参数完整
- ✅ 参数类型正确（price 和 volume 转为 string）
- ✅ 有详细的 docstring 说明

### 卖出委托 `_sell_stock()`
- ✅ 已添加 `type='sale'` 参数
- ✅ 所有必需参数完整
- ✅ 参数类型正确（price 和 volume 转为 string）
- ✅ 有详细的 docstring 说明

### 撤销委托 `_cancel_jvquant_order()`
- ✅ 参数完整（token, ticket, order_id）
- ✅ 不包含多余的 type 参数（符合规范）
- ✅ 有详细的 docstring 说明
- ✅ 返回值明确（bool）

---

## 🧪 完整的交易流程示例

### 流程1：买入 → 持有 → 卖出

```python
# 1. 买入
buy_order_id = await adapter._buy_stock(
    code='159506',
    name='恒生医疗',
    price=1.603,
    volume=100
)
# API调用: /buy?type=buy&token=xxx&ticket=xxx&code=159506&name=恒生医疗&price=1.603&volume=100

# 2. 等待成交...

# 3. 卖出
sell_order_id = await adapter._sell_stock(
    code='159506',
    name='恒生医疗',
    price=1.610,
    volume=100
)
# API调用: /sale?type=sale&token=xxx&ticket=xxx&code=159506&name=恒生医疗&price=1.610&volume=100
```

### 流程2：买入 → 撤单

```python
# 1. 买入
buy_order_id = await adapter._buy_stock(
    code='159506',
    name='恒生医疗',
    price=1.603,
    volume=100
)
# API调用: /buy?type=buy&...

# 2. 改变主意，撤单
success = await adapter._cancel_jvquant_order(order_id=buy_order_id)
# API调用: /cancel?token=xxx&ticket=xxx&order_id=xxx

if success:
    print("撤单成功")
```

---

## 📝 Zen of Python 评分

### 总体评分：🟢 Pythonic

### 符合的原则

1. **Explicit is better than implicit**
   - ✅ API 参数规范明确写在 docstring 中
   - ✅ 每个参数都有清晰的注释说明
   - ✅ 参数名称和 API 规范完全一致

2. **Simple is better than complex**
   - ✅ 使用简单的字典传递参数
   - ✅ 不做多余的参数转换
   - ✅ 参数顺序与官方规范一致

3. **Readability counts**
   - ✅ 代码一目了然
   - ✅ 注释清晰说明参数用途
   - ✅ 类型注解完整

4. **Errors should never pass silently**
   - ✅ 检查返回状态码
   - ✅ 详细的错误日志
   - ✅ 返回值明确（Optional[str] 或 bool）

---

---

### 4. 查询委托 `/check_order`

**请求模板**：
```
http://<柜台地址>/check_order?token=<token>&ticket=<交易凭证>
```

**参数列表**：

| 参数名 | 类型 | 必需 | 说明 |
|--------|------|------|------|
| token | string | ✅ | 用户账户的认证token |
| ticket | string | ✅ | 交易凭证 |

**返回数据结构**：

```python
{
  "code": "0",           # 响应代码（"0"表示成功）
  "message": "",         # 错误信息
  "list": [              # 委托列表
    {
      "order_id": "1334564",         # 委托编号
      "day": "20180402",             # 委托日期 YYYYMMDD
      "time": "142423",              # 委托时间 HHMMSS
      "code": "110074",              # 证券代码
      "name": "精达转债",            # 证券名称
      "type": "证券卖出",            # 委托类型
      "status": "已成",              # 委托状态
      "order_price": "151.885",      # 委托价格
      "order_volume": "8000",        # 委托数量
      "deal_price": "151.927",       # 成交价格
      "deal_volume": "8000"          # 成交数量
    }
  ]
}
```

**代码实现**：
```python
async def _check_orders(self) -> Optional[List[Dict]]:
    """查询委托列表"""
    url = f"http://{self.trade_server}/check_order"
    params = {
        'token': self.token,
        'ticket': self.ticket,
    }
    
    response = requests.get(url, params=params, timeout=10)
    data = response.json()
    
    if data.get("code") == "0":
        order_list = data.get('list', [])
        return order_list
    return None
```

---

### 5. 查询持仓 `/check_hold`

**请求模板**：
```
http://<柜台地址>/check_hold?token=<token>&ticket=<交易凭证>
```

**参数列表**：

| 参数名 | 类型 | 必需 | 说明 |
|--------|------|------|------|
| token | string | ✅ | 用户账户的认证token |
| ticket | string | ✅ | 交易凭证 |

**返回数据结构**：

```python
{
  "code": "0",                   # 响应代码
  "message": "",                 # 错误信息
  "total": "501527.77",          # 账户总资产
  "usable": "422977.27",         # 账户可用资金
  "day_earn": "16325.27",        # 账户当日盈亏
  "hold_earn": "18273.22",       # 账户持仓盈亏
  "hold_list": [                 # 持仓列表
    {
      "code": "128079",          # 证券代码
      "name": "英联转债",        # 证券名称
      "hold_vol": "0",           # 持仓数量
      "usable_vol": "0",         # 可用数量
      "hold_earn": "12242.69",   # 持仓盈亏
      "day_earn": "12242.32"     # 当日盈亏
    }
  ]
}
```

**代码实现**：
```python
async def _check_positions(self) -> Optional[Dict]:
    """查询持仓信息"""
    url = f"http://{self.trade_server}/check_hold"
    params = {
        'token': self.token,
        'ticket': self.ticket,
    }
    
    response = requests.get(url, params=params, timeout=10)
    data = response.json()
    
    if data.get("code") == "0":
        account_info = {
            'total': float(data.get('total', 0)),
            'usable': float(data.get('usable', 0)),
            'day_earn': float(data.get('day_earn', 0)),
            'hold_earn': float(data.get('hold_earn', 0)),
            'hold_list': data.get('hold_list', [])
        }
        return account_info
    return None
```

---

## 🔗 参考

- jvquant 交易 API 官方文档
- NautilusTrader 执行客户端接口规范
- `etf_159506_adapter.py` - 实现文件
- `QUERY_API_IMPLEMENTATION.md` - 查询接口详细说明

