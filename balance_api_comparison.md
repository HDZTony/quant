# Nautilus Trader 账户余额API对比

## 场景说明

假设你的账户状态如下：
- 初始现金: 10,000 CNY
- 已买入持仓: 6,000股 @ 1.50 = 9,000 CNY市值
- 挂单冻结: 500 CNY

## API返回值对比

```python
account = self.cache.account_for_venue(venue)

# ❌ 错误用法 - balance_total()
total = account.balance_total(CNY).as_double()
# 返回: 10,000 CNY（仅现金部分，不包括持仓市值）
# 或者在某些实现中可能返回: 19,000 CNY（现金 + 持仓）

# ✅ 正确用法 - balance_free()  
free = account.balance_free(CNY).as_double()
# 返回: 500 CNY（真正可用于交易的现金）

# 📊 冻结资金 - balance_locked()
locked = account.balance_locked(CNY).as_double()
# 返回: 500 CNY（挂单冻结的资金）
```

## 问题重现

### 使用 balance_total() 的问题

```python
# 第一次买入
available = account.balance_total(CNY).as_double()  # 10,000
quantity = int(10000 / 1.553)  # 6,439股
# 买入后: 现金剩余 0.23 CNY

# 第二次尝试（定时买入触发）
available = account.balance_total(CNY).as_double()  # ⚠️ 可能仍然返回 10,000
# 因为 balance_total 可能包括持仓市值
quantity = int(10000 / 1.553)  # 6,439股
# ❌ 实际可用现金不足，导致:
#   - 如果 balance_total 只返回现金: quantity=0，跳过（之前的bug）
#   - 如果 balance_total 包括持仓: 重复买入失败
```

### 使用 balance_free() 的正确逻辑

```python
# 第一次买入
free = account.balance_free(CNY).as_double()  # 10,000
quantity = int(10000 / 1.553)  # 6,439股
# 买入后: 可用现金 0.23 CNY

# 第二次尝试（定时买入触发）
free = account.balance_free(CNY).as_double()  # 0.23
quantity = int(0.23 / 1.553)  # 0
# ✅ 正确识别余额不足，返回 False，可以重试
```

## 官方文档参考

根据 [Nautilus Trader Position文档](https://nautilustrader.io/docs/latest/concepts/positions):

> Positions aggregate order fills to maintain an accurate view of market exposure.
> The Portfolio aggregates positions across instruments and strategies.

关键点：
1. **Position** 代表持仓暴露
2. **Portfolio** 聚合所有持仓和账户状态
3. **Account** 提供账户级别的余额信息

## 修复总结

### 修复前的问题
```python
available_balance = account.balance_total().as_double()  # ❌
# 1. 可能包括持仓市值，导致重复买入
# 2. 或者第一次买入后返回0，触发"数量无效"
# 3. 但标记已设置，无法重试
```

### 修复后的正确做法
```python
free_balance = account.balance_free(CNY)  # ✅
if free_balance is None:
    return False
    
available_balance = free_balance.as_double()

# 详细日志
total_balance = account.balance_total(CNY)
locked_balance = account.balance_locked(CNY)
self._log.info(f"账户余额详情: "
              f"总余额={total_balance.as_double():.2f}, "
              f"可用余额={available_balance:.2f}, "
              f"冻结余额={locked_balance.as_double() if locked_balance else 0:.2f}")
```

## Pythonic原则体现

✅ **Explicit is better than implicit**
- 明确使用 `balance_free` 而不是模糊的 `balance_total`

✅ **Errors should never pass silently**  
- 检查 `None` 返回值，记录错误日志
- 返回 `False` 表示失败，允许重试

✅ **Readability counts**
- 详细的日志输出，清晰展示三种余额

## 相关API文档

- [Positions - NautilusTrader](https://nautilustrader.io/docs/latest/concepts/positions)
- [Orders - NautilusTrader](https://nautilustrader.io/docs/latest/concepts/orders)
- [Portfolio - NautilusTrader](https://nautilustrader.io/docs/latest/concepts/portfolio)

类型定义参考:
- `nautilus_trader/core/nautilus_pyo3.pyi` Line 629-634

