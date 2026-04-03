# ML 策略流水线：泄漏、类别分布与验证方法

本文档对照 [ml_feature_builder.py](../ml_feature_builder.py)、[ml_signal_trainer.py](../ml_signal_trainer.py) 中的实现，说明「数据泄漏」、标签类别分布，以及与本项目 **TimeSeriesSplit + gap** 相关的注意点；并简述 *Advances in Financial Machine Learning*（AFML）中 **Purged K-Fold / 事件驱动标签** 与当前实现的差异。

---

## 1. 标签如何定义（`FeatureConfig`）

- `label_forward_minutes`（默认 `5`）：用当前 bar 收盘价与未来第 N 根 bar 的收盘价计算远期收益率  
  `future_return = (close.shift(-N) - close) / close`。
- `label_threshold`（默认 `0.002`）：  
  - `future_return > threshold` → 标签 `1`（买入机会）  
  - `future_return < -threshold` → 标签 `-1`（卖出机会）  
  - 其余 → `0`（持有/中性）

**类别分布**：`threshold` 越大，`|future_return|` 超过阈值的样本越少，`|1|` 与 `|-1|` 类通常变少，`0` 类占比上升。`N` 越大，末尾被 `shift(-N)` 丢掉的行越多，有效样本略减。

训练时 [ml_signal_trainer.py](../ml_signal_trainer.py) 将 `-1` 映射为类别索引 `2`，`0/1` 保持为 `0/1`，以适配 XGBoost 多分类。

---

## 2. 「泄漏」在本项目里指什么

### 2.1 特征侧

- 特征仅使用 **当前及过去** 的 OHLCV（及由它们算出的指标），不包含 `future_return` / `label` 进入特征列（`get_feature_columns` 已排除）。
- 若在别处误把 `future_return` 当特征，则属于 **直接泄漏**，本项目正常路径应避免。

### 2.2 标签侧（时间重叠）

- 相邻分钟 bar 的「未来 N 分钟收益」在时间轴上 **高度重叠**：第 `t` 与 `t+1` 行的标签都依赖 `t` 之后的一段价格路径。
- 若随机打乱样本或做普通 K 折，训练集与测试集会包含 **信息上重叠** 的样本，验证分数会 **过于乐观**。

### 2.3 当前缓解：`TimeSeriesSplit` + `GAP`

[ml_signal_trainer.py](../ml_signal_trainer.py) 使用 `TimeSeriesSplit(n_splits=N_SPLITS, gap=GAP)`（默认 `N_SPLITS=5`，`GAP=30` 分钟）。

- **时序划分**：测试块在时间上晚于训练块，符合「用过去预测未来」。
- **gap**：在训练块与测试块之间 **挖掉 `gap` 行**，削弱训练尾部与测试头部因标签窗口重叠而产生的依赖（与 AFML 的 *embargo* 思想类似，但实现更简单）。

**实践建议**：`GAP` 不宜远小于 `label_forward_minutes`；若增大 `label_forward_minutes`，可考虑同步增大 `GAP`，使「训练段末尾」与「测试段开头」在时间上隔得更开（具体数值需结合数据频率与实验权衡）。

---

## 3. Walk-Forward 与回测曲线为何可能不一致

- Walk-Forward 报告的是 **各折测试段上的分类指标**（precision / recall / F1）。
- [ml_backtest_runner.py](../ml_backtest_runner.py) 在全样本时间轴上按 **概率阈值** 模拟交易，并含佣金、滑点、整手等约束。
- 因此 **F1 高不等于资金曲线好**：类别平衡、阈值、成本与交易频率都会让二者脱节。应用 [ml_label_sensitivity.py](../ml_label_sensitivity.py) 与 [ml_threshold_sweep.py](../ml_threshold_sweep.py) 做对比实验。

---

## 4. AFML：Purged CV / 事件标签 与当前代码的差异（摘要）

以下对应 *Advances in Financial Machine Learning* 中常见提法，便于与当前仓库对比（非全书摘录）。

| 概念 | AFML 典型做法 | 当前项目 |
|------|----------------|----------|
| 标签 | 三重障碍（triple barrier）、波动率缩放阈值、事件采样等 | 固定 horizon 的远期收益 + 对称阈值 → 三分类 |
| 交叉验证 | Purged K-Fold：删除与测试集事件 **时间重叠** 的训练样本；常配合 **embargo**（测试后一段禁止训练） | `TimeSeriesSplit` + `gap`，按 **行** 切分，不做基于事件重叠的 purge |
| 泄漏控制 | 与标签窗口、持仓时间显式对齐 | 依赖时序折 + gap，较粗粒度 |

**结论**：当前实现 **更简单、易维护**；若标签窗口变长或采用事件驱动标签，可再评估是否引入「按时间 purge」或增大 `GAP` / embargo，以更接近 AFML 的统计假设。

---

## 5. 相关脚本

| 脚本 | 用途 |
|------|------|
| [ml_label_sensitivity.py](../ml_label_sensitivity.py) | 扫描 `label_forward_minutes` × `label_threshold`，输出 Walk-Forward 指标与全样本回测摘要 |
| [ml_threshold_sweep.py](../ml_threshold_sweep.py) | 固定已训练模型，扫描买卖概率阈值及可选佣金、滑点 |
