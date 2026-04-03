# 基于项目笔记的 ML 策略学习清单

本材料由 [ml_learning_notes.md](ml_learning_notes.md) 扩展而来，按「概念 → 自测 → 动手」组织，可直接按周推进。

---

## 使用方式

- 每节先读笔记对应章节，再完成**自测题**；能闭卷答出即可进入下一节。
- **实践任务**在仓库根目录执行，优先使用 `uv run python <脚本>.py`（与项目约定一致）。
- 遇到术语可查文末**术语表**。

---

## 模块 A：标签与类别分布

**对应笔记**：§1  
**你要能回答**：标签如何从价格生成；三类样本各代表什么；改参数会怎么改变类别比例。

### 核心要点

| 参数 | 含义 | 调大时常见效果 |
|------|------|----------------|
| `label_forward_minutes` | 向前看 N 根 K 线算收益 | 末尾无效行变多，样本略减 |
| `label_threshold` | 判定强多/强空的收益门槛 | 极端类变少，hold 变多 |

### 自测题

1. 若 `label_threshold` 从 `0.002` 调到 `0.005`，三类标签的数量大致如何变化？为什么？
2. 训练时 `-1` 为何要映射成类别 `2`？（提示：XGBoost 多分类类别索引）
3. `future_return` 会不会作为特征进入 `X`？在代码里如何排除？

### 实践任务

1. 运行特征构建 CLI，观察标签分布：
   ```bash
   uv run python ml_feature_builder.py
   ```
   记下控制台打印的 `value_counts`。
2. 修改 `FeatureConfig` 中 `label_threshold` 再运行（或写一小段脚本只 `build_features` 并打印 `label.value_counts(normalize=True)`），对比两类设置下的 hold / buy / sell 占比。

---

## 模块 B：泄漏与时序验证

**对应笔记**：§2  
**你要能回答**：什么是标签重叠；为何不能对分钟数据做随机 K 折；`gap` 在缓解什么。

### 核心要点

- **特征泄漏**：把未来信息或标签泄漏进特征（本项目正常路径不包含 `future_return` 作特征）。
- **标签重叠泄漏**：相邻行标签共享未来价格路径；随机划分会让验证集「见过类似未来」，指标虚高。
- **TimeSeriesSplit**：按时间切块，测试总在训练之后。
- **gap**：训练块与测试块之间空出若干行，削弱边界上的统计依赖（思想接近 AFML 的 embargo）。

### 自测题

1. 用一句话说明：为什么「相邻两分钟的两个样本」在统计上不独立？
2. `GAP=30` 与 `label_forward_minutes=5` 的关系：gap 太小可能带来什么问题？
3. Purged K-Fold 与本项目的 `TimeSeriesSplit+gap` 相比，多做了哪类额外操作？（见笔记 §4 表格）

### 实践任务

1. 打开 [ml_signal_trainer.py](../ml_signal_trainer.py)，确认 `TimeSeriesSplit` 的 `n_splits` 与 `gap` 常量。
2. （可选）将 `GAP` 改为 `10` 与 `50` 各训练一次，观察平均 F1 变化——**仅作直觉实验**，不据此单点调参下结论。

---

## 模块 C：分类指标 vs 资金曲线

**对应笔记**：§3  
**你要能回答**：Walk-Forward 的 F1 衡量什么；回测衡量什么；二者为何可能背离。

### 核心要点

| 维度 | Walk-Forward（分类） | 全样本回测 |
|------|----------------------|------------|
| 优化目标 | 预测对「标签」有多好 | 按阈值交易后赚不赚钱 |
| 成本 | 通常未直接进损失函数 | 佣金、滑点、整手等 |

### 自测题

1. 举出一个「F1 不错但回测很差」的可能原因（至少一条）。
2. 举出一个「回测不错但 F1 一般」的可能原因（提示：阈值、交易频率、趋势段）。

### 实践任务

1. 跑标签敏感性（会同时给出 WF 指标与回测摘要）：
   ```bash
   uv run python ml_label_sensitivity.py --forwards 5 10 --thresholds 0.0015 0.002
   ```
2. 打开生成的 `models/label_sensitivity_report.csv`，找一行 `wf_avg_f1` 高但 `bt_total_return` 偏低的组合，用模块 C 的知识写两三句解释假设。

---

## 模块 D：阈值、成本与决策

**对应笔记**：§3、§5  
**你要能回答**：概率阈值如何改变交易次数；佣金与滑点如何挤压边际利润。

### 实践任务

1. 在固定模型前提下扫描阈值与成本：
   ```bash
   uv run python ml_threshold_sweep.py --buy 0.35 0.45 0.55 --sell 0.35 0.45 0.55 --commission 0.0002 0.0003 --slippage 0.0005 0.001
   ```
2. 阅读 `models/threshold_cost_sweep.csv` 排序后的结果：在相似 `total_return` 下，`total_trades` 更高通常意味着什么风险？

---

## 模块 E：AFML 进阶方向（选学）

**对应笔记**：§4  
**学习目标**：能向他人说明 triple barrier、purged CV、embargo 各解决什么问题；知道本项目未实现哪些、何时值得引入。

### 自测题

1. Triple barrier 标签与「固定 N 分钟收益阈值」相比，多考虑了哪些现实因素？
2. 若你改用「持仓直到触及止损/止盈」的事件标签，交叉验证应更注意什么？

### 延伸阅读（与笔记一致）

- Marcos López de Prado：*Advances in Financial Machine Learning*（标签与 CV 相关章节）。
- 统计学习基础：James et al. *ISL*（分类与验证集思维）。

---

## 与仓库脚本的对应关系（速查）

| 学习模块 | 主要脚本 |
|----------|----------|
| A 标签 | [ml_feature_builder.py](../ml_feature_builder.py)，[ml_label_sensitivity.py](../ml_label_sensitivity.py) |
| B 训练/划分 | [ml_signal_trainer.py](../ml_signal_trainer.py) |
| C 回测 | [ml_backtest_runner.py](../ml_backtest_runner.py)，`ml_label_sensitivity.py` |
| D 阈值/成本 | [ml_threshold_sweep.py](../ml_threshold_sweep.py)，`BacktestConfig` |
| 可解释性 | [ml_feature_analysis.py](../ml_feature_analysis.py) |
| 实盘一致 | [ml_incremental_features.py](../ml_incremental_features.py)，[ml_paper_trading.py](../ml_paper_trading.py) |

---

## 建议周节奏（可按需压缩）

| 周次 | 重点 | 完成标志 |
|------|------|----------|
| 第 1 周 | 模块 A + B：标签含义、泄漏与 gap | 自测题能口述；跑通 `ml_feature_builder.py` |
| 第 2 周 | 模块 C + D：指标与回测、阈值扫描 | 完成 `ml_label_sensitivity` 与 `ml_threshold_sweep` 各一次并写简短结论 |
| 第 3 周 | SHAP + 模拟盘对齐（可选） | 跑 [ml_feature_analysis.py](../ml_feature_analysis.py)；对比 paper 与回测信号频率 |
| 持续 | 模块 E + AFML 选章 | 能画一张「本项目 vs AFML」差异表（笔记 §4 可复用） |

---

## 术语表

| 术语 | 简释 |
|------|------|
| Walk-Forward | 沿时间滚动的训练/测试划分，本项目用 `TimeSeriesSplit` 实现多折。 |
| gap / embargo | 在训练与测试之间留空，减少因标签窗口重叠带来的虚假相关。 |
| 标签重叠 | 相邻样本的标签依赖重叠的未来价格区间，导致样本不独立。 |
| Triple barrier | 用上/下/时间三障碍决定标签，常用于事件驱动研究。 |
| Purged K-Fold | 删除与测试集时间段重叠的训练样本后再训练。 |
| 校准 | 让模型输出的概率与实际频率一致，对阈值策略有意义。 |

---

## 学习成果自检（总复习）

完成下列任三项即表示已把笔记吃透并能指导实验：

- [ ] 向他人解释 `label_forward_minutes` 与 `label_threshold` 对类别分布的影响。
- [ ] 解释为何随机 K 折在分钟级标签上不可靠。
- [ ] 用一次 `label_sensitivity` 或 `threshold_sweep` 的结果说明「F1 与收益不必同向」。
- [ ] 说出 AFML 中 Purged CV 与本项目 `gap` 的至少一点差异。
- [ ] 列出 `ml_paper_trading` 与 `ml_backtest_runner` 在输入与假设上的至少一条差异。
