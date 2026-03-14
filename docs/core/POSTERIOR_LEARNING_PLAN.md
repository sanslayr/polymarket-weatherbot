# Posterior Learning Plan

Last updated: 2026-03-14
Status: Working design

## 1. 文档目标

本文件替代旧版 posterior learning 草案，统一回答 4 个问题：

1. 当前 runtime posterior 架构已经到了哪一步
2. 当前架构还存在哪些关键问题
3. 它是否已经支持 posterior 的持续学习
4. 接下来应该沿什么方法论继续推进，包括经典统计路线、ML 路线和更偏元方法的启发

这份文档不再把 posterior learning 仅仅描述成“离线训练一个模型”，而是把它视为一个完整系统：

- runtime 负责稳定推理
- logging 负责留样
- labeling 负责形成日级真值
- research 负责训练和评估
- artifact 负责发布、回滚和灰度
- guardrail 负责保证线上安全

## 2. 执行摘要

### 2.1 当前结论

当前 weather posterior 已经不再只是一个“把模型峰值换成分位数”的包装层。
它已经形成了可扩展的 runtime 骨架：

- `canonical_raw_state`
- `posterior_feature_vector`
- `quality_snapshot`
- `weather_posterior_core`
- `weather_posterior_calibration`

并且近窗收束已经从 snapshot 兜底，部分前移到了 posterior 自身。

### 2.2 当前最大现实

当前系统已经“支持插入学习”，但还没有“持续学习闭环”。

更准确地说：

- 已有可学习 posterior 的 runtime 边界
- 还没有完整的样本、标签、artifact、发布、监控链路

所以今天它更像：

- `learnable posterior runtime`

而不是：

- `continually learning posterior system`

### 2.3 最重要的哲学判断

posterior learning 不应试图“学习天气本身”，而应学习：

- 在既定物理路径下，误差如何演化
- 在不同 phase / station family / regime family 下，区间如何收缩或保留尾部
- 在高质量实况接近 resolution 时，何时应快速收敛

换句话说：

- weather core 负责解释世界
- posterior 负责量化不确定性
- learning 负责校正 posterior，而不是篡位成为新的天气主链

## 3. 当前 runtime posterior 结构

## 3.1 当前主链

当前运行时主链已经是：

1. `canonical_raw_state_service.py`
2. `posterior_feature_service.py`
3. `quality_snapshot_service.py`
4. `weather_posterior_core.py`
5. `weather_posterior_calibration.py`
6. `analysis_snapshot_service.py`

其中：

- `weather_posterior_core` 负责物理和机制锚定
- `weather_posterior_calibration` 负责质量修正、进度收缩、尾部约束
- `analysis_snapshot_service` 负责下游展示与 guardrail merge

## 3.2 当前 posterior 已经做了什么

当前 posterior 已具备以下能力：

1. 中心锚定
   - `modeled_peak`
   - `observed_max`
   - `daily_peak_state`
   - `short_term_state`
   - `ensemble alignment`
   - `regime adjustment`
2. 不确定性拆层
   - `heuristic spread`
   - `quality widening`
   - `progress shrink`
3. 事件概率
   - `P(new_high_next_60m)`
   - `P(lock_by_window_end)`
   - `P(exceed_modeled_peak)`
4. 上尾裁决
   - 用 observed anchor、event probabilities、phase 和 headroom 限制暖尾

当前 runtime 逻辑已可概括为：

- `spread_final = spread_core * quality_spread_multiplier * progress_spread_multiplier`
- `upper_tail <= observed_anchor + dynamic_allowance`

## 3.3 当前近窗收束依赖的特征

当前 runtime 已把以下变量明确接入 posterior：

- `modeled_headroom_c`
- `time_since_observed_peak_h`
- `reports_since_observed_peak`
- `latest_gap_below_observed_c`
- `hours_to_peak`
- `hours_to_window_end`
- `analysis_window_mode`

这意味着 posterior 已经开始从“只看状态标签”升级到“直接看实况进度”。

## 4. 当前架构 review

## 4.1 已做对的地方

### A. 边界已经成型

最值得肯定的是，posterior 已经从 render 层分离出来。
这件事比单个算法细节更重要，因为它决定了未来是否能接训练、评估和 artifact 发布。

### B. 已经具备多层后验结构

当前 posterior 不是一个单点函数，而是：

- core
- quality
- progress
- tail cap

这比“一个模型直接吐区间”更适合长期演化，因为每层都可以单独替换或单独学习。

### C. 已经尊重物理路径

当前设计没有把 learning 直接放在最上游覆盖天气解释链，而是保留：

- phase decision
- shape analysis
- synoptic / boundary-layer reasoning
- posterior 只做量化校正

这条原则非常重要，应该继续保持。

### D. 已开始显式追踪近窗进度

区间何时该快速收束，本质不是单一 `locked/open` 标签，而是一个连续进度问题。
现在 runtime 已经开始显式建模这个进度，这是走向真正 posterior 的必要一步。

## 4.2 当前关键问题

### A. 最终区间真相仍然分裂

当前最终展示区间仍经过 `analysis_snapshot_service.py` 的 merge / path-cap。
这意味着系统里仍存在两套区间影响力：

- posterior quantiles
- downstream guardrail merge

这在工程上是合理的，但在学习上有一个隐患：

- 训练时到底应该学习 posterior 原输出
- 还是学习最终展示输出
- 还是同时记录两者

如果这个问题不明确，后续 attribution 和 error analysis 会变得混乱。

### B. 持续学习需要的 offline 实体还没有真正存在

当前文档里原先提出这些模块：

- `posterior_factor_service.py`
- `posterior_training_log_service.py`
- `posterior_training_dataset_service.py`
- `posterior_daily_trainer.py`
- `posterior_artifact_registry.py`
- `posterior_case_index_service.py`

其中这批接口现在已经部分落地：

- `analysis_snapshot_view.py`
- `posterior_learning_sample_service.py`
- `posterior_case_index_service.py`
- `posterior_training_log_service.py`

但它们目前还是“积木层”，还没有全面接入 runtime entrypoint、label 回填和 artifact 发布链路。

所以系统虽然有 learning 的插槽，却还没有 learning 的生产线。

### C. calibration 还主要是硬编码

当前：

- quality widening 是公式
- progress shrink 是公式
- upper-tail allowance 是公式

这对现在非常有帮助，但还不构成“在线可升级的 calibration artifact”。

更直接地说：

- 今天更新 posterior，主要靠改代码
- 不是靠发布新 artifact

### D. city-specific 差异还主要停留在上游 heuristics

当前 station 差异主要通过这些途径间接进入 posterior：

- station prior
- phase decision
- 少量 regime rule

但 posterior 自身还没有真正形成：

- station-family calibration
- regime-family calibration
- hierarchical shrinkage

这会限制它对不同城市近窗行为差异的学习能力。

### E. learning 参数边界还不够干净

当前不少 heuristic runtime 参数仍放在：

- `config/tmax_learning_params.json`

这会造成一个长期问题：

- runtime heuristic params
- report policy params
- posterior learning params

容易混在一起。

一旦开始真正发布 learning artifact，这种混放会迅速变成维护风险。

### F. 契约和 provenance 仍需更严格

持续学习真正怕的不是模型弱，而是：

- schema 漂移
- 口径漂移
- 训练标签和线上输出不一致
- 线上版本不可追溯

因此 posterior 的每个训练样本、每个线上输出、每个 artifact，都必须更严格地记录：

- schema version
- extractor version
- label policy version
- runtime heuristic version
- artifact version
- fallback provenance

## 5. 当前架构是否支持 posterior 持续学习

## 5.1 结论

结论分成两层：

### 从架构可扩展性看

支持。

因为当前系统已经具备：

- 原始状态层
- feature 层
- posterior core
- calibration hook
- snapshot 汇总层

这正是学习后验所需的基本骨架。

### 从系统闭环能力看

还不支持。

因为持续学习至少还缺：

- append-only sample logging
- 日终 label 回填
- training dataset builder
- artifact manifest / registry
- promote / rollback / canary
- drift monitor
- online scorecard

因此更准确的表述应是：

- `架构支持持续学习`
- `当前实现尚不具备持续学习运营能力`

## 5.2 readiness matrix

### 已具备

- `canonical_raw_state`
- `posterior_feature_vector`
- `quality_snapshot`
- `weather_posterior_core`
- `weather_posterior_calibration`
- `analysis_snapshot`
- schema version 常量

### 部分具备

- station prior
- regime patch
- near-window progress-aware shrink
- posterior downstream guardrail

### 尚未具备

- posterior sample log
- final day label store
- factor bundle registry
- artifact registry
- case index
- promote gate
- rollback policy
- drift dashboard
- per-phase calibration scorecard

## 6. 目标系统：从可学习到可持续学习

## 6.1 总体原则

目标不是把 runtime 改成一个在线自训练系统。

目标是：

- runtime 稳定推理
- offline 训练和评估
- artifact 周期性发布
- runtime 只读 artifact

这是 weather posterior 这种高风险后验层更稳妥的路线。

## 6.2 推荐系统分层

### A. Runtime repo

职责：

- 构造原始状态
- 提取 posterior feature
- 生成 posterior
- 记录样本
- 加载 artifact
- 执行 fallback

### B. Research repo

职责：

- 构建训练视图
- 回填 label
- 训练 / 回测 / 评估
- 产出 artifact
- 发布 manifest

### C. Artifact layer

推荐产物：

- `feature_registry`
- `station_family_priors`
- `regime_family_priors`
- `center_model`
- `spread_calibration`
- `upper_tail_calibration`
- `event_probability_calibration`
- `case_index`
- `manifest`

## 6.3 runtime 必须记录哪些样本

推荐 append-only 记录 posterior sample。

每条样本至少包括：

- `sample_id`
- `station_icao`
- `date_local`
- `sample_time_local`
- `phase`
- `canonical_raw_state_ref`
- `posterior_feature_vector`
- `quality_snapshot`
- `weather_posterior_core`
- `weather_posterior`
- `final_snapshot_ranges`
- `range_truth_source`
- `sampling_reason`
- `schema versions`

当前 runtime 已经可以直接通过这些接口生成标准化样本：

- `build_posterior_learning_sample(...)`
- `build_posterior_case_index(...)`
- `append_posterior_learning_sample(...)`

建议 research / worker / cron 都通过这层取样，而不是各自从 `analysis_snapshot` 手工拼字段。

## 6.4 sampling reason

建议显式记录采样原因，而不是只做固定 cadence 采样。

例如：

- phase transition
- daily peak state change
- second peak state change
- posterior range material change
- event probability material change
- market anomaly
- pre-resolution / near-resolution checkpoint

## 6.5 日终 label

label 需要与 sample 分离存储。

建议 label 至少包括：

- `final_tmax_c`
- `final_tmax_lo_c`
- `final_tmax_hi_c`
- `final_peak_time_local`
- `lock_flag`
- `late_surge_flag`
- `second_peak_realized_flag`
- `label_policy_version`

## 7. learning 目标应如何拆解

posterior learning 不应是一体化黑箱。

推荐拆成 5 个头：

### 7.1 中心校正

- `delta_median_c`

解释：

- 在物理锚定之上，再学习中心偏移

### 7.2 进度条件化 spread 缩放

- `progress_spread_scale`

解释：

- 学习“在这个 phase、这个实况进度下，区间该收多快”

### 7.3 上尾 allowance

- `upper_tail_allowance_c`

解释：

- 不对称地学习暖尾应该保留多少空间

### 7.4 事件概率校正

- `P(new_high_next_60m)`
- `P(lock_by_window_end)`
- `P(exceed_modeled_peak)`

解释：

- 概率层要和 quantile 层相互一致，而不是彼此各算各的

### 7.5 峰时刻校正

- optional

解释：

- 这不是第一优先级，但可以作为后续扩展头

## 8. 方法路线图

## 8.1 第一阶段：经典统计后处理

这是最推荐的先手路线。

### 方法

- EMOS / NGR
- BMA
- 分位回归
- 动态模型平均
- conformal wrapper

### 为什么适合现在

因为当前系统已经是：

- 物理路径先行
- posterior 校正后置

而经典 weather post-processing 的核心哲学正是：

- 不重建天气
- 只校正后验分布

### 推荐用途

- `center_model`
- `spread_calibration`
- `event calibration`

### 风险

- 表达能力有限
- station-specific 非线性关系可能不够

## 8.2 第二阶段：分层 / mixture / regime-aware learning

这是最贴当前业务形态的升级方向。

### 方法

- station-family calibration
- regime-family calibration
- hierarchical shrinkage
- mixture-of-experts
- dynamic model averaging

### 哲学

不是所有城市共用一个 posterior，也不是每个城市各训一套模型。

更合理的中间道路是：

- 共享全局先验
- 在 family / regime 层做条件化校正
- 在样本稀疏站点上做 shrinkage

### 为什么 fit

你当前系统已经天然有这些离散结构：

- phase
- station prior
- regime detection
- second peak / multi-peak states

它们天然适合作为 mixture gate 或 hierarchical key。

## 8.3 第三阶段：灵活 ML calibrator

当样本和 artifact 体系成熟后，可再进入这一层。

### 方法

- gradient boosting / quantile boosting
- random forest quantile regression
- shallow MLP calibrator
- deep ensemble calibrator

### 适合做什么

- 学 `delta_median`
- 学 `spread_scale`
- 学 `upper_tail_allowance`

### 什么时候值得做

当以下条件成立时：

- sample logging 稳定
- label 回填稳定
- per-phase scorecard 稳定
- family/regime 特征充分

否则复杂模型会更像噪声放大器。

## 8.4 第四阶段：更激进的神经网络式元方法

这部分更偏“研究型启发”，不是当前第一优先级。

### A. Deep Ensembles

启发：

- 用多个神经网络的分歧近似 epistemic uncertainty

适合程度：

- 中等

适合原因：

- 工程实现相对务实
- 可以作为 learned calibrator 的强 baseline

### B. Neural Processes

启发：

- 学一个跨任务的函数先验
- 让新站点或新 regime 只用少量样本就能适配

适合程度：

- 中低，但很有启发

适合原因：

- weather posterior 天然是“小样本站点 + 跨站共享结构”的问题

### C. MAML / meta-learning

启发：

- 不追求一个万能模型
- 追求“容易快速微调”的初始模型

适合程度：

- 中低，但对 station family adaptation 很有思想价值

### D. Continual-learning / EWC

启发：

- 新 regime 学进来时，不要把旧 regime 全忘掉

适合程度：

- 现在偏低
- 如果未来走共享 neural calibrator，这会变重要

### E. Permutation-invariant ensemble nets

启发：

- 不要过早把 ensemble members 压成几个 summary stats
- ensemble 是集合对象，不是固定顺序向量

适合程度：

- 如果未来能拿到稳定的 member-level NWP，适合程度会明显上升

## 9. 我们真正应该学什么

posterior learning 最重要的不是“换更强模型”，而是学对对象。

推荐优先学习的不是：

- 原始天气场
- 报告文案
- 人工总结句

而是以下 4 类量：

### 9.1 误差的条件结构

- 在什么 phase 下偏宽
- 在什么 regime 下偏慢收
- 在什么 quality 状态下该保守

### 9.2 剩余上冲空间

这比“是否锁定”更本质。

因为 resolution 风险大多来自：

- 还可能冲多少
- 这个尾巴是真尾巴还是幻觉

### 9.3 时间上的不可逆性

接近窗口尾、已过峰时刻、连续多报未创新高，这些都不是普通特征。
它们是一种时间单向性信息。

好的 posterior 必须尊重这种不可逆性。

### 9.4 忘记与保留的平衡

持续学习不是永远记住一切。

真正困难的是：

- 在新气候态和新城市行为出现时会更新
- 但不把旧经验全部抹掉

所以 posterior 的持续学习，本质上是“受约束的更新”，不是“永不停止的拟合”。

## 10. 评估框架

## 10.1 总体指标

- CRPS
- weighted interval score
- P50 / P75 / P90 coverage
- sharpness
- Brier score
- probability calibration error

## 10.2 业务关键专项指标

必须单独跟踪：

- near-window upper-tail overshoot rate
- post-window false rebreak rate
- lock miss rate
- late surge miss rate
- settled-post width
- path-cap trigger rate

如果这些指标不单独看，只看总体 CRPS，容易掩盖真正影响 resolution 的问题。

## 10.3 切片评估

至少按以下维度切片：

- `phase`
- `station`
- `station_family`
- `regime_family`
- `quality_confidence`
- `single_peak / multi_peak / plateau`
- `same_day / near_window / in_window / post`

## 10.4 promote gate

artifact 不应因为“总体分数略好”就自动发布。

推荐 promote 条件：

- 全局 CRPS 不恶化
- near-window coverage 不恶化
- post-window false warm tail 不恶化
- 核心站点组无显著退化
- resolution-sensitive 指标无恶化

若不满足：

- 保留旧 artifact
- 或仅发布某个子头，例如 event calibration

## 11. runtime 接入原则

## 11.1 什么应该在线读取 artifact

- center calibration
- spread calibration
- upper-tail allowance calibration
- event probability calibration
- family priors
- case index

## 11.2 什么不应该交给 artifact

- 原始天气链解释
- phase 的基础语义
- 报告层文案
- 最终风控 guardrail

## 11.3 runtime fallback

runtime 必须支持多级 fallback：

1. 最新 artifact
2. 上一稳定 artifact
3. 纯 heuristic calibration
4. core-only emergency mode

这不是保守，而是高风险后验系统的必要设计。

## 12. 未来 3 个阶段的落地建议

## 12.1 近阶段：2 周内可落地

1. 在 runtime entrypoint 接 `posterior_training_log_service.py`
2. 记录 append-only posterior samples
3. 明确 `posterior raw output` 与 `final displayed range` 双留档
4. 新增 `artifact manifest` 读取层
5. 做一版 per-phase calibration scorecard

## 12.2 中阶段：1 个月内可落地

1. 落日终 label 回填
2. 产出 `posterior-factor-bundle`
3. 做 station-family / regime-family 分层
4. 先训练经典统计校正头：
   - center delta
   - spread scale
   - event calibration
5. 在 runtime 里接入 artifact 发布和回滚
6. 在最外层加 conformal 或 coverage guardrail

## 12.3 远阶段：研究型探索

1. mixture-of-experts posterior
2. dynamic model averaging
3. deep ensemble calibrator
4. neural process / meta-learning for new station adaptation
5. member-level ensemble set model

## 13. 方法论与哲学备忘

下面这些原则比具体模型更重要。

### 13.1 学误差，不学世界本体

weather core 继续解释天气，
posterior learning 只解释：

- 误差如何产生
- 不确定性如何演化

### 13.2 多元机制优于单一真理

天气 posterior 不应被想成“唯一正确函数”。
它更像多个机制的竞争与切换：

- 海洋层云型
- 晴空干混合型
- 对流抑制型
- 晚峰型
- 平台峰型

因此 mixture / family / regime 的思想，比单一全球黑箱更贴近问题本质。

### 13.3 最终目标不是最窄，而是最可信

posterior 的核心目标不是一味变窄，而是：

- sharp when it should be sharp
- humble when it should be humble

也就是：

- `sharpness subject to calibration`

### 13.4 guardrail 应该拥有主权

学习模块应该是：

- proposal layer

而不是：

- sovereign layer

真正拥有主权的应当是：

- calibration validity
- runtime fallback
- resolution-sensitive guardrail

### 13.5 忘记不是失败

持续学习系统不能假设过去永远正确。

它必须允许：

- 新气候态修正旧经验
- 新城市行为修正旧 family
- 新 regime 出现时扩展假设空间

但这种遗忘必须是：

- 有证据的
- 可追踪的
- 可回滚的

## 14. 推荐参考

以下参考并不是要求全部照搬，而是为 posterior 设计提供方法论坐标。

### 经典 weather post-processing

- Raftery, Balabdaoui, Gneiting, Polakowski.
  "Using Bayesian Model Averaging to Calibrate Forecast Ensembles."
  https://sites.stat.washington.edu/people/raftery/Research/PDF/fadoua.pdf
- Gneiting, Raftery, Westveld, Goldman.
  "Calibrated Probabilistic Forecasting Using Ensemble Model Output Statistics and Minimum CRPS Estimation."
  https://sites.stat.washington.edu/MURI/PDF/gneiting2005.pdf
- Gneiting, Balabdaoui, Raftery.
  "Probabilistic Forecasts, Calibration and Sharpness."
  https://sites.stat.washington.edu/raftery/Research/PDF/Gneiting2007jrssb.pdf
- Kárný, Guy, Ettler, Raftery.
  "Online Prediction Under Model Uncertainty via Dynamic Model Averaging."
  https://sites.stat.washington.edu/people/raftery/Research/PDF/Karny2010.pdf

### Uncertainty / online validity / meta ideas

- Gibbs, Candès.
  "Adaptive Conformal Inference Under Distribution Shift."
  https://proceedings.neurips.cc/paper/2021/hash/0d441de75945e5acbc865406fc9a2559-Abstract.html
- Lakshminarayanan, Pritzel, Blundell.
  "Simple and Scalable Predictive Uncertainty Estimation using Deep Ensembles."
  https://arxiv.org/abs/1612.01474
- Finn, Abbeel, Levine.
  "Model-Agnostic Meta-Learning for Fast Adaptation of Deep Networks."
  https://proceedings.mlr.press/v70/finn17a.html
- Garnelo et al.
  "Neural Processes."
  https://arxiv.org/abs/1807.01622
- Kirkpatrick et al.
  "Overcoming Catastrophic Forgetting in Neural Networks."
  https://pmc.ncbi.nlm.nih.gov/articles/PMC5380101/

## 15. 最终结论

下一阶段不应把精力首先投向“更复杂的神经网络”。

更高优先级的顺序应该是：

1. 固化 sample / label / artifact / rollback 基础设施
2. 明确 posterior 原输出与最终展示输出的双轨留档
3. 先完成经典统计校正和分层校正
4. 再进入 mixture / dynamic averaging
5. 最后才考虑更激进的 neural meta 方法

如果这条顺序不倒置，posterior learning 会越来越稳。
如果顺序倒置，系统很容易得到一个更复杂但更脆弱的后验层。
