# Posterior Learning Plan

Last updated: 2026-03-13
Status: Draft

## 1. 目标

本文件定义 weather posterior 的离线学习/在线校正方案，重点解决：

1. 如何把数值预报分析、实况状态和两者偏离结构化为同一套 factor system
2. 如何以日级节奏训练/评估/发布校正参数，而不是在线实时重训
3. 如何让 learning 只校正 posterior，不反向污染 weather core 和 runtime 主链路

本方案默认：

- runtime repo 继续负责在线推理
- research/training/backtest 可以放在独立 research repo
- runtime 只消费 artifact

## 2. 设计原则

### 2.1 单向依赖

- `canonical_raw_state` 只承载原始结构化状态，不承载训练逻辑
- `posterior_factor_bundle` 只承载特征表达，不承载 label 和模型参数
- `posterior_learning_artifact` 只承载训练产物，不承载 runtime 推理代码
- `weather_posterior_core` 继续做物理/机制锚定
- learning 层只在 calibration hook 里做中心、区间、概率校正

### 2.2 语义先于数据源

特征定义按物理语义组织，而不是按 provider/METAR/forecast source 命名。

反例：

- `metar_temp_bias`
- `ecmwf_cloud_cover`
- `openmeteo_peak_temp`

推荐：

- `obs_thermal_progress.latest_temp_c`
- `cloud_radiation.cloud_suppression_expected`
- `obs_model_innovation.peak_temp_resid_c`

### 2.3 通道化表示优于多份平行 schema

不建议长期使用“同名三视图”直接展开成三套平行字段。

更优表示：

- `context`
- `live`
- `innovation`
- `quality`

其中：

- `context` = 慢变量 + 数值预报/环流/边界层预期
- `live` = 当前实况与已观测进度
- `innovation` = `live - E[live | context]` 的条件残差，而不是简单差值
- `quality` = 数据可信度、缺失、量化、cadence、fallback 等控制变量

### 2.4 版本、口径、缺失必须显式化

learning 方案不能依赖隐式默认值。

所有 artifact 都必须显式带：

- `schema_version`
- `feature_manifest_version`
- `normalization_version`
- `label_policy_version`
- `training_window`
- `artifact_built_at`

## 3. 进一步的去耦化建议

## 3.1 代码层去耦

建议新增并保持边界清晰的模块：

- `posterior_factor_service.py`
  - 从 `canonical_raw_state` 产出统一 factor bundle
- `posterior_training_log_service.py`
  - 负责在线样本落盘，不做训练
- `posterior_training_dataset_service.py`
  - 负责 `bronze + labels -> silver`
- `posterior_daily_trainer.py`
  - 负责日级训练与评估
- `posterior_artifact_registry.py`
  - 负责 artifact manifest 与 promote/fallback
- `posterior_case_index_service.py`
  - 负责历史案例索引与检索

不建议：

- 把训练特征拼装继续塞进 `analysis_snapshot_service.py`
- 把训练参数直接混进 `config/tmax_learning_params.json`
- 把历史案例检索逻辑塞进 `report_render_service.py`

## 3.2 数据层去耦

建议把以下实体拆开存储：

- runtime snapshot sample
- final day label
- training view
- published artifact
- case index

原因：

- snapshot 是 append-only，不能被 label 回写污染
- label 有自然滞后，必须和采样分离
- training view 是可重建中间层，不应成为唯一真相
- artifact 是发布物，不应和原始样本混放

## 3.3 语义层去耦

建议把 factor 分成 8 个 block：

1. `station_background`
2. `synoptic_forcing`
3. `boundary_layer_support`
4. `cloud_radiation`
5. `obs_thermal_progress`
6. `obs_model_innovation`
7. `peak_shape`
8. `quality`

每个 block 内再走 `context/live/innovation/quality` 通道；并非每个 block 都必须 4 通道全满，但语义框架保持一致。

## 4. 进一步的结构化/标准化建议

## 4.1 factor registry 先于 extractor

建议先维护一份显式 factor registry，而不是让字段散落在 extractor 代码里。

registry 至少记录：

- `factor_name`
- `block`
- `channel`
- `dtype`
- `unit`
- `allowed_range`
- `enum_vocab`
- `missing_policy`
- `default_imputation`
- `normalization_policy`
- `used_for_training`
- `used_for_case_retrieval`

## 4.2 缺失与质量不做隐式填补

每个连续特征建议同时带三类信息：

- `value`
- `is_missing`
- `source_confidence`

原因：

- `0.0` 不应和“缺失后填 0”混淆
- 案例匹配时，质量维度通常应作为 mask 或弱权重，而不是主距离轴

## 4.3 数值标准化规则

建议统一：

- 温度全部存 `C`
- 风速全部存 `kt`
- 时间差全部存 `hours`
- 概率全部存 `[0, 1]`
- 覆盖/效率类全部存 `[0, 1]`
- 离散枚举统一在 registry 中维护词表

连续变量建议记录：

- raw value
- clipped value
- normalized value

runtime 侧只需要 raw value；research 侧可生成 clipped/normalized 视图。

## 4.4 创新量不直接用 raw diff

`innovation` 建议优先使用条件残差：

`innovation = realized - expected_given_context`

而不是：

`innovation = obs - model`

因为后者会把 station background、季节位相、已知 synoptic forcing 重复计入，难以去耦。

第一版可先用简化近似：

- global/station-family 的 expectation baseline
- 随后升级为 `E[live | context]`

## 4.5 训练特征与检索特征分开标记

不是所有训练特征都适合做相似案例检索。

建议：

- `quality` block 默认不参与主距离，只参与 mask/权重
- `obs_model_innovation` 在早盘 case retrieval 中默认关闭
- `station_background + synoptic_forcing + boundary_layer_support + cloud_radiation` 作为 forecast-stage 主检索轴
- 近窗阶段再追加 `obs_thermal_progress + innovation + peak_shape`

## 5. 统一 factor schema 建议

建议新增 schema：`posterior-factor-bundle.v1`

顶层结构建议：

```json
{
  "schema_version": "posterior-factor-bundle.v1",
  "meta": {},
  "context": {},
  "live": {},
  "innovation": {},
  "quality": {},
  "masks": {},
  "registry_ref": {}
}
```

## 5.1 `meta`

建议包含：

- `station_icao`
- `station_family`
- `date_local`
- `sample_time_local`
- `hours_to_peak`
- `hours_to_window_end`
- `forecast_runtime`
- `provider_bundle`
- `canonical_raw_state_version`
- `extractor_version`

## 5.2 `context`

建议从 forecast/synoptic/3D/boundary-layer 侧提取：

- `station_background.diurnal_swing_class`
- `station_background.late_surge_risk`
- `synoptic_forcing.h500_tmax_support_score`
- `synoptic_forcing.thermal_advection_score`
- `synoptic_forcing.track_proximity_score`
- `boundary_layer_support.surface_coupling_score`
- `boundary_layer_support.cap_suppression_score`
- `cloud_radiation.cloud_suppression_expected`
- `cloud_radiation.radiation_support_expected`
- `peak_shape.model_peak_width_h`
- `peak_shape.model_peak_sharpness`

## 5.3 `live`

建议从当前实况与 intra-day progress 提取：

- `obs_thermal_progress.latest_temp_c`
- `obs_thermal_progress.observed_max_temp_c`
- `obs_thermal_progress.temp_trend_cph`
- `obs_thermal_progress.temp_accel_cph2`
- `obs_thermal_progress.dewpoint_trend_cph`
- `cloud_radiation.cloud_cover_realized`
- `cloud_radiation.radiation_eff_realized`
- `peak_shape.daily_peak_state_code`
- `peak_shape.short_term_state_code`
- `peak_shape.second_peak_potential_code`

## 5.4 `innovation`

建议保留真正用于校正的创新轴：

- `obs_model_innovation.latest_temp_resid_c`
- `obs_model_innovation.peak_temp_resid_c`
- `obs_model_innovation.cloud_suppression_resid`
- `obs_model_innovation.radiation_support_resid`
- `obs_model_innovation.peak_timing_resid_h`
- `obs_model_innovation.path_alignment_score`
- `obs_model_innovation.transport_realization_resid`

## 5.5 `quality`

建议：

- `quality.provider_score`
- `quality.coverage_score`
- `quality.sounding_score`
- `quality.obs_score`
- `quality.metar_quantized_flag`
- `quality.provider_fallback_flag`
- `quality.missing_layers_count`
- `quality.metar_recent_interval_min`

## 5.6 `masks`

建议保存：

- `available_channels`
- `block_present_mask`
- `feature_missing_mask`
- `safe_for_case_retrieval_mask`

## 6. 数值预报板块如何提取为同一套向量

答案是：应该，而且必须。

数值预报板块不应只输出 narrative 诊断，还应映射到与实况同语义的 factor axes。推荐做法：

- forecast 端输出“预期值/预期状态”
- obs 端输出“已实现值/已实现状态”
- innovation 端输出“条件残差”

例如：

- 预报侧：`thermal_advection_score = 0.62`
- 实况侧：`transport_realization_score = 0.18`
- 创新侧：`transport_realization_resid = -0.44`

这样：

- 训练模型能知道“预报说会暖，但实况没兑现”
- 相似案例检索也能同时做 forecast-stage 和 live-stage 两种匹配

## 7. 训练数据存储设计

不建议直接用单一 JSONL 作为长期主存。推荐：

- debug/审阅可附加 JSONL
- 正式训练主存用 Parquet
- 查询/构建训练集用 DuckDB

## 7.1 Bronze: snapshot samples

用途：append-only 原始训练样本日志。

建议分区：

- `dataset=posterior_samples/station_icao=XXXX/date_local=YYYY-MM-DD/*.parquet`

主键：

- `sample_id = station_icao + date_local + sample_time_local + extractor_version`

字段：

- `sample_id`
- `station_icao`
- `date_local`
- `sample_time_local`
- `hours_to_peak`
- `hours_to_window_end`
- `sampling_reason`
- `factor_bundle`
- `baseline_posterior`
- `raw_snapshot_ref`
- `schema_version`
- `extractor_version`

更新策略：

- 仅 append
- 不回写 label
- 不做 inplace 修改

## 7.2 Labels: final day outcome table

用途：本地日结束后的最终真值。

建议分区：

- `dataset=posterior_labels/date_local=YYYY-MM-DD/*.parquet`

主键：

- `station_icao + date_local + label_policy_version`

字段：

- `station_icao`
- `date_local`
- `final_tmax_c`
- `final_tmax_lo_c`
- `final_tmax_hi_c`
- `final_peak_time_local`
- `late_surge_flag`
- `lock_by_window_end_flag`
- `exceed_modeled_peak_flag`
- `label_policy_version`
- `labeled_at_utc`

## 7.3 Silver: training view

用途：研究/训练直接消费的数据视图，可每日重建。

字段：

- `meta.*`
- `context.*`
- `live.*`
- `innovation.*`
- `quality.*`
- `target.delta_median_c`
- `target.headroom_c`
- `target.abs_residual_c`
- `target.late_surge_flag`
- `target.lock_flag`
- `target.exceed_modeled_peak_flag`
- `sample_weight`

原则：

- silver 可重建
- 不作为唯一真相
- 任何标准化/clip/residualization 版本变化都要重建

## 7.4 Gold: published artifacts

用途：runtime 消费的稳定发布物。

建议内容：

- `posterior_learning_manifest.json`
- `posterior_center_model.*`
- `posterior_spread_calibration.*`
- `posterior_event_calibration.*`
- `posterior_case_index.parquet`
- `training_metrics.json`

manifest 应包含：

- artifact 版本
- 训练窗口
- station 覆盖
- 支持的 schema versions
- fallback artifact
- promotion 时间

## 8. 日级样本更新策略

## 8.1 采样时机

不建议每分钟落样本。建议 anchor + event-driven 混合策略：

- routine METAR
- SPECI
- `hours_to_peak` 跨关键阈值
- `daily_peak_state` 变化
- `short_term_state` 变化
- 市场异动或关键标签变化

采样原因应显式记录在 `sampling_reason`。

## 8.2 label 回填

在 local day 明确结束后回填。

建议：

- 正常情况在次日固定时间回填
- 若市场/报文仍不稳定，可延迟回填
- label 与 bronze 分离，不回写原样本

## 8.3 日级训练窗口

建议滚动窗口：

- 默认近 120 天
- 最近 30 天加权
- 同季节窗样本加权
- 极端天气型可额外放大样本权重

## 9. 训练算法设计

## 9.1 学习目标

不直接让模型重建天气全链路，只学习 posterior 的校正量。

建议四个目标头：

1. 中心校正
   - `delta_median_c = final_tmax_c - baseline_posterior_p50_c`
2. 剩余上冲空间
   - `headroom_c = final_tmax_c - max(observed_max_temp_c, latest_temp_c)`
3. 区间宽度/残差尺度
   - `abs_residual_c`
4. 事件概率
   - `late_surge_flag`
   - `lock_by_window_end_flag`
   - `exceed_modeled_peak_flag`

## 9.2 特征预处理

建议顺序：

1. block 内 clip
2. block 内标准化
3. 枚举 one-hot / ordinal 编码
4. `innovation` 构建
5. blockwise residualization
6. 生成训练视图

blockwise residualization 建议顺序：

1. `station_background`
2. `synoptic_forcing`
3. `boundary_layer_support`
4. `cloud_radiation`
5. `obs_thermal_progress`
6. `obs_model_innovation`
7. `peak_shape`
8. `quality`

目的不是做完全数学正交，而是减少重复解释量。

## 9.3 模型栈建议

### A. 中心校正模型

第一版建议：

- `HuberRegressor` 或 `ElasticNet`

输入：

- `context + live + innovation`

输出：

- `delta_median_c`

原因：

- 稳
- 可解释
- 对异常天不太脆弱

### B. 区间宽度校正

第一版建议：

- 分位回归或 split-conformal calibration

输入：

- `context + innovation + quality`

输出：

- `spread_multiplier`
- 或 `residual_quantiles`

建议优先：

- 先保留当前 heuristic spread
- learning 只学习 correction factor

### C. 事件概率校正

第一版建议：

- `LogisticRegression`
- 再叠 `Isotonic`/`Platt`

输出：

- `P(late_surge)`
- `P(lock_by_window_end)`
- `P(exceed_modeled_peak)`

### D. 历史相似案例检索

建议和监督模型并行，不混为一个黑箱：

- forecast-stage retrieval
- live-stage retrieval

距离度量建议：

- block-weighted Mahalanobis
- 或 cosine + block weights

默认不让 `quality` 主导距离，只做 mask/惩罚。

## 9.4 层级回退链

建议显式采用 shrinkage/fallback，而不是单站独立硬切：

1. `station × regime × phase_bucket`
2. `station_family × regime × phase_bucket`
3. `station × phase_bucket`
4. `regime × phase_bucket`
5. `global × phase_bucket`
6. `global`

样本不足时：

- 不做激进中心修正
- 只放宽 spread
- 概率向 0.5 收缩

## 9.5 样本权重

建议样本权重由以下因子组成：

- recency weight
- quality weight
- phase relevance weight
- rare-case bonus
- station balance weight

不建议让单一高频站点吞掉训练分布。

## 10. 日级训练/评估/发布流程

## 10.1 训练流程

1. 收集当日 bronze samples
2. 回填上一日 labels
3. 重建 silver training view
4. 训练 candidate models
5. 做 rolling backtest
6. 与当前 production artifact 比较
7. 若通过门槛则 promote，否则保留旧版本

## 10.2 评估指标

主指标：

- `MAE`
- `Bias`
- `p50 calibration`
- `coverage@display_range`
- `coverage@core_range`

业务指标：

- `warm_tail_overrate`
- `rounded_top_overforecast_rate`
- `late_surge_miss_rate`
- `peak_lock_false_lock_rate`

分组评估：

- 按站点
- 按 station family
- 按 regime
- 按 `hours_to_peak` bucket
- 按 `daily_peak_state`

## 10.3 promote gate

建议 candidate 只有在同时满足以下条件时才发布：

- 全局 `MAE` 不劣化
- `late_surge_miss_rate` 不恶化到阈值外
- display/core coverage 不明显变差
- 至少 `N` 个核心站点分组未显著恶化

若不满足：

- 允许保留旧 artifact
- 允许只发布某个子模块，如 spread calibration

## 11. runtime 接入建议

runtime 只读 artifact，不参与训练。

建议接入点：

- `weather_posterior_core.py`
  - 保持物理锚定，不直接读 learning weights
- `weather_posterior_calibration.py`
  - 读取 center/spread/probability artifact 并应用
- `posterior_case_index_service.py`
  - 在需要时返回相似案例摘要，但不干预 core

建议新增配置：

- `config/posterior_learning_manifest.json`
- `config/posterior_learning_params.json`

不要把 posterior learning 参数继续塞进：

- `config/tmax_learning_params.json`

原因是该文件目前更偏 heuristic runtime params，而不是学习 artifact registry。

## 12. 建议的 schema 草案

## 12.1 `posterior-factor-bundle.v1`

建议字段：

- `schema_version`
- `meta`
- `context`
- `live`
- `innovation`
- `quality`
- `masks`
- `registry_ref`

## 12.2 `posterior-training-sample.v1`

建议字段：

- `schema_version`
- `sample_id`
- `station_icao`
- `date_local`
- `sample_time_local`
- `sampling_reason`
- `factor_bundle_ref`
- `baseline_posterior`
- `hours_to_peak`
- `hours_to_window_end`
- `extractor_version`

## 12.3 `posterior-day-label.v1`

建议字段：

- `schema_version`
- `station_icao`
- `date_local`
- `final_tmax_c`
- `final_tmax_lo_c`
- `final_tmax_hi_c`
- `final_peak_time_local`
- `late_surge_flag`
- `lock_flag`
- `label_policy_version`

## 12.4 `posterior-learning-manifest.v1`

建议字段：

- `schema_version`
- `artifact_version`
- `training_window`
- `feature_manifest_version`
- `supported_factor_schema_versions`
- `center_model_ref`
- `spread_model_ref`
- `event_model_ref`
- `case_index_ref`
- `fallback_artifact_version`

## 13. 第一阶段落地顺序

### Phase-1

1. 固化 `factor registry`
2. 产出 `posterior-factor-bundle.v1`
3. 落 bronze samples + labels
4. 重建 silver training view
5. 训练 `delta_median` + `spread calibration`
6. 发布只读 artifact
7. 在 `weather_posterior_calibration.py` 接入

### Phase-2

1. 引入条件残差型 `innovation`
2. 引入 case index
3. 引入 station-family / regime shrinkage
4. 引入 event probability calibration

## 14. 当前结论

最重要的不是立刻换复杂模型，而是先把以下三件事固定下来：

1. factor 语义本体
2. 样本/标签/artifact 的分层存储
3. learning 只校正 posterior，不替代 weather core

只要这三件事先稳住，后续从 heuristic calibration 升级到更强模型时，runtime 主链路不需要再被重构。
