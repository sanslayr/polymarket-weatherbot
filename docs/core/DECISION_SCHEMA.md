# Forecast Decision Schema

Last updated: 2026-03-09

`forecast_pipeline.py` 产出的统一决策对象结构。报告层和 snapshot 层应优先消费该结构，而不是反向读取 provider 原始返回。

当前 schema version：`forecast-decision.v8`

## 1) 顶层

- `schema_version`
- `meta`
- `quality`
- `features`
- `decision`

## 2) `meta`

- `station`
- `date`
- `model`
- `synoptic_provider`
  - 实际写入的是本次 decision 使用的 3D provider
- `runtime`
- `window.start_local`
- `window.end_local`

## 3) `quality`

- `source_state`
  - `fresh | cache-hit | fallback-cache | degraded`
- `missing_layers`
- `synoptic_anchors_total`
- `synoptic_anchors_ok`
- `synoptic_coverage`
- `synoptic_pass_strategy`
- `synoptic_provider_requested`
- `synoptic_provider_used`
- `synoptic_provider_fallback`（可选）
- `synoptic_outer500_anchors_total`
- `synoptic_outer500_anchors_ok`
- `synoptic_anchor_events`（可选）
- `synoptic_anchor_error_counts`（可选）
- `synoptic_analysis_runtime_used` / `synoptic_previous_runtime_used` 等实际 provider runtime 元数据（可选）

## 4) `features`

### `objects_3d`

当前对象 schema：`objects-3d.v2`

关键字段：

- `main_object`
- `candidates`
- `tracks`
- `anchors_count`

### `h500`

- `phase`
- `phase_hint`
- `pva_proxy`
- `regime_label`
- `proximity`
- `confidence`
- `forcing_text`
- `impact_weight`
- `tmax_weight_score`
- `surface_coupling`
- 以及 subtropical / westerly 相关辅助诊断

### `h850`

- `advection`
- `review`
  - `thermal_advection_state`
  - `transport_state`
  - `surface_coupling_state`
  - `surface_role`
  - `surface_bias`
  - `surface_effect_weight`
  - `summary_line`

### `h700`

- `summary`

### `h925`

- `summary`

### `sounding`

- `path_bias`
- `layer_findings`
- `actionable`
- `profile_source`
- `confidence`
- `thermo`
  - `coverage`
  - `layer_relationships`
  - `relationship_findings`
  - 其余 thermo/cape/cin/lcl/lfc 等字段

## 5) `decision`

- `main_path`
- `bottleneck`
- `trigger`
- `object_3d_main`
- `override_risk`
- `background`
  - `phase_mode`
  - `phase500`
  - `pva500`
  - `phase_hint`
  - `line_500`
  - `line_850`
  - `extra`

## 6) 读取约束

1. 报告层优先读 `features` 和 `decision.background` 的结构化字段。
2. 用户可见结论不应仅靠解析 `line_850` 之类文本字段得出。
3. 若 `schema_version` 不匹配，应直接视为缓存失效并重建。
