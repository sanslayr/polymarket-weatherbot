# polymarket-weatherbot Architecture

Last updated: 2026-03-09

目标：让 `/look` 的天气分析链路足够清晰、可扩展、可复用，并为后续 Tmax 概率层和自动交易层保留稳定接口。

## 1) 当前主链路

### A. Ingress / Orchestrator

- `scripts/telegram_report_cli.py`
  - 负责 `/look` 入口、运行编排、降级兜底、最终文本拼装
- `scripts/look_command.py`
  - 命令解析与帮助文本
- `scripts/station_catalog.py`
  - 站点解析、别名、时区、站点元信息

### B. Provider / Raw Data

- 小时预报：`scripts/hourly_data_service.py`
  - `open-meteo` primary
  - 负责小时曲线、候选峰值窗、fallback 与缓存
- 3D 场 provider router：`scripts/synoptic_provider_router.py`
  - 默认 `ecmwf-open-data`
  - fallback `gfs-grib2`
- ECMWF provider：`scripts/ecmwf_open_data_provider.py`
- GFS provider：`scripts/gfs_grib_provider.py`
- 实况：`scripts/metar_utils.py`
- 实测探空：`scripts/sounding_obs_service.py`

### C. Synoptic / Forecast Decision

- `scripts/synoptic_runner.py`
  - 多 anchor synoptic 构建与 provider fallback
- `scripts/forecast_pipeline.py`
  - 汇总 anchor slices
  - 构建 forecast decision
  - 写入 decision cache / 3D bundle cache
- `scripts/vertical_3d.py`
  - 3D object build + 轻量 3-5 anchor tracking
- `scripts/advection_review.py`
  - 平流代表性 / 落地性 review
  - 当前核心状态：
    - `thermal_advection_state`
    - `transport_state`
    - `surface_coupling_state`
    - `surface_role`
- 诊断层：
  - `scripts/diagnostics_500.py`
  - `scripts/diagnostics_700.py`
  - `scripts/diagnostics_925.py`
  - `scripts/diagnostics_sounding.py`

### D. Realtime / Analysis Layer

- `scripts/temperature_shape_analysis.py`
  - 区分单峰、多峰、平台峰、宽平台
- `scripts/temperature_window_resolver.py`
  - 模型峰值窗与实况窗口重锚
- `scripts/temperature_phase_decision.py`
  - `short_term_state / daily_peak_state / second_peak_potential`
- `scripts/boundary_layer_regime.py`
  - 主导机制、层结摘要、关注变量跟踪线
  - 当前正式机制字段：`dominant_mechanism`
  - `dominant_question` 仅作为兼容别名保留
- `scripts/synoptic_summary_service.py`
  - 结构化环流摘要
- `scripts/peak_range_service.py`
  - 结构化峰值区间分析 + 峰值区间文本块

### E. Snapshot / Render Layer

- `scripts/analysis_snapshot_service.py`
  - 汇总当前主要结构化分析结果
  - 当前 schema：`analysis-snapshot.v2`
- `scripts/report_render_service.py`
  - 只负责把 snapshot 和 report evidence 转成正文
- `scripts/metar_analysis_service.py`
  - 实况诊断与 METAR block
- `scripts/polymarket_render_service.py`
  - 盘口解释与展示
- `scripts/market_label_policy.py`
  - 市场标签策略

## 2) 当前数据策略

- 小时预报：
  - `open-meteo` 继续保留为轻量 primary，用于日内窗口识别与参考
- 3D 背景：
  - `ecmwf-open-data` default
  - `gfs-grib2` fallback
- 最终降级：
  - 若 forecast/synoptic 都不可用，则允许退到 `METAR-only`

这意味着：

- 小时曲线与 3D 背景当前并非同源
- 但 3D 背景的默认源已不再是 GFS
- 文档和用户可见头部必须以“实际使用 provider / runtime”为准，而不是旧的“分析基准模型: GFS”

## 3) 运行时契约

集中版本常量在 `scripts/contracts.py`：

- `forecast-decision.v8`
- `forecast-3d-bundle.v2`
- `objects-3d.v2`
- `analysis-snapshot.v2`
- `synoptic-cache.v3`

运行时缓存 envelope 统一由 `runtime-cache.v1` 包裹。

## 4) 当前结构优点

1. 3D provider 已从单一 GFS 改成 router 模式，默认 ECMWF，更利于后续多模型扩展。
2. `analysis_snapshot_service.py` 已建立结构化 handoff，报告层不再是唯一逻辑中心。
3. `peak_range_service.py` 和 `synoptic_summary_service.py` 已把一部分“边渲染边推理”逻辑收回分析层。
4. `vertical_3d.py` 已具备基础 tracking 能力，不再只是静态单帧 object 摘要。

## 5) 当前仍需继续收口的问题

1. 缺少正式的 `canonical_raw_state`
   - provider 原始状态仍然分散在 hourly / synoptic / metar / sounding 多条链
2. 缺少独立的 `posterior_feature_vector`
   - 当前 `analysis_snapshot` 仍混有部分面向报告的字段
3. `report_render_service.py` 仍有 fallback 推理
   - 目标仍应是“render consumes analysis”, 而不是“render retries analysis”
4. `peak_range_service.py` 已成为新的复杂度热点
   - 后续建议拆成：
     - peak posterior / range computation
     - peak text render
5. 3D tracking 仍是轻量 heuristic
   - 目前能区分 `approaching / receding / passing / steady`
   - 还不等于完整的 split/merge/trajectory solver

## 6) 关联文档

- 当前运行时契约与输出约束：
  - [DECISION_SCHEMA.md](/home/ubuntu/.openclaw/workspace/skills/polymarket-weatherbot/docs/core/DECISION_SCHEMA.md)
  - [FORECAST_3D_STORAGE.md](/home/ubuntu/.openclaw/workspace/skills/polymarket-weatherbot/docs/core/FORECAST_3D_STORAGE.md)
  - [LOOK_OUTPUT_CONTRACT.md](/home/ubuntu/.openclaw/workspace/skills/polymarket-weatherbot/docs/core/LOOK_OUTPUT_CONTRACT.md)
- 目标版 weather/market/research 分层：
  - [TARGET_ARCHITECTURE.md](/home/ubuntu/.openclaw/workspace/skills/polymarket-weatherbot/docs/core/TARGET_ARCHITECTURE.md)
