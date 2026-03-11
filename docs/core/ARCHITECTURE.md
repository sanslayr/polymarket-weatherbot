# polymarket-weatherbot Architecture

Last updated: 2026-03-09

目标：让天气分析链路足够清晰、可扩展、可复用，并为后续 Tmax 概率层、自动扫描和自动交易层保留稳定接口。

## 1) 当前主链路

### A. Ingress / Orchestrator

- `scripts/telegram_report_cli.py`
  - 当前主要负责 `/look` 人类查询入口、运行编排、降级兜底、最终文本拼装
  - 长期目标不是唯一入口；后续可被定时扫描器和策略运行器旁路复用
- `scripts/look_command.py`
  - 命令解析与帮助文本
- `scripts/station_catalog.py`
  - 站点解析、别名、时区、站点元信息

### A1. Host Runtime Boundary

- Telegram `/look` 入口必须由单一 OpenClaw user service 承载：
  - CLI front door：`/home/ubuntu/.local/bin/openclaw`
  - package root：`/home/ubuntu/.npm-global/lib/node_modules/openclaw`
  - gateway unit：`/home/ubuntu/.config/systemd/user/openclaw-gateway.service`
- weatherbot repo 负责 `/look` 领域逻辑、live METAR 刷新和 live Polymarket 刷新。
- OpenClaw gateway 只负责 channel ingress、tool dispatch 和报告回推。
- 不允许并行保留 `/usr/lib/node_modules/openclaw` + `/etc/systemd/system/openclaw-gateway.service` 这一类 system-level 旧轨道。
- 运维细节见：
  - [OPENCLAW_RUNTIME_BOUNDARY.md](/home/ubuntu/.openclaw/workspace/skills/polymarket-weatherbot/docs/operations/OPENCLAW_RUNTIME_BOUNDARY.md)

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
- `scripts/synoptic_summary_service.py`
  - 结构化环流摘要
- `scripts/peak_range_service.py`
  - 结构化峰值区间分析
- `scripts/peak_range_signal_service.py`
  - 区间分析用的信号评分与探空评分 helper
- `scripts/peak_range_history_service.py`
  - 历史参考融合与历史参考展示字段构造
- `scripts/peak_range_render_service.py`
  - 峰值区间文本块渲染

### E. Snapshot / Render Layer

- `scripts/analysis_snapshot_service.py`
  - 汇总当前主要结构化分析结果
  - 当前 schema：`analysis-snapshot.v6`
  - 当前已包含：
    - `canonical_raw_state`
    - `posterior_feature_vector`
    - `quality_snapshot`
    - `weather_posterior`
- `scripts/report_focus_service.py`
  - 负责“关注变量 / 实况分析附注 / market label policy”这类报告支持块
  - `report_render_service.py` 只消费其结构化结果，不再承担主推理
- `scripts/report_render_service.py`
  - 只负责把 snapshot 和 report evidence 转成正文
  - 已清除旧的变量/市场 fallback 推理函数
- `scripts/metar_analysis_service.py`
  - 实况诊断与 METAR block
- `scripts/polymarket_render_service.py`
  - 盘口解释与展示
- `scripts/market_label_policy.py`
  - 市场标签策略

### F. Market Alert / Notification Layer

- `scripts/market_metadata_service.py`
  - 市场 bucket / token id 元信息映射
- `scripts/market_stream_service.py`
  - websocket 行情订阅、基线窗口与事件窗口监控
- `scripts/market_monitor_service.py`
  - 串联市场元信息、订阅计划、实时状态和 signal 推理
- `scripts/market_implied_weather_signal.py`
  - 从盘口异动推断 `market-implied observation hint`
- `scripts/market_signal_alert_service.py`
  - 将 signal 渲染成简短 Telegram 告警文本
- `scripts/alert_delivery_policy.py`
  - Telegram 告警目标解析与 direct/group 优先级策略
- `scripts/telegram_notifier.py`
  - weatherbot workspace 内的 Telegram 主动发送适配层
- `scripts/market_alert_worker.py`
  - 基于 METAR 常规报窗口调度主动监控和去重推送

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
- `canonical-raw-state.v2`
- `posterior-feature-vector.v2`
- `quality-snapshot.v2`
- `weather-posterior-core.v1`
- `weather-posterior.v1`
- `analysis-snapshot.v6`
- `synoptic-cache.v3`

运行时缓存 envelope 统一由 `runtime-cache.v1` 包裹。

## 4) 当前结构优点

1. 3D provider 已从单一 GFS 改成 router 模式，默认 ECMWF，更利于后续多模型扩展。
2. `analysis_snapshot_service.py` 已建立结构化 handoff，报告层不再是唯一逻辑中心。
3. `analysis_snapshot_service.py` 已显式产出 `canonical_raw_state`、`posterior_feature_vector`、`quality_snapshot` 与 `weather_posterior`，天气后验层已有 `core + calibration hook` 结构。
4. `peak_range_service.py`、`peak_range_history_service.py`、`peak_range_signal_service.py` 和 `synoptic_summary_service.py` 已把一部分“边渲染边推理”逻辑收回分析层。
5. `vertical_3d.py` 已具备基础 tracking 能力，不再只是静态单帧 object 摘要。
6. 市场异动通知链路已经从 `/look` 主链路中剥离出来，形成独立的 monitor -> signal -> notifier -> worker 层，适合放在 weatherbot workspace 内长期运行。

## 5) 当前仍需继续收口的问题

1. `canonical_raw_state` 和 `posterior_feature_vector` 已开始落地
   - provider 原始状态与 feature 轴已初步收口，后续仍需继续扩字段并压缩零散兼容输入
2. `report_render_service.py` 的主路径已退出变量/后验推理
   - 关注变量与市场标签策略已迁到 `report_focus_service.py`
   - 当前已基本收成纯 render；后续重点应转向删减零散 presentation fallback
3. `peak_range_service.py` 已成为新的复杂度热点
   - 后续建议拆成：
     - peak posterior / range computation
     - historical/calibration helper
4. 3D tracking 仍是轻量 heuristic
   - 目前能区分 `approaching / receding / passing / steady`
   - 还不等于完整的 split/merge/trajectory solver

## 6) 关联文档

- 当前运行时契约与输出约束：
  - [DECISION_SCHEMA.md](/home/ubuntu/.openclaw/workspace/skills/polymarket-weatherbot/docs/core/DECISION_SCHEMA.md)
  - [FORECAST_3D_STORAGE.md](/home/ubuntu/.openclaw/workspace/skills/polymarket-weatherbot/docs/core/FORECAST_3D_STORAGE.md)
  - [LOOK_OUTPUT_CONTRACT.md](/home/ubuntu/.openclaw/workspace/skills/polymarket-weatherbot/docs/core/LOOK_OUTPUT_CONTRACT.md)
- 目标版 weather/market/research 分层：
  - [TARGET_ARCHITECTURE.md](/home/ubuntu/.openclaw/workspace/skills/polymarket-weatherbot/docs/core/TARGET_ARCHITECTURE.md)
  - [MARKET_ARCHITECTURE.md](/home/ubuntu/.openclaw/workspace/skills/polymarket-weatherbot/docs/core/MARKET_ARCHITECTURE.md)
- 主动告警运维说明：
  - [MARKET_ALERT_WORKER.md](/home/ubuntu/.openclaw/workspace/skills/polymarket-weatherbot/docs/operations/MARKET_ALERT_WORKER.md)
