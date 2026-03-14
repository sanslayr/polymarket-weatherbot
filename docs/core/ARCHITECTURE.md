# polymarket-weatherbot Architecture

Last updated: 2026-03-11

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
- 站点外部参考 adapter：`scripts/station_external_reference_service.py`
  - 承载站点特定的外部参考源（当前为 LTAC/MGM）
  - 约束：station-specific source 不能继续塞进 `/look` orchestrator
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
- `scripts/report_synoptic_service.py`
  - 负责背景句 / 环流句的压缩、去模板化与句式收口
  - 保持这类机制短句留在 render-local helper 层，而不是回流到 `/look` orchestrator
- `scripts/report_render_service.py`
  - 只负责把 snapshot、focus bundle 和 synoptic render helper 结果编排成正文
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
  - `run_market_monitor_cycle()` 与 `run_market_monitor_event_window()` 共用同一套 subscription/signal pipeline helper
- `scripts/market_implied_weather_signal.py`
  - 从盘口异动推断 `market-implied observation hint`
- `scripts/market_signal_alert_service.py`
  - 将 signal 渲染成简短 Telegram 告警文本
  - 当前标题口径：`盘口归零异动`
- `scripts/alert_delivery_policy.py`
  - Telegram 告警目标解析与 direct/group 优先级策略
- `scripts/telegram_notifier.py`
  - weatherbot workspace 内的 Telegram 主动发送适配层
- `scripts/market_alert_scheduler.py`
  - 负责 station row 装载、routine cadence 推断、窗口计算和 event URL 生成
- `scripts/market_alert_runtime_state.py`
  - 负责 singleton lock、runtime state、pid/log/state 文件落盘
- `scripts/market_alert_delivery_service.py`
  - 负责 alert dedupe key、worker cooldown 和 Telegram 投递回执
- `scripts/market_alert_worker.py`
  - 薄 orchestrator
  - 只负责任务并发、窗口日志、调度模块调用与 delivery handoff

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
6. 市场异动通知链路已经从 `/look` 主链路中剥离出来，并进一步拆成 `scheduler -> monitor -> signal -> formatter -> delivery -> worker`，适合在 weatherbot workspace 内长期运行。

## 4.1) 当前 runtime posterior 算法

当前 `weather_posterior` 已不再只是“model peak + quality widening”。runtime 现阶段已经形成以下分层：

1. `canonical_raw_state_service.py`
   - 统一收口窗口、实况、forecast decision、ensemble 与 `analysis_window_mode`
   - 保留后续 posterior 需要的原始进度字段，而不是把这些信号留在 render 层
2. `posterior_feature_service.py`
   - 将输入压成正交 feature block
   - 当前近窗收束直接依赖的进度特征包括：
     - `modeled_headroom_c`
     - `time_since_observed_peak_h`
     - `reports_since_observed_peak`
     - `latest_gap_below_observed_c`
     - `hours_to_peak`
     - `hours_to_window_end`
     - `analysis_window_mode`
3. `weather_posterior_core.py`
   - 先生成未校正 posterior center / spread / event probabilities
   - center 仍以物理锚定为主：`modeled peak / observed max / locked state / regime adjustment`
   - spread 现已拆成：
     - `heuristic spread`
     - `ensemble live alignment adjustment`
     - `progress spread adjustment`
   - 近窗/窗内/后窗的 progress shrink 由“剩余 headroom、离已观测高点多久、已经过了几报、是否已过模型峰时刻、离窗口结束还有多久”共同控制
4. `weather_posterior_calibration.py`
   - 先应用 `quality_spread_multiplier` 做数据质量放宽
   - 再应用 `progress_spread_multiplier`，允许在强实况证据下 `< 1.0`
   - 最后用 `upper_tail_cap_c` 约束上尾，避免 `lock_by_window_end` 已高、`new_high_next_60m` 已低时 quantile 仍保留过宽暖尾

当前 runtime 可概括为：

- `spread_final = spread_core * quality_spread_multiplier * progress_spread_multiplier`
- `upper_tail <= observed_anchor + dynamic_allowance`

其中 `dynamic_allowance` 由：

- `lock_by_window_end`
- `new_high_next_60m`
- `modeled_headroom_c`
- `hours_to_peak`
- `hours_to_window_end`
- `time_since_observed_peak_h`
- `analysis_window_mode`

共同控制。

## 4.2) 当前 posterior 与 peak range 的关系

- `weather_posterior` 是天气侧数值后验层，负责给市场/策略提供 quantiles、event probabilities、peak timing。
- `peak_range_service.py` 仍保留路径解释和历史/信号侧分析，但最终 snapshot 输出已优先以 `weather_posterior` quantiles 为真值来源。
- `analysis_snapshot_service.py` 中的 path cap 继续保留，但定位已下沉为 downstream guard：
  - 当 posterior 在近窗被异常放宽，或已存在 settled/obs anchor 时，防止最终展示重新发散
  - 正常情况下，希望收束主要由 posterior 自身完成，而不是依赖 render/snapshot 层补救

## 5) 当前仍需继续收口的问题

1. `canonical_raw_state` 和 `posterior_feature_vector` 已开始落地
   - provider 原始状态与 feature 轴已初步收口，后续仍需继续扩字段并压缩零散兼容输入
2. `/look` 报告层已收成 `report_focus_service.py + report_synoptic_service.py + report_render_service.py`
   - 当前主路径已退出变量/后验推理
   - 后续重点不再是继续拆 render，而是控制 presentation helper 不重新回流到 orchestrator
3. `peak_range_service.py` 已成为新的复杂度热点
   - 后续建议拆成：
     - peak posterior / range computation
     - historical/calibration helper
4. 盘口异动 worker 主循环已拆薄
   - 当前剩余关注点转为 alert policy / schedule override 的继续结构化，而不是再把状态/投递塞回 worker
5. 3D tracking 仍是轻量 heuristic
   - 目前能区分 `approaching / receding / passing / steady`
   - 还不等于完整的 split/merge/trajectory solver
6. `weather_posterior` 虽已具备 progress-aware shrink 和 upper-tail cap，但本质上仍以 heuristic runtime posterior 为主
   - 多数城市仍共享同一套 spread / cap 规则
   - city-specific 差异目前主要通过 station prior 和少量 regime rule 间接体现
   - 下一步应继续推进 station-family / regime-family calibration，而不是把更多特判塞回 render 层

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
- 主动告警推送契约：
  - [MARKET_ALERT_NOTIFICATION_FLOW.md](/home/ubuntu/.openclaw/workspace/skills/polymarket-weatherbot/docs/core/MARKET_ALERT_NOTIFICATION_FLOW.md)
