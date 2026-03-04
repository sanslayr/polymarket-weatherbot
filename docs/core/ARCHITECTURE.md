# polymarket-weatherbot Architecture (2026-03-01)

> 目标：输出简练、可解释、可复用的 `/look` 城市最高温分析。

> 文档导航：先看 `DOCS_INDEX.md`。特殊情形规则集中在 `SPECIAL_CASE_PLAYBOOK.md`。
> Agent 代码更新边界：`AGENT_UPDATE_GUARDRAILS.md`。

---

## 1) 模块分层（当前主链路）

### A. Ingress / Orchestrator
- `scripts/telegram_report_cli.py`
- 职责：
  - 调用命令解析层（`scripts/look_command.py`）
  - 调用站点目录层（`scripts/station_catalog.py`）
  - 调用小时预报服务层（`scripts/hourly_data_service.py`）
  - 数据抓取编排（小时预报 + METAR + synoptic pipeline）
  - 调用渲染服务层（`scripts/report_render_service.py`）
  - 仅做最终消息封装与输出

### B. Data Access
- 小时预报：
  - 统一在 `scripts/hourly_data_service.py` 实现：
    - 首选 `open-meteo`
    - 回退 `gfs-grib2 hourly-like`（`gfs_grib_provider.fetch_hourly_like`）
    - 包含缓存/断路器/prev-cycle fallback
- 3D 场：
  - 默认 `gfs-grib2`（`gfs_grib_provider.build_2d_grid_payload_gfs`）
  - 备用 `build_2d_grid_payload.py`（open-meteo 网格）
- 实况：
  - `scripts/metar_utils.py`（AviationWeather METAR 24h + 观测量化区间工具）
  - `scripts/metar_analysis_service.py`（METAR 诊断特征与实况分析文案）
- 盘口事件：
  - `scripts/polymarket_client.py`（事件抓取 + 缓存 + 预取）
  - `scripts/polymarket_render_service.py`（盘口档位解析与区间渲染）

### C. Synoptic Engine
- `scripts/synoptic_runner.py`
  - pass 运行模式：
    - `full`：每个 anchor 执行 `inner + outer500`
    - `split_outer500`（默认）：每个 anchor 执行 `inner`，仅关键 anchor 执行 `outer500`
  - 关键 outer500 anchor 数量由 `FORECAST_OUTER500_ANCHOR_MAX` 控制（默认 4）
  - runner 层已改为内存直连：build + detect 均走函数调用，减少 JSON 临时文件与进程启动开销
  - `outer500` pass 走轻量字段/检测（`field_profile=outer500` + `detector_mode=outer500_only`）
  - 调用 `synoptic_2d_detector.py`
- `scripts/synoptic_2d_detector.py`
  - MSLP 高低压
  - 850 平流
  - 500 槽脊
  - 扩展：frontogenesis / llj_shear / dry_intrusion_700 / baroclinic_coupling

### D. Decision Engine
- `scripts/forecast_pipeline.py`
  - 多锚点构建（全日 anchor）
  - 覆盖率统计（anchors_total/ok/coverage）
  - 3D object 构建（`vertical_3d.py`）
  - 诊断层融合（500/700/925/sounding）
  - 产出 `forecast-decision.v4`

### E. Realtime Gate + Renderer
- `scripts/realtime_pipeline.py`
  - far/near/in/post window 相位判定
  - 触发器筛选
- `scripts/report_render_service.py::choose_section_text`
  - 报告输出协议：
    - 环流背景（主导/次级/关键证据/探空提示）
    - METAR
    - 最高温主带 + 条件尾部
    - 关注变量
    - Polymarket
- `scripts/report_peak_module.py`
  - 最高温区间计算与尾部约束逻辑（从渲染编排层拆出，便于独立迭代）。

### F. Parameter Store（学习友好层）
- `scripts/param_store.py`
  - 统一加载可学习参数（带默认值 + 热更新缓存）
- `config/tmax_learning_params.json`
  - 多层云量映射/层权重
  - wx 透过率
  - rounded-top 阈值（斜率/加速度/太阳几何/辐射恢复）
- 目的：将“经验常量”外置，便于历史回放学习后直接更新配置。

---

## 2) 当前数据策略（已对齐）

- 小时预报：`open-meteo` 优先
- 3D 场：`gfs-grib2` 优先
- 分析基准模型默认显示：`GFS`
- 手动 `model/provider` 参数：当前不对外支持

---

## 3) 缓存与存储结构

### Runtime JSON 缓存
- `cache/runtime/hourly_*.json`
- `cache/runtime/hourly_gfs_*.json`
- `cache/runtime/forecast_decision_*.json`（v4）
- `cache/runtime/forecast_3d_bundle_*.json`（v1）
- `cache/runtime/synoptic_*.json`

> 统一 envelope：`runtime-cache.v1`
> - `cache_schema_version`
> - `updated_at`
> - `source_state`
> - `payload_schema_version`
> - `payload`

### Binary 缓存
- `cache/runtime/gfs_grib/*.grib2`

### 核心契约
- `DECISION_SCHEMA.md`：`forecast-decision.v4`
- `FORECAST_3D_STORAGE.md`：key 规则、bundle/synoptic cache 说明

---

## 4) 已修复的版本/契约冲突

1. 决策 schema 文档从 v2 升级到 v4（与代码一致）
2. 3D 存储文档 key 规则更新为 provider-aware（与代码一致）
3. 新增集中版本常量：`scripts/contracts.py`
   - `forecast-decision.v4`
   - `forecast-3d-bundle.v1`
   - `objects-3d.v1`
   - `synoptic-cache.v2`
4. `synoptic_runner` cache key 纳入 provider + cache schema version，避免跨 provider 缓存串用

---

## 5) 数据获取 / 存储优化空间（review 结论）

### 高优先级
1. **统一 cache metadata envelope**
   - 现状：hourly/forecast/synoptic wrapper 格式不完全一致
   - 建议：统一 `updated_at/schema_version/payload/source_state`

2. **gfs_grib 二进制缓存生命周期管理**
   - 现状：JSON 会 prune，grib 文件未统一 prune
   - 建议：加独立保留窗口（24~48h）与大小上限

3. **anchor 级错误遥测结构化**
   - 现状：字符串错误为主
   - 建议：记录 `{anchor, stage(build|detect), error_type(429/404/timeout), provider}`

### 中优先级
4. **gfs parser 去 subprocess 化（后续）**
   - 现状：runner 主链路已去 subprocess；但 gfs grib 解析仍通过独立 python 进程调用 `.venv_gfs`
   - 目标：减少跨进程开销并统一异常栈

5. **缓存键标准化 helper**
   - 目标：hourly/synoptic/decision 使用统一 key builder，减少重复/漂移

6. **数据质量分级输出标准化**
   - 输出层统一使用 `fresh | cache-hit | fallback-cache | degraded`

---

## 6) 输出层原则（固定）

- 结论优先，不堆砌原始变量
- 保留可解释证据（2~3 条）
- 主带与尾部分离（避免混淆概率层级）
- 探空因子融入环流背景，不单独“技术块堆参数”

---

## 7) 当前已知边界

- Open-Meteo 429 具外部不确定性，只能缓解不能消除
- NOMADS 新 cycle 发布时可能暂时 404，已通过 cycle fallback 缓解
- sounding thermo 目前以可用字段为主，完整本地 profile solver 仍可继续增强

## 8) 历史学习与在线更新

- 设计目标：参数可由历史回放学习驱动，而非长期手工硬编码。
- 当前基础：参数层已配置化（`config/tmax_learning_params.json` + `scripts/param_store.py`）。
- 详细路线图：见 `HISTORICAL_LEARNING_ARCHITECTURE.md`。
