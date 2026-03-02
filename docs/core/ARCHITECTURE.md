# polymarket-weatherbot Architecture (2026-03-01)

> 目标：输出简练、可解释、可复用的 `/look` 城市最高温分析。

> 文档导航：先看 `DOCS_INDEX.md`。特殊情形规则集中在 `SPECIAL_CASE_PLAYBOOK.md`。

---

## 1) 模块分层（当前主链路）

### A. Ingress / Orchestrator
- `scripts/telegram_report_cli.py`
- 职责：
  - 命令解析（`/look`）
  - 站点解析（`station_links.csv`）
  - 数据抓取编排（小时预报 + METAR + synoptic pipeline）
  - 文本渲染（环流背景、实况、主带/尾部、盘口）

### B. Data Access
- 小时预报：
  - 首选 `open-meteo`（`fetch_hourly_openmeteo`）
  - 回退 `gfs-grib2 hourly-like`（`gfs_grib_provider.fetch_hourly_like`）
- 3D 场：
  - 默认 `gfs-grib2`（`gfs_grib_provider.build_2d_grid_payload_gfs`）
  - 备用 `build_2d_grid_payload.py`（open-meteo 网格）
- 实况：
  - AviationWeather METAR 24h

### C. Synoptic Engine
- `scripts/synoptic_runner.py`
  - 双 pass（inner + outer500）
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
- `scripts/telegram_report_cli.py::choose_section_text`
  - 报告输出协议：
    - 环流背景（主导/次级/关键证据/探空提示）
    - METAR
    - 最高温主带 + 条件尾部
    - 关注变量
    - Polymarket

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
4. **synoptic runner 去 subprocess 化（逐步）**
   - 目标：减少 IO 与进程创建开销
   - 方式：内存对象直连 detector

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
