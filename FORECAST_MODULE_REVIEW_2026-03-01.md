# Forecast Module Review — 2026-03-01

## Scope
对“环流背景分析系统 + 数据获取/存储模块”做一次结构性 review，目标：
1) 架构分层清晰
2) 版本契约不冲突
3) 数据获取/存储路径可维护、可优化

---

## Findings (before fix)

### F1. 文档契约与代码版本漂移
- `DECISION_SCHEMA.md` 仍写 `forecast-decision.v2`
- 代码实际已使用 `forecast-decision.v4`

### F2. 存储 key 说明与实现不一致
- 文档 key 规则未体现 provider/runtime 维度
- 实际 `forecast_decision` / `forecast_3d_bundle` 已 provider-aware

### F3. Synoptic runner 缓存存在 provider 串用风险
- `synoptic_runner` cache key 未纳入 `provider`
- 不同 provider 可能命中同 key（潜在冲突）

### F4. 版本字符串分散
- 多处硬编码 schema version，未来升级容易漏改

### F5. 二进制缓存生命周期不明确
- JSON runtime cache 有 prune
- `cache/runtime/gfs_grib/*.grib2` 无统一清理窗口

---

## Fixes applied in this review

### A. 统一版本常量（代码）
- 新增 `scripts/contracts.py`
  - `FORECAST_DECISION_SCHEMA_VERSION = forecast-decision.v4`
  - `FORECAST_3D_BUNDLE_SCHEMA_VERSION = forecast-3d-bundle.v1`
  - `OBJECTS_3D_SCHEMA_VERSION = objects-3d.v1`
  - `SYNOPTIC_CACHE_SCHEMA_VERSION = synoptic-cache.v2`
- `forecast_pipeline.py` / `vertical_3d.py` 改为引用常量

### B. 修复 synoptic cache 冲突（代码）
- `synoptic_runner.py`
  - cache key 纳入 `provider + schema version`
  - cache 文件改为 wrapper（`schema_version + updated_at + payload`）
  - 保留 legacy payload-only 兼容读取

### C. runtime 清理补全（代码）
- `telegram_report_cli.py::_prune_runtime_cache`
  - 增加 `gfs_grib/*.grib2` 清理
  - 环境变量：`GFS_GRIB_CACHE_HOURS`（默认 36h）

### D. 文档对齐（文档）
- 重写 `DECISION_SCHEMA.md`（v4）
- 重写 `FORECAST_3D_STORAGE.md`（provider-aware key）
- 重写 `ARCHITECTURE.md`（现状分层与链路）

### E. 缓存 envelope 与遥测（代码）
- 新增 `scripts/cache_envelope.py`，统一 `runtime-cache.v1` envelope
- hourly / synoptic / forecast_decision 均支持 envelope + 兼容旧格式读取
- 锚点级 telemetry 已回写到 `forecast_decision.quality`

---

## Current architecture (concise)

1. `/look` orchestrator：`telegram_report_cli.py`
2. hourly acquisition：Open-Meteo 优先，GFS hourly-like 回退
3. synoptic acquisition：GFS grib2 优先（inner+outer500）
4. decision build：`forecast_pipeline.py`（coverage-aware）
5. render：主带 + 条件尾部 + 简洁证据

---

## Optimization opportunities (next)

### O1 (high)
统一 runtime cache envelope（hourly/synoptic/decision）为同一 metadata schema，减少工具链分支处理。

### O2 (high) — 已完成第一阶段
anchor/stage 级错误遥测结构化已落地：
- synoptic runner 记录 pass 级事件（build/detect, status, error_type, elapsed）
- forecast quality 写入：
  - `synoptic_anchor_events`
  - `synoptic_anchor_error_counts`
后续可继续把该遥测接入用户侧降级提示文案。

### O3 (medium)
逐步减少 synoptic subprocess 编排，改为内存链路调用（降低 IO/进程开销）。

### O4 (medium)
建立缓存健康指标（命中率、fallback率、degraded率）并持久化周报。

---

## Validation
- `python3 -m py_compile` 全链路通过
- `/look ank` 实测通过，输出包含新的探空融合与稳定区间判定
