# Forecast Decision Schema (v4)

`forecast_pipeline.py` 产出的统一决策对象结构（报告层只消费该结构，不直接耦合底层抓取细节）。

## 顶层
- `schema_version`: `forecast-decision.v4`
- `meta`: 识别与缓存失效关键元数据
- `quality`: 数据质量/覆盖率
- `features`: 事实层（不直接面向用户文案）
- `decision`: 裁决层（报告转译主输入）

---

## meta
- `station` / `date` / `model`
- `synoptic_provider`：当前 3D 场来源（现默认 `gfs-grib2`）
- `runtime`：运行时次标签
- `window.start_local` / `window.end_local`

## quality
- `source_state`: `fresh | cache-hit | fallback-cache | degraded`
- `missing_layers`: 缺失层列表（如 `['synoptic']`）
- `synoptic_anchors_total`: 计划锚点数
- `synoptic_anchors_ok`: 成功锚点数
- `synoptic_coverage`: 锚点覆盖率（0~1）

## features（事实层）
- `objects_3d`：`objects-3d.v1`
  - `main_object`
  - `candidates`
- `h500.phase / phase_hint / pva_proxy`
- `h850.advection`
- `h700.summary`
- `h925.summary`
- `sounding.path_bias`
- `sounding.thermo`（可选）：
  - `has_profile` / `quality`
  - `sbcape_jkg / mlcape_jkg / mucape_jkg`
  - `sbcin_jkg / mlcin_jkg`
  - `lcl_m / lfc_m / el_m`

## decision（裁决层）
- `main_path`: 预报主导 / 过渡 / 实况主导
- `bottleneck`: 当前主要约束
- `trigger`: 触发提醒
- `object_3d_main`: 主 3D 对象（若有）
- `override_risk`: `low | high`
- `background`：报告层环流背景标准字段
  - `phase_mode`
  - `phase500`
  - `pva500`
  - `phase_hint`
  - `line_500`
  - `line_850`
  - `extra`

---

## 兼容策略
- 报告层优先读取 `decision.background`
- 缓存读取严格校验 `schema_version`；版本不匹配直接视为失效并重建
