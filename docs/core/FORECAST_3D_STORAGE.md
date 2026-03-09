# Forecast 3D Storage

Last updated: 2026-03-09

## 1) 决策缓存（主读）

- 文件：`cache/runtime/forecast_decision_<key>.json`
- payload：`forecast-decision.v8`
- 外层 envelope：`runtime-cache.v1`
- 用途：`/look` 主流程优先读取，避免重复重算 decision

## 2) 3D 场切片包（回放 / 诊断）

- 文件：`cache/runtime/forecast_3d_bundle_<key>.json`
- schema：`forecast-3d-bundle.v2`
- 关键字段：
  - `station`
  - `date`
  - `model`
  - `synoptic_provider`
  - `synoptic_provider_used`
  - `synoptic_pass_strategy`
  - `runtime`
  - `anchors_local`
  - `outer500_anchors_local`
  - `slices`

用途：

- 锚点覆盖率核验
- 3D object / track 回放
- provider fallback 后的 runtime 诊断

## 3) Synoptic 锚点缓存（runner 内部）

- 文件：`cache/runtime/synoptic_<key>.json`
- 外层 envelope：`runtime-cache.v1`
- `payload_schema_version`：`synoptic-cache.v3`

关键点：

- key 已纳入 `provider`
- key 已纳入 cache schema version
- payload 为标准化 synoptic slice

## 4) Key 规则

### forecast_decision key

`sha1(station|date|model|synoptic_provider|runtime|peak_local)`

### forecast_3d_bundle key

`sha1(station|date|model|synoptic_provider|runtime)`

### synoptic runner key

`sha1(station|date|model|provider|runtime|peak_local|tz|synoptic-cache.v3)`

## 5) Provider 与 runtime 元数据

由于当前 3D 链路是：

- `ecmwf-open-data` primary
- `gfs-grib2` fallback

因此缓存与 decision 中都必须区分：

- `synoptic_provider_requested`
- `synoptic_provider_used`
- `synoptic_provider_fallback`

不能再假定“请求什么就一定用了什么”。

## 6) 退化回退

当当前 runtime 锚点构建失败：

- 优先读取最近同 `station/date/model/provider` 的 `forecast_3d_bundle`
- 时间窗受 `FORECAST_SYNOPTIC_FALLBACK_HOURS` 控制

回退后：

- `quality.source_state` 记为 `fallback-cache`
- 若覆盖不足或 outer500 缺失，也可能进一步降级为 `degraded`

## 7) 清理原则

- runtime JSON 走统一 prune
- `gfs_grib/` 二进制缓存单独设置保留窗口
- ECMWF / GFS provider 的临时文件都应视为可丢弃 runtime 数据
