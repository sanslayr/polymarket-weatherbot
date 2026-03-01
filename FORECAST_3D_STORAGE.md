# Forecast 3D Storage (current)

## 1) 决策缓存（主读）
- 文件：`cache/runtime/forecast_decision_<key>.json`
- 内容：`forecast-decision.v4`
- 用途：`/look` 主流程优先读取（避免重复重算）

## 2) 3D 场切片包（回放/诊断）
- 文件：`cache/runtime/forecast_3d_bundle_<key>.json`
- schema：`forecast-3d-bundle.v1`
- 关键字段：
  - `station/date/model/synoptic_provider/runtime`
  - `anchors_local`
  - `slices`（每个 anchor 的 synoptic 输出）
- 用途：
  - 覆盖率核验（anchors_total vs anchors_ok）
  - 3D object linker 输入回放

## 3) Synoptic 锚点缓存（runner 内部）
- 文件：`cache/runtime/synoptic_<key>.json`
- wrapper schema：`synoptic-cache.v2`
- 关键点：
  - key 已纳入 `provider`
  - payload 为标准 `scale_summary.synoptic.systems`

## 4) Key 规则（当前）
### forecast_decision key
`sha1(station|date|model|synoptic_provider|runtime)`

### forecast_3d_bundle key
`sha1(station|date|model|synoptic_provider|runtime)`

### synoptic runner key
`sha1(station|date|model|provider|runtime|peak_local|tz|synoptic-cache.v2)`

## 5) 退化回退
- 当当前 runtime 锚点构建失败：
  - 优先读最近同 `station/date/model/provider` 的 `forecast_3d_bundle`（时间窗受 `FORECAST_SYNOPTIC_FALLBACK_HOURS` 控制）
- 回退数据会在 `quality.source_state` 标记为 `fallback-cache` 或 `degraded`

## 6) 清理策略（建议）
- runtime JSON 由统一 prune 处理（按文件 mtime）
- `gfs_grib/` 二进制缓存建议单独设置保留窗口（如 24~48h）
