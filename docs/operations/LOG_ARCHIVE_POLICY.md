# /look 日志与归档策略（Log Archive Policy）

Last updated: 2026-03-02

> 目标：保证可追溯、可复盘、可清理，避免日志和快照越积越乱。

## 1) 归档对象

### A. 运行日志（operational logs）
- `cache/runtime/perf.log`（阶段耗时）
- 运行期降级/错误摘要（若后续单独落盘）

### B. 决策快照（analysis snapshots）
- `cache/runtime/forecast_decision_*.json`
- `cache/runtime/forecast_3d_bundle_*.json`
- `cache/runtime/synoptic_*.json`

### C. 模块级 cache+archive（由脚本管理）
- `scripts/cache_archive_manager.py`
- 输出路径（默认）：`archive/YYYY-MM-DD/*.json`

---

## 2) 目录约定

- 热数据（在线读取）：`cache/runtime/`
- 冷归档（复盘追溯）：`archive/YYYY-MM-DD/`
- 复盘文档（人工总结）：`LOOK_FIX_REGISTRY_2026-03.md` / review 文档

---

## 3) 留存策略（建议）

### 热层（runtime）
- `perf.log`：保留 7 天（滚动）
- runtime json cache：按现有 prune 逻辑 + 体积上限

### 冷层（archive）
- 日级快照保留 30 天
- 关键故障日（429/源降级/重大偏差）可延长到 90 天

### 长期层（知识）
- 关键结论写入：
  - `SPECIAL_CASE_PLAYBOOK.md`（天气形势规则）
  - `TECHNICAL_IMPLEMENTATION_NOTES.md`（技术实现）
  - `LOOK_FIX_REGISTRY_2026-03.md`（变更索引）

---

## 4) 触发时机

- 每次重大规则修正后：
  1) 记录 commit
  2) 必要时留一份当日关键快照到 `archive/YYYY-MM-DD/`
  3) 在 `LOOK_FIX_REGISTRY_2026-03.md` 记“修正-影响-验证”

- 每日收尾（可选）：
  - 检查 runtime 是否需要清理
  - 将需要长期追溯的样例转入 archive

---

## 5) 最小审计字段（建议）

归档文件建议至少包含：
- `station`
- `target_date`
- `model`
- `runtime`
- `source_state`（fresh/cache-hit/fallback/degraded）
- `window phase`
- `observed_max_temp_c`
- `display/core range`
- 关键触发（云/降水/风场）

---

## 6) 边界
- 本文件是“日志与归档治理”，不是天气判据。
- 天气形势规则只放 `SPECIAL_CASE_PLAYBOOK.md`。
- 工程细节只放 `TECHNICAL_IMPLEMENTATION_NOTES.md`。
