# BUGFIX_RKSI_METAR_DNS_AND_SOUNDING_CACHE.md

## Issue
`/look RKSI` 在网络/DNS不可用场景会直接失败：

- 报错：`aviationweather.gov ... NameResolutionError`
- 结果：报告无法生成（执行直接中断）

同时，探空实测在缓存过期后会再次尝试网络抓取，存在不必要的重复请求。

---

## Scope
- 该问题不只影响 RKSI。`fetch_metar_24h` 是所有站点共用路径，因此所有站点都可能在同类网络异常下失败。
- 探空缓存复用策略同样是全站点共用逻辑。

---

## Repro
命令（2026-03-06）：

```bash
LOOK_SHOW_PERF=1 PYTHONPATH=scripts python3 scripts/telegram_report_cli.py --command '/look RKSI'
```

旧行为：直接抛异常退出。  
新行为：输出可降级报告（METAR 不可用时给出 `"无可用METAR数据。"`）。

---

## Root Cause
1. `scripts/metar_utils.py::fetch_metar_24h`
   - 仅网络请求+重试，失败后直接抛异常；
   - 无 runtime 缓存回退。
2. `scripts/sounding_obs_service.py`
   - 缓存过期后即重新抓取；
   - 缺少“过期但仍可复用”的 stale 策略，导致高频重复抓取。

---

## Fix Strategy
1. `scripts/metar_utils.py`
   - 增加 `cache/runtime/metar24_<ICAO>.json` 缓存；
   - 优先读新鲜缓存；
   - 网络失败时读 stale 缓存（36h）；
   - 无可用缓存时返回空列表并写入 5 分钟负缓存（避免每次重试都打满）。
2. `scripts/metar_analysis_service.py`
   - 当 `metar24` 为空时，返回最小诊断字段（含 `latest_report_local/latest_report_utc` 当前时间兜底），避免相位逻辑失真。
3. `scripts/sounding_obs_service.py`
   - 增加 stale 复用规则：
     - `use_sounding_obs=true` 且 `obs_age<=24h` 可复用；
     - 稳定禁用原因可长期复用；
     - 短时失败类原因允许 3 小时内 backoff 复用；
   - 调整 TTL：有效探空 6h，`retrieval_failed` 60m，`no_valid_obs/qc_failed` 90m，稳定禁用 24h。

---

## Regression Cases
### Case A: RKSI 生成稳定性
- 时间：2026-03-06 01:02 UTC
- 结果：报告成功生成，无进程级异常。
- 耗时：`ELAPSED=0.13s`；报告内 `total=0.01s`，`process=0.08s`（缓存命中场景）。

### Case B: 早结论关键语句检查
- 文本中未出现：
  - `峰值窗已过`
  - `按已观测最高温锚定`
  - `高点大概率已定`

### Case C: 探空复用行为
- 在缓存过期后（但仍在 backoff/可复用范围内）不重复抓取；
- 超过 backoff 窗口后恢复抓取尝试。

---

## Expected Outcome
- `/look` 在外部 METAR 源不可达时仍可输出可读报告，不因单点依赖直接失败。
- 探空不再“每次都抓”，请求频率显著下降，整体生成耗时与稳定性更可控。
