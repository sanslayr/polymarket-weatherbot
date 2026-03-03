# /look 技术实现与维护备注（非天气形势）

Last updated: 2026-03-02

> 本文件用于记录工程/数据技术层面的实现点，避免污染天气形势规则手册。

## 1) 日期与时区
- 默认目标日期使用站点 local date（非 UTC day）。
- Polymarket 事件 URL 按 local date 组装。

## 2) 市场档位解析
- 支持 F/C 单点与范围桶（如 `42-43F`、`31c`、`30corbelow`）。
- 避免年份段误匹配（如 `...-2026-31c` 不应解析为温度范围）。
- 过滤时统一单位后再比较，防止 F/C 错位。

## 3) METAR 数据技术处理
- 量化温度台阶识别（`metar_temp_quantized`）。
- 临近窗口结束时限制“单报跳变”造成的过度上修。
- 最高温区间下沿强制不低于当日已观测最高温对应的可行下沿（量化 METAR 情况下用观测桶下边界）。
- 新增 `observed_max_interval_lo_c / observed_max_interval_hi_c`，用于观测量化区间锚定（非美国整数°C桶、美国站点 ±0.5°F 桶）。
- 统一由 `_observed_max_interval_c(...)` 生成锚定区间，避免渲染层/市场层各自重复计算造成新旧口径混杂。
- 云层字段合并：`rawOb + clouds[] + cover`（技术实现层）。
- 新增两步温度加速度信号（`temp_accel_2step_c`）用于识别“升温减速/圆弧顶”。
- 新增夜间增温辅助信号：`wind_speed_trend_1step_kt`、`dewpoint_trend_1step_c`，用于 after-sunset reheat 组合判定。
- 新增 METAR 采样节律识别：`metar_routine_cadence_min`、`metar_recent_interval_min`、`metar_speci_active`，用于动态调整短时判读窗口（半小时站/整点站/SPECI 加密采样）。
- 新增 SPECI 可能性前瞻信号：`metar_speci_likely / metar_speci_likely_score`（温度/风/云/天气现象突变组合），用于避免长周期站点在异常前夜被单报过早锚定。
- 增加近24h历史特征提取并用于阈值自适应：`metar_speci_count_24h`、`metar_speci_ratio_24h`、`metar_rapid_temp_jump_count_24h`、`metar_rapid_wind_jump_count_24h`、`metar_wx_transition_count_24h`、`metar_speci_likely_threshold`。
- 相关阈值参数已外置到 `config/tmax_learning_params.json`，由 `scripts/param_store.py` 统一加载。

## 3.1 太阳辐射简化曲线（清空日）
- 在渲染层引入基于经纬度 + 本地时刻的理论晴空辐射相对量（0~1）：
  - 使用 NOAA 近似（太阳赤纬 + equation of time + 经度修正本地太阳时）
  - 指标：`solar_now / solar_prev / solar_next` 与 `solar_slope_next`
- 用途：
  - 与“晴空 + 斜率走平/减速”联合触发 rounded-top 锁高约束
  - 太阳高度较低且 `solar_slope_next` 走平/转弱时，进一步抑制惯性高估
  - 若 `solar_slope_next` 明显上升，避免过早压死末段冲高空间

## 3.2 夜间增温组合门控（nocturnal_rewarm）
- 参数组：`nocturnal_rewarm`。
- 目标：避免“窗口后一刀切压顶”漏掉少数夜间小幅回升场景。
- 触发结构：
  - 夜间背景（低太阳辐射或本地时刻进入夜段）
  - 证据组合（暖平流 + 混合/云被/露点/气压）达到最小分值
  - 降水门控：`new/intensify` 触发硬抑制；`end/weaken + 低云` 施加残余冷却扣分；中高强度降水自动失效
  - 轻降水细化：若同时满足暖平流落地 + 露点回升/混合增强，可从“硬抑制”降级为“轻拖累”
- 作用方式：
  - 抑制窗口后惯性压顶仅在“无夜间回升证据”时生效
  - `post` 反超评估中，夜间证据仅提供“有限上修”而非白天级别上冲空间

## 3.3 Polymarket 档位渲染统一机制
- 统一使用“并集连续渲染”单机制：展示区间 = `天气Tmax区间 ∪ 市场定价期望区间`。
- 在该并集区间内按温度档位连续展开，避免多层补丁式裁剪。
- 额外硬约束：始终包含市场最高概率档位（dominant bucket）。

## 4) 输出渲染技术增强
- 最新报标题内嵌上一报时间。
- 风字段增加与上一报对比文本。
- 云层对比输出完整 token 变化，并对“相近高度层位的覆盖度变化”（如 `SCT230 -> FEW200`）优先识别为带高度数值的减弱/增强（例：`约20000-23000ft高度层云量减弱`），避免误判为结构重排。
- 天气现象附中文释义（如 `-RA（小雨）`），并扩展覆盖雾霾烟尘/雷暴/飑/漏斗云/沙尘暴/附近现象等常见 METAR 代码。

## 5) 探空图链接策略
- TropicalTidbits 探空图链接默认优先 `ECMWF`（`model=ecmwf`）。
- 分析主模型可与探空链接模型不同；探空链接用于统一可读性与对比口径。
- 链接有效时段自动匹配当前分析窗口：
  - 常规：峰值窗口
  - `post` 且有潜在二峰：潜在反超窗口
  - `post` 且判定难以二峰：后段验证窗口（用于解释“为何不支持反超”）

## 6) 维护边界
- 本文件不承载“天气形势判断规则”，只记录实现细节。
- 形势规则统一写入：`SPECIAL_CASE_PLAYBOOK.md`。
