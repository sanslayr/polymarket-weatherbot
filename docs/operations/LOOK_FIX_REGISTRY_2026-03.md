# /look 修复总表（2026-03）

Last updated: 2026-03-03

> 本表聚合近期高频修正（含群聊中提到的修正），按主题分组，便于回溯。

## 1) 时间/日期与本地化

- NYC 时区修复：`KLGA -> America/New_York`（避免 Local 显示 UTC）  
  - commit: `bf9f456`
- 默认 target day 改为“站点本地日期”（非 UTC 日期）并用于 Polymarket 事件拉取  
  - commit: `8c51004`

## 2) 温度区间引擎（Tmax）

- 近窗低云偏差修复：低云/降水状态下取消不合理 warm floor 下限抬升  
  - commit: `2a8328d`
- 雨停后残余冷却约束：BKN/OVC 持续时不立即暖反弹  
  - commit: `696ac4b`
- 午后弱斜率平台期约束（solar-decay gate）  
  - commit: `1f97ad8`
- 量化 METAR + 临近窗结束约束（near-end cap）  
  - commit: `11dafe7`
- 窗口后再创新高冲突修复（post-window realized-peak guard）  
  - commit: `3a8a917`
- 晴空圆弧顶锁高约束：
  - 新增两步温度加速度信号（temp_accel_2step_c）
  - clear-sky + 斜率走平/减弱 + 临近峰值 时，强化上沿压制，降低惯性高估
  - 引入太阳辐射简化曲线（经纬度+本地太阳时）辅助判定：
    - `solar_now/solar_next` 走平或回落时，压制上沿更积极
    - `solar_slope_next` 仍强上升时，避免过早压死末段冲高
  - 引入 METAR 多层云量+天气现象量化的 `radiation_eff`（0~1）：
    - `radiation_eff` 低且不回升时，压制上沿
    - `radiation_eff` 高且回升时，保留末段冲高空间

## 3) 云层/实况解析

- 云层全字段合并：`rawOb + clouds[] + cover`，主层按最强约束层  
  - commit: `7454fb0`
- METAR 展示增强：
  - 标题含上一报时间
  - 风变化解释（转向/风速变化）
  - 云层比较写全层 token 并解释“新增/消退/重排/稳定”
  - 天气现象中文释义（如 `-RA（小雨）`）
  - commit: `813252f`

## 4) 环流证据去模板化

- 700/500hPa 证据“有信号才显示”，过滤泛化句  
  - commit: `1608961`
- 500hPa 识别增强（减少漏检）：
  - 由单轴 `dzx` + 曲率改为方向无关的 `|∇z500| + Laplacian` 判据
  - 新增弱场 fallback（weak_ridge/weak_trough）避免“500 完全识别不出”
  - 接入前后场趋势标记（strengthening/deepening/filling/weakening）并纳入 500 置信度约束

## 5) Polymarket 解析/标签逻辑

- 美盘范围桶支持与单位换算修复（`42-43F` 等）  
  - commit: `75e7e33`
- 美站 Tmax 区间显示华氏（K* ICAO）  
  - commit: `f1a26d7`
- 摄氏市场年份误匹配修复（防止 `2026-31c` 被当范围）  
  - commit: `114d3da`
- 标签改为天气一致性优先（最有可能/alpha）并限制 cheap-but-off-range 误标  
  - commits: `4a19122`, `891b407`, `fb3c546`, `3443eec`
- 性能优化（P1/P2/P3）：
  - P1: Polymarket 事件短 TTL 缓存 + 请求超时收敛（默认 3s）
  - P2: 去除盘口过滤阶段 O(n²) label 回查，改为单次解析复用
  - P3: 主流程内提前并发预取 Polymarket 事件（与 forecast pipeline 重叠）

## 6) 相位判定与窗口行为

- 冷平流 far-window 反弹上限（北向来流）  
  - commit: `f5dbbe1`
- post 阶段文案改为“反超前高门槛”导向，避免只给泛化提示  
  - commit: `3de3200`

## 7) 输出/交互一致性

- `/look` 单条最终报告直出（禁预告）  
  - 规则固化于文档与实现链路
- 关键实况提示避免机械化（去模板）  
  - 多次迭代已并入当前逻辑
- `关注变量` far 阶段去模板化：
  - 移除固定“远离峰值窗口”强插句
  - far 阶段引入时距/斜率/偏差驱动的轻量实时触发
  - far 阶段在弱信号时自动收敛条目数（1条），强信号时保留2条

## 8) 版式与文案整合（2026-03-02）

- 头部风险提示文案统一：`**🦞龙虾学习中，不提供交易建议🦞**`  
  - commits: `241ab47`, `9842125`
- 市场期望行改为分层展示并支持动态覆盖率：`X｜L~H（N%范围）`  
  - commits: `4f62dc1`, `c065c93`, `067cae4`
- Polymarket 分层排版：`市场定价期望`（前）+ `博弈区间`（后），并清理空行/缩进符风格  
  - commits: `738fa9b`, `0f3f7b3`, `fe67f04`
- METAR 排版回归：恢复“最新报 + 字段 bullet 平铺”基线样式  
  - commit: `40139ad`
- 温度精度口径统一：非美站不强补 `.0`；同小时偏差固定 1 位小数  
  - commits: `d30bdfd`, `3893cb8`

## 9) 学习友好架构整合

- 新增参数存储层：
  - `scripts/param_store.py`（默认值 + 配置覆盖 + 热更新缓存）
  - `config/tmax_learning_params.json`（可学习参数面板）
- 已将云量/天气透过率与 rounded-top 关键阈值切换为配置驱动，便于后续历史回放学习后直接更新参数。
- 新增文档：`docs/core/HISTORICAL_LEARNING_ARCHITECTURE.md`（离线学习 + 在线更新 + 典型案例库路线）。

## 10) 2026-03-03 结构收口与防回归整合

- 时区差分收口：关键时差计算统一到 `_hours_between_iso(...)`，替换分散的手写 `fromisoformat/replace(tzinfo)` 差分路径。  
  - commits: `4e752af`
- 远离窗口冷平流封顶误触发修复：`hleft` 时区对齐 + 仅在接近峰值时启用 far cap，避免清晨长升温跑道被误封顶（Ankara case）。  
  - commits: `b1a2f8f`, `4e752af`
- 市场档位渲染机制统一：按 `天气区间 ∪ 市场期望区间` 连续展示，并强制包含市场最高概率档位，减少补丁式裁剪冲突。  
  - commits: `e285e56`, `631d2ad`, `1142dec`
- Post-window 文案分层：把“峰值窗已过/关键报平稳”等事实移入 METAR 下 `实况分析`；`关注变量` 仅保留下一报情景与观测维度。  
  - commit: `da038aa`
- Alpha 标签门控改为证据驱动（非一刀切禁用）：收敛态默认抑制，出现再冲高证据时可恢复。  
  - commit: `e7e0193`
- 防绕过文档硬约束：在 Contract/Technical Notes 增加“时差计算统一工具”强约束与 `TZ-WAIVER` 例外流程。  
  - commit: `939c8f5`
- far 阶段晴空日振幅修正：引入“昨日实况振幅 vs 今日模型振幅”有限上修，并加环流上下文门控（弱强迫/辐射主导才启用）。  
  - commits: `3e1968d`, `8cc8646`

## 11) 2026-03-03 OpenClaw 调度延迟收口（/look 执行策略）

- 明确 `/look` 为“单次阻塞执行优先”：
  - 先走 blocking `exec`（建议 `yieldMs=20~25s`）；
  - 仅在首次 `exec` 返回活跃会话时，才允许 `process(action=poll)` 回退。
- `process poll` 超时策略收敛：
  - 首次回退 `timeout=8~10s`；
  - 必要时第二次可放宽到 `15s`；
  - 不再默认 `30s` 轮询，减少“脚本12s但端到端40s”尾段空等。
- 规则已固化到：
  - `AGENTS.md`
  - `workspace/AGENTS.md`

## 12) 2026-03-06 探空实测接入硬规则收口

- 新增/更新运行规范文档：`docs/operations/SOUNDING_OBS_ANALYSIS_PIPELINE.md`
- 核心硬规则：
  - 探空实测必须在 **24h 内**，超时直接禁用实测探空；
  - 距离 `>150km` 或地形区不一致（含大湖两侧/明显地形屏障）直接禁用实测探空；
  - 低代表性不做降级加权，直接走“模式剖面 + 本地METAR”。
- 站点侧约束：
  - Toronto 不采用 Buffalo（地形/下垫面代表性不一致）；
  - Seoul 指定 Incheon 站点；若 24h 内无实测则仍禁用实测探空。

---

## 仍在持续优化（未封板）
- 跨城市回归：海风/夜间平流逆温/沿海湿冷场景
- 早段已出高点的二次反超概率标定（区域化）
- 700/500 触发阈值进一步站点化
