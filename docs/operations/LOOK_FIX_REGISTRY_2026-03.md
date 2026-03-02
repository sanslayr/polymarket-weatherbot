# /look 修复总表（2026-03）

Last updated: 2026-03-02

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

- `/look` & `/lookhelp` 单条最终报告直出（禁预告）  
  - 规则固化于文档与实现链路
- 关键实况提示避免机械化（去模板）  
  - 多次迭代已并入当前逻辑

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

---

## 仍在持续优化（未封板）
- 跨城市回归：海风/夜间平流逆温/沿海湿冷场景
- 早段已出高点的二次反超概率标定（区域化）
- 700/500 触发阈值进一步站点化
