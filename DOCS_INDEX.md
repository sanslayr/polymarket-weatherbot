# polymarket-weatherbot Docs Index (structured)

Last updated: 2026-03-02

## 0) 先看这里（当前生效的核心文档）
1. `LOOK_OUTPUT_CONTRACT.md`  
   - `/look` 输出行为、文案约束、阶段化展示规则（**最高优先级**）
2. `SPECIAL_CASE_PLAYBOOK.md`  
   - 特殊情形触发条件与处理规则（防遗漏）
3. `ARCHITECTURE.md`  
   - 模块分层、主链路、缓存与降级策略
4. `DECISION_SCHEMA.md`  
   - `forecast-decision.v4` 契约
5. `FORECAST_3D_STORAGE.md`  
   - runtime cache / bundle / synoptic key 规则
6. `TELEGRAM_COMMANDS.md`  
   - Telegram 命令入口与参数约束
7. `LOOK_FIX_REGISTRY_2026-03.md`  
   - 近期修复清单（按主题归纳 + commit）

---

## 1) 设计与实现（active）
- `ARCHITECTURE.md`
- `DECISION_SCHEMA.md`
- `FORECAST_3D_STORAGE.md`
- `CIRCULATION_SOUNDING_REQUIREMENTS.md`
- `LOOK_OUTPUT_CONTRACT.md`
- `SPECIAL_CASE_PLAYBOOK.md`
- `TELEGRAM_COMMANDS.md`

## 2) 变更与复盘（history/review）
- `LOOK_FIX_REGISTRY_2026-03.md`（当前汇总入口）
- `FORECAST_MODULE_REVIEW_2026-03-01.md`
- `FORECAST_PIPELINE_REVIEW_2026-03-01.md`
- `FORECAST_PIPELINE_REVIEW_2026-02-28.md`
- `P2_P3_REVIEW_2026-03-01.md`

## 3) 旧规划/模板（archive-reference）
- `LOOK_MD_TEMPLATE.md`
- `PROBABILITY_LIBRARY.md`
- `SCENARIO_QUANT_FEATURES.md`
- `City_Tmax_Analysis_Skill_Planning.md`

## 4) 运行人格/会话文件（non-product docs）
> 这些不是 weather 引擎规格说明，避免误当成业务约束。
- `AGENTS.md`
- `SOUL.md`
- `USER.md`
- `IDENTITY.md`
- `TOOLS.md`
- `BOOTSTRAP.md`
- `HEARTBEAT.md`
- `memory/*.md`

---

## 5) 文档维护规则（避免再次混乱）
- 新增/修改规则时：
  1) 先改 `SPECIAL_CASE_PLAYBOOK.md`（规则）
  2) 再改 `LOOK_OUTPUT_CONTRACT.md`（输出口径）
  3) 最后在 `LOOK_FIX_REGISTRY_2026-03.md` 记一条变更（含 commit）
- 任何“仅临时实验”结论，不写进 CONTRACT；只写入 REGISTRY 并标注 `trial`。
- 同一主题只保留一个“真源文档”（single source of truth）。
