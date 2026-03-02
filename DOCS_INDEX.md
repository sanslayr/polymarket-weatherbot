# polymarket-weatherbot Docs Index (structured)

Last updated: 2026-03-02

## 0) 先看这里（当前生效的核心文档）
1. `LOOK_OUTPUT_CONTRACT.md`  
   - `/look` 输出行为、文案约束、阶段化展示规则（**最高优先级**）
2. `SPECIAL_CASE_PLAYBOOK.md`  
   - 天气形势特殊情形规则（跨城市 + 站点可扩展）
3. `TECHNICAL_IMPLEMENTATION_NOTES.md`  
   - 非形势的工程实现备注（市场解析/数据精度/渲染细节）
4. `ARCHITECTURE.md`  
   - 模块分层、主链路、缓存与降级策略
5. `DECISION_SCHEMA.md`  
   - `forecast-decision.v4` 契约
6. `FORECAST_3D_STORAGE.md`  
   - runtime cache / bundle / synoptic key 规则
7. `TELEGRAM_COMMANDS.md`  
   - Telegram 命令入口与参数约束
8. `LOOK_FIX_REGISTRY_2026-03.md`  
   - 近期修复清单（按主题归纳 + commit）

---

## 1) 设计与实现（active）
- `ARCHITECTURE.md`
- `DECISION_SCHEMA.md`
- `FORECAST_3D_STORAGE.md`
- `CIRCULATION_SOUNDING_REQUIREMENTS.md`
- `LOOK_OUTPUT_CONTRACT.md`
- `SPECIAL_CASE_PLAYBOOK.md`
- `TECHNICAL_IMPLEMENTATION_NOTES.md`
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
- 若是**天气形势判断规则**：
  1) 先改 `SPECIAL_CASE_PLAYBOOK.md`
  2) 如影响外显口径，再改 `LOOK_OUTPUT_CONTRACT.md`
  3) 在 `LOOK_FIX_REGISTRY_2026-03.md` 记变更（含 commit）
- 若是**技术实现细节**（市场解析/数据精度/渲染）：
  1) 改 `TECHNICAL_IMPLEMENTATION_NOTES.md`
  2) 如影响用户输出，再补 `LOOK_OUTPUT_CONTRACT.md`
  3) 在 REGISTRY 记变更
- 任何“仅临时实验”结论，不写进 CONTRACT；只写入 REGISTRY 并标注 `trial`。
- 同一主题只保留一个“真源文档”（single source of truth）。
