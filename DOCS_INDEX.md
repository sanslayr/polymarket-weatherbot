# polymarket-weatherbot Docs Index (structured)

Last updated: 2026-03-09

## TL;DR（先读顺序）
1. `docs/core/LOOK_OUTPUT_CONTRACT.md`（输出约束，最高优先级）
2. `docs/core/SPECIAL_CASE_PLAYBOOK.md`（天气形势规则）
3. `docs/core/ARCHITECTURE.md`（当前运行时结构）
4. `docs/core/TARGET_ARCHITECTURE.md`（目标版 weather/market/research 架构）
5. `docs/core/DECISION_SCHEMA.md`（运行时决策契约）
6. `docs/core/TECHNICAL_IMPLEMENTATION_NOTES.md`（技术实现细节）

---

## 1) 文档目录结构（当前）

### A. Core（稳定规则 / 设计真源）
- `docs/core/ARCHITECTURE.md`
- `docs/core/TARGET_ARCHITECTURE.md`
- `docs/core/MARKET_ARCHITECTURE.md`
- `docs/core/MARKET_TRADING_PLAN.md`
- `docs/core/DECISION_SCHEMA.md`
- `docs/core/FORECAST_3D_STORAGE.md`
- `docs/core/LOOK_OUTPUT_CONTRACT.md`
- `docs/core/SPECIAL_CASE_PLAYBOOK.md`
- `docs/core/CIRCULATION_SOUNDING_REQUIREMENTS.md`
- `docs/core/TECHNICAL_IMPLEMENTATION_NOTES.md`
- `docs/core/PROJECT_OVERVIEW.md`
- `docs/core/AGENT_UPDATE_GUARDRAILS.md`
- `docs/core/TELEGRAM_COMMANDS.md`

### B. Operations（运维与变更）
- `docs/operations/LOG_ARCHIVE_POLICY.md`
- `docs/operations/LOOK_FIX_REGISTRY_2026-03.md`
- `docs/operations/SOUNDING_OBS_ANALYSIS_PIPELINE.md`
- `docs/operations/city-background/README.md`

### C. Reviews（历史复盘）
- `docs/reviews/ARCHITECTURE_FRAMEWORK_REVIEW_2026-03-09.md`
- `docs/reviews/ARCH_REVIEW_2026-03-03.md`
- `docs/reviews/FORECAST_MODULE_REVIEW_2026-03-01.md`
- `docs/reviews/FORECAST_PIPELINE_REVIEW_2026-03-01.md`
- `docs/reviews/FORECAST_PIPELINE_REVIEW_2026-02-28.md`
- `docs/reviews/P2_P3_REVIEW_2026-03-01.md`

### D. Archive（旧规划/模板，非当前真源）
- `docs/archive/City_Tmax_Analysis_Skill_Planning.md`
- `docs/archive/LOOK_MD_TEMPLATE.md`

---

## 2) 维护分工（防混乱）
- **天气形势规则变更** → 改 `docs/core/SPECIAL_CASE_PLAYBOOK.md`
- **技术实现细节变更**（市场解析/数据精度/渲染）→ 改 `docs/core/TECHNICAL_IMPLEMENTATION_NOTES.md`
- **项目总览 / 当前能力边界变更** → 改 `docs/core/PROJECT_OVERVIEW.md`
- **输出口径变更** → 改 `docs/core/LOOK_OUTPUT_CONTRACT.md`
- **日志与归档治理变更** → 改 `docs/operations/LOG_ARCHIVE_POLICY.md`
- **探空实测接入/时效/代表性策略变更** → 改 `docs/operations/SOUNDING_OBS_ANALYSIS_PIPELINE.md`
- 所有重要改动统一记入 `docs/operations/LOOK_FIX_REGISTRY_2026-03.md`

---

## 3) 非产品文档（不作为规则真源）
以下文件属于会话人格/运行环境，不作为 weather 规则来源：
- `AGENTS.md`, `SOUL.md`, `USER.md`, `IDENTITY.md`, `TOOLS.md`, `BOOTSTRAP.md`, `HEARTBEAT.md`, `memory/*.md`
