# Agent 更新约束（架构护栏）

Last updated: 2026-03-09

目标：避免后续迭代再次把补丁集中灌入 `telegram_report_cli.py`，导致新旧堆砌/职责混杂。

## 1) 分层职责（硬约束）
- `scripts/telegram_report_cli.py`
  - 仅负责：`/look` 命令入口、编排调用、最终文本拼装。
  - 不新增：复杂业务判定算法、外部数据抓取细节、市场解析规则实现。
- `scripts/forecast_pipeline.py` / `scripts/synoptic_runner.py`
  - 仅负责环流/诊断决策流水线。
- `scripts/station_catalog.py`
  - 站点解析、站点元信息、站点默认模型/时区策略。
- `scripts/look_command.py`
  - 命令文本解析与帮助文本。
- `scripts/polymarket_client.py`
  - Polymarket API 请求、缓存、预取、slug 解析。
- `scripts/polymarket_render_service.py`
  - Polymarket 温度档位解析、盘口筛选、博弈区间渲染。
- `scripts/market_label_policy.py`
  - 盘口标签门控（`👍最有可能` / `😇潜在Alpha`）与阈值策略。
- `scripts/hourly_data_service.py`
  - 小时预报抓取/回退/缓存、窗口识别与 post-window 衍生窗口选择。
- `scripts/metar_utils.py`
  - METAR 抓取与通用量化区间工具（`fetch_metar_24h`、observed-max interval 口径）。
- `scripts/metar_analysis_service.py`
  - METAR 实况诊断特征提取与“实况分析”文本渲染（温度/云量/SPECI/量化区间等）。
- `scripts/analysis_snapshot_service.py`
  - 结构化分析快照组装，作为 render 的主输入。
- `scripts/synoptic_summary_service.py`
  - 结构化环流摘要，不应再由渲染层重算。
- `scripts/report_render_service.py`
  - `/look` 主体分段渲染（不承载核心推理）。
- `scripts/peak_range_service.py`
  - 最高温主带/尾部区间分析与文本块渲染。

## 2) 变更路由规则（必须遵守）
- 改命令解析/参数兼容 → 改 `look_command.py`
- 改站点匹配/站点特征/默认模型策略 → 改 `station_catalog.py`
- 改市场请求/缓存/超时策略 → 改 `polymarket_client.py`
- 改盘口温度档位解析/展示策略 → 改 `polymarket_render_service.py`
- 改盘口标签门控与阈值策略 → 改 `market_label_policy.py`
- 改小时预报抓取/回退/缓存/窗口识别 → 改 `hourly_data_service.py`
- 改 METAR 抓取与观测量化区间口径工具 → 改 `metar_utils.py`
- 改 METAR 实况诊断特征/诊断文本 → 改 `metar_analysis_service.py`
- 改 analysis snapshot contract / 字段归并 → 改 `analysis_snapshot_service.py`
- 改环流摘要结构化生成 → 改 `synoptic_summary_service.py`
- 改报告分段渲染策略（非数据抓取）→ 改 `report_render_service.py`
- 改最高温区间分析/历史参考融合/尾注约束 → 改 `peak_range_service.py`
- 改环流锚点/覆盖率/降级策略 → 改 `forecast_pipeline.py` / `synoptic_runner.py`
- 只有“跨模块编排与最终文案”才允许改 `telegram_report_cli.py`

## 3) PR/提交自检清单
- 是否把业务细节塞进了 `telegram_report_cli.py`？
- 是否出现同一工具函数在多个文件重复实现？
- 是否新增了“临时补丁式分支”但未沉淀到对应模块？
- 是否更新 `docs/core/ARCHITECTURE.md` 或本文件（当职责边界变化时）？

## 4) 触发回退条件
- 若单次改动让 `telegram_report_cli.py` 净增 > 120 行，且新增为业务规则逻辑，视为违背分层，应先拆模块再合并。

## 5) 文件体积与函数体积上限（新增）
- 目标：避免“单文件继续膨胀”导致后续迭代困难。
- 建议上限（软红线）：
  - `scripts/telegram_report_cli.py`：≤ 900 行
  - `scripts/report_render_service.py`：≤ 1800 行
  - `scripts/peak_range_service.py`：≤ 2000 行
  - `scripts/metar_analysis_service.py`：≤ 1400 行
  - `scripts/polymarket_render_service.py`：≤ 900 行
- 函数体积（软红线）：
  - 单函数 > 400 行时，必须拆为私有 helper（同文件或同层模块）。
  - 单函数新增 > 120 行时，PR 说明里必须给出“为何不拆”的理由。
- 超限处理：
  - 优先做“无行为变更重构”（extract helper / split module）。
  - 重构后再叠加新业务改动，禁止一并混入大补丁。
