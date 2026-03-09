# Skill 架构复盘（2026-03-03，历史文档）

## 结论摘要
- 当前主链路能运行，但仍存在“单文件过重 + 历史补丁叠加”的结构债务。
- 最突出的热点曾是 `scripts/telegram_report_cli.py`；本轮已继续拆分并明显瘦身。
- 本次新增拆分后，CLI 已收敛为“入口编排 + 最终封装”，重逻辑迁至服务层模块。

## 主要问题（按优先级）
1. **入口层仍需持续守护**
   - 虽已拆分，但后续迭代若继续在 CLI 叠加业务规则，仍会回归到“单文件过重”。
2. **逻辑边界混杂**
   - 运行期编排与具体业务规则（尤其 Polymarket 和 METAR 细节）耦合。
3. **历史兼容分支仍多**
   - 代码中存在多处 legacy/fallback 分支，后续需要按“保留必要回退、清理无效兼容”逐步收敛。
4. **工具函数重复/残留**
   - 存在重复工具函数与未使用函数（本次已清理一部分）。

## 本次已实施优化
- 新增 `scripts/polymarket_client.py`：
  - 集中维护 Polymarket 事件抓取、TTL 缓存、URL slug 解析与预取。
- 新增 `scripts/look_command.py`：
  - 集中维护 `/look` 命令解析与帮助文本。
- 新增 `scripts/station_catalog.py`：
  - 集中维护站点解析、站点时区、站点元信息、默认模型策略。
- 新增 `scripts/hourly_data_service.py`：
  - 集中维护小时预报抓取/回退/缓存、窗口识别与 post-window 衍生窗口选择。
- 新增 `scripts/metar_utils.py`：
  - 集中维护 METAR 抓取与观测量化区间工具函数。
- 新增 `scripts/metar_analysis_service.py`：
  - 承接 `metar_observation_block` 全量逻辑（METAR 诊断特征 + 实况分析文本）。
- 新增 `scripts/polymarket_render_service.py`：
  - 承接 Polymarket 档位解析、筛选与盘口区间渲染逻辑。
- 新增 `scripts/report_render_service.py`：
  - 承接 `choose_section_text` 与统一时差工具，负责主报告分段渲染。
- 新增 `scripts/report_peak_module.py`：
  - 承接“可能最高温区间”计算与尾部约束逻辑，减少 `report_render_service.py` 单文件复杂度。
- `scripts/telegram_report_cli.py`：
  - 改为调用 `look_command` / `station_catalog` / `hourly_data_service` / `metar_analysis_service` / `report_render_service`。
  - 仅保留入口编排、synoptic runner 调度、降级兜底与最终消息封装。
  - 清理未使用函数 `_haversine_km`。
- `scripts/synoptic_runner.py`：
  - 清理未使用函数 `_haversine_km` 与对应无用依赖。

## 下一步建议（建议按顺序）
1. 在 `report_render_service.py` 内继续细分“环流段渲染/峰值主带渲染/变量提示渲染”子函数，降低函数体复杂度。
2. 对 `metar_analysis_service.py` 和 `polymarket_render_service.py` 增补轻量回归样例（固定输入→固定文本片段），防止后续改动引入渲染漂移。
3. 建立模块级性能埋点（METAR 解析、盘口渲染、synoptic 调度），形成慢点定位闭环。
