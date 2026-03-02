# /look 特殊情形处理手册（集中归档）

Last updated: 2026-03-02

> 目标：把“容易漏/容易冲突”的规则放到一处，减少调用混乱。

## A. 峰值窗口与再创新高

### A1) 早段已出高点（early-day max）
- 触发：`phase=post` 且已存在 `observed_max_temp_c`
- 处理：
  - 区间上沿启用 `post-window realized-peak guard`
  - 默认抑制“再创新高”尾部，除非满足明确反超条件
- 反超条件（rebound_ok）：
  - 晴空/少云稳定（无 BKN/OVC/VV）
  - 无降水干扰
  - 温度斜率持续偏正（`t_cons` 强）
  - 与前高时间间隔仍合理（不是过久后硬冲）
- 代码锚点：`telegram_report_cli.py`（post-window guard）

### A2) post 阶段文案
- 目标：不只说“窗口后已定”，要回答“能不能反超前高”。
- 输出优先：
  1) 还差多少温度可反超（门槛）
  2) 当前条件是否支持（云/雨/斜率）
  3) 再给 1-2 条触发变量
- 代码锚点：`realtime_pipeline.py::select_realtime_triggers` + `telegram_report_cli.py` 合并逻辑

---

## B. 云层相关

### B1) 云层判定必须看全字段
- 数据源：`rawOb + clouds[] + cover`
- 规则：
  - 主云层级别使用“约束最强层”（不是第一层）
  - 云层变化比较用全层 token 集合（新增/消退/重排）
- 代码锚点：`metar_observation_block` 中 `_collect_cloud_pairs/_cloud_code/_cloud_change_text`

### B2) 低云持续压制
- 条件：当前 `BKN/OVC/VV` 且无开窗信号
- 处理：
  - 限制 center 上推
  - 限制 tail 上沿扩展
- 代码锚点：`telegram_report_cli.py`（persistent low-cloud guard）

---

## C. 降水与相态

### C1) 降水演变是一级驱动
- 分类：`new/intensify/weaken/end/steady/none`
- 影响：
  - `new/intensify`：上沿下压 + 不确定性上升
  - `end + BKN/OVC`：残余冷却持续，不立即暖反弹
- 代码锚点：`metar_observation_block` + `_signal_scores` + 区间修正段

---

## D. 斜率与量化观测（METAR quantized）

### D1) 站点整数温度台阶风险
- 条件：`metar_temp_quantized=true`
- 处理：
  - 临近窗口结束时，单次 +1°C 不视作持续加速
  - 启用 near-end cap
- 代码锚点：`telegram_report_cli.py`（Quantized-METAR near-end guard）

### D2) 午后平台期约束
- 条件：`near/in window + clear-sky stable + 斜率弱`
- 处理：
  - 启用 solar-decay/plateau cap，抑制乐观晚窗上冲
- 代码锚点：`telegram_report_cli.py`（Afternoon solar-decay + plateau gate）

---

## E. 冷暖平流场景

### E1) 冷平流 far-window 反弹上限
- 条件：`phase=far + 冷平流 + 北向来流`
- 处理：
  - 对远窗口快速反弹加上限，避免过暖偏差
- 代码锚点：`telegram_report_cli.py`（Far-window cold-advection sanity cap）

### E2) 暖平流不能自动推高
- 仅在“云量/斜率/降水”协同时放大上沿
- 否则暖平流仅保留为背景支撑，不直接给大尾部

---

## F. Polymarket 档位与时间

### F1) 本地日期选市场（非UTC日）
- 默认 `target_date`：站点 local date
- 影响：Polymarket event URL 按本地日期拼接
- 代码锚点：`render_report`（local-date default）

### F2) Fahrenheit/Celsius 桶解析
- 支持：`42-43F`、`44-45F`、`31c`、`30corbelow` 等
- 防误判：避免把 `...-2026-31c` 的年份段当温度范围
- 代码锚点：`_poly_parse_interval` / `_poly_label`

### F3) Tag 规则（forecast-first）
- `最有可能`：天气一致性优先，不是纯盘口报价最大
- `潜在Alpha`：必须满足最低天气一致性，非“便宜即 alpha”

---

## G. 输出文案一致性

### G1) 单条直出
- `/look` 与 `/lookhelp`：单条最终消息，禁止预告/占位

### G2) 关键证据去模板化
- 700/500hPa 仅在有判别力时显示
- 过滤泛化 boilerplate（避免每次都“看起来一样”）

### G3) METAR 展示增强
- `最新报`标题内显示上一报时间
- 风：给“上一报对比 + 转向/风速变化”
- 云：显示上一报完整层结 + 变化类型
- 天气现象：附中文释义（如 `-RA（小雨）`）
