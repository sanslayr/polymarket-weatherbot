# Telegram Commands (current)

Last updated: 2026-03-02

## 1) OpenClaw native
- `/new`
- `/reset`
- `/restart`（若实例配置开启）

## 2) Weather skill command

### `/look`
生成城市/机场最高温分析（单条最终报告直出）。

#### 用法
- `/look <city|icao|alias>`
- `/look <city|icao|alias> <date>`（可选）

#### 日期参数
- 支持：`YYYY-MM-DD` 或 `YYYYMMDD`
- 若不传日期：默认取**站点本地日期**（不是 UTC 日期）

#### 示例
- `/look nyc`
- `/look seo`
- `/look bue 2026-03-02`

#### 说明
- 输出固定为统一主报告（不再对外提供 `mode/section/model/provider` 参数）。
- 美国站点（ICAO `K*`）最高温区间默认显示华氏；其余站点显示摄氏。
- Polymarket 市场按站点 local date 选取当日事件。

### `/lookhelp`
返回 `/look` 使用说明。

---

## 3) Alias 提示（常用）
- `nyc` -> New York
- `seo/sel` -> Seoul
- `sea` -> Seattle
- `bue/ba` -> Buenos Aires
- `ank` -> Ankara
- `lon` -> London
- `par` -> Paris
- `atl` -> Atlanta
- `mia` -> Miami
- `dal` -> Dallas
- `chi` -> Chicago
- `tor` -> Toronto
- `wel` -> Wellington
