# Telegram Commands

Last updated: 2026-03-09

## 1) OpenClaw native

- `/new`
- `/reset`
- `/restart`（若实例配置开启）

## 2) Weather skill

### `/look`

生成站点/城市最高温分析报告。

#### 用法

- `/look <city|icao|alias>`
- `/look <city|icao|alias> <date>`

#### 日期参数

- 支持：`YYYY-MM-DD` 或 `YYYYMMDD`
- 若不传日期：默认取站点本地日期

#### 示例

- `/look ank`
- `/look London`
- `/look seo 2026-03-09`

#### 当前支持站点

- Ankara (LTAC)
- Atlanta (KATL)
- Buenos Aires (SAEZ)
- Chicago (KORD)
- Dallas (KDAL)
- London (EGLC)
- Lucknow (VILK)
- Miami (KMIA)
- Munich (EDDM)
- New York (KLGA)
- Paris (LFPG)
- Sao Paulo (SBGR)
- Seattle (KSEA)
- Seoul (RKSI)
- Toronto (CYYZ)
- Wellington (NZWN)

#### 常用别名

- `ank` -> Ankara
- `lon` -> London
- `par` -> Paris
- `nyc` -> New York
- `sea` -> Seattle
- `tor` -> Toronto
- `seo` / `sel` -> Seoul
- `ba` / `bue` -> Buenos Aires
- `sao` -> Sao Paulo
- `lko` -> Lucknow
- `mun` -> Munich
- `mia` -> Miami
- `atl` -> Atlanta
- `dal` -> Dallas
- `chi` -> Chicago
- `wel` -> Wellington

#### 说明

- 对外接口只要求站点和日期；内部 provider/model 选路不作为用户命令参数。
- 美国站点（ICAO `K*`）温度展示默认用华氏，其余站点默认用摄氏。
- Polymarket 事件按站点 local date 选择。
