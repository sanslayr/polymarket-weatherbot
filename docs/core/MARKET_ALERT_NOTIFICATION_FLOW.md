# Market Alert Notification Flow

Last updated: 2026-03-12

目标：把“盘口异动推送”这条链路的触发、去重、版式和运行边界写成单一真源，避免信息分散在 plan / worker / formatter / 临时修复记录里。

## 1) 边界

- 这条链路独立于 `/look`
- `/look` 负责 request/response 报告
- `market_alert_worker.py` 负责 routine METAR 报时附近的主动监控和推送
- worker 也会对 `recent_speci_2h / speci_active / speci_likely` 站点进入 resident block
- 告警文本必须保持 `market-implied` / `盘口归零异动` 语气，不能表述成官方实况已确认

## 2) 运行链路

运行顺序：

1. `market_alert_worker.py`
2. `market_alert_scheduler.py`
3. `market_monitor_service.py`
4. `market_stream_service.py`
5. `market_implied_weather_signal.py`
6. `market_signal_alert_service.py`
7. `market_alert_delivery_service.py`
8. `telegram_notifier.py`

职责拆分：

- `market_alert_worker.py`
  - 薄 orchestrator
  - 管理线程池、窗口执行日志和 scheduler/delivery handoff
- `market_alert_scheduler.py`
  - 选择站点
  - 估算 routine METAR cadence
  - 打开 report-time monitoring window 或 resident monitoring block
  - 生成 event URL / schedule drift 上下文
- `market_alert_runtime_state.py`
  - 管理 singleton lock、state 文件、pid 文件、worker log 路径
- `market_monitor_service.py`
  - 组装 catalog + subscription plan + state snapshot
  - 在事件窗口内保留 pre-report baseline
  - resident block 不继承旧 baseline
  - 回到 routine 时再重新建立 pre-report baseline
  - 调用 signal inference
  - `cycle` / `event_window` 共用同一套 subscription/signal helper
- `market_implied_weather_signal.py`
  - 从 bucket repricing 推断 `signal_type`
- `market_signal_alert_service.py`
  - 只负责 Telegram 文本渲染
- `market_alert_delivery_service.py`
  - 负责 dedupe key、worker cooldown 和 Telegram delivery report

## 3) 触发窗口

- 基础模式是在 routine METAR report window 内主动监控
- routine 事件窗口默认从 report timestamp 开始，最长监控约 `245s`
- 为避免把预热期波动误判成正式报后异动：
  - pre-report 阶段持续更新 baseline
  - post-report `+30s` 后才进入正式 signal 判定窗口
- 对下列站点追加 resident mode：
  - `recent_speci_2h == true`
  - 或 `speci_active == true`
  - 或 `speci_likely == true`
- resident mode 以连续 `240s` block 运行
- resident block 不再依赖 routine report `+30s` gate
- 进入 resident mode 时丢弃 inherited baseline；回到 routine 后再用新的 pre-report snapshot 建 baseline
- resident block 如果会撞上下一次 routine report，会先截断并让 routine window 接管

## 4) Signal 类型

当前主 signal：

- `report_temp_scan_floor_stop`
  - 低档连续被打死，第一档仍有有效报价的 bucket 成为当前隐含最新报
- `report_temp_top_bucket_lock_in`
  - 所有关键低档失效，只剩最高 `or higher` 顶档维持有效报价
- `report_temp_lower_bound_jump`
  - 单档快速归零，市场大概率已按“最新报高于该档”交易

细节判据以 [MARKET_IMPLIED_REPORT_SIGNAL_PLAN.md](/home/ubuntu/.openclaw/workspace/skills/polymarket-weatherbot/docs/core/MARKET_IMPLIED_REPORT_SIGNAL_PLAN.md) 和实现 [market_implied_weather_signal.py](/home/ubuntu/.openclaw/workspace/skills/polymarket-weatherbot/scripts/market_implied_weather_signal.py) 为准。

## 5) 去重与推送

- duplicate suppression key:
  - `station + signal_type + event_url + bucket`
- if an event day has no tradable active market, that station/event pair is skipped for the rest of the local event day
- worker 级冷却：
  - `MARKET_ALERT_COOLDOWN_SECONDS`
  - 默认 `900s`
- Telegram delivery:
  - 默认优先 direct chat，再到 group targets
  - `disable_web_page_preview=False`

## 6) 输出契约

当前告警文本包含：

- 标题：`盘口归零异动 | City @ date`
- 推测最新报最高温
- 已记录 METAR 最高温
- 盘口观察
- 当前市场盘口价格 ladder
- 异动时间
- disclaimer
- Polymarket 市场链接

版式约束：

- 多个 collapsed buckets 合并成一句
- 除标题和 ladder heading 外，正文信息行统一使用圆点 bullet
- ladder heading 使用 `*当前市场盘口价格：*`
- ladder heading 前保留一个空行
- disclaimer 放在市场链接前
- disclaimer 不带 bullet
- 市场链接不带 bullet
- 链接开启 preview

## 7) 反耦合规则

- 不允许把 market alert trigger 逻辑塞回 `/look` render path
- 不允许把 Telegram 文案判断回塞进 `market_implied_weather_signal.py`
- 不允许把站点调度 / 去重逻辑混入 formatter
- 不允许让 worker 文档替代 signal contract 文档

## 8) 相关文档

- 设计判据：
  - [MARKET_IMPLIED_REPORT_SIGNAL_PLAN.md](/home/ubuntu/.openclaw/workspace/skills/polymarket-weatherbot/docs/core/MARKET_IMPLIED_REPORT_SIGNAL_PLAN.md)
- 运行说明：
  - [MARKET_ALERT_WORKER.md](/home/ubuntu/.openclaw/workspace/skills/polymarket-weatherbot/docs/operations/MARKET_ALERT_WORKER.md)
- 市场总架构：
  - [MARKET_ARCHITECTURE.md](/home/ubuntu/.openclaw/workspace/skills/polymarket-weatherbot/docs/core/MARKET_ARCHITECTURE.md)
