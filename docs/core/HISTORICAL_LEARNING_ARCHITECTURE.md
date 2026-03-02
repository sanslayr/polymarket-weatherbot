# /look 历史学习与实况更新架构（草案）

Last updated: 2026-03-02

## 1) 目标
- 让 Tmax 判断参数可从历史数据学习，而不是长期手工拍脑袋。
- 支持“离线回放学习 + 在线实况小步更新”。
- 保持可解释：每个参数都能追溯到业务含义。

## 2) 分层架构

### A. 参数层（已落地）
- 文件：`config/tmax_learning_params.json`
- 加载器：`scripts/param_store.py`
- 当前可调：
  - 多层云覆盖映射与层权重
  - wx 透过率映射
  - rounded-top 阈值（斜率/加速度/太阳几何/辐射恢复）

### B. 特征层（已落地核心）
- 来自 METAR 的可学习特征：
  - `cloud_effective_cover` / `cloud_effective_cover_smooth`
  - `cloud_transmittance` / `wx_transmittance`
  - `radiation_eff` / `radiation_eff_smooth` / `radiation_eff_trend_1step`
  - `temp_trend_smooth_c` / `temp_accel_2step_c`

### C. 学习层（下一步）
- 输入：历史日期样本（站点、预报、METAR 序列、实况 Tmax）
- 输出：参数建议（覆盖映射、阈值、权重）
- 方式（建议）：
  1) 先做约束网格搜索/Bayesian（稳健、可解释）
  2) 再做站点族分组校准（内陆/沿海/高纬）
  3) 采用滚动时间验证，避免时序泄漏

### D. 案例层（下一步）
- 建立典型 case 库：
  - 晴空圆弧顶
  - 先震荡后末段冲高
  - 低云压制
  - 雨后快速解封
- 在线分析时返回“匹配案例标签 + 相似度”，辅助解释。

## 3) 推荐训练目标
- 主目标：Tmax MAE / bias
- 次目标：
  - 上沿过高率（over-warm tail rate）
  - 末段冲高漏报率（late-surge miss rate）
- 约束：
  - 参数变化幅度限制
  - 物理一致性（不违背已观测最高温约束）

## 4) 在线更新策略（建议）
- 不做每报实时重训；采用“日级/周级批量更新”。
- 更新流程：
  1) 回放最近 N 天
  2) 生成候选参数
  3) 与当前参数 A/B 离线对比
  4) 通过阈值后再发布到 `config/tmax_learning_params.json`

## 5) 维护约束
- 规则解释优先，黑箱模型谨慎引入。
- 参数变更必须写入 `docs/operations/LOOK_FIX_REGISTRY_2026-03.md`。
- 大变更必须保留“回滚参数包”。
