# /look 历史数据分析工作草案（v0）

Last updated: 2026-03-02
Status: Draft（先设计，后实施）

## 1) 目标

围绕 Tmax 判断链路，建立可持续的数据学习闭环：
1. 用历史数据量化当前规则的偏差结构（尤其 warm bias）
2. 用回放方式验证参数调整效果（不影响线上）
3. 建立“典型案例库”，给在线判断提供可解释参照

---

## 2) 本阶段范围（Phase-1）

### In scope
- 历史回放（METAR + 当时预报快照）
- 参数评估（`config/tmax_learning_params.json`）
- 指标体系（MAE/bias + 高估率 + 末段冲高漏报）
- 典型案例标签化（圆弧顶、末段冲高、低云压制、雨后解封）

### Out of scope（先不做）
- 端到端黑箱模型替换规则引擎
- 线上自动实时重训
- 大规模跨源再分析资料融合（ERA5 全量回灌）

---

## 3) 数据需求与最小样本

## 3.1 必要字段
- 站点维度：ICAO、lat/lon、时区、地形标签
- METAR 序列：温度、露点、风、云层多层、wx、报文时间
- 预报序列：小时温度、云量、850风场、3D/synoptic诊断输出
- 标签：当日实测 Tmax（Local day）

## 3.2 最小样本建议
- 每站至少 60~90 天（覆盖晴天/阴天/降水/过渡季）
- 首批站点：`ank / par / nyc / lon`

---

## 4) 回放框架设计

## 4.1 时间切片
- 以 Local day 为单位
- 回放时按实际报文时间推进（模拟“当时可见信息”）

## 4.2 输出记录
每个回放时刻保存：
- 当时预测区间（lo/hi/core_lo/core_hi）
- 当时关键特征（trend、bias、radiation_eff、solar_slope_next）
- 最终实测 Tmax（用于离线打分）

## 4.3 存储建议
- `runtime/replay/YYYY-MM-DD/<station>.jsonl`
- 每行一时刻，便于后续聚合

---

## 5) 指标体系（先统一口径）

## 5.1 主指标
- Tmax 点误差：`MAE`
- 系统偏差：`Bias = mean(pred_center - obs_tmax)`

## 5.2 业务指标
- Warm-tail overrate：预测上沿超过实测过多的比例
- Rounded-top over-forecast rate：圆弧顶场景下的高估率
- Late-surge miss rate：末段冲高漏报率
- Coverage consistency：`obs_tmax` 落入主带/展示带的比例

## 5.3 分场景评估
按场景拆分指标：
- clear-sky rounded-top
- pre-peak oscillation then surge
- persistent low cloud
- precip residual cooling

---

## 6) 参数学习策略（稳健优先）

## 6.1 参数组
- 云量映射：`cloud_cover_map`
- 云底权重：`cloud_base_weight`
- 分层衰减：`layer_gamma`
- wx透过率：`wx_transmittance`
- 圆弧顶阈值：`rounded_top.*`

## 6.2 优化方式（建议顺序）
1. 约束网格搜索（先粗后细）
2. Bayesian/SMBO（在合理边界内寻优）
3. 站点族分组参数（内陆/沿海/高纬）

## 6.3 约束条件
- 参数改动幅度限制（防抖）
- 物理一致性约束（不能破坏 obs_max 下界规则）
- 不得显著恶化 late-surge miss

---

## 7) 典型案例库（Case Library）

## 7.1 结构草案
每个 case 包含：
- case_id / station / date
- 案例类型（可多标签）
- 关键时间点（平台、冲高、锁高）
- 关键特征序列（trend/bias/radiation_eff/cloud layers）
- 当时模型输出与实测对比
- 复盘结论与规则建议

## 7.2 初始类型
- `clear_rounded_top`
- `late_surge_after_oscillation`
- `low_cloud_capping`
- `post_precip_unlock`

---

## 8) 执行里程碑（建议）

### M1（短期，1-2天）
- [ ] 回放数据结构定稿（jsonl schema）
- [ ] 指标计算脚本雏形

### M2（中期，2-4天）
- [ ] 首批站点样本跑通（ank/par/nyc/lon）
- [ ] 输出 baseline 报告（当前参数性能）

### M3（中期，3-5天）
- [ ] 参数搜索 + A/B 离线对比
- [ ] 形成首版“建议参数包”

### M4（持续）
- [ ] 典型案例库累计与标签修正
- [ ] 周期性复盘（周报）

---

## 9) 风险与防护

- 风险：过拟合单站点天气型
  - 防护：滚动时序验证 + 多站点联合评分
- 风险：压低 warm bias 时误伤末段冲高
  - 防护：把 `late_surge miss` 设为硬约束
- 风险：参数频繁变动导致行为漂移
  - 防护：参数发布节奏改为“日级/周级批量”

---

## 10) 产出物清单（计划）

- 回放数据：`runtime/replay/*.jsonl`
- 指标汇总：`docs/reviews/HISTORICAL_BACKTEST_*.md`
- 参数建议：`config/tmax_learning_params.candidate.json`
- 案例库：`docs/reviews/cases/*.md`

---

## 11) 备注

本草案用于统一后续推进方向。实施时优先保证：
1) 可解释
2) 可回滚
3) 指标可复现
