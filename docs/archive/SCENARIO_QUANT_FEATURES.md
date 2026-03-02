# 情景概率判定：量化特征与向量化（/look）

## 目标
将“环流/探空/实况”转为可计算特征，用于情景权重（A基线/B上冲/C压制）估计。

## 一、特征分组（建议）

### 1) 时间与阶段
- `hours_to_peak_start`：距峰值窗口开始小时数
- `hours_to_peak_center`：距峰值窗口中点小时数
- `is_pre_peak_gt4h` / `is_pre_peak_3_4h` / `is_pre_peak_lt3h`

### 2) 热力与偏差（METAR + 模式）
- `temp_bias_c`：同小时温度偏差（METAR - 模式）
- `temp_bias_trend_2obs`：最近两报偏差变化斜率
- `dewpoint_spread_c`：T-Td（干湿背景）
- `dewpoint_trend_2obs`

### 3) 云与辐射约束
- `low_cloud_pct_forecast`（模式低云）
- `cloud_base_ft_latest`（METAR云底）
- `cloud_regime_code`（CAVOK/CLR/FEW/SCT/BKN/OVC -> 序数编码）
- `cloud_change_2obs`（开窗/回补）

### 4) 风场与平流
- `wdir_deg_latest`, `wspd_kt_latest`
- `wind_regime_bin`（北向/南向/西南暖平流等 one-hot）
- `t850_c`, `w850_kmh`, `wd850_deg`
- `cold_adv_reach_score`, `warm_adv_reach_score`

### 5) 动力与压场
- `pmsl_hpa`, `pmsl_trend_2obs`
- `regime_onehot`（槽脊并存/槽前暖区/槽后冷区/阻塞候选）
- `vorticity_proxy_500`（若可得）

### 6) 探空结构（sounding regime）
- `inv_strength_idx`（逆温强度）
- `mixing_depth_idx`（混合层深度）
- `moist_process_idx`（湿过程约束）
- `conv_trigger_idx`（对流触发概率）

### 7) 盘口结构（用于“低估/彩票”）
- `market_mid_price_by_bin[]`
- `bid_ask_spread_by_bin[]`
- `tail_liquidity_score`
- `market_entropy`（筹码集中度）

### 8) 站点校正项（慢变量，不单独升主轴）
- `orography_offset`（地形/下坡增温或冷空气滞留修正）
- `uhi_offset`（城市热岛修正）
- `soil_moisture_offset`（地表湿度/蒸发冷却修正）
- `agency_bias_offset`（本地气象局/官方报文风格偏差修正）
- `agency_reliability_score`（官方报告时效与历史命中稳定度）

---

## 二、向量化策略（多维空间）

构造统一向量：
`x = [时间特征, 热力偏差, 云特征, 风平流, 压场动力, 探空结构, 盘口结构, 站点校正项]`

处理规则：
1. 连续特征：标准化（z-score）
2. 类别特征：one-hot（如 regime、云型）
3. 缺失值：
   - 实况缺失用最近可用值 + `is_missing_*` 指示位
   - 模式缺失回退到 station climatology
4. 时间衰减：最近观测权重更高（EWMA）

---

## 三、情景权重估计（可解释优先）

### 方案A（当前推荐）规则打分 + softmax
- 对 A/B/C 分别定义打分函数：
  - `score_A(x)`: 基线维持
  - `score_B(x)`: 上冲（开窗/暖平流/混合抬升）
  - `score_C(x)`: 压制（低云下压/冷平流/锋面提前）
- 先计算主轴分数，再叠加校正项：
  - `score_i = score_i(main_axes) + score_i(correction_offsets)`
- 官方报告修正规则：
  - 初始阶段：`agency_bias_offset ~ N(0, σ_settle^2)`（零均值、非零波动；默认信任但保留结算层不确定性）
  - 建议初始 `σ_settle`：0.15°C（常规）/ 0.25°C（高波动天气）
  - 若 `agency_reliability_score` 高且当前报文与实况一致：收窄 `σ_settle`
  - 若历史显示该机构在该类天气“偏激进/偏迟缓”：通过 `agency_bias_offset` 均值偏移进行修正
- 权重：`w_i = softmax(score_i)`
- 优点：可解释、可人工修正

### 方案B（后续升级）监督学习
- 用历史样本拟合 `P(scene|x)`：
  - 逻辑回归 / GBDT / XGBoost
- 再做概率校准（Platt/Isotonic）
- 结合规则作安全约束（避免模型漂移）

---

## 四、档位概率映射
给定各情景下 Tmax 分布 `p(T|scene_i)`，混合为：
`p(T) = Σ w_i * p(T|scene_i)`

盘口档位概率：
`P(bin_j) = ∫_{L_j}^{U_j} p(T) dT`

输出：
- 最可能区间（P25-P75）
- 偏高路径（P75-P95）
- 偏低路径（P05-P25）
- 低估彩票盘口：`model_prob - implied_prob > threshold`

---

## 五、实践建议（L1.5 快速迭代版）
1. 先跑规则版 + 情景混合（L1）
2. 叠加三道可靠性护栏（L1.5）：
   - 概率校准（每日/每周）
   - 不确定性下限（避免过窄分布）
   - 回测告警（连续偏差触发规则降权）
3. 累积 30-60 天样本后做回测
4. 评估指标：Brier / LogLoss / 档位命中率 / 尾部召回
5. 再进入学习版灰度

## 六、聊天反馈驱动修正（强制支持）
- 支持通过日常聊天反馈直接修正：
  - 规则权重（例如某站点风向触发过敏/迟钝）
  - 情景阈值（云窗、风速、偏差门槛）
  - 变量关注文案与风险排序
- 每次修正必须：
  1) 写入 skill 文档（或对应配置）
  2) 记录变更原因（反馈触发）
  3) 在后续报告中按新规则执行

## 七、低匹配情形处理
- 当特征向量与已知路径原型相似度低于阈值时：
  - 不强行映射到既有路径
  - 报告中直接输出“当前未充分建模情形”
  - 列出关键触发证据（云/风/压场/实况偏差）
  - 说明该情形超出当前概率计算适用范围，整体不确定性上调
