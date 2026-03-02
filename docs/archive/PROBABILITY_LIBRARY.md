# Probability Library Layout (整理版)

## 目标
把权重来源整理为“分层配置 + 运行时覆盖”，减少硬编码与冲突。

## 配置来源优先级
1. `config/profiles/global.json`
2. `config/profiles/stations/<ICAO>.json`
3. `config/profiles/seasons/<key>.json`
4. `config/profiles/regimes/<key>.json`
5. `runtime/online_adjustments.json`

说明：
- `config/probability_weights.json` 保留为基础 fallback（`hours_to_peak`、通用 hardening）
- `scoring/hardening` 优先走 profiles 分层库

## 代码模块
- `scripts/probability_layer.py`：概率计算核心
- `scripts/profile_loader.py`：分层配置加载/深度合并

## 维护建议
- 站点特异修正统一写入 `stations/<ICAO>.json`
- 快速实验写入 `runtime/online_adjustments.json`（短期）
- 复盘确认后再回灌到长期 profile 文件
