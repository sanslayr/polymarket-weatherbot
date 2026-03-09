# Weatherbot Architecture Framework Review (2026-03-09)

## 结论

当前 weatherbot 已从“CLI/渲染层堆逻辑”的状态，进化到“provider -> decision -> analysis snapshot -> render”的主干架构，方向正确。

但它还没有完全进入“面向概率层和自动交易”的稳定形态。最主要的剩余问题不是功能缺失，而是：

1. 缺少正式 `canonical_raw_state`
2. 缺少独立 `posterior_feature_vector`
3. `peak_range_service.py` 已成为新的复杂度热点
4. render 层仍保留少量 fallback 推理

## 当前优点

- 3D provider 已支持 router，默认 `ECMWF Open Data`，`GFS` fallback
- `analysis_snapshot_service.py` 已建立结构化 handoff
- `synoptic_summary_service.py` 与 `peak_range_service.py` 已把部分推理从 render 层回收到分析层
- 3D object 已具备轻量 tracking，而不再只是静态单帧摘要
- 天气判断和市场展示基本保持单向解耦

## 仍需继续收口的点

### 1. Raw state 仍分散

hourly / synoptic / METAR / sounding 目前各自成链，缺少统一 raw contract。

建议：

- 增加 `canonical_raw_state`
- 只做标准化，不做结论翻译

### 2. Analysis snapshot 仍混有 presentation 痕迹

当前 snapshot 已很有价值，但仍携带 `headline / layer_summary / block` 这类给人看的字段。

建议：

- 新增 `posterior_feature_service.py`
- 输出纯定量、尽量正交的 feature vector

### 3. Peak analysis hotspot 转移

`report_peak_module.py` 已被削薄，但复杂度已转移到 `peak_range_service.py`。

建议下一步拆成：

- `peak_posterior_service`
- `peak_text_renderer`

### 4. 历史训练不应直接压进 runtime repo

历史训练、ERA5、回测数据应保留在独立 research/archive repo，通过 artifact 对 runtime repo 供给：

- station priors
- analog index
- regime embeddings
- posterior weights / calibration tables

## 推荐目标链路

1. `canonical_raw_state`
2. `physical_feature_layer`
3. `posterior_layer`
4. `presentation_layer`
5. `market_execution_layer`

这意味着：

- 报告层只是输出模块
- 天气分析主链不能围着文案设计
- 自动交易层只能消费 posterior，不应反向污染天气判断
