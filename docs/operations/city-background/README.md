# City Background Docs

本目录同时保留两类文档：

- 人工研究归档：历史单城市调研、临时研究结论
- 自动生成 L2 画像：基于 archive 的 2022-2025 METAR/ISD 历史特征 + 站点级特殊备注

## 自动生成 L2 画像

- `docs/operations/city-background/profiles/CYYZ_toronto.md`
- `docs/operations/city-background/profiles/EDDM_munich.md`
- `docs/operations/city-background/profiles/EGLC_london.md`
- `docs/operations/city-background/profiles/KATL_atlanta.md`
- `docs/operations/city-background/profiles/KDAL_dallas.md`
- `docs/operations/city-background/profiles/KLGA_new_york.md`
- `docs/operations/city-background/profiles/KMIA_miami.md`
- `docs/operations/city-background/profiles/KORD_chicago.md`
- `docs/operations/city-background/profiles/KSEA_seattle.md`
- `docs/operations/city-background/profiles/LFPG_paris.md`
- `docs/operations/city-background/profiles/LTAC_ankara.md`
- `docs/operations/city-background/profiles/NZWN_wellington.md`
- `docs/operations/city-background/profiles/RKSI_seoul.md`
- `docs/operations/city-background/profiles/SAEZ_buenos_aires.md`
- `docs/operations/city-background/profiles/SBGR_sao_paulo.md`
- `docs/operations/city-background/profiles/VILK_lucknow.md`

## 既有人工研究

- `docs/operations/city-background/EDDM_Munich_Recent_METAR_Study.md`
- `docs/operations/city-background/LLBG_Tel_Aviv.md`
- `docs/operations/city-background/RJTT_Tokyo.md`
- `docs/operations/city-background/VILK_Lucknow.md`

## 更新方式

```bash
python3 scripts/sync_historical_reference.py
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=scripts python3 scripts/generate_city_background_docs.py
```
