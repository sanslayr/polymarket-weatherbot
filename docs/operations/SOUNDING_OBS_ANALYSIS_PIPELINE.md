# SOUNDING_OBS_ANALYSIS_PIPELINE.md

## Purpose
Standardize how `/look` uses **observed sounding** (not model profile) in intraday analysis.

This document focuses on: station selection, time-validity, retrieval, and hard fallback behavior.

---

## 1) Hard Operational Policy (2026-03-06)

### 1.1 Use observed sounding only when all conditions pass
`use_sounding_obs = true` only if:
1. A designated sounding station is selected for the METAR airport.
2. Station representativeness passes (distance/terrain checks below).
3. Latest valid observed sounding exists **within 24 hours** of current UTC.
4. Parsing/QC succeeds.

If any check fails:
- `use_sounding_obs = false`
- Fallback to **model profile + local METAR real-time analysis**.

### 1.2 24h freshness is mandatory
- Observed sounding older than 24h: **do not use**.
- If retrieval gets no 24h record: **do not use**.

### 1.3 Low representativeness = direct reject
Do not use observed sounding when:
- Distance from METAR airport to sounding station `> 150 km`, or
- Not in same terrain regime / obvious geographic mismatch.

Examples of geographic mismatch:
- Opposite sides of a major lake/sea-influenced boundary.
- Separated by dominant mountain barrier.
- Coastal vs inland regime mismatch.

---

## 2) Station Selection Rule

### 2.1 Base rule
For each METAR airport:
- Select the **nearest sounding station** first.

### 2.2 Representativeness hard filters
- Filter A: `distance <= 150 km`
- Filter B: same terrain regime (manual/static tag check)

Fail any filter => sounding obs disabled for that cycle.

### 2.3 Current manual overrides from group discussion
- **Toronto (CYYZ)**: do **not** use Buffalo profile (opposite lake side regime mismatch).
- **Seoul (RKSI)**: force station = **Incheon (47113)**.
  - If 47113 has no valid obs within 24h, sounding obs remains disabled.

---

## 3) Retrieval Method (UWyo)

Observed sounding URL template:

```text
https://weather.uwyo.edu/wsgi/sounding?datetime=YYYY-MM-DD%20HH:MM:SS&id=STATION_ID&src=FM35&type=TEXT:LIST
```

Inventory check template:

```text
https://weather.uwyo.edu/wsgi/sounding?datetime=YYYY-MM-DD%20HH:MM:SS&id=STATION_ID&src=FM35&type=INVENTORY
```

Recommended retrieval order (latest first):
1. nearest synoptic cycle near now (usually 00Z/12Z)
2. previous cycle
3. continue fallback until either:
   - first valid record within 24h found, or
   - 24h window exceeded (then disable sounding obs)

---

## 4) Integration with Forecast Workflow

When sounding obs is valid:
- Near-surface/low layer still anchored by **latest METAR trend + model evolution**.
- Sounding obs is mainly used to constrain vertical structure (especially mid-level environment).

When sounding obs is invalid:
- Use **model profile + local METAR** only.
- Report should clearly state sounding-observation unavailable/disabled reason.

---

## 5) Output Requirements
Every run should log at least:
- `sounding_station_id`
- `distance_km`
- `terrain_match` (true/false)
- `obs_time_utc`
- `obs_age_hours`
- `use_sounding_obs` (true/false)
- `disable_reason` (if false)
- `source_url`

---

## Change Log
- 2026-03-05: Initial version created from observed-sounding workflow discussion.
- 2026-03-06: Added hard rules from group decisions:
  - 24h freshness mandatory.
  - Distance >150 km or terrain mismatch => disable sounding obs.
  - Toronto/Buffalo mismatch rejection.
  - Seoul forced to Incheon station rule (with 24h data requirement).
