# SOUNDING_OBS_ANALYSIS_PIPELINE.md

## Purpose
Standardize how `/look` uses **observed sounding** (not model profile) for intraday analysis and market discussion.

This is the merged baseline for:
- station selection & representativeness,
- time-validity and retrieval,
- profile QC,
- layer interpretation,
- fallback behavior and output logging.

---

## 1) Hard Operational Policy (highest priority)

### 1.1 Eligibility gate (all required)
Set `use_sounding_obs = true` only if all pass:
1. A designated sounding station is selected for the METAR airport.
2. Representativeness passes (`distance <= 150 km` and terrain-regime compatible).
3. Latest valid observed sounding is within **24h** of current UTC.
4. Retrieval + parsing + QC pass.

If any check fails:
- `use_sounding_obs = false`
- fallback to **model profile + local METAR real-time analysis**.

### 1.2 24h freshness is mandatory
- Observed sounding older than 24h: **do not use**.
- No valid sounding found within 24h: **do not use**.

### 1.3 Low representativeness = direct reject
Do not weaken-weight; directly disable observed sounding when:
- `distance > 150 km`, or
- not in same terrain regime / obvious geographic mismatch.

Typical mismatch examples:
- opposite sides of major lake/sea boundary,
- separated by dominant mountain barrier,
- coastal vs inland regime mismatch.

---

## 2) Station Selection Rule

### 2.1 Base rule
For each METAR airport:
- select the **nearest sounding station** first.

### 2.2 Representativeness filters (hard)
- Filter A: `distance <= 150 km`
- Filter B: same terrain regime (static/manual tag check)

Fail any filter => sounding obs disabled for that cycle.

### 2.3 Current manual overrides
- **Toronto (CYYZ)**: do **not** use Buffalo profile (lake opposite-side mismatch).
- **Seoul (RKSI)**: force station = **Incheon (47113)**.
  - If 47113 has no valid obs within 24h, sounding obs remains disabled.

---

## 3) Allowed Data Sources (Observed)
Priority order:
1. official national source (if available),
2. University of Wyoming Upper Air (UWyo),
3. mirrors only for cross-check (never sole truth source).

For current automation, UWyo FM35 endpoint is the default operational source.

---

## 4) Retrieval Method (UWyo)

Observed sounding URL template:

```text
https://weather.uwyo.edu/wsgi/sounding?datetime=YYYY-MM-DD%20HH:MM:SS&id=STATION_ID&src=FM35&type=TEXT:LIST
```

Inventory check template:

```text
https://weather.uwyo.edu/wsgi/sounding?datetime=YYYY-MM-DD%20HH:MM:SS&id=STATION_ID&src=FM35&type=INVENTORY
```

Recommended retrieval order (latest first):
1. nearest synoptic cycle near now (usually 00Z/12Z),
2. previous cycle,
3. continue fallback until either:
   - first valid record within 24h found, or
   - 24h window exceeded (then disable sounding obs).

---

## 5) Retrieval Checklist (must log)
Per run, keep reproducibility fields:
- `source_url`
- `station_id`
- `obs_time_utc`
- `retrieved_at_utc`
- `is_proxy_station` (bool)

And operational fields:
- `distance_km`
- `terrain_match`
- `obs_age_hours`
- `use_sounding_obs`
- `disable_reason` (if false)

---

## 6) Data Integrity QC (mandatory)

### 6.1 Structural completeness
- Profile should include meaningful low/mid troposphere (e.g., around 925/850/700 hPa).
- If only shallow fragment exists, mark partial/low-confidence.

### 6.2 Duplication/noise sanity
- Handle repeated near-level lines (common in some exports).
- De-duplicate / representative-by-pressure before summary.

### 6.3 Physical plausibility
- Pressure decreases with height.
- Remove obvious corrupted spikes before diagnostics.

If QC fails:
- mark profile **partial/low-confidence**,
- restrict conclusions to valid layers only.

---

## 7) Core Layer Reading (weather/market oriented)
Focus order:
- **Surface–925 hPa**: marine inflow, moist layer, cloud-base support
- **925–850 hPa**: cap / weak inversion / mixing gate
- **850–700 hPa**: dry intrusion and entrainment support

Key fields:
- T, Td, RH, wind direction/speed.

---

## 8) Derived Indicators (standard)
Use these in each sounding analysis:
1. low-level directional shear (sfc→925→850),
2. low-level speed shear,
3. bulk shear (prefer 0–1km / 0–3km where feasible),
4. low-level stability gate (around 900–850 hPa),
5. dry-over-wet structure test.

Optional:
- Gradient Richardson number (if vertical resolution supports it).

---

## 9) Integration with Forecast Workflow

When sounding obs is valid:
- low layers remain anchored by **latest METAR trend + model evolution**,
- observed sounding mainly constrains vertical structure (esp. mid-level environment).

When sounding obs is invalid:
- use **model profile + local METAR** only,
- report must state sounding-observation unavailable/disabled reason.

---

## 10) Output Template (chat)
Keep concise:
1. data-quality statement (complete/partial/proxy),
2. up to 3 layer findings,
3. 1-line actionable conclusion,
4. confidence tag (H/M/L + reason).

---

## 11) Hard Guardrails
- Never present model profile as observed sounding.
- Never hide proxy usage.
- Never use observed sounding older than 24h.
- Never use low-representativeness station via soft weighting (direct reject).
- Avoid certainty inflation when profile is partial.

---

## Change Log
- 2026-03-05: initial observed-sounding workflow draft.
- 2026-03-06: hard-rule update from group decisions (24h freshness, 150km limit, terrain mismatch reject, Toronto/Seoul overrides).
- 2026-03-06: merged with earlier pipeline sections (QC, layer diagnostics, indicators, output template) into one unified doc.
