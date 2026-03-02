# City Tmax Analysis Skill -- Project Planning Document

**Version:** 1.0\
**Generated:** 2026-02-26T06:02:51.295095 UTC

------------------------------------------------------------------------

# 1. Project Objective

Design and implement a modular, fast-response weather analysis skill
focused on:

-   Urban / airport-based daily maximum temperature (Tmax) analysis
-   Synoptic background recognition
-   Sounding-based thermal structure interpretation
-   Real-time METAR adjustment
-   Open-Meteo forecast integration
-   Fast follow-up updates reflecting observed changes

The system must:

-   Respond quickly (sub-second for summary queries)
-   Support module-specific deep dives
-   Update dynamically as real-time observations evolve
-   Be extensible toward archive and regime classification

------------------------------------------------------------------------

# 2. Core Design Philosophy

## 2.1 Lightweight but Accurate

Avoid heavy dynamic diagnostics (e.g., Q-vector, PV).\
Focus on:

-   Major pressure systems
-   Trend detection
-   Mixing depth potential
-   Synoptic regime transitions

## 2.2 Cached Synoptic Engine

Heavy computations (GRIB parsing, center detection) must run in
background jobs.

User queries should:

-   Read cached synoptic state
-   Pull latest METAR
-   Apply light adjustments
-   Generate narrative

------------------------------------------------------------------------

# 3. System Architecture

CityTmaxEngine ├── SynopticModule ├── SoundingModule ├── ForecastModule
├── MetarModule ├── AdjustmentModule └── NarrativeModule

------------------------------------------------------------------------

# 4. Module Specifications

## 4.1 SynopticModule

Purpose: Identify primary large-scale circulation systems and trends.

Inputs: - MSLP grid (GRIB) - 500hPa height (optional but recommended)

Outputs: - Primary low/high center - Bearing & distance from station -
Movement trend (approaching / departing) - Intensity trend (deepening /
filling) - Pressure gradient classification - Regime classification
(e.g., post-trough NW flow)

Update Frequency: - Every 3 hours

Lightweight Detection Rules: - Window radius \~1000 km - Identify local
min/max of MSLP - Track strongest center only - Compare 24h pressure
changes

------------------------------------------------------------------------

## 4.2 SoundingModule

Purpose: Assess Tmax thermal structure.

Sounding Selection Rule: - Determine Tmax hour via Open-Meteo hourly
forecast - Select nearest model time (3h or 6h resolution)

Key Parameters (minimal set): - Low-level lapse rate - Inversion
presence - 850hPa temperature - Mixing depth estimate - LCL height
(cloud sensitivity proxy)

Modes: - summary → short thermal verdict - deep → vertical structure
breakdown

Update Frequency: - Every 3 hours (model run dependent)

------------------------------------------------------------------------

## 4.3 ForecastModule (Open-Meteo)

Purpose: - Determine predicted Tmax hour - Provide baseline Tmax value -
Provide hourly temperature trend

Update Frequency: - Hourly

------------------------------------------------------------------------

## 4.4 MetarModule

Purpose: - Real-time observation ingestion - Detect deviations from
expected evolution

Inputs: - METAR temperature - Wind direction/speed - QNH - Cloud cover

Adjustment Triggers: - Wind shift \> 90° - Pressure trend deviation -
Temperature rising faster/slower than forecast

------------------------------------------------------------------------

## 4.5 AdjustmentModule

Purpose: - Compare forecast vs observation - Detect regime transition -
Flag deviation risk

Outputs: - regime_shift_detected (bool) - Tmax_adjustment_bias (positive
/ negative / neutral)

------------------------------------------------------------------------

## 4.6 NarrativeModule

Purpose: - Convert structured data to natural-language explanation -
Support layered response modes

Response Modes: - summary (default) - module-specific - deep (triggered
by "dive in")

------------------------------------------------------------------------

# 5. Query Routing Logic

if "环流" → SynopticModule\
if "sounding" → SoundingModule\
if "实况" → MetarModule\
if "预报" → ForecastModule\
if "dive in" → deep mode\
else → full summary

------------------------------------------------------------------------

# 6. Response Strategy

## 6.1 Default Query

User: "今日最高温形势？"

Pipeline: 1. Read cached synoptic state 2. Read Tmax sounding analysis
3. Pull latest METAR 4. Apply adjustment logic 5. Generate concise
summary

------------------------------------------------------------------------

## 6.2 Module-Specific Query

User: "环流背景如何？"

→ Only SynopticModule summary returned.

User: "sounding dive in"

→ SoundingModule deep mode activated.

------------------------------------------------------------------------

# 7. Regime Classification (Lightweight)

Possible regimes:

-   post_trough_cold
-   warm_advection
-   high_pressure_stable
-   frontal_transition

Regime switch triggers: - Wind quadrant change - Pressure trend
reversal - Main low center passing closest approach

------------------------------------------------------------------------

# 8. Performance Strategy

Heavy tasks: - GRIB parsing - Center detection

Run asynchronously every 3 hours.

Fast tasks: - METAR fetch - Narrative generation

Query response target: - \< 1 second

------------------------------------------------------------------------

# 9. Future Extensions

-   Archive regime tagging
-   Same-type historical day matching
-   Confidence scoring
-   Extreme Tmax detection module
-   ERA5-based climatology bias correction

------------------------------------------------------------------------

# 10. Implementation Stack Recommendation

Preferred:

-   Python for GRIB + diagnostics (xarray + cfgrib)
-   Node/OpenClaw for orchestration & routing
-   Redis or in-memory cache for synoptic summaries

------------------------------------------------------------------------

# 11. Summary

This design ensures:

-   Fast response
-   Modular extensibility
-   Clear separation of concerns
-   Lightweight but meteorologically sound reasoning
-   Real-time adaptive updates

------------------------------------------------------------------------

# 12. Skill Update -- Boundary-Layer Mixing Signals (Operational Rules)

This section defines mandatory mixing diagnostics for future runs.

## 12.1 Core Mixing Signal Set (must evaluate every run)

Use the following signals as standard references for boundary-layer
mixing diagnosis:

-   Thermal trigger: cloud reduction during daytime (e.g., BKN -> SCT/FEW)
-   Thermal structure: low-level lapse tendency toward dry-adiabatic
    profile
-   Mechanical trigger: sustained/increasing low-level wind (surface/925/850)
-   Dry-air entrainment evidence: short-term pattern of T rising with Td
    falling
-   Consistency check: changes persist across >=2 consecutive METAR
    reports

## 12.2 Deterministic Mixing-Boost Flag

Set `mixing_boost=true` when all conditions below are met in the recent
window (default 1 hour):

-   dT >= +1.0 C
-   dTd <= -0.5 C
-   wind >= 10 kt
-   low cloud not increasing

Else set `mixing_boost=false`.

## 12.3 Interpretation Rules for Temperature Impact

-   Mixing controls *rate of warming* (how fast temperature climbs).
-   Synoptic advection controls *ceiling of Tmax* (how high it can go).
-   If `mixing_boost=true`, apply short-lead positive adjustment to Tmax
    trajectory.
-   If cold advection remains significant, cap the upside even with
    strong mixing.

## 12.4 Required Structured Output Fields

Add these fields to analysis payload:

-   `mixing_boost` (bool)
-   `mixing_evidence` (array of strings)
-   `dT_1h_c`
-   `dTd_1h_c`
-   `wind_kt`
-   `cloud_trend` (increasing/decreasing/steady)
-   `mixing_temp_adjustment_c`

## 12.5 Report-Level Requirements (Conditional)

Report must include mixing section only when `mixing_boost=true` or
mixing signals are near-threshold and operationally relevant.

If no obvious mixing signal is detected, skip mixing-reference output and
do not force a mixing paragraph.

When included, report must explicitly include:

-   Whether mixing is active now
-   Evidence list (T/Td/wind/cloud trend)
-   Short-term impact on Tmax timing and value
-   Ceiling constraint statement if cold advection persists

------------------------------------------------------------------------
END OF DOCUMENT
