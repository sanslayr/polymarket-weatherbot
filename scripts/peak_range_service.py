#!/usr/bin/env python3
"""Peak-range computation module for /look rendering."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from metar_utils import observed_max_interval_c as _observed_max_interval_c
from realtime_pipeline import classify_window_phase
from layer_signal_policy import h700_effective_dry_factor, h700_is_moist_constraint
from peak_range_history_service import apply_historical_reference, build_peak_historical_reference
from peak_range_signal_service import render_signal_scores, render_sounding_factor_pack
from temperature_phase_decision import build_temperature_phase_decision
from advection_review import thermal_advection_direction


def _parse_iso_dt(v: Any) -> datetime | None:
    try:
        s = str(v or "")
        return datetime.fromisoformat(s) if s else None
    except Exception:
        return None


def _coerce_same_tz(a: datetime | None, b: datetime | None) -> tuple[datetime | None, datetime | None]:
    if a is None or b is None:
        return a, b
    try:
        if a.tzinfo is not None and b.tzinfo is None:
            b = b.replace(tzinfo=a.tzinfo)
        elif a.tzinfo is None and b.tzinfo is not None:
            a = a.replace(tzinfo=b.tzinfo)
    except Exception:
        pass
    return a, b


def _hours_between(later: datetime | None, earlier: datetime | None, nonneg: bool = False) -> float | None:
    later, earlier = _coerce_same_tz(later, earlier)
    if later is None or earlier is None:
        return None
    try:
        h = (later - earlier).total_seconds() / 3600.0
    except Exception:
        return None
    if nonneg:
        return max(0.0, h)
    return h


def _hours_between_iso(later_iso: Any, earlier_iso: Any, nonneg: bool = False) -> float | None:
    return _hours_between(_parse_iso_dt(later_iso), _parse_iso_dt(earlier_iso), nonneg=nonneg)


def _hm(s: Any) -> str:
    try:
        dt = datetime.strptime(str(s), "%Y-%m-%dT%H:%M")
        return dt.strftime("%H:%M")
    except Exception:
        return str(s)


def _fmt_hour_float(value: Any) -> str:
    v = _safe_float(value)
    if v is None:
        return "n/a"
    total_minutes = max(0, int(round(v * 60.0)))
    hour = (total_minutes // 60) % 24
    minute = total_minutes % 60
    return f"{hour:02d}:{minute:02d}"


def _fmt_temp_unit(value_c: Any, unit: str) -> str:
    v = _safe_float(value_c)
    if v is None:
        return "n/a"
    if str(unit).upper() == "F":
        value = v * 9.0 / 5.0 + 32.0
        return f"{value:.1f}°F"
    return f"{v:.1f}°C"


def _extract_quoted_token(text: str) -> str:
    s = str(text or "").strip()
    if not s:
        return ""
    parts = s.split("`")
    if len(parts) >= 3 and parts[1].strip():
        return parts[1].strip()
    return s


def _range_tail_note(
    *,
    skew: float,
    cloud_code: str,
    advection_review: dict[str, Any] | None,
    temp_state: dict[str, Any],
) -> str:
    cloud_code_up = str(cloud_code or "").upper()
    adv_review = advection_review if isinstance(advection_review, dict) else {}
    adv_state = str(adv_review.get("thermal_advection_state") or "")
    adv_direction = thermal_advection_direction(adv_review)
    second_peak = str(temp_state.get("second_peak_potential") or "none")
    rebound_mode = str(temp_state.get("rebound_mode") or "none")

    if skew >= 0.20:
        if cloud_code_up in {"CLR", "CAVOK", "SKC"}:
            base = "晴空维持"
        elif cloud_code_up in {"FEW", "SCT"}:
            base = "少云维持"
        else:
            base = "开窗延续"
        if adv_direction == "warm" and adv_state in {"confirmed", "probable"}:
            return f"{base} + 暖平流落地"
        if second_peak in {"moderate", "high"} and rebound_mode == "second_peak":
            return f"{base} + 防再次冲高"
        return base

    if skew <= -0.20:
        if adv_direction == "cold" and adv_state == "confirmed":
            return "云量回补 + 冷平流落地"
        if adv_direction == "cold":
            return "云量回补 + 偏冷背景增强"
        if cloud_code_up in {"BKN", "OVC", "VV"}:
            return "低云加厚"
        return "升温转弱"

    return ""


def _fmt_delta_unit(delta_c: Any, unit: str) -> str:
    v = _safe_float(delta_c)
    if v is None:
        return "n/a"
    if str(unit).upper() == "F":
        value = v * 9.0 / 5.0
        return f"{value:+.1f}°F"
    return f"{v:+.1f}°C"


def _contains_any_text(text: str, keys: list[str]) -> bool:
    s = str(text or "")
    return any(k in s for k in keys)


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _h500_weight_score(feature: dict[str, Any] | None) -> float:
    if not isinstance(feature, dict):
        return 0.0
    try:
        return max(-1.0, min(1.0, float(feature.get("tmax_weight_score") or 0.0)))
    except Exception:
        return 0.0


def _h500_has_key_signal(feature: dict[str, Any] | None) -> bool:
    if not isinstance(feature, dict):
        return False
    if str(feature.get("impact_weight") or "") in {"medium", "high"}:
        return True
    return abs(_h500_weight_score(feature)) >= 0.22


def _pick_prefixed_line(lines: list[str], prefix: str) -> str | None:
    for line in lines:
        text = str(line or "").strip()
        if text.startswith(prefix):
            return text[len(prefix):].strip()
    return None


def _is_generic_500_text(s: str) -> bool:
    t = str(s or "")
    generic_tokens = [
        "高空仍有抬升触发条件",
        "云层若放开更易再冲高",
        "高空背景信号有限",
        "高空背景一般",
        "500hPa弱信号背景",
    ]
    return any(k in t for k in generic_tokens)


def build_peak_range_summary(
    primary_window: dict[str, Any],
    syn_w: dict[str, Any],
    calc_window: dict[str, Any],
    metar_diag: dict[str, Any],
    quality: dict[str, Any],
    obj: dict[str, Any],
    line500: str,
    h500_feature: dict[str, Any] | None,
    line850: str,
    advection_review: dict[str, Any] | None,
    extra: str,
    h700_summary: str,
    h925_summary: str,
    snd_thermo: dict[str, Any],
    cloud_code_now: str,
    precip_state: str,
    precip_trend: str,
    unit: str,
    rt_accel_neg: float,
    rt_flat: float,
    rt_weak: float,
    rt_near_peak_h: float,
    rt_near_end_h: float,
    rt_solar_stall: float,
    rt_solar_rise: float,
    rt_rad_low: float,
    rt_rad_recover: float,
    rt_rad_recover_tr: float,
    rt_night_solar: float,
    rt_night_hour_start: float,
    rt_night_hour_end: float,
    rt_night_warm_bias: float,
    rt_night_wind_jump: float,
    rt_night_wind_mix_min: float,
    rt_night_dp_rise: float,
    rt_night_pres_fall: float,
    rt_night_score_min: float,
    solar_clear_score_fn,
    temp_phase_decision: dict[str, Any] | None = None,
) -> dict[str, Any]:
    peak_c = float(calc_window.get('peak_temp_c') or 0.0)
    obs_max = None
    try:
        obs_max = float(metar_diag.get('observed_max_temp_c')) if metar_diag.get('observed_max_temp_c') is not None else None
    except Exception:
        obs_max = None

    obs_floor = None
    obs_ceil = None
    try:
        if metar_diag.get("observed_max_interval_lo_c") is not None and metar_diag.get("observed_max_interval_hi_c") is not None:
            obs_floor = float(metar_diag.get("observed_max_interval_lo_c"))
            obs_ceil = float(metar_diag.get("observed_max_interval_hi_c"))
    except Exception:
        obs_floor = None
        obs_ceil = None

    if obs_max is not None and (obs_floor is None or obs_ceil is None):
        obs_floor, obs_ceil = _observed_max_interval_c(obs_max, unit)
    if obs_floor is not None and obs_ceil is not None and obs_floor > obs_ceil:
        obs_floor, obs_ceil = obs_ceil, obs_floor

    temp_state = temp_phase_decision if isinstance(temp_phase_decision, dict) else build_temperature_phase_decision(
        primary_window,
        metar_diag,
        line850=line850,
        advection_review=advection_review,
    )
    gate = dict(temp_state.get("gate") or classify_window_phase(primary_window, metar_diag))
    phase_now = str(gate.get('phase') or 'unknown')
    daily_peak_state = str(temp_state.get("daily_peak_state") or "open")
    use_early_peak_wording = bool(temp_state.get("should_use_early_peak_wording"))
    rebound_mode = str(temp_state.get("rebound_mode") or "none")
    dominant_shape = str(temp_state.get("dominant_shape") or "")
    plateau_hold_state = str(temp_state.get("plateau_hold_state") or "none")
    should_discuss_second_peak = bool(temp_state.get("should_discuss_second_peak"))
    multi_peak_evidence_level = str(temp_state.get("multi_peak_evidence_level") or "none")
    h500_score = _h500_weight_score(h500_feature)

    # horizon to fused peak (used by uncertainty and market-label confidence gating)
    try:
        latest_local_txt = str(metar_diag.get("latest_report_local") or "")
        peak_fused_txt = str(gate.get("peak_fused") or "")
        h_to_peak = _hours_between_iso(peak_fused_txt, latest_local_txt, nonneg=True)
    except Exception:
        h_to_peak = None

    # Quantization-aware dynamic range (METAR usually integer-rounded; avoid overfitting tiny jumps)
    # Target: main-band (majority scenarios) + optional tail-risk note.
    u = 0.0
    q_state = str((quality or {}).get("source_state") or "")
    if q_state in {"degraded", "fallback-cache"}:
        u += 0.22

    if phase_now == "far":
        u += 0.12
    elif phase_now == "near_window":
        u += 0.06
    elif phase_now == "in_window":
        u -= 0.06
    elif phase_now == "post":
        u += 0.03

    conf = str((obj or {}).get("confidence") or "")
    if conf == "high":
        u -= 0.12
    elif conf == "low":
        u += 0.12
    elif not conf:
        u += 0.08

    imp = str((obj or {}).get("impact_scope") or "")
    if imp == "station_relevant":
        u -= 0.08
    elif imp == "possible_override":
        u += 0.06
    elif imp == "background_only":
        u += 0.12

    try:
        b_src = metar_diag.get("temp_bias_smooth_c") if metar_diag.get("temp_bias_smooth_c") is not None else metar_diag.get("temp_bias_c")
        b = float(b_src or 0.0)
        babs = abs(b)
    except Exception:
        b = 0.0
        babs = 0.0
    if babs >= 2.0:
        u += 0.15
    elif babs >= 1.0:
        u += 0.08

    try:
        wchg = float(metar_diag.get("wind_dir_change_deg") or 0.0)
    except Exception:
        wchg = 0.0
    if wchg >= 45:
        u += 0.06

    half_range = min(1.25, max(0.55, 0.75 + u))

    # Far-window + large model-vs-obs bias: explicitly widen range to express dispersion.
    low_conf_far = False
    if phase_now == "far":
        if (h_to_peak is not None and h_to_peak >= 5.0 and babs >= 1.5) or (h_to_peak is not None and h_to_peak >= 8.0):
            low_conf_far = True
    if low_conf_far:
        # widen, but moderately (avoid over-dispersion)
        spread_boost = 0.16 + min(0.14, max(0.0, babs - 1.5) * 0.05)
        if q_state in {"degraded", "fallback-cache"}:
            spread_boost += 0.05
        half_range = min(1.55, half_range + spread_boost)

    # center = near-term prefers observed state; farther out keeps model baseline.
    center = float(peak_c)
    try:
        tstep_src = (
            metar_diag.get("temp_trend_effective_c")
            if metar_diag.get("temp_trend_effective_c") is not None
            else (
                metar_diag.get("temp_trend_smooth_c")
                if metar_diag.get("temp_trend_smooth_c") is not None
                else metar_diag.get("temp_trend_1step_c")
            )
        )
        tstep = float(tstep_src or 0.0)
    except Exception:
        tstep = 0.0

    # Bias/trend-driven center adjustment (quantization-aware):
    # avoid using current absolute temp directly, which can understate rapid-rise regimes before peak.
    b_cap = max(-1.2, min(1.8, b))
    t_up = max(0.0, min(0.9, tstep))
    obs_anchor = obs_max if obs_max is not None else None
    try:
        latest_temp_anchor = float(metar_diag.get("latest_temp")) if metar_diag.get("latest_temp") is not None else None
    except Exception:
        latest_temp_anchor = None
    if latest_temp_anchor is not None:
        obs_anchor = latest_temp_anchor if obs_anchor is None else max(float(obs_anchor), latest_temp_anchor)

    if phase_now in {"near_window", "in_window", "post"} and obs_anchor is not None:
        gain_cap = {
            "near_window": 1.00,
            "in_window": 0.60,
            "post": 0.25,
        }.get(phase_now, 0.80)
        horizon_gain = 0.0
        if h_to_peak is not None and phase_now in {"near_window", "in_window"}:
            horizon_gain = min(gain_cap, max(0.0, float(h_to_peak)) * (0.16 if phase_now == "near_window" else 0.08))
        obs_gain = min(
            gain_cap,
            0.70 * t_up + 0.22 * max(0.0, b_cap) + horizon_gain,
        )
        if phase_now == "post":
            obs_gain = min(gain_cap, 0.20 * t_up + 0.10 * max(0.0, b_cap))
        obs_proj = float(obs_anchor) + obs_gain
        obs_proj = min(obs_proj, float(peak_c) + (0.90 if phase_now == "near_window" else 0.60))
    else:
        obs_proj = float(peak_c) + 0.55 * b_cap + 0.35 * t_up

    w_center = {
        "far": 0.18,
        "near_window": 0.52,
        "in_window": 0.68,
        "post": 0.82,
    }.get(phase_now, 0.22)
    center = (1 - w_center) * center + w_center * obs_proj

    # avoid double-counting model priced-in move: only use excess bias above tolerance.
    excess_up = max(0.0, b - 0.9)
    excess_dn = max(0.0, -b - 0.9)
    center += min(0.45, 0.18 * excess_up)
    center -= min(0.45, 0.18 * excess_dn)

    up_s, down_s, _ = render_signal_scores(
        primary_window=primary_window,
        metar_diag=metar_diag,
        line850=line850,
        extra=extra,
        precip_state=precip_state,
        precip_trend=precip_trend,
        calc_window=calc_window,
        snd_thermo=snd_thermo,
        h700_summary=h700_summary,
        h925_summary=h925_summary,
        cloud_code_now=cloud_code_now,
    )
    denom = max(1e-6, up_s + down_s)
    skew = max(-0.8, min(0.8, (up_s - down_s) / denom))
    if phase_now == "post":
        # Post-window: suppress aggressive one-sided skew tails unless strong rebound is observed in real time.
        skew = max(-0.12, min(0.12, skew))

    # Direction-range consistency correction:
    # when directional evidence is strongly one-sided, interval center should follow with higher weight.
    try:
        b_cons = float((metar_diag.get("temp_bias_smooth_c") if metar_diag.get("temp_bias_smooth_c") is not None else metar_diag.get("temp_bias_c")) or 0.0)
    except Exception:
        b_cons = 0.0
    try:
        t_cons = float((
            metar_diag.get("temp_trend_effective_c")
            if metar_diag.get("temp_trend_effective_c") is not None
            else (
                metar_diag.get("temp_trend_smooth_c")
                if metar_diag.get("temp_trend_smooth_c") is not None
                else metar_diag.get("temp_trend_1step_c")
            )
        ) or 0.0)
    except Exception:
        t_cons = 0.0
    try:
        t_acc = (
            float(metar_diag.get("temp_accel_effective_c"))
            if metar_diag.get("temp_accel_effective_c") is not None
            else (float(metar_diag.get("temp_accel_2step_c")) if metar_diag.get("temp_accel_2step_c") is not None else None)
        )
    except Exception:
        t_acc = None

    solar_now = None
    solar_prev = None
    solar_next = None
    solar_slope_next = None
    latest_local_dt = None
    try:
        lat_s = float(metar_diag.get("station_lat")) if metar_diag.get("station_lat") is not None else None
        lon_s = float(metar_diag.get("station_lon")) if metar_diag.get("station_lon") is not None else None
        latest_local_txt = str(metar_diag.get("latest_report_local") or "")
        latest_local_dt = datetime.fromisoformat(latest_local_txt) if latest_local_txt else None
        if latest_local_dt is not None and lat_s is not None and lon_s is not None:
            solar_now = solar_clear_score_fn(lat_s, lon_s, latest_local_dt)
            solar_prev = solar_clear_score_fn(lat_s, lon_s, latest_local_dt - timedelta(hours=1))
            solar_next = solar_clear_score_fn(lat_s, lon_s, latest_local_dt + timedelta(hours=1))
            solar_slope_next = float(solar_next - solar_now)
    except Exception:
        solar_now = None
        solar_prev = None
        solar_next = None
        solar_slope_next = None
        latest_local_dt = None

    rad_eff = None
    rad_eff_tr = None
    try:
        if metar_diag.get("radiation_eff_smooth") is not None:
            rad_eff = float(metar_diag.get("radiation_eff_smooth"))
        elif metar_diag.get("radiation_eff") is not None:
            rad_eff = float(metar_diag.get("radiation_eff"))
        if metar_diag.get("radiation_eff_trend_1step") is not None:
            rad_eff_tr = float(metar_diag.get("radiation_eff_trend_1step"))
    except Exception:
        rad_eff = None
        rad_eff_tr = None

    # Night-time residual warming factors (for post-peak / after-sunset reheat feasibility):
    # 1) warm advection still landing, 2) wind mixing strengthens, 3) cloud blanket rebuild,
    # 4) dewpoint rises (air-mass/moist advection), 5) pressure falls.
    try:
        latest_wspd = float(metar_diag.get("latest_wspd")) if metar_diag.get("latest_wspd") is not None else None
    except Exception:
        latest_wspd = None
    try:
        ws_step = float(metar_diag.get("wind_speed_trend_1step_kt")) if metar_diag.get("wind_speed_trend_1step_kt") is not None else None
    except Exception:
        ws_step = None
    try:
        dp_step = float(metar_diag.get("dewpoint_trend_1step_c")) if metar_diag.get("dewpoint_trend_1step_c") is not None else None
    except Exception:
        dp_step = None
    try:
        p_step = float(metar_diag.get("pressure_trend_1step_hpa")) if metar_diag.get("pressure_trend_1step_hpa") is not None else None
    except Exception:
        p_step = None

    hour_local_float = None
    if latest_local_dt is not None:
        try:
            hour_local_float = float(latest_local_dt.hour + latest_local_dt.minute / 60.0)
        except Exception:
            hour_local_float = None

    nighttime_active = False
    try:
        if solar_now is not None:
            nighttime_active = bool(solar_now <= rt_night_solar)
        elif hour_local_float is not None:
            nighttime_active = bool((hour_local_float >= rt_night_hour_start) or (hour_local_float <= rt_night_hour_end))
    except Exception:
        nighttime_active = False

    warm_adv_signal = bool(("暖平流" in line850) and (b_cons >= rt_night_warm_bias))
    mix_signal = bool(
        ws_step is not None
        and latest_wspd is not None
        and ws_step >= rt_night_wind_jump
        and latest_wspd >= rt_night_wind_mix_min
    )
    cloud_trend_for_night = str(metar_diag.get("cloud_trend") or "")
    cloud_blanket_signal = bool(
        cloud_code_now in {"SCT", "BKN", "OVC", "VV"}
        and (("回补" in cloud_trend_for_night) or ("增加" in cloud_trend_for_night))
        and precip_state in {"none", "light"}
    )
    moist_adv_signal = bool(dp_step is not None and dp_step >= rt_night_dp_rise)
    pressure_fall_signal = bool(p_step is not None and p_step <= rt_night_pres_fall)

    # Precipitation-related night penalties:
    # - ongoing/new precip usually adds evaporative/wet-ground cooling,
    # - even after precip ends, low-cloud residual can delay re-warm.
    # - Exception: light warm-advection rain can be less suppressive than convective/cold rain.
    precip_warm_relief = bool(
        precip_state == "light"
        and precip_trend in {"new", "intensify", "steady"}
        and warm_adv_signal
        and (moist_adv_signal or b_cons >= max(0.55, rt_night_warm_bias + 0.05))
        and (mix_signal or t_cons >= 0.10)
        and cloud_code_now not in {"VV"}
    )
    precip_hard_block = bool(
        precip_state in {"moderate", "heavy", "convective"}
        or (
            precip_trend in {"new", "intensify"}
            and precip_state == "light"
            and (not precip_warm_relief)
        )
    )
    precip_light_drag = bool(
        precip_state == "light"
        and precip_trend in {"steady", "none", "weaken", "end"}
        and (not precip_warm_relief)
    )
    precip_residual_drag = bool(
        precip_trend in {"end", "weaken"}
        and cloud_code_now in {"BKN", "OVC", "VV"}
    )

    nocturnal_score = 0.0
    if warm_adv_signal:
        nocturnal_score += 0.95
    if mix_signal:
        nocturnal_score += 0.80
    if cloud_blanket_signal:
        nocturnal_score += 0.65
    if moist_adv_signal:
        nocturnal_score += 0.55
    if pressure_fall_signal:
        nocturnal_score += 0.45

    if precip_hard_block:
        nocturnal_score -= 1.05
    elif precip_residual_drag:
        nocturnal_score -= 0.40
    elif precip_light_drag:
        nocturnal_score -= 0.25
    elif precip_warm_relief:
        nocturnal_score -= 0.10

    nocturnal_gate = warm_adv_signal or (mix_signal and moist_adv_signal and pressure_fall_signal)
    nocturnal_reheat_signal = bool(
        nighttime_active
        and nocturnal_gate
        and nocturnal_score >= rt_night_score_min
        and (not precip_hard_block)
    )

    if nocturnal_reheat_signal:
        rs: list[str] = []
        if warm_adv_signal:
            rs.append("暖平流")
        if mix_signal:
            rs.append("混合作用")
        if cloud_blanket_signal:
            rs.append("云被回补")
        if moist_adv_signal:
            rs.append("露点回升")
        if pressure_fall_signal:
            rs.append("气压走低")
        metar_diag["nocturnal_reheat_reasons"] = "、".join(rs[:3])
    metar_diag["nocturnal_reheat_signal"] = nocturnal_reheat_signal
    metar_diag["nocturnal_reheat_score"] = round(nocturnal_score, 2)
    metar_diag["nocturnal_precip_hard_block"] = precip_hard_block
    metar_diag["nocturnal_precip_residual_drag"] = precip_residual_drag
    metar_diag["nocturnal_precip_warm_relief"] = precip_warm_relief

    dir_delta = up_s - down_s
    consistency_shift = 0.0
    if dir_delta >= 0.8:
        consistency_shift += 0.22
        if b_cons >= 1.0:
            consistency_shift += 0.25
        elif b_cons >= 0.6:
            consistency_shift += 0.15
        if t_cons >= 0.4:
            consistency_shift += 0.14
        if phase_now in {"near_window", "in_window"}:
            consistency_shift += 0.08
    elif dir_delta <= -0.8:
        consistency_shift -= 0.22
        if b_cons <= -1.0:
            consistency_shift -= 0.25
        elif b_cons <= -0.6:
            consistency_shift -= 0.15
        if t_cons <= -0.4:
            consistency_shift -= 0.14
        if phase_now in {"near_window", "in_window"}:
            consistency_shift -= 0.08

    # Clear-sky solar guard: under stable clear/less-cloud states, once slope stops accelerating
    # near peak window, avoid inertial warm over-shift.
    cloud_trend_txt = str(metar_diag.get("cloud_trend") or "")
    clear_sky_stable = (
        cloud_code_now in {"CLR", "CAVOK", "SKC", "FEW", "SCT"}
        and ("回补" not in cloud_trend_txt)
        and ("增加" not in cloud_trend_txt)
    )
    if clear_sky_stable and phase_now in {"near_window", "in_window"}:
        if t_cons <= 0.15:
            damp = 0.62
            if solar_now is not None and solar_now <= 0.45:
                damp = 0.54
            consistency_shift *= damp
        if b_cons > 0.0 and t_cons <= 0.05:
            cap_shift = 0.12
            if solar_now is not None and solar_now <= 0.35:
                cap_shift = 0.08
            consistency_shift = min(consistency_shift, cap_shift)

    # Hourly-cycle station anti-single-step inertia:
    # if latest hourly report is already flat and no SPECI pressure, avoid carrying prior-step momentum too far.
    try:
        cad_min_local = float(metar_diag.get("metar_routine_cadence_min")) if metar_diag.get("metar_routine_cadence_min") is not None else None
    except Exception:
        cad_min_local = None
    try:
        t_step_latest = float(metar_diag.get("temp_trend_1step_c")) if metar_diag.get("temp_trend_1step_c") is not None else 0.0
    except Exception:
        t_step_latest = 0.0
    speci_pressure = bool(metar_diag.get("metar_speci_active")) or bool(metar_diag.get("metar_speci_likely"))
    if (
        cad_min_local is not None
        and cad_min_local >= 50.0
        and (not speci_pressure)
        and phase_now in {"near_window", "in_window", "post"}
        and abs(t_step_latest) <= 0.12
    ):
        t_cons = min(t_cons, 0.12)
        if b_cons > 0:
            b_cons = min(b_cons, 0.9)
        metar_diag["hourly_flat_anchor_active"] = True

    precip_cooling = False
    precip_residual = False
    precip_warm_relief_day = False
    if phase_now in {"near_window", "in_window"}:
        precip_warm_relief_day = bool(
            precip_state == "light"
            and precip_trend in {"new", "intensify", "steady"}
            and ("暖平流" in line850)
            and b_cons >= 0.45
            and ((dp_step is not None and dp_step >= 0.4) or t_cons >= 0.15)
            and cloud_code_now not in {"VV"}
        )

        if precip_state in {"moderate", "heavy", "convective"} and precip_trend in {"new", "intensify", "steady", "none"}:
            precip_cooling = True
        elif precip_trend in {"new", "intensify"} and precip_state == "light":
            if precip_warm_relief_day:
                precip_residual = True
            else:
                precip_cooling = True
        elif precip_trend in {"end", "weaken"} and cloud_code_now in {"BKN", "OVC", "VV"}:
            # precip just ended but cloud deck remains: cooling impact can linger
            precip_residual = True
        elif precip_state == "light" and precip_trend in {"steady", "none"} and (not precip_warm_relief_day):
            precip_residual = True

    if precip_cooling:
        consistency_shift = min(consistency_shift, 0.10)
        center -= 0.18
    elif precip_residual:
        consistency_shift = min(consistency_shift, 0.15)
        center -= (0.05 if precip_warm_relief_day else 0.08)

    metar_diag["precip_warm_relief_day"] = bool(precip_warm_relief_day)

    # Persistent low-cloud guard: avoid pushing center too high when BKN/OVC remains and no opening signal.
    cloud_opening = ("开窗" in str(metar_diag.get("cloud_trend") or "")) or ("减弱" in str(metar_diag.get("cloud_trend") or ""))
    if phase_now in {"near_window", "in_window"} and cloud_code_now in {"BKN", "OVC", "VV"} and not cloud_opening:
        consistency_shift = min(consistency_shift, 0.16)

    consistency_shift = max(-0.75, min(0.75, consistency_shift))
    center += consistency_shift
    h500_phase_scale = {
        "far": 0.85,
        "near_window": 0.72,
        "in_window": 0.56,
        "post": 0.30,
    }.get(phase_now, 0.60)
    h500_center_shift = max(-0.35, min(0.35, 0.32 * h500_score * h500_phase_scale))
    center += h500_center_shift
    if abs(h500_score) >= 0.12:
        metar_diag["h500_tmax_weight_score"] = round(h500_score, 2)
        metar_diag["h500_center_shift_c"] = round(h500_center_shift, 2)
        if isinstance(h500_feature, dict):
            metar_diag["h500_thermal_role"] = str(h500_feature.get("thermal_role") or "")

    # Far-window clear-sky diurnal amplitude uplift:
    # if yesterday observed diurnal range is materially larger than model daily range,
    # and current early-period signals are clear-sky/stable, avoid under-projecting daytime rise.
    diurnal_uplift = 0.0
    try:
        prev_rng = float(metar_diag.get("observed_prev_day_range_c")) if metar_diag.get("observed_prev_day_range_c") is not None else None
    except Exception:
        prev_rng = None
    try:
        model_rng = float(metar_diag.get("model_day_range_c")) if metar_diag.get("model_day_range_c") is not None else None
    except Exception:
        model_rng = None
    try:
        low_cloud_peak_est = float(calc_window.get("low_cloud_pct") or 0.0)
    except Exception:
        low_cloud_peak_est = 0.0

    clear_now = cloud_code_now in {"CLR", "CAVOK", "SKC", "FEW", "SCT"}

    # Circulation-context weighted gate for day-to-day amplitude learning:
    # yesterday's range is informative by degree, not a binary on/off.
    strong_dyn = (
        _h500_has_key_signal(h500_feature)
        or any(k in str(extra) for k in ["锋生", "强迫", "切变", "对流"])
        or (
            (not isinstance(h500_feature, dict) or not h500_feature)
            and any(k in str(line500) for k in ["副热带高压", "冷高压", "深槽", "低压槽", "暖脊", "高压脊", "短波", "急流", "涡度", "PVA", "NVA"])
        )
    )
    circ_weak_forced = not strong_dyn
    if _is_generic_500_text(line500) and abs(h500_score) < 0.18:
        circ_weak_forced = True
    strong_cold_adv = ("冷平流" in line850) and ("暖平流" not in line850)
    dry_support = h700_effective_dry_factor(
        h700_summary,
        low_cloud_pct=low_cloud_peak_est,
        cloud_code_now=cloud_code_now,
    )

    circ_context_score = 0.55
    if circ_weak_forced:
        circ_context_score += 0.22
    if dry_support > 0:
        circ_context_score += 0.10 * dry_support
    if ("暖平流" in line850) and b_cons > -0.4:
        circ_context_score += 0.08
    if strong_dyn:
        circ_context_score -= 0.20
    circ_context_score += 0.16 * h500_score
    if strong_cold_adv:
        circ_context_score -= 0.22
    if low_cloud_peak_est >= 35.0:
        circ_context_score -= 0.10
    if b_cons <= -1.2:
        circ_context_score -= 0.08
    circ_context_score = max(0.0, min(1.0, circ_context_score))

    if (
        phase_now == "far"
        and h_to_peak is not None
        and h_to_peak >= 4.5
        and clear_now
        and precip_state == "none"
        and low_cloud_peak_est <= 45.0
        and prev_rng is not None
        and model_rng is not None
    ):
        amp_gap = prev_rng - model_rng
        if amp_gap >= 1.2 and circ_context_score >= 0.18:
            raw_uplift = min(1.30, 0.30 * amp_gap)
            if solar_slope_next is not None and solar_slope_next >= 0.03:
                raw_uplift += 0.15
            if strong_cold_adv and b_cons <= -1.0:
                raw_uplift -= 0.10
            raw_uplift = max(0.10, raw_uplift)
            diurnal_uplift = max(0.0, raw_uplift * circ_context_score)
            if diurnal_uplift >= 0.08:
                center += diurnal_uplift
                metar_diag["diurnal_uplift_applied"] = True
                metar_diag["diurnal_uplift_c"] = round(diurnal_uplift, 2)
                metar_diag["diurnal_context_score"] = round(circ_context_score, 2)

    if phase_now in {"near_window", "in_window"} and cloud_code_now in {"BKN", "OVC", "VV"} and not cloud_opening:
        # cap center by model peak + modest allowance; only slightly relax if slope is clearly positive
        cap_add = 0.65 + 0.20 * max(0.0, min(0.8, t_cons - 0.35))
        center = min(center, float(peak_c) + cap_add)

    major_half = min(1.05, max(0.45, half_range * 0.68))
    left_hw = major_half * (1.0 - 0.35 * skew)
    right_hw = major_half * (1.0 + 0.35 * skew)

    lo = center - left_hw
    hi = center + right_hw
    if diurnal_uplift > 0:
        hi += min(0.9, 0.75 * diurnal_uplift)
    if obs_floor is not None:
        # Physical consistency with METAR quantization: daily maximum cannot be below observed-bin lower edge.
        lo = max(lo, float(obs_floor))

    # anti-collapse guard near peak: avoid over-compressing main band when warm-support evidence is aligned.
    sf_local = render_sounding_factor_pack(
        calc_window=calc_window,
        metar_diag=metar_diag,
        snd_thermo=snd_thermo,
        h700_summary=h700_summary,
        h925_summary=h925_summary,
        cloud_code_now=cloud_code_now,
    )

    try:
        low_cloud_peak = float(calc_window.get("low_cloud_pct") or 0.0)
    except Exception:
        low_cloud_peak = 0.0
    try:
        w850_peak = float(calc_window.get("w850_kmh") or 0.0)
    except Exception:
        w850_peak = 0.0
    if cloud_code_now in {"BKN", "OVC", "VV"}:
        low_cloud_peak = max(low_cloud_peak, 75.0)

    warm_support = 0.0
    if "暖平流" in line850:
        warm_support += 0.6
    if dry_support > 0:
        warm_support += 0.18 * dry_support
    if rad_eff is not None:
        if rad_eff >= rt_rad_recover:
            warm_support += 0.55
        elif rad_eff >= rt_rad_low:
            warm_support += 0.30
        elif rad_eff >= max(0.40, rt_rad_low - 0.15):
            warm_support += 0.12
        else:
            warm_support -= 0.08
    elif cloud_code_now not in {"BKN", "OVC", "VV"}:
        warm_support += 0.5
    if isinstance(metar_diag.get("temp_bias_smooth_c"), (int, float)) and float(metar_diag.get("temp_bias_smooth_c") or 0.0) >= 0.5:
        warm_support += 0.4
    warm_support += min(0.6, max(0.0, float(sf_local.get("up_adj") or 0.0) - 0.3 * float(sf_local.get("down_adj") or 0.0)))
    if h500_score > 0:
        warm_support += min(0.45, 0.40 * h500_score)
    elif h500_score < 0:
        warm_support -= min(0.40, 0.36 * abs(h500_score))

    if (
        phase_now in {"near_window", "in_window"}
        and warm_support >= 1.2
        and obs_max is not None
        and low_cloud_peak < 60
        and cloud_code_now not in {"BKN", "OVC", "VV"}
        and (not precip_cooling)
        and (not precip_residual)
    ):
        try:
            peak_local_txt = str(primary_window.get("peak_local") or "")
            latest_local_txt = str(metar_diag.get("latest_report_local") or "")
            hleft = _hours_between_iso(peak_local_txt, latest_local_txt, nonneg=True)
            if hleft is None:
                hleft = 0.0
        except Exception:
            hleft = 0.0

        floor_hi = float(obs_max) + min(4.2, 1.5 + 0.55 * hleft)
        floor_lo = float(obs_max) + min(2.8, 0.9 + 0.35 * hleft)
        hi = max(hi, floor_hi)
        lo = max(lo, min(floor_lo, hi - 0.2))

    # Thermal-balance cap: prevent inertial high overestimation under persistent low-cloud constraint.
    thermal_cap_hi = None
    if phase_now in {"near_window", "in_window"}:
        if low_cloud_peak >= 80:
            thermal_cap_hi = float(peak_c) + 0.9
        elif low_cloud_peak >= 65:
            thermal_cap_hi = float(peak_c) + 1.2

        if precip_cooling:
            thermal_cap_hi = min(thermal_cap_hi, float(peak_c) + 1.0) if thermal_cap_hi is not None else (float(peak_c) + 1.0)
        elif precip_residual:
            thermal_cap_hi = min(thermal_cap_hi, float(peak_c) + 1.05) if thermal_cap_hi is not None else (float(peak_c) + 1.05)

        if thermal_cap_hi is not None:
            if t_cons <= 0.15:
                thermal_cap_hi -= 0.15
            if w850_peak >= 30:
                thermal_cap_hi += 0.20  # strong-wind cities can keep mixed layer warmer
            if "暖平流" in line850 and b_cons >= 1.0:
                thermal_cap_hi += 0.10
            if precip_trend in {"new", "intensify"}:
                thermal_cap_hi -= 0.20
            if obs_max is not None:
                thermal_cap_hi = max(thermal_cap_hi, float(obs_max) + 0.6)
            hi = min(hi, thermal_cap_hi)
            lo = min(lo, hi - 0.2)

    strict_late_cap = False

    # Afternoon solar-decay + plateau gate:
    # if already in/near window with weak temp slope, avoid optimistic late-window rebound tails.
    if phase_now in {"near_window", "in_window"} and clear_sky_stable and (not precip_cooling) and (not precip_residual):
        try:
            latest_local_txt = str(metar_diag.get("latest_report_local") or "")
            peak_local_txt = str(primary_window.get("peak_local") or "")
            latest_dt = _parse_iso_dt(latest_local_txt)
            hour_local = (latest_dt.hour + latest_dt.minute / 60.0) if latest_dt else None
            hleft = _hours_between_iso(peak_local_txt, latest_local_txt, nonneg=True)
        except Exception:
            hour_local = None
            hleft = None
        try:
            t_now = float(metar_diag.get("latest_temp"))
        except Exception:
            t_now = None

        if t_now is not None and t_cons <= rt_flat:
            late_enough = (hour_local is None) or (hour_local >= 14.0)
            close_to_peak = (hleft is None) or (hleft <= 1.6)
            if late_enough and close_to_peak:
                add = 1.05
                if hour_local is not None and hour_local >= 15.0:
                    add = 0.85
                if "暖平流" in line850 and b_cons >= 0.6 and t_cons > 0.05:
                    add += 0.15
                solar_plateau_cap = t_now + add
                if obs_max is not None:
                    solar_plateau_cap = max(solar_plateau_cap, float(obs_max) + 0.35)
                hi = min(hi, solar_plateau_cap)
                lo = min(lo, hi - 0.2)

    # Clear-sky rounded-top lock:
    # on typical sunny-radiation days, once slope flattens and acceleration turns down near peak,
    # avoid inertial warm tails (common over-forecast pattern).
    if phase_now in {"near_window", "in_window"} and clear_sky_stable and (not precip_cooling) and (not precip_residual):
        try:
            latest_local_txt = str(metar_diag.get("latest_report_local") or "")
            peak_local_txt = str(primary_window.get("peak_local") or "")
            hleft_rt = _hours_between_iso(peak_local_txt, latest_local_txt, nonneg=True)
        except Exception:
            hleft_rt = None
        try:
            t_now_rt = float(metar_diag.get("latest_temp"))
        except Exception:
            t_now_rt = None

        decel = (t_acc is not None and t_acc <= rt_accel_neg)
        flat_or_down = t_cons <= rt_flat
        near_peak = (hleft_rt is None) or (hleft_rt <= rt_near_peak_h)
        solar_stalling = bool(solar_slope_next is not None and solar_slope_next <= rt_solar_stall)
        solar_strong_rise = bool(solar_slope_next is not None and solar_slope_next >= rt_solar_rise)
        rad_stall_tr = max(0.008, 0.4 * rt_rad_recover_tr)
        rad_stalling = bool((rad_eff is not None and rad_eff <= rt_rad_low) and (rad_eff_tr is None or rad_eff_tr <= rad_stall_tr))
        rad_recover = bool((rad_eff is not None and rad_eff >= rt_rad_recover) and (rad_eff_tr is not None and rad_eff_tr >= rt_rad_recover_tr))

        rounded_top_signal = (
            flat_or_down
            or (t_cons <= rt_weak and decel)
            or (t_cons <= min(rt_weak, 0.20) and solar_stalling)
            or (t_cons <= min(rt_weak, 0.20) and rad_stalling)
        )

        if t_now_rt is not None and near_peak and rounded_top_signal and ((not solar_strong_rise and not rad_recover) or decel):
            add_rt = 0.55
            if solar_now is not None:
                if solar_now <= 0.30:
                    add_rt = 0.35
                elif solar_now <= 0.50:
                    add_rt = 0.45
            if rad_eff is not None:
                if rad_eff <= 0.40:
                    add_rt = min(add_rt, 0.35)
                elif rad_eff <= rt_rad_low:
                    add_rt = min(add_rt, 0.45)
                elif rad_eff >= 0.75 and (rad_eff_tr is not None and rad_eff_tr > 0.02):
                    add_rt += 0.08
            if hleft_rt is not None and hleft_rt <= rt_near_end_h:
                add_rt = min(add_rt, 0.40)
            if "暖平流" in line850 and b_cons >= 0.9 and t_cons >= 0.10 and (not solar_stalling) and (not rad_stalling):
                add_rt += 0.10
            rounded_cap = t_now_rt + add_rt
            if obs_max is not None:
                rounded_cap = max(rounded_cap, float(obs_max) + 0.20)
            hi = min(hi, rounded_cap)
            lo = min(lo, hi - 0.2)
            strict_late_cap = True
            metar_diag["rounded_top_cap_applied"] = True

    # Quantized-METAR near-end guard:
    # many stations report integer-like temp steps; when close to window end, avoid reading a single +1C step
    # as sustained acceleration.
    if phase_now in {"near_window", "in_window"} and bool(metar_diag.get("metar_temp_quantized")):
        try:
            latest_local_txt = str(metar_diag.get("latest_report_local") or "")
            end_local_txt = str(primary_window.get("end_local") or "")
            h_to_end = _hours_between_iso(end_local_txt, latest_local_txt, nonneg=True)
        except Exception:
            h_to_end = None
        try:
            t_now = float(metar_diag.get("latest_temp"))
        except Exception:
            t_now = None
        if t_now is not None and (h_to_end is not None) and h_to_end <= rt_near_end_h:
            q_add = 0.95
            if t_cons <= 0.20:
                q_add = 0.65
            elif t_cons <= 0.45:
                q_add = 0.80
            elif t_cons <= 0.75:
                q_add = 0.95
            else:
                q_add = 1.05
            if "暖平流" in line850 and b_cons >= 0.8 and t_cons >= 0.4:
                q_add += 0.10
            q_cap = t_now + q_add
            if obs_max is not None:
                q_cap = max(q_cap, float(obs_max) + 0.25)
            hi = min(hi, q_cap)
            lo = min(lo, hi - 0.2)
            strict_late_cap = True

    # Window-end inertia cap:
    # even if phase classifier still near/in, once clock passes window end and slope is flat/decelerating,
    # suppress inertial warm tails unless there is explicit re-heating evidence.
    if phase_now in {"near_window", "in_window", "post"}:
        try:
            latest_local_txt = str(metar_diag.get("latest_report_local") or "")
            end_local_txt = str(primary_window.get("end_local") or "")
            h_after_end = _hours_between_iso(latest_local_txt, end_local_txt, nonneg=False)
        except Exception:
            h_after_end = None

        try:
            t_now_end = float(metar_diag.get("latest_temp"))
        except Exception:
            t_now_end = None

        decel_or_flat = (t_cons <= rt_weak) and (t_acc is None or t_acc <= 0.0)
        rad_reheat = bool(
            (rad_eff is not None and rad_eff >= rt_rad_recover)
            and (rad_eff_tr is not None and rad_eff_tr >= max(0.02, rt_rad_recover_tr * 0.8))
        )
        dyn_reheat = bool(("暖平流" in line850) and (b_cons >= 0.8) and (t_cons >= 0.40))

        cadence_wait_h = 0.5
        try:
            cad_min = float(metar_diag.get("metar_routine_cadence_min")) if metar_diag.get("metar_routine_cadence_min") is not None else None
        except Exception:
            cad_min = None
        if cad_min is not None and cad_min > 0:
            cadence_wait_h = max(0.35, min(0.80, (cad_min / 60.0) * 0.75))

        if (
            t_now_end is not None
            and h_after_end is not None
            and h_after_end >= cadence_wait_h
            and decel_or_flat
            and (not rad_reheat)
            and (not dyn_reheat)
            and (not nocturnal_reheat_signal)
        ):
            end_add = 0.35
            if "暖平流" in line850 and b_cons >= 0.6:
                end_add += 0.10
            if rad_eff is not None and rad_eff <= rt_rad_low:
                end_add -= 0.05
            end_add = max(0.20, min(0.60, end_add))
            end_cap = t_now_end + end_add
            if obs_max is not None:
                end_cap = max(end_cap, float(obs_max) + 0.15)
            hi = min(hi, end_cap)
            lo = min(lo, hi - 0.2)
            strict_late_cap = True
            metar_diag["late_end_cap_applied"] = True

    # Post-window realized-peak guard:
    # once peak window is over, avoid optimistic "new high" tails unless there is clear rebound evidence.
    if phase_now == "post" and obs_max is not None:
        try:
            latest_local_txt = str(metar_diag.get("latest_report_local") or "")
            obs_peak_local_txt = str(metar_diag.get("observed_max_time_local") or "")
            h_since_obs_peak = _hours_between_iso(latest_local_txt, obs_peak_local_txt, nonneg=True)
        except Exception:
            h_since_obs_peak = None

        try:
            t_now = float(metar_diag.get("latest_temp"))
        except Exception:
            t_now = None

        wet_now = (precip_state in {"light", "moderate", "heavy", "convective"}) or (precip_trend in {"new", "intensify", "steady", "end"})
        cloudy_now = cloud_code_now in {"BKN", "OVC", "VV"}

        rebound_ok = (
            clear_sky_stable
            and (not wet_now)
            and (not cloudy_now)
            and t_cons >= 0.45
            and b_cons >= 0.4
            and ((h_since_obs_peak is None) or (h_since_obs_peak <= 2.0))
        )
        nocturnal_rebreak_ok = (
            bool(nocturnal_reheat_signal)
            and (not wet_now)
            and t_cons >= -0.05
            and b_cons >= 0.15
            and ((h_since_obs_peak is None) or (h_since_obs_peak <= 3.5))
        )

        if rebound_ok:
            post_add = 1.15
        elif nocturnal_rebreak_ok:
            # Night-time reheat usually supports only modest re-break space.
            post_add = 0.70
        else:
            post_add = 0.55
            if h_since_obs_peak is not None and h_since_obs_peak >= 4.0:
                post_add = 0.35
            if cloudy_now:
                post_add -= 0.10
            if wet_now:
                post_add -= 0.10
            if t_cons <= 0.05:
                post_add -= 0.05
            post_add = max(0.20, post_add)

        post_cap_hi = float(obs_max) + post_add

        # Programmatic re-break feasibility gate:
        # when the prospective secondary-peak model itself does not challenge obs max,
        # or current weather remains wet/cloudy with weak slope, cap aggressively.
        try:
            pf_peak = float(metar_diag.get("post_focus_peak_temp_c")) if metar_diag.get("post_focus_peak_temp_c") is not None else None
        except Exception:
            pf_peak = None
        if pf_peak is not None and pf_peak <= float(obs_max) - 0.4:
            post_cap_hi = min(post_cap_hi, float(obs_max) + 0.25)
        if bool(metar_diag.get("post_focus_window_active")) and (wet_now or cloudy_now) and t_cons <= 0.2:
            post_cap_hi = min(post_cap_hi, float(obs_max) + 0.25)

        if t_now is not None:
            post_cap_hi = max(post_cap_hi, t_now + 0.15)

        hi = min(hi, post_cap_hi)
        lo = min(lo, hi - 0.2)

    # Far-window cold-advection sanity cap: avoid over-projecting short-term rebound
    # only when peak is not far away. (Do NOT hard-cap early-morning cases with long daytime heating runway.)
    if phase_now == "far" and "冷平流" in line850:
        try:
            wdir_now = float(metar_diag.get("latest_wdir")) if metar_diag.get("latest_wdir") not in (None, "", "VRB") else None
        except Exception:
            wdir_now = None
        northerly = (wdir_now is not None) and ((wdir_now >= 300.0) or (wdir_now <= 60.0))
        if northerly and b_cons <= 0.3:
            try:
                peak_local_txt = str(primary_window.get("peak_local") or "")
                latest_local_txt = str(metar_diag.get("latest_report_local") or "")
                hleft = _hours_between_iso(peak_local_txt, latest_local_txt, nonneg=True)
                if hleft is None:
                    hleft = 2.0
            except Exception:
                hleft = 2.0
            try:
                t_now = float(metar_diag.get("latest_temp"))
            except Exception:
                t_now = None

            # apply cap only when relatively close to peak; skip long-runway warming periods
            apply_far_cap = bool(hleft <= 4.5)
            if hleft >= 6.0:
                apply_far_cap = False

            if t_now is not None and apply_far_cap:
                rise_cap = min(5.2, 1.8 + 0.60 * hleft)
                far_cap_hi = t_now + rise_cap
                if obs_max is not None:
                    far_cap_hi = max(far_cap_hi, float(obs_max) + 0.6)
                hi = min(hi, far_cap_hi)
                lo = min(lo, hi - 0.2)

    # Physical consistency: daily Tmax cannot be below observed-bin lower edge.
    if obs_floor is not None:
        lo = max(lo, float(obs_floor))
        hi = max(hi, lo + 0.2)

    def _soft_snap(v: float) -> float:
        iv = round(v)
        if abs(v - iv) <= 0.12:
            return float(iv)
        return round(v, 1)

    def _enforce_obs_floor_range(lo_v: float, hi_v: float, min_span: float = 0.2) -> tuple[float, float]:
        lo2 = float(lo_v)
        hi2 = max(float(hi_v), lo2 + float(min_span))
        if obs_floor is not None:
            lo2 = max(lo2, float(obs_floor))
            hi2 = max(hi2, lo2 + float(min_span))
        return lo2, hi2

    lo, hi = _enforce_obs_floor_range(lo, hi, 0.2)
    lo = _soft_snap(lo)
    hi = _soft_snap(max(hi, lo + 0.2))
    lo, hi = _enforce_obs_floor_range(lo, hi, 0.2)

    # Compact settled-post mode:
    # when peak window is clearly over and re-heating evidence is weak, output a concise
    # observed-max-anchored range (instead of extended conditional branching).
    compact_settled_mode = False
    h_after_end_compact = None
    try:
        latest_local_txt = str(metar_diag.get("latest_report_local") or "")
        end_local_txt = str(primary_window.get("end_local") or "")
        h_after_end_compact = _hours_between_iso(latest_local_txt, end_local_txt, nonneg=False)
    except Exception:
        h_after_end_compact = None

    warm_reheat_hint = bool(("暖平流" in line850) and (b_cons >= 0.75) and (t_cons >= 0.30))
    compact_wait_h = 0.6
    try:
        cad_min = float(metar_diag.get("metar_routine_cadence_min")) if metar_diag.get("metar_routine_cadence_min") is not None else None
    except Exception:
        cad_min = None
    if cad_min is not None and cad_min > 0:
        compact_wait_h = max(0.40, min(0.95, (cad_min / 60.0) * 0.90))

    decisive_hourly_report = False
    try:
        t_step_latest2 = float(metar_diag.get("temp_trend_1step_c")) if metar_diag.get("temp_trend_1step_c") is not None else 0.0
    except Exception:
        t_step_latest2 = 0.0
    if (
        cad_min is not None
        and cad_min >= 50.0
        and h_after_end_compact is not None
        and h_after_end_compact >= max(0.35, compact_wait_h * 0.65)
        and clear_sky_stable
        and abs(t_step_latest2) <= 0.12
        and (not bool(metar_diag.get("metar_speci_active")))
        and (not bool(metar_diag.get("metar_speci_likely")))
        and (not bool(metar_diag.get("nocturnal_reheat_signal")))
        and (not warm_reheat_hint)
        and precip_state not in {"moderate", "heavy", "convective"}
    ):
        decisive_hourly_report = True

    metar_diag["decisive_hourly_report"] = bool(decisive_hourly_report)
    clear_sky_settled = False
    h_since_obs_peak = None
    t_now_compact = None
    if obs_max is not None and clear_sky_stable and phase_now in {"near_window", "in_window", "post"}:
        try:
            latest_local_txt = str(metar_diag.get("latest_report_local") or "")
            obs_peak_local_txt = str(metar_diag.get("observed_max_time_local") or "")
            h_since_obs_peak = _hours_between_iso(latest_local_txt, obs_peak_local_txt, nonneg=True)
        except Exception:
            h_since_obs_peak = None
        try:
            t_now_compact = float(metar_diag.get("latest_temp")) if metar_diag.get("latest_temp") is not None else None
        except Exception:
            t_now_compact = None
        if (
            h_since_obs_peak is not None
            and h_since_obs_peak >= 0.40
            and t_now_compact is not None
            and t_now_compact <= float(obs_max) - 0.4
            and t_cons <= min(rt_weak, 0.12)
            and (t_acc is None or t_acc <= 0.0)
            and (not bool(metar_diag.get("nocturnal_reheat_signal")))
            and (not warm_reheat_hint)
            and (not bool(metar_diag.get("metar_speci_active")))
            and (not bool(metar_diag.get("metar_speci_likely")))
            and precip_state not in {"moderate", "heavy", "convective"}
            and cloud_code_now not in {"BKN", "OVC", "VV"}
        ):
            clear_sky_settled = True
    metar_diag["clear_sky_settled"] = bool(clear_sky_settled)

    if (
        obs_max is not None
        and h_after_end_compact is not None
        and h_after_end_compact >= compact_wait_h
        and t_cons <= rt_weak
        and (t_acc is None or t_acc <= 0.05)
        and (not bool(metar_diag.get("nocturnal_reheat_signal")))
        and (not warm_reheat_hint)
        and (not bool(metar_diag.get("metar_speci_likely")))
        and precip_state not in {"moderate", "heavy", "convective"}
        and daily_peak_state == "locked"
    ):
        compact_settled_mode = True
    elif decisive_hourly_report and obs_max is not None and daily_peak_state == "locked":
        compact_settled_mode = True
    elif clear_sky_settled and obs_max is not None and daily_peak_state == "locked":
        compact_settled_mode = True

    core_lo, core_hi, disp_lo, disp_hi, historical_blend = apply_historical_reference(
        metar_diag=metar_diag,
        phase_now=phase_now,
        compact_settled_mode=compact_settled_mode,
        core_lo=float(lo),
        core_hi=float(hi),
        disp_lo=float(lo),
        disp_hi=float(hi),
    )

    if bool(metar_diag.get("post_focus_window_active")) and syn_w:
        post_mode = str(metar_diag.get("post_window_mode") or "")
        window_label = "潜在二峰窗" if post_mode != "no_rebreak_eval" else "后段验证窗"
        window_txt = f"{_hm(syn_w.get('start_local'))}~{_hm(syn_w.get('end_local'))} Local"
    else:
        window_label = "峰值窗"
        window_txt = f"{_hm(primary_window.get('start_local'))}~{_hm(primary_window.get('end_local'))} Local"
    cloud_code = str(metar_diag.get("latest_cloud_code") or "").upper()
    tail_cond = _range_tail_note(
        skew=skew,
        cloud_code=cloud_code,
        advection_review=advection_review,
        temp_state=temp_state,
    )

    core_lo = float(core_lo)
    core_hi = float(core_hi)
    disp_lo = float(disp_lo)
    disp_hi = float(disp_hi)

    if skew >= 0.20:
        tail_ext = min(0.8, 0.4 + 0.3 * max(0.0, skew))
        if strict_late_cap:
            tail_ext = min(tail_ext, 0.18)
        if clear_sky_stable and phase_now in {"near_window", "in_window"} and t_cons <= 0.15:
            tail_ext = min(tail_ext, 0.45)
        if phase_now in {"near_window", "in_window"} and low_cloud_peak >= 70 and t_cons <= 0.20:
            tail_ext = min(tail_ext, 0.25)
        if precip_cooling:
            tail_ext = min(tail_ext, 0.20)
        elif precip_residual:
            tail_ext = min(tail_ext, 0.24)
        tail_hi = _soft_snap(hi + tail_ext)
        disp_hi = tail_hi
    elif skew <= -0.20:
        tail_lo = _soft_snap(max(lo - min(0.8, 0.4 + 0.3 * max(0.0, -skew)), lo - 1.0))
        disp_lo = tail_lo

    disp_lo, disp_hi = _enforce_obs_floor_range(float(disp_lo), float(disp_hi), 0.2)
    core_lo, core_hi = _enforce_obs_floor_range(float(core_lo), float(core_hi), 0.2)
    settled_range: dict[str, Any] | None = None
    if compact_settled_mode and obs_max is not None:
        if (
            obs_floor is not None
            and obs_ceil is not None
            and (obs_ceil - obs_floor) >= 0.30
        ):
            # Quantized METAR anchor: e.g. observed -5°C means approx [-5.5, -4.51]°C.
            settle_lo = _soft_snap(float(obs_floor))
            settle_hi = _soft_snap(max(settle_lo + 0.10, float(obs_ceil)))
        else:
            settle_lo = _soft_snap(max(float(obs_floor if obs_floor is not None else obs_max), lo))
            settle_up = 0.25
            if ("暖平流" in line850) and b_cons >= 0.55:
                settle_up = 0.35
            if bool(metar_diag.get("nocturnal_reheat_signal")):
                settle_up = max(settle_up, 0.40)
            if bool(metar_diag.get("clear_sky_settled")):
                settle_up = min(settle_up, 0.18)
            settle_hi = _soft_snap(min(hi, max(settle_lo + 0.10, float(obs_max) + settle_up)))

        settle_lo, settle_hi = _enforce_obs_floor_range(float(settle_lo), float(settle_hi), 0.10)

        settle_reason = "按已观测最高温锚定；峰值窗已过"
        if bool(metar_diag.get("clear_sky_settled")) and phase_now in {"near_window", "in_window"}:
            settle_reason = "按已观测最高温锚定；晴空辐射主导下峰值基本确认"
        settled_range = {
            "active": True,
            "lo": float(settle_lo),
            "hi": float(settle_hi),
            "reason": settle_reason,
        }

    annotations: list[str] = []
    if (settled_range is None) and use_early_peak_wording:
        annotations.append("- 注：当前更像已先出现早峰，短线动能转弱；全天是否锁定仍待后续实况确认。")
    analysis_window_mode = str(metar_diag.get("analysis_window_mode") or "")
    if analysis_window_mode == "obs_plateau_reanchor":
        annotations.append("- 注：已按实况横盘重锚峰值窗，未直接沿用模型晚段尾部。")
    if bool(metar_diag.get("obs_correction_applied")):
        annotations.append("- 注：已应用实况纠偏（模型峰值偏低，窗口锚定到当日实况峰值时段）。")
    if should_discuss_second_peak and multi_peak_evidence_level in {"moderate", "strong"}:
        annotations.append("- 注：当前路径更偏分离式多峰，后段仍需防次峰改写前高。")
    historical_reference = build_peak_historical_reference(
        metar_diag=metar_diag,
        historical_blend=historical_blend,
        unit=unit,
        fmt_delta_unit=_fmt_delta_unit,
    )

    skew_bucket = "neutral"
    if skew >= 0.20:
        skew_bucket = "upper_tail"
    elif skew <= -0.20:
        skew_bucket = "lower_tail"

    return {
        "observed": {
            "max_temp_c": obs_max,
            "interval_lo_c": obs_floor,
            "interval_hi_c": obs_ceil,
        },
        "gate": gate,
        "phase_now": phase_now,
        "confidence": {
            "low_conf_far": low_conf_far,
            "compact_settled_mode": compact_settled_mode,
        },
        "consistency": {
            "cloud_code": cloud_code,
            "temp_trend_consistency_c": t_cons,
            "temp_bias_consistency_c": b_cons,
        },
        "ranges": {
            "display": {"lo": float(disp_lo), "hi": float(disp_hi)},
            "core": {"lo": float(core_lo), "hi": float(core_hi)},
            "settled": settled_range or {"active": False},
            "window": {"label": window_label, "text": window_txt},
            "tail_note": tail_cond,
            "skew_bucket": skew_bucket,
        },
        "annotations": annotations,
        "historical_reference": historical_reference,
        "historical_blend": historical_blend,
        "temp_phase_decision": temp_state,
    }
