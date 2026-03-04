#!/usr/bin/env python3
"""Policy builder for Polymarket label gating and thresholds."""

from __future__ import annotations

from typing import Any


def _f(v: Any, default: float) -> float:
    try:
        x = float(v)
        return x
    except Exception:
        return float(default)


def build_market_label_policy(
    *,
    quality: dict[str, Any] | None,
    obj: dict[str, Any] | None,
    low_conf_far: bool,
    phase_now: str,
    metar_diag: dict[str, Any],
    t_cons: float,
    b_cons: float,
    compact_settled_mode: bool,
    policy_params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cfg = policy_params if isinstance(policy_params, dict) else {}

    q_cov = _f((quality or {}).get("synoptic_coverage"), -1.0)
    obj_conf = str((obj or {}).get("confidence") or "").lower()

    base_lead_min = _f(cfg.get("best_lead_min"), 0.05)
    base_weather_min = _f(cfg.get("best_weather_min"), 0.30)

    lead_min = base_lead_min
    weather_min = base_weather_min

    if 0.0 <= q_cov < 0.75:
        lead_min += _f(cfg.get("best_lead_low_cov_add"), 0.015)
        weather_min += _f(cfg.get("best_weather_low_cov_add"), 0.02)
    if phase_now in {"far", "post"}:
        lead_min += _f(cfg.get("best_lead_phase_add"), 0.01)
    if low_conf_far:
        lead_min += _f(cfg.get("best_lead_low_conf_add"), 0.015)
        weather_min += _f(cfg.get("best_weather_low_conf_add"), 0.02)

    allow_best = True
    if obj_conf in {"low", ""}:
        allow_best = False
    if 0.0 <= q_cov < _f(cfg.get("best_min_coverage"), 0.60):
        allow_best = False
    if low_conf_far:
        allow_best = False

    rebreak_evidence = bool(
        bool(metar_diag.get("nocturnal_reheat_signal"))
        or bool(metar_diag.get("metar_speci_active"))
        or bool(metar_diag.get("metar_speci_likely"))
        or (t_cons >= _f(cfg.get("rebreak_t_cons_min"), 0.35) and b_cons >= _f(cfg.get("rebreak_b_cons_min"), 0.45))
        or (phase_now in {"near_window", "in_window"} and t_cons >= _f(cfg.get("rebreak_near_t_cons_min"), 0.45))
    )

    settled_bias = bool(compact_settled_mode) or bool(metar_diag.get("decisive_hourly_report"))
    late_cap_no_reheat = bool(
        phase_now == "post"
        and bool(metar_diag.get("late_end_cap_applied"))
        and (not bool(metar_diag.get("nocturnal_reheat_signal")))
    )

    allow_alpha = True
    if obj_conf in {"low", ""} and phase_now in {"far", "post"} and (not rebreak_evidence):
        allow_alpha = False
    if 0.0 <= q_cov < _f(cfg.get("alpha_min_coverage"), 0.50) and (not rebreak_evidence):
        allow_alpha = False
    if low_conf_far and (not rebreak_evidence):
        allow_alpha = False
    if (settled_bias or late_cap_no_reheat) and (not rebreak_evidence):
        allow_alpha = False

    return {
        "allow_best_label": bool(allow_best),
        "allow_alpha_label": bool(allow_alpha),
        "best_lead_min": max(0.035, min(0.11, float(lead_min))),
        "best_weather_min": max(0.20, min(0.60, float(weather_min))),
        "alpha_cheap_ask_max": _f(cfg.get("alpha_cheap_ask_max"), 0.14),
        "alpha_cheap_spread_max": _f(cfg.get("alpha_cheap_spread_max"), 0.08),
        "alpha_cheap_weather_min": _f(cfg.get("alpha_cheap_weather_min"), 0.14),
        "alpha_cheap_score_min": _f(cfg.get("alpha_cheap_score_min"), 0.24),
        "alpha_mid_ask_max": _f(cfg.get("alpha_mid_ask_max"), 0.18),
        "alpha_mid_spread_max": _f(cfg.get("alpha_mid_spread_max"), 0.06),
        "alpha_mid_weather_min": _f(cfg.get("alpha_mid_weather_min"), 0.48),
        "alpha_mid_score_min": _f(cfg.get("alpha_mid_score_min"), 0.32),
    }

