from __future__ import annotations

import json
from pathlib import Path
from threading import Lock
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
PARAM_PATH = ROOT / "config" / "tmax_learning_params.json"

_DEFAULTS: dict[str, Any] = {
    "metar_radiation": {
        "cloud_cover_map": {
            "CLR": 0.0,
            "CAVOK": 0.0,
            "SKC": 0.0,
            "FEW": 0.20,
            "SCT": 0.45,
            "BKN": 0.75,
            "OVC": 1.00,
            "VV": 1.00,
            "UNKNOWN": 0.45,
        },
        "cloud_base_weight": {
            "lt_2500": 1.0,
            "lt_7000": 0.75,
            "lt_15000": 0.45,
            "ge_15000": 0.25,
            "unknown": 0.65,
        },
        "layer_gamma": [1.0, 0.55, 0.30, 0.20],
        "transmittance": {
            "factor": 0.85,
            "power": 1.2,
            "floor": 0.12,
        },
        "wx_transmittance": {
            "TS": 0.45,
            "FG": 0.60,
            "OBSCURATION": 0.90,
            "HEAVY_PRECIP": 0.55,
            "LIGHT_PRECIP": 0.75,
            "PRECIP": 0.60,
            "DEFAULT": 1.00,
        },
    },
    "rounded_top": {
        "temp_accel_neg_threshold": -0.25,
        "flat_trend_threshold": 0.12,
        "weak_trend_threshold": 0.22,
        "near_peak_hours": 1.8,
        "near_end_hours": 1.0,
        "solar_stalling_slope": 0.012,
        "solar_strong_rise_slope": 0.030,
        "rad_low_threshold": 0.55,
        "rad_recover_threshold": 0.72,
        "rad_recover_trend": 0.025,
    },
    "nocturnal_rewarm": {
        "night_solar_max": 0.08,
        "night_hour_start": 17.5,
        "night_hour_end": 7.0,
        "warm_advection_bias_min": 0.45,
        "wind_speed_jump_kt": 3.0,
        "wind_speed_mix_min_kt": 7.0,
        "dewpoint_rise_min_c": 0.8,
        "pressure_fall_min_hpa": -0.6,
        "score_min": 1.5,
    },
    "market_labels": {
        "best_min_coverage": 0.60,
        "best_lead_min": 0.05,
        "best_lead_low_cov_add": 0.015,
        "best_lead_phase_add": 0.01,
        "best_lead_low_conf_add": 0.015,
        "best_weather_min": 0.30,
        "best_weather_low_cov_add": 0.02,
        "best_weather_low_conf_add": 0.02,
        "alpha_min_coverage": 0.50,
        "rebreak_t_cons_min": 0.35,
        "rebreak_b_cons_min": 0.45,
        "rebreak_near_t_cons_min": 0.45,
        "alpha_cheap_ask_max": 0.14,
        "alpha_cheap_spread_max": 0.08,
        "alpha_cheap_weather_min": 0.14,
        "alpha_cheap_score_min": 0.24,
        "alpha_mid_ask_max": 0.18,
        "alpha_mid_spread_max": 0.06,
        "alpha_mid_weather_min": 0.48,
        "alpha_mid_score_min": 0.32,
    },
}

_CACHE_LOCK = Lock()
_CACHE_MTIME: float | None = None
_CACHE_VALUE: dict[str, Any] | None = None


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def load_tmax_learning_params() -> dict[str, Any]:
    global _CACHE_MTIME, _CACHE_VALUE
    try:
        mtime = PARAM_PATH.stat().st_mtime
    except Exception:
        mtime = None

    with _CACHE_LOCK:
        if _CACHE_VALUE is not None and _CACHE_MTIME == mtime:
            return _CACHE_VALUE

        cfg: dict[str, Any] = {}
        if mtime is not None:
            try:
                cfg = json.loads(PARAM_PATH.read_text(encoding="utf-8"))
                if not isinstance(cfg, dict):
                    cfg = {}
            except Exception:
                cfg = {}

        merged = _deep_merge(_DEFAULTS, cfg)
        _CACHE_MTIME = mtime
        _CACHE_VALUE = merged
        return merged
