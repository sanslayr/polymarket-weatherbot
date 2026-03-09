from __future__ import annotations

import math
from typing import Any

from boundary_layer_regime import build_model_sounding_proxy


def _fmt_temp_unit(value_c: Any, unit: str) -> str:
    try:
        v = float(value_c)
    except Exception:
        return str(value_c)
    if str(unit).upper() == "F":
        return f"{(v * 9.0 / 5.0 + 32.0):.1f}°F"
    return f"{v:.1f}°C"


def _fmt_delta_unit(value_c: Any, unit: str, force_sign: bool = False) -> str:
    try:
        v = float(value_c)
    except Exception:
        return str(value_c)
    if str(unit).upper() == "F":
        out = v * 9.0 / 5.0
        return f"{out:+.1f}°F" if force_sign else f"{out:.1f}°F"
    return f"{v:+.1f}°C" if force_sign else f"{v:.1f}°C"


def _present(value: Any) -> bool:
    return value not in (None, "", [], {})


def _build_profile_coverage(thermo: dict[str, Any]) -> dict[str, Any]:
    humidity_levels = [level for level, key in (("925", "rh925_pct"), ("850", "rh850_pct"), ("700", "rh700_pct")) if _present(thermo.get(key))]
    wind_levels = [level for level, key in (("925", "wind925_kt"), ("850", "wind850_kt"), ("700", "wind700_kt")) if _present(thermo.get(key))]
    thermal_gradients = []
    if _present(thermo.get("t925_t850_c")):
        thermal_gradients.append("925-850")
    if _present(thermo.get("midlevel_rh_pct")):
        thermal_gradients.append("850-700-rh")

    convective_metrics = [
        key
        for key in ("sbcape_jkg", "mlcape_jkg", "mucape_jkg", "sbcin_jkg", "mlcin_jkg", "lcl_m", "lfc_m", "el_m")
        if _present(thermo.get(key))
    ]
    core_axes = 0
    if humidity_levels:
        core_axes += 1
    if wind_levels:
        core_axes += 1
    if thermal_gradients:
        core_axes += 1
    if convective_metrics:
        core_axes += 1

    density_class = "sparse"
    if len(humidity_levels) >= 3 and len(wind_levels) >= 3 and _present(thermo.get("t925_t850_c")) and len(convective_metrics) >= 2:
        density_class = "rich"
    elif core_axes >= 3 or (len(humidity_levels) >= 2 and len(wind_levels) >= 2):
        density_class = "moderate"

    missing_recommendations: list[str] = []
    if len(humidity_levels) < 3:
        missing_recommendations.append("补足925/850/700湿度层")
    if not _present(thermo.get("t925_t850_c")):
        missing_recommendations.append("补低层温度梯度")
    if len(wind_levels) < 3:
        missing_recommendations.append("补足925/850/700风层")
    if density_class != "rich":
        missing_recommendations.append("增加1000/950/900/800层以识别浅逆温与相变层")

    return {
        "humidity_levels": humidity_levels,
        "wind_levels": wind_levels,
        "thermal_gradients": thermal_gradients,
        "convective_metrics": convective_metrics,
        "density_class": density_class,
        "core_axes_count": core_axes,
        "recommendations": missing_recommendations,
    }


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _turning_deg(lower_deg: Any, upper_deg: Any) -> float | None:
    lower = _safe_float(lower_deg)
    upper = _safe_float(upper_deg)
    if lower is None or upper is None:
        return None
    return float(((upper - lower + 540.0) % 360.0) - 180.0)


def _build_layer_relationships(thermo: dict[str, Any]) -> dict[str, Any]:
    rh925 = _safe_float(thermo.get("rh925_pct"))
    rh850 = _safe_float(thermo.get("rh850_pct"))
    rh700 = _safe_float(thermo.get("rh700_pct"))
    mid_rh = _safe_float(thermo.get("midlevel_rh_pct"))
    t925_t850 = _safe_float(thermo.get("t925_t850_c"))
    w925 = _safe_float(thermo.get("wind925_kt"))
    w850 = _safe_float(thermo.get("wind850_kt"))
    w700 = _safe_float(thermo.get("wind700_kt"))
    d925 = _safe_float(thermo.get("wind925_dir_deg"))
    d850 = _safe_float(thermo.get("wind850_dir_deg"))
    d700 = _safe_float(thermo.get("wind700_dir_deg"))
    low_cap = _safe_float(thermo.get("low_level_cap_score")) or 0.0
    mixing_support = _safe_float(thermo.get("mixing_support_score")) or 0.0

    thermal_structure = "unknown"
    if t925_t850 is not None:
        if t925_t850 >= 2.5:
            thermal_structure = "capped"
        elif t925_t850 >= 1.2:
            thermal_structure = "weak_cap"
        elif t925_t850 <= 0.4:
            thermal_structure = "well_mixed"
        else:
            thermal_structure = "transitional"

    if mid_rh is None:
        mid_candidates = [value for value in (rh850, rh700) if value is not None]
        if mid_candidates:
            mid_rh = float(sum(mid_candidates) / len(mid_candidates))

    moisture_layering = "unknown"
    if rh925 is not None and mid_rh is not None:
        if rh925 >= 75.0 and mid_rh <= 50.0:
            moisture_layering = "low_moist_mid_dry"
        elif rh925 >= 75.0 and mid_rh >= 70.0:
            moisture_layering = "deep_moist"
        elif rh925 <= 55.0 and mid_rh <= 45.0:
            moisture_layering = "deep_dry"
        elif rh925 <= 60.0 and mid_rh >= 70.0:
            moisture_layering = "elevated_moist_layer"
        else:
            moisture_layering = "mixed_layering"

    turn_925_850 = _turning_deg(d925, d850)
    turn_850_700 = _turning_deg(d850, d700)
    shear_925_850 = None if (w925 is None or w850 is None) else float(w850 - w925)
    shear_850_700 = None if (w850 is None or w700 is None) else float(w700 - w850)

    wind_turning_state = "unknown"
    available_turns = [value for value in (turn_925_850, turn_850_700) if value is not None]
    if len(available_turns) >= 2:
        if all(value >= 25.0 for value in available_turns):
            wind_turning_state = "veering_with_height"
        elif all(value <= -25.0 for value in available_turns):
            wind_turning_state = "backing_with_height"
        elif all(abs(value) <= 20.0 for value in available_turns):
            wind_turning_state = "vertically_aligned"
        else:
            wind_turning_state = "layered_turning"
    elif len(available_turns) == 1:
        if available_turns[0] >= 25.0:
            wind_turning_state = "veering_with_height"
        elif available_turns[0] <= -25.0:
            wind_turning_state = "backing_with_height"
        elif abs(available_turns[0]) <= 20.0:
            wind_turning_state = "vertically_aligned"
        else:
            wind_turning_state = "layered_turning"

    coupling_chain_state = "partial"
    if thermal_structure == "unknown" and not available_turns:
        if low_cap >= 0.65:
            coupling_chain_state = "decoupled"
        elif mixing_support >= 0.65:
            coupling_chain_state = "coupled"
    else:
        positive_shear = [value for value in (shear_925_850, shear_850_700) if value is not None and value >= 4.0]
        if thermal_structure in {"well_mixed", "transitional"} and wind_turning_state in {"vertically_aligned", "veering_with_height"} and positive_shear:
            coupling_chain_state = "coupled"
        elif thermal_structure in {"capped", "weak_cap"} and (wind_turning_state in {"backing_with_height", "layered_turning"} or low_cap >= 0.65):
            coupling_chain_state = "decoupled"

    findings: list[str] = []
    if moisture_layering == "low_moist_mid_dry":
        findings.append("低层湿层上接中层干层，若后续开云，低云侵蚀速度可能加快。")
    elif moisture_layering == "deep_moist":
        findings.append("低层到中层湿层较连贯，云层维持不只局限于近地层。")
    elif moisture_layering == "elevated_moist_layer":
        findings.append("低层相对较干但上方湿层偏明显，云量变化更易受中层湿层控制。")
    elif moisture_layering == "deep_dry":
        findings.append("低层到中层整体偏干，若辐射放开，增温效率更容易被放大。")

    if coupling_chain_state == "coupled":
        if wind_turning_state == "veering_with_height":
            findings.append("925–700hPa风向随高度顺转且风速增强，层间输送链条较完整。")
        elif wind_turning_state == "vertically_aligned":
            findings.append("925–700hPa风向较一致，低层到中层输送方向较统一。")
        else:
            findings.append("925–700hPa层间耦合较顺，低层与中层信号更容易联动。")
    elif coupling_chain_state == "decoupled":
        findings.append("925–700hPa层间耦合偏弱，低层与中层信号未必能直接下传到地面。")

    return {
        "thermal_structure": thermal_structure,
        "moisture_layering": moisture_layering,
        "wind_turning_state": wind_turning_state,
        "coupling_chain_state": coupling_chain_state,
        "turning_925_850_deg": round(turn_925_850, 1) if turn_925_850 is not None else None,
        "turning_850_700_deg": round(turn_850_700, 1) if turn_850_700 is not None else None,
        "speed_shear_925_850_kt": round(shear_925_850, 1) if shear_925_850 is not None else None,
        "speed_shear_850_700_kt": round(shear_850_700, 1) if shear_850_700 is not None else None,
        "findings": findings[:2],
    }


def diagnose_sounding(
    primary_window: dict[str, Any],
    metar_diag: dict[str, Any],
    *,
    temp_unit: str = "C",
    obs_context: dict[str, Any] | None = None,
    h700_summary: str = "",
    h925_summary: str = "",
    cloud_code_now: str = "",
) -> dict[str, Any]:
    t850 = primary_window.get("t850_c")
    w850 = primary_window.get("w850_kmh")
    cloud = primary_window.get("low_cloud_pct")
    temp_bias = metar_diag.get("temp_bias_c")

    items: list[str] = []

    if w850 is not None:
        if w850 >= 55:
            items.append(f"边界层混合偏强（W850≈{w850:.1f} km/h）")
        elif w850 <= 15:
            items.append(f"边界层混合偏弱（W850≈{w850:.1f} km/h）")

    if cloud is not None:
        if cloud >= 70:
            items.append(f"低云偏多（CloudLow≈{cloud:.0f}%），上沿易受压")
        elif cloud <= 20:
            items.append(f"低云较少（CloudLow≈{cloud:.0f}%），辐射增温条件较好")

    if t850 is not None:
        items.append(f"850层热力背景：T850≈{_fmt_temp_unit(t850, temp_unit)}")

    if temp_bias is not None:
        if temp_bias >= 1.0:
            items.append(f"实况较模型偏暖（{_fmt_delta_unit(temp_bias, temp_unit, force_sign=True)}），短临上沿可上修")
        elif temp_bias <= -1.0:
            items.append(f"实况较模型偏冷（{_fmt_delta_unit(temp_bias, temp_unit, force_sign=True)}），短临上沿需下修")

    thermo = {
        "has_profile": False,
        "quality": "missing_profile",
        "profile_source": "model",
        "use_sounding_obs": False,
        "sounding_confidence": "L",
        "obs_age_hours": None,
        "is_proxy_station": False,
        "distance_km": None,
        "layer_findings": [],
        "relationship_findings": [],
        "actionable": "",
        "sbcape_jkg": primary_window.get("sbcape_jkg"),
        "mlcape_jkg": primary_window.get("mlcape_jkg"),
        "mucape_jkg": primary_window.get("mucape_jkg"),
        "sbcin_jkg": primary_window.get("sbcin_jkg"),
        "mlcin_jkg": primary_window.get("mlcin_jkg"),
        "lcl_m": primary_window.get("lcl_m"),
        "lfc_m": primary_window.get("lfc_m"),
        "el_m": primary_window.get("el_m"),
    }
    if isinstance(obs_context, dict) and bool(obs_context.get("use_sounding_obs")):
        obs_thermo = dict(obs_context.get("thermo") or {})
        if obs_thermo:
            thermo.update(obs_thermo)
        thermo["profile_source"] = "obs"
        thermo["use_sounding_obs"] = True
        thermo["sounding_confidence"] = str(obs_context.get("confidence") or "L")
        thermo["obs_age_hours"] = obs_context.get("obs_age_hours")
        thermo["is_proxy_station"] = bool(obs_context.get("is_proxy_station"))
        thermo["distance_km"] = obs_context.get("distance_km")
        thermo["layer_findings"] = list(obs_context.get("layer_findings") or [])
        thermo["actionable"] = str(obs_context.get("actionable") or "")

        for finding in thermo["layer_findings"][:3]:
            txt = str(finding).strip()
            if txt:
                items.append(f"探空分层：{txt}")
        if thermo.get("actionable"):
            items.append(f"探空提示：{thermo['actionable']}")
    else:
        proxy_thermo = build_model_sounding_proxy(
            primary_window,
            metar_diag,
            h700_summary=h700_summary,
            h925_summary=h925_summary,
            cloud_code_now=cloud_code_now,
        )
        thermo.update(proxy_thermo)
        thermo["quality"] = str(thermo.get("quality") or "model_proxy")
        thermo["profile_source"] = str(thermo.get("profile_source") or "model_proxy")
        thermo["layer_findings"] = list(proxy_thermo.get("layer_findings") or [])
        thermo["actionable"] = str(proxy_thermo.get("actionable") or "")

        for finding in thermo["layer_findings"][:2]:
            txt = str(finding).strip()
            if txt:
                items.append(f"模式层结：{txt}")
        if thermo.get("actionable"):
            items.append(f"层结提示：{thermo['actionable']}")

    profile_value_keys = {
        "sbcape_jkg",
        "mlcape_jkg",
        "mucape_jkg",
        "sbcin_jkg",
        "mlcin_jkg",
        "lcl_m",
        "lfc_m",
        "el_m",
        "rh925_pct",
        "rh850_pct",
        "rh700_pct",
        "t925_t850_c",
        "midlevel_rh_pct",
        "wind925_kt",
        "wind850_kt",
        "wind700_kt",
        "low_level_cap_score",
        "low_level_mix_score",
        "midlevel_dry_score",
        "midlevel_moist_score",
        "wind_profile_mix_score",
        "mixing_support_score",
        "suppression_score",
    }
    if any(thermo.get(k) is not None for k in profile_value_keys):
        thermo["has_profile"] = True
        if str(thermo.get("quality") or "") in {"", "missing_profile"}:
            thermo["quality"] = "ok"
        capev = thermo.get("sbcape_jkg") or thermo.get("mlcape_jkg") or thermo.get("mucape_jkg")
        cinv = thermo.get("sbcin_jkg") if thermo.get("sbcin_jkg") is not None else thermo.get("mlcin_jkg")
        if capev is not None:
            items.append(f"探空热力：CAPE≈{float(capev):.0f} J/kg")
        if cinv is not None:
            items.append(f"探空抑制：CIN≈{float(cinv):.0f} J/kg")

    layer_relationships = _build_layer_relationships(thermo)
    thermo["layer_relationships"] = layer_relationships
    thermo["relationship_findings"] = list(layer_relationships.get("findings") or [])
    thermo["coverage"] = _build_profile_coverage(thermo)

    for finding in thermo["relationship_findings"][:1]:
        txt = str(finding).strip()
        if txt:
            if thermo.get("profile_source") == "obs":
                items.append(f"探空层间关系：{txt}")
            else:
                items.append(f"层间关系：{txt}")

    if not items:
        items.append("本时次探空有效信号有限，优先跟踪实况斜率")

    return {
        "items": items,
        "path_bias": str((obs_context or {}).get("path_bias") or ("高位再试探" if (temp_bias is not None and temp_bias > 0) else "高位收敛")),
        "thermo": thermo,
    }
