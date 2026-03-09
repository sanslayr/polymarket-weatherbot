from __future__ import annotations

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

    if not items:
        items.append("本时次探空有效信号有限，优先跟踪实况斜率")

    return {
        "items": items,
        "path_bias": str((obs_context or {}).get("path_bias") or ("高位再试探" if (temp_bias is not None and temp_bias > 0) else "高位收敛")),
        "thermo": thermo,
    }
