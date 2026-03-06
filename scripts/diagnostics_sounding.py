from __future__ import annotations

from typing import Any


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


def diagnose_sounding(primary_window: dict[str, Any], metar_diag: dict[str, Any], *, temp_unit: str = "C") -> dict[str, Any]:
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

    # Thermodynamic diagnostics: consume explicit sounding-derived fields if upstream provides them.
    thermo = {
        "has_profile": False,
        "quality": "missing_profile",
        "sbcape_jkg": primary_window.get("sbcape_jkg"),
        "mlcape_jkg": primary_window.get("mlcape_jkg"),
        "mucape_jkg": primary_window.get("mucape_jkg"),
        "sbcin_jkg": primary_window.get("sbcin_jkg"),
        "mlcin_jkg": primary_window.get("mlcin_jkg"),
        "lcl_m": primary_window.get("lcl_m"),
        "lfc_m": primary_window.get("lfc_m"),
        "el_m": primary_window.get("el_m"),
    }
    if any(v is not None for k, v in thermo.items() if k not in {"has_profile", "quality"}):
        thermo["has_profile"] = True
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
        "path_bias": "高位再试探" if (temp_bias is not None and temp_bias > 0) else "高位收敛",
        "thermo": thermo,
    }
