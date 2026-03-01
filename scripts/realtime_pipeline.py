from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any


def _dt(s: Any) -> datetime | None:
    try:
        d = datetime.fromisoformat(str(s))
        if d.tzinfo is not None:
            d = d.replace(tzinfo=None)
        return d
    except Exception:
        return None


def classify_window_phase(primary_window: dict[str, Any], metar_diag: dict[str, Any]) -> dict[str, Any]:
    model_start = _dt(primary_window.get("start_local"))
    model_end = _dt(primary_window.get("end_local"))
    model_peak = _dt(primary_window.get("peak_local"))
    obs_peak = _dt(metar_diag.get("observed_max_time_local"))
    latest = _dt(metar_diag.get("latest_report_local")) or model_peak or model_start

    if not (model_start and model_end and model_peak and latest):
        return {"phase": "unknown"}

    # only trust observed max as peak anchor when it is close to model window; avoid early-day false locking
    obs_near_window = bool(obs_peak and (obs_peak >= (model_start - timedelta(hours=2))))
    anchor_peak = obs_peak if obs_near_window else model_peak

    start_fused = min(model_start, (anchor_peak - timedelta(hours=1)) if anchor_peak else model_start)
    extra_h = 2 if (metar_diag.get("temp_trend_1step_c") or 0.0) >= 0.3 else 1
    end_fused = max(model_end, (anchor_peak + timedelta(hours=extra_h)) if anchor_peak else model_end)
    peak_fused = anchor_peak or model_peak

    # hard rule: two-step peak lock confirmation can collapse to post-window
    lock_confirmed = bool(metar_diag.get("peak_lock_confirmed"))
    if lock_confirmed and latest > peak_fused:
        phase = "post"
    else:
        d_h = abs((latest - peak_fused).total_seconds()) / 3600.0
        if latest > (end_fused + timedelta(hours=1)):
            phase = "post"
        elif d_h <= 1:
            phase = "in_window"
        elif d_h <= 3:
            phase = "near_window"
        else:
            phase = "far"

    return {
        "phase": phase,
        "start_fused": start_fused.isoformat(timespec="minutes"),
        "peak_fused": peak_fused.isoformat(timespec="minutes"),
        "end_fused": end_fused.isoformat(timespec="minutes"),
    }


def select_realtime_triggers(primary_window: dict[str, Any], metar_diag: dict[str, Any]) -> list[str]:
    phase = classify_window_phase(primary_window, metar_diag).get("phase", "unknown")
    t_tr = float(metar_diag.get("temp_trend_1step_c") or 0.0)
    p_tr = float(metar_diag.get("pressure_trend_1step_hpa") or 0.0)
    bias = float(metar_diag.get("temp_bias_c") or 0.0)
    wind_chg = float(metar_diag.get("wind_dir_change_deg") or 0.0)
    cloud_tr = str(metar_diag.get("cloud_trend") or "")

    peak_lock = (t_tr <= -0.3 and ("增加" in cloud_tr or "回补" in cloud_tr) and wind_chg >= 20)
    upside = (t_tr >= 0.3 and ("开窗" in cloud_tr or "减弱" in cloud_tr) and bias >= 0.8)
    shift = (wind_chg >= 45) or ("回补" in cloud_tr) or ("开窗" in cloud_tr)
    downside = (t_tr <= -0.6 and p_tr >= 0.8)

    out: list[str] = []
    if phase in {"near_window", "in_window"}:
        if peak_lock:
            out.append("• 温度转负 + 云层回补 + 风场转差 → 高点基本定局，后续以上沿回落为主。")
        if upside:
            out.append("• 升温斜率维持正值且云层开窗 → 最高温上沿存在小幅上修风险。")
        if shift:
            out.append("• 风向/云量节奏出现相变 → 峰值时段可能前移或后移。")
        if downside:
            out.append("• 短时降温叠加压升信号 → 下沿下修与提前封顶风险上升。")
    elif phase == "post":
        if peak_lock or t_tr <= 0:
            out.append("• 窗口后阶段：高点大概率已定，后续以回落或横盘为主。")
    else:
        # far/unknown: keep minimal
        if shift and ("开窗" in cloud_tr or "回补" in cloud_tr):
            out.append("• 临窗前提示：云量/风向节奏已变化，后续请重点盯窗口期触发。")

    return out[:3]
