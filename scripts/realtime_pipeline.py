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


def _fmt_temp_unit(value_c: Any, unit: str) -> str:
    try:
        v = float(value_c)
    except Exception:
        return str(value_c)
    if str(unit).upper() == "F":
        return f"{(v * 9.0 / 5.0 + 32.0):.1f}°F"
    return f"{v:.1f}°C"


def _fmt_delta_unit(delta_c: Any, unit: str) -> str:
    try:
        v = float(delta_c)
    except Exception:
        return str(delta_c)
    if str(unit).upper() == "F":
        return f"{(v * 9.0 / 5.0):.1f}°F"
    return f"{v:.1f}°C"


def classify_window_phase(primary_window: dict[str, Any], metar_diag: dict[str, Any]) -> dict[str, Any]:
    model_start = _dt(primary_window.get("start_local"))
    model_end = _dt(primary_window.get("end_local"))
    model_peak = _dt(primary_window.get("peak_local"))
    obs_peak = _dt(metar_diag.get("observed_max_time_local"))
    latest = _dt(metar_diag.get("latest_report_local")) or model_peak or model_start

    if not (model_start and model_end and model_peak and latest):
        return {"phase": "unknown"}

    # only trust observed max as peak anchor when it is close to model window AND thermally plausible.
    # This avoids early-day false anchoring that can collapse near/in-window logic.
    lock_confirmed = bool(metar_diag.get("peak_lock_confirmed"))
    try:
        obs_t = float(metar_diag.get("observed_max_temp_c")) if metar_diag.get("observed_max_temp_c") is not None else None
    except Exception:
        obs_t = None
    try:
        model_peak_t = float(primary_window.get("peak_temp_c")) if primary_window.get("peak_temp_c") is not None else None
    except Exception:
        model_peak_t = None

    obs_time_ok = bool(
        obs_peak
        and (obs_peak >= (model_start - timedelta(hours=2)))
        and (obs_peak <= (model_end + timedelta(hours=1)))
    )
    obs_temp_ok = bool(
        (obs_t is not None)
        and (
            (model_peak_t is None)
            or (obs_t >= (model_peak_t - 1.2))
            or lock_confirmed
        )
    )
    obs_near_window = bool(obs_time_ok and obs_temp_ok)
    anchor_peak = obs_peak if obs_near_window else model_peak

    start_fused = min(model_start, (anchor_peak - timedelta(hours=1)) if anchor_peak else model_start)
    t_ref = metar_diag.get("temp_trend_smooth_c") if metar_diag.get("temp_trend_smooth_c") is not None else metar_diag.get("temp_trend_1step_c")
    extra_h = 2 if (t_ref or 0.0) >= 0.3 else 1
    end_fused = max(model_end, (anchor_peak + timedelta(hours=extra_h)) if anchor_peak else model_end)
    peak_fused = anchor_peak or model_peak

    # hard rule: two-step peak lock confirmation can collapse to post-window
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


def select_realtime_triggers(
    primary_window: dict[str, Any],
    metar_diag: dict[str, Any],
    *,
    temp_unit: str = "C",
    temp_phase_decision: dict[str, Any] | None = None,
) -> list[str]:
    if isinstance(temp_phase_decision, dict):
        temp_state = temp_phase_decision
    else:
        from temperature_phase_decision import build_temperature_phase_decision

        temp_state = build_temperature_phase_decision(primary_window, metar_diag)
    phase = str(temp_state.get("phase") or classify_window_phase(primary_window, metar_diag).get("phase", "unknown"))
    daily_peak_state = str(temp_state.get("daily_peak_state") or "open")
    keep_second_peak_open = bool(temp_state.get("should_keep_second_peak_open"))
    early_peak_wording = bool(temp_state.get("should_use_early_peak_wording"))
    rebound_mode = str(temp_state.get("rebound_mode") or "none")
    dominant_shape = str(temp_state.get("dominant_shape") or "")
    plateau_hold_state = str(temp_state.get("plateau_hold_state") or "none")
    should_discuss_second_peak = bool(temp_state.get("should_discuss_second_peak"))
    t_src = metar_diag.get("temp_trend_smooth_c") if metar_diag.get("temp_trend_smooth_c") is not None else metar_diag.get("temp_trend_1step_c")
    t_tr = float(t_src or 0.0)
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
            if daily_peak_state == "locked":
                out.append("• 温度转负 + 云层回补 + 风场转差 → 高点基本定局，后续以上沿回落为主。")
            elif early_peak_wording:
                out.append("• 早段高点已出现，短线再冲动能转弱；但全天是否锁定仍待午前后云量、降水与风场确认。")
            else:
                out.append("• 短线升温动能转弱；若后续云量不再开窗且风场继续偏差，全天高点才会逐步转向锁定。")
        if upside:
            out.append("• 升温斜率维持正值且云层开窗 → 最高温上沿存在小幅上修风险。")
        if shift:
            out.append("• 风向/云量节奏出现相变 → 峰值时段可能前移或后移。")
        if downside:
            out.append("• 短时降温叠加压升信号 → 下沿下修与提前封顶风险上升。")
    elif phase == "post":
        try:
            obs_max = float(metar_diag.get("observed_max_temp_c")) if metar_diag.get("observed_max_temp_c") is not None else None
        except Exception:
            obs_max = None
        try:
            t_now = float(metar_diag.get("latest_temp")) if metar_diag.get("latest_temp") is not None else None
        except Exception:
            t_now = None

        cloud_code = str(metar_diag.get("latest_cloud_code") or "").upper()
        precip_state = str(metar_diag.get("latest_precip_state") or "none").lower()
        precip_trend = str(metar_diag.get("precip_trend") or "none").lower()
        wet_now = (precip_state in {"light", "moderate", "heavy", "convective"}) or (precip_trend in {"new", "intensify", "steady", "end"})
        cloudy_now = cloud_code in {"BKN", "OVC", "VV"}

        if daily_peak_state == "locked" and (peak_lock or t_tr <= 0):
            out.append("• 窗口后阶段：高点大概率已定，后续以回落或横盘为主。")
            return out[:3]
        if early_peak_wording:
            out.append("• 早段高点已出现，短线再冲动能转弱；但全天是否锁定仍待午前后云量、降水与风场确认。")
            if keep_second_peak_open:
                if should_discuss_second_peak:
                    out.append("• 若午前后出现真实开云并恢复正斜率，仍可回摸前高或形成弱二峰。")
            return out[:3]
        if keep_second_peak_open:
            if should_discuss_second_peak:
                out.append("• 当前更像早峰后整理；若后续开云并恢复正斜率，仍可回摸前高或形成弱二峰。")
            return out[:3]

        if obs_max is not None and t_now is not None:
            need = max(0.0, obs_max - t_now + 0.1)
            if need >= 0.8:
                if wet_now or cloudy_now or t_tr <= 0.2:
                    out.append(f"• 反超前高门槛：还差约{_fmt_delta_unit(need, temp_unit)}；当前云雨/弱斜率背景下，反超概率偏低。")
                else:
                    out.append(f"• 反超前高门槛：还差约{_fmt_delta_unit(need, temp_unit)}；需连续2报升温且云层继续开窗，才有机会改写前高。")
            elif need >= 0.3:
                out.append(f"• 接近前高（差约{_fmt_delta_unit(need, temp_unit)}）：未来1小时若维持正斜率并减云，可小幅反超。")
            elif t_tr >= 0.3 and (not wet_now) and (not cloudy_now):
                out.append("• 已贴近前高：若下一报继续升温且不回补云层，存在小幅反超机会。")
            else:
                if daily_peak_state == "locked":
                    out.append("• 窗口后阶段：高点大概率已定，后续以回落或横盘为主。")
        else:
            if daily_peak_state == "locked" and (peak_lock or t_tr <= 0):
                out.append("• 窗口后阶段：高点大概率已定，后续以回落或横盘为主。")
    else:
        # far/unknown: lightweight dynamic cues (avoid rigid template outputs)
        try:
            latest_dt = _dt(metar_diag.get("latest_report_local"))
            peak_dt = _dt(primary_window.get("peak_local"))
            hleft = max(0.0, (peak_dt - latest_dt).total_seconds() / 3600.0) if (latest_dt and peak_dt) else None
        except Exception:
            hleft = None
        htxt = f"{hleft:.1f}h" if hleft is not None else "数小时"

        if phase == "far":
            if t_tr >= 0.6 and bias >= 0.8:
                out.append(f"• 距峰值约{htxt}：升温斜率与偏暖信号同向，若下一报继续转强，上沿可小幅上修。")
            elif t_tr <= -0.6 and bias <= -0.8:
                out.append(f"• 距峰值约{htxt}：降温斜率与偏冷信号同向，后段上沿需防回撤。")
            elif shift and ("开窗" in cloud_tr or "回补" in cloud_tr):
                out.append(f"• 距峰值约{htxt}：云量/风向节奏已变化，窗口前需先确认触发方向。")
            elif abs(bias) >= 1.2 and abs(t_tr) < 0.3:
                out.append(f"• 距峰值约{htxt}：当前偏差较大但斜率未放大，先等下一报确认方向。")
        else:
            if shift and ("开窗" in cloud_tr or "回补" in cloud_tr):
                out.append("• 临窗前提示：云量/风向节奏已变化，后续请重点盯窗口期触发。")

    return out[:3]
