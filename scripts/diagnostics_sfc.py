from __future__ import annotations


def diagnose_sfc(metar_diag: dict) -> dict | None:
    wdir = metar_diag.get("latest_wdir")
    wspd = metar_diag.get("latest_wspd")
    if wdir is None and wspd is None:
        return None

    source = "风向来源待判"
    try:
        d = int(wdir)
        if 180 <= d <= 260:
            source = "偏南到西南风（暖输送倾向）"
        elif 280 <= d <= 360 or 0 <= d <= 30:
            source = "偏北到西北风（冷输送倾向）"
    except Exception:
        if str(wdir).upper() == "VRB":
            source = "风向可变（边界层信号不稳定）"

    return {"summary": f"地面层风场：{source}，风速约 {wspd}kt"}
