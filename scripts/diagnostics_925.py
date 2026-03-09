from __future__ import annotations


def diagnose_925(primary_window: dict, temp_bias_c: float | None) -> dict | None:
    w850 = primary_window.get("w850_kmh")
    if w850 is None and temp_bias_c is None:
        return None

    coupling = None
    coupling_state = "neutral"
    landing_signal = "neutral"
    evidence_source: list[str] = []
    coupling_score = 0.5
    if w850 is not None:
        if w850 >= 45:
            coupling = "低层耦合偏强（925-850 传输更易落地）"
            coupling_state = "strong"
            coupling_score = 0.82
            evidence_source.append("w850")
        elif w850 >= 28:
            coupling_state = "partial"
            coupling_score = 0.62
            evidence_source.append("w850")
        elif w850 <= 15:
            coupling = "低层耦合偏弱（高空信号落地效率有限）"
            coupling_state = "weak"
            coupling_score = 0.24
            evidence_source.append("w850")
        else:
            coupling_state = "partial"
            coupling_score = 0.48
            evidence_source.append("w850")

    if coupling is None and temp_bias_c is not None:
        if temp_bias_c > 1.0:
            coupling = "实况偏暖支持低层输送已落地"
            landing_signal = "warm_landing"
            evidence_source.append("temp_bias")
        elif temp_bias_c < -1.0:
            coupling = "实况偏冷提示低层输送落地偏弱"
            landing_signal = "cold_underperforming"
            evidence_source.append("temp_bias")

    if landing_signal == "neutral" and temp_bias_c is not None:
        if temp_bias_c >= 0.4:
            landing_signal = "warm_tilt"
            evidence_source.append("temp_bias")
        elif temp_bias_c <= -0.4:
            landing_signal = "cold_tilt"
            evidence_source.append("temp_bias")

    return {
        "summary": str(coupling or ""),
        "coupling_state": coupling_state,
        "landing_signal": landing_signal,
        "evidence_source": evidence_source,
        "w850_kmh": float(w850) if w850 is not None else None,
        "temp_bias_c": float(temp_bias_c) if temp_bias_c is not None else None,
        "coupling_score": coupling_score,
    }
