from __future__ import annotations


def diagnose_925(primary_window: dict, temp_bias_c: float | None) -> dict | None:
    w850 = primary_window.get("w850_kmh")
    if w850 is None and temp_bias_c is None:
        return None

    coupling = None
    if w850 is not None:
        if w850 >= 45:
            coupling = "低层耦合偏强（925-850 传输更易落地）"
        elif w850 <= 15:
            coupling = "低层耦合偏弱（高空信号落地效率有限）"

    if coupling is None and temp_bias_c is not None:
        if temp_bias_c > 1.0:
            coupling = "实况偏暖支持低层输送已落地"
        elif temp_bias_c < -1.0:
            coupling = "实况偏冷提示低层输送落地偏弱"

    if not coupling:
        return None
    return {"summary": coupling}
