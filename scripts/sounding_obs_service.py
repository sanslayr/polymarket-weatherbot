from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote
from urllib.request import Request, urlopen

from station_catalog import Station, station_meta_for

ROOT = Path(__file__).resolve().parent.parent
CACHE_DIR = ROOT / "cache" / "runtime"

UWYO_TEXT_URL = (
    "https://weather.uwyo.edu/wsgi/sounding"
    "?datetime={datetime}&id={station_id}&src=FM35&type=TEXT:LIST"
)


@dataclass(frozen=True)
class SoundingStation:
    station_id: str
    lat: float
    lon: float
    terrain_class: str
    label: str


SOUNDING_STATION_MAP: dict[str, SoundingStation] = {
    "KMIA": SoundingStation("72202", 25.75, -80.37, "coastal", "Miami/MFL"),
    "KATL": SoundingStation("72215", 33.36, -84.57, "inland", "Peachtree City"),
    "KLGA": SoundingStation("72503", 40.87, -72.86, "coastal", "Upton/OKX"),
    "KDAL": SoundingStation("72249", 32.83, -97.30, "inland", "Fort Worth/FWD"),
    "KORD": SoundingStation("72632", 41.60, -88.08, "inland", "Chicago area"),
    "KSEA": SoundingStation("72797", 47.95, -124.55, "coastal", "Quillayute"),
    "EGLC": SoundingStation("03743", 50.89, 0.32, "coastal", "Herstmonceux"),
    "LFPG": SoundingStation("07145", 48.77, 2.01, "inland", "Trappes/Paris"),
    "EDDM": SoundingStation("10868", 48.25, 11.55, "inland", "Oberschleissheim"),
    "LTAC": SoundingStation("17130", 39.95, 32.88, "inland", "Ankara"),
    "RKSI": SoundingStation("47113", 37.46, 126.62, "coastal", "Incheon"),
    "NZWN": SoundingStation("93439", -40.90, 174.98, "coastal", "Paraparaumu"),
    "SAEZ": SoundingStation("87576", -34.82, -58.54, "inland", "Ezeiza"),
    "SBGR": SoundingStation("83779", -23.50, -46.61, "inland", "Sao Paulo"),
    "VILK": SoundingStation("42369", 26.76, 80.88, "inland", "Lucknow"),
}

MANUAL_DISABLE_OVERRIDES: dict[str, str] = {
    # Policy hard rule: Toronto should not use Buffalo profile because of lake-side representativeness mismatch.
    "CYYZ": "station_override_toronto_lake_mismatch",
}

DISABLE_REASON_TEXT: dict[str, str] = {
    "ok": "实测探空可用",
    "no_designated_station": "未配置可用探空站",
    "station_override_toronto_lake_mismatch": "Toronto 禁用 Buffalo（湖区两侧代表性不一致）",
    "distance_gt_150km": "探空站距离超过150km，代表性不足",
    "terrain_mismatch": "探空站与目标站地形/下垫面不一致",
    "retrieval_failed": "探空实测拉取失败",
    "no_valid_obs_24h": "24小时内无有效探空实测",
    "obs_older_than_24h": "最近探空实测已超过24小时",
    "qc_failed": "探空实测QC未通过",
}

STABLE_DISABLE_REASONS = {
    "distance_gt_150km",
    "terrain_mismatch",
    "station_override_toronto_lake_mismatch",
    "no_designated_station",
}
TRANSIENT_DISABLE_REASONS = {
    "retrieval_failed",
    "no_valid_obs_24h",
    "obs_older_than_24h",
    "qc_failed",
}


def _cache_file(icao: str) -> Path:
    return CACHE_DIR / f"sounding_obs_{str(icao).upper()}.json"


def _iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_iso_utc(v: Any) -> datetime | None:
    try:
        s = str(v or "")
        if not s:
            return None
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def _cached_obs_age_hours(payload: dict[str, Any], now_utc: datetime) -> float | None:
    obs_dt = _parse_iso_utc(payload.get("obs_time_utc"))
    if obs_dt is None:
        return None
    return (now_utc - obs_dt).total_seconds() / 3600.0


def _can_reuse_stale_payload(payload: dict[str, Any], now_utc: datetime, updated_at: datetime | None) -> bool:
    if bool(payload.get("use_sounding_obs")):
        age_h = _cached_obs_age_hours(payload, now_utc)
        return age_h is not None and age_h <= 24.0

    reason = str(payload.get("disable_reason") or "")
    if reason in STABLE_DISABLE_REASONS:
        return True

    if reason in TRANSIENT_DISABLE_REASONS and updated_at is not None:
        backoff_h = (now_utc - updated_at).total_seconds() / 3600.0
        return backoff_h <= 3.0
    return False


def _read_cache(icao: str, now_utc: datetime, *, allow_stale: bool = False) -> dict[str, Any] | None:
    p = _cache_file(icao)
    if not p.exists():
        return None
    try:
        doc = json.loads(p.read_text(encoding="utf-8"))
        exp = _parse_iso_utc(doc.get("expires_at_utc"))
        updated = _parse_iso_utc(doc.get("updated_at_utc"))
        payload = doc.get("payload")
        if exp is None or not isinstance(payload, dict):
            return None
        if now_utc <= exp:
            return payload
        if allow_stale and _can_reuse_stale_payload(payload, now_utc, updated):
            return payload
    except Exception:
        return None
    return None


def _write_cache(icao: str, payload: dict[str, Any], now_utc: datetime, ttl_minutes: int) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    p = _cache_file(icao)
    doc = {
        "updated_at_utc": _iso_utc(now_utc),
        "expires_at_utc": _iso_utc(now_utc + timedelta(minutes=max(5, int(ttl_minutes)))),
        "payload": payload,
    }
    p.write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")


def _reason_text(reason: str) -> str:
    return DISABLE_REASON_TEXT.get(str(reason), "探空实测不可用")


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lam = math.radians(lon2 - lon1)
    a = math.sin(d_phi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lam / 2.0) ** 2
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return r * c


def _classify_station_terrain(icao: str) -> str:
    meta = station_meta_for(icao)
    water_factor = str(meta.get("water_factor") or "")
    terrain = str(meta.get("terrain") or "")
    txt = f"{water_factor} {terrain}"
    if any(k in txt for k in ("沿海", "海滨", "湾岸", "近水体", "湖滨", "填海", "临水")):
        return "coastal"
    if any(k in txt for k in ("内陆", "高地", "山", "丘陵", "平原")):
        return "inland"
    return "unknown"


def _terrain_match(icao: str, station_cls: str, sounding_cls: str) -> bool:
    if str(icao).upper() == "CYYZ":
        return False
    if station_cls == "unknown" or sounding_cls == "unknown":
        return True
    return station_cls == sounding_cls


def _candidate_cycles(now_utc: datetime) -> list[datetime]:
    cur = now_utc.replace(minute=0, second=0, microsecond=0)
    cur = cur.replace(hour=12 if cur.hour >= 12 else 0)
    if cur > now_utc:
        cur -= timedelta(hours=12)
    out: list[datetime] = []
    for i in range(0, 6):
        dt = cur - timedelta(hours=12 * i)
        if (now_utc - dt) > timedelta(hours=30):
            break
        out.append(dt)
    return out


def _fetch_uwyo_text(station_id: str, cycle_utc: datetime, timeout_s: float) -> tuple[str, str] | None:
    dt_txt = cycle_utc.strftime("%Y-%m-%d %H:%M:%S")
    url = UWYO_TEXT_URL.format(datetime=quote(dt_txt, safe=""), station_id=quote(station_id, safe=""))
    req = Request(
        url=url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/plain, text/html;q=0.9,*/*;q=0.8",
        },
    )
    with urlopen(req, timeout=max(1.0, float(timeout_s))) as resp:
        raw = resp.read()
    text = raw.decode("utf-8", errors="ignore")
    low = text.lower()
    if "can't get" in low or "no data" in low or "not found" in low:
        return None
    if "station identifier" not in low or "observation time" not in low:
        return None
    return text, url


def _extract_obs_time_utc(text: str) -> datetime | None:
    m = re.search(r"Observation time:\s*([0-9]{6}/[0-9]{4})", text)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%y%m%d/%H%M").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _is_float_token(v: str) -> bool:
    try:
        float(v)
        return True
    except Exception:
        return False


def _parse_profile_rows(text: str) -> list[dict[str, float]]:
    rows: list[dict[str, float]] = []
    in_table = False
    for line in text.splitlines():
        s = line.strip()
        if not s:
            continue
        if (not in_table) and ("PRES" in s) and ("TEMP" in s) and ("DWPT" in s):
            in_table = True
            continue
        if not in_table:
            continue
        if s.startswith("-"):
            continue
        parts = s.split()
        if len(parts) < 5:
            continue
        if not (_is_float_token(parts[0]) and _is_float_token(parts[2]) and _is_float_token(parts[3])):
            continue
        p = float(parts[0])
        t = float(parts[2])
        td = float(parts[3])
        rh = float(parts[4]) if _is_float_token(parts[4]) else float("nan")
        wdir = float(parts[6]) if len(parts) > 6 and _is_float_token(parts[6]) else float("nan")
        wspd = float(parts[7]) if len(parts) > 7 and _is_float_token(parts[7]) else float("nan")
        rows.append(
            {
                "pressure_hpa": p,
                "temp_c": t,
                "dewpoint_c": td,
                "rh_pct": rh,
                "wind_dir_deg": wdir,
                "wind_kt": wspd,
            }
        )

    uniq: dict[int, dict[str, float]] = {}
    for row in rows:
        key = int(round(float(row.get("pressure_hpa", 0.0)) * 10.0))
        if key not in uniq:
            uniq[key] = row
    out = sorted(uniq.values(), key=lambda x: float(x.get("pressure_hpa", 0.0)), reverse=True)
    return out


def _pick_level(rows: list[dict[str, float]], target_hpa: float, tol_hpa: float) -> dict[str, float] | None:
    best = None
    best_d = None
    for row in rows:
        p = float(row.get("pressure_hpa", 0.0))
        d = abs(p - target_hpa)
        if d <= tol_hpa and (best_d is None or d < best_d):
            best = row
            best_d = d
    return best


def _qc_profile(rows: list[dict[str, float]]) -> tuple[bool, str, dict[str, dict[str, float] | None]]:
    if len(rows) < 6:
        return False, "missing_levels", {"925": None, "850": None, "700": None}
    for i in range(1, len(rows)):
        p_prev = float(rows[i - 1].get("pressure_hpa", 0.0))
        p_cur = float(rows[i].get("pressure_hpa", 0.0))
        if p_cur >= p_prev:
            return False, "non_monotonic_pressure", {"925": None, "850": None, "700": None}

    lv925 = _pick_level(rows, 925.0, 35.0)
    lv850 = _pick_level(rows, 850.0, 30.0)
    lv700 = _pick_level(rows, 700.0, 35.0)
    count = sum(1 for x in (lv925, lv850, lv700) if x is not None)
    if count >= 3:
        quality = "complete"
    elif count == 2:
        quality = "partial"
    elif count == 1:
        quality = "low_confidence"
    else:
        return False, "missing_levels", {"925": lv925, "850": lv850, "700": lv700}
    return True, quality, {"925": lv925, "850": lv850, "700": lv700}


def _parse_metric(text: str, patterns: list[str]) -> float | None:
    for pat in patterns:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if not m:
            continue
        try:
            return float(m.group(1))
        except Exception:
            continue
    return None


def _parse_thermo(text: str, quality: str) -> dict[str, Any]:
    sbcape = _parse_metric(text, [r"\bSBCAPE\b[^0-9\-]{0,12}(-?\d+(?:\.\d+)?)"])
    mlcape = _parse_metric(text, [r"\bMLCAPE\b[^0-9\-]{0,12}(-?\d+(?:\.\d+)?)"])
    mucape = _parse_metric(text, [r"\bMUCAPE\b[^0-9\-]{0,12}(-?\d+(?:\.\d+)?)"])
    sbcin = _parse_metric(text, [r"\bSBCIN\b[^0-9\-]{0,12}(-?\d+(?:\.\d+)?)"])
    mlcin = _parse_metric(text, [r"\bMLCIN\b[^0-9\-]{0,12}(-?\d+(?:\.\d+)?)"])

    return {
        "has_profile": True,
        "quality": quality,
        "sbcape_jkg": sbcape,
        "mlcape_jkg": mlcape,
        "mucape_jkg": mucape,
        "sbcin_jkg": sbcin,
        "mlcin_jkg": mlcin,
        "lcl_m": None,
        "lfc_m": None,
        "el_m": None,
    }


def _layer_findings(levels: dict[str, dict[str, float] | None]) -> tuple[list[str], str, str]:
    lv925 = levels.get("925")
    lv850 = levels.get("850")
    lv700 = levels.get("700")
    findings: list[str] = []

    if lv925 is not None:
        rh925 = lv925.get("rh_pct")
        if isinstance(rh925, float) and not math.isnan(rh925):
            if rh925 >= 75.0:
                findings.append("地面至925hPa湿层偏厚，低云维持概率较高。")
            elif rh925 <= 45.0:
                findings.append("地面至925hPa偏干，若云开更利于地面增温。")

    if lv925 is not None and lv850 is not None:
        dt = float(lv925.get("temp_c", 0.0)) - float(lv850.get("temp_c", 0.0))
        if dt >= 2.0:
            findings.append("925–850hPa存在稳定层（封盖信号），冲高持续性受限。")
        elif dt <= 0.3:
            findings.append("925–850hPa无明显封盖，低层混合作用较易下传。")

    if lv850 is not None and lv700 is not None:
        rhs = []
        for lv in (lv850, lv700):
            rv = lv.get("rh_pct")
            if isinstance(rv, float) and not math.isnan(rv):
                rhs.append(float(rv))
        if rhs:
            rh_mid = sum(rhs) / max(1, len(rhs))
            if rh_mid <= 40.0:
                findings.append("850–700hPa中层偏干，云开时升温效率可被放大。")
            elif rh_mid >= 70.0:
                findings.append("850–700hPa中层偏湿，云量约束信号偏强。")

    if not findings:
        findings.append("探空分层信号有限，优先跟踪下一报温度斜率与云量开合。")

    dry_like = any("偏干" in x for x in findings)
    cap_like = any(("稳定层" in x) or ("偏湿" in x) for x in findings)
    if dry_like and not cap_like:
        actionable = "若后续维持开窗与正斜率，上沿仍有小幅再试探空间。"
        path_bias = "高位再试探"
    elif cap_like:
        actionable = "层结约束偏强，短临更需警惕上沿受压与提前封顶。"
        path_bias = "高位收敛"
    else:
        actionable = "信号中性，建议以实况温度斜率和风云相位变化为主。"
        path_bias = "高位收敛"
    return findings[:3], actionable, path_bias


def _confidence(quality: str, obs_age_hours: float) -> str:
    if quality == "complete" and obs_age_hours <= 12.0:
        return "H"
    if quality in {"complete", "partial"} and obs_age_hours <= 24.0:
        return "M"
    return "L"


def _base_payload(st: Station, now_utc: datetime, plan: SoundingStation | None) -> dict[str, Any]:
    return {
        "use_sounding_obs": False,
        "disable_reason": "retrieval_failed",
        "disable_reason_text": _reason_text("retrieval_failed"),
        "retrieved_at_utc": _iso_utc(now_utc),
        "station_id": plan.station_id if plan else None,
        "station_label": plan.label if plan else None,
        "is_proxy_station": bool(plan and plan.station_id != str(st.icao).upper()),
        "distance_km": None,
        "terrain_match": None,
        "obs_time_utc": None,
        "obs_age_hours": None,
        "source_url": None,
        "profile_quality": "missing",
        "confidence": "L",
        "layer_findings": [],
        "actionable": "暂无可用探空实测，回退模式剖面 + 本地METAR。",
        "path_bias": "高位收敛",
        "thermo": {
            "has_profile": False,
            "quality": "missing_profile",
            "sbcape_jkg": None,
            "mlcape_jkg": None,
            "mucape_jkg": None,
            "sbcin_jkg": None,
            "mlcin_jkg": None,
            "lcl_m": None,
            "lfc_m": None,
            "el_m": None,
        },
    }


def _cache_ttl_minutes(reason: str, use_sounding_obs: bool) -> int:
    if use_sounding_obs:
        # Radiosonde cadence is 12h; avoid refetching every run.
        return 6 * 60
    if reason in STABLE_DISABLE_REASONS:
        return 24 * 60
    if reason in {"no_valid_obs_24h", "obs_older_than_24h", "qc_failed"}:
        return 90
    if reason == "retrieval_failed":
        return 60
    return 45


def build_sounding_obs_context(
    *,
    station: Station,
    now_utc: datetime,
    timeout_s: float = 2.5,
) -> dict[str, Any]:
    icao = str(station.icao).upper()
    cached = _read_cache(icao, now_utc)
    if cached is not None:
        return cached
    stale_cached = _read_cache(icao, now_utc, allow_stale=True)
    if stale_cached is not None:
        return stale_cached

    plan = SOUNDING_STATION_MAP.get(icao)
    payload = _base_payload(station, now_utc, plan)

    if icao in MANUAL_DISABLE_OVERRIDES:
        reason = MANUAL_DISABLE_OVERRIDES[icao]
        payload["disable_reason"] = reason
        payload["disable_reason_text"] = _reason_text(reason)
        _write_cache(icao, payload, now_utc, _cache_ttl_minutes(reason, False))
        return payload

    if plan is None:
        reason = "no_designated_station"
        payload["disable_reason"] = reason
        payload["disable_reason_text"] = _reason_text(reason)
        _write_cache(icao, payload, now_utc, _cache_ttl_minutes(reason, False))
        return payload

    dist = _haversine_km(float(station.lat), float(station.lon), plan.lat, plan.lon)
    payload["distance_km"] = round(dist, 1)

    st_cls = _classify_station_terrain(icao)
    terr_ok = _terrain_match(icao, st_cls, plan.terrain_class)
    payload["terrain_match"] = terr_ok

    if dist > 150.0:
        reason = "distance_gt_150km"
        payload["disable_reason"] = reason
        payload["disable_reason_text"] = _reason_text(reason)
        _write_cache(icao, payload, now_utc, _cache_ttl_minutes(reason, False))
        return payload

    if not terr_ok:
        reason = "terrain_mismatch"
        payload["disable_reason"] = reason
        payload["disable_reason_text"] = _reason_text(reason)
        _write_cache(icao, payload, now_utc, _cache_ttl_minutes(reason, False))
        return payload

    try:
        profile_text = None
        source_url = None
        obs_time_utc = None
        for cycle in _candidate_cycles(now_utc):
            got = _fetch_uwyo_text(plan.station_id, cycle, timeout_s=timeout_s)
            if got is None:
                continue
            text, url = got
            obs_time = _extract_obs_time_utc(text)
            if obs_time is None:
                continue
            age_h = (now_utc - obs_time).total_seconds() / 3600.0
            if age_h > 24.0:
                continue
            profile_text = text
            source_url = url
            obs_time_utc = obs_time
            break

        if profile_text is None or obs_time_utc is None:
            reason = "no_valid_obs_24h"
            payload["disable_reason"] = reason
            payload["disable_reason_text"] = _reason_text(reason)
            _write_cache(icao, payload, now_utc, _cache_ttl_minutes(reason, False))
            return payload

        obs_age_hours = (now_utc - obs_time_utc).total_seconds() / 3600.0
        payload["obs_time_utc"] = _iso_utc(obs_time_utc)
        payload["obs_age_hours"] = round(max(0.0, obs_age_hours), 2)
        payload["source_url"] = source_url

        if obs_age_hours > 24.0:
            reason = "obs_older_than_24h"
            payload["disable_reason"] = reason
            payload["disable_reason_text"] = _reason_text(reason)
            _write_cache(icao, payload, now_utc, _cache_ttl_minutes(reason, False))
            return payload

        rows = _parse_profile_rows(profile_text)
        qc_ok, quality, levels = _qc_profile(rows)
        if not qc_ok:
            reason = "qc_failed"
            payload["disable_reason"] = reason
            payload["disable_reason_text"] = _reason_text(reason)
            payload["profile_quality"] = str(quality)
            _write_cache(icao, payload, now_utc, _cache_ttl_minutes(reason, False))
            return payload

        findings, actionable, path_bias = _layer_findings(levels)
        thermo = _parse_thermo(profile_text, quality=quality)

        payload["use_sounding_obs"] = True
        payload["disable_reason"] = "ok"
        payload["disable_reason_text"] = _reason_text("ok")
        payload["profile_quality"] = quality
        payload["layer_findings"] = findings
        payload["actionable"] = actionable
        payload["path_bias"] = path_bias
        payload["confidence"] = _confidence(quality, max(0.0, obs_age_hours))
        payload["thermo"] = thermo
        _write_cache(icao, payload, now_utc, _cache_ttl_minutes("ok", True))
        return payload
    except Exception:
        reason = "retrieval_failed"
        payload["disable_reason"] = reason
        payload["disable_reason_text"] = _reason_text(reason)
        _write_cache(icao, payload, now_utc, _cache_ttl_minutes(reason, False))
        return payload
