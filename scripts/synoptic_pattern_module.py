#!/usr/bin/env python3
"""Scale-aware circulation pattern recognition module.

Input: JSON containing current features/systems and optional future timeline.
Output: JSON with pattern identification across planetary/synoptic/mesoscale,
        evidence, and trend transitions.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable


def _f(features: dict[str, Any], key: str, default: float = 0.0) -> float:
    value = features.get(key, default)
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _b(features: dict[str, Any], key: str, default: bool = False) -> bool:
    value = features.get(key)
    if value is None:
        return default
    return bool(value)


@dataclass(frozen=True)
class PatternRule:
    pattern: str
    scale: str
    description: str
    min_score: float
    scorer: Callable[[dict[str, Any]], tuple[float, list[str]]]


def _score_subtropical_high(features: dict[str, Any]) -> tuple[float, list[str]]:
    ridge_lat = _f(features, "subtropical_high_northern_edge_lat")
    strength = _f(features, "subtropical_high_strength_hpa")
    z500 = _f(features, "z500_anom_std")
    score = 0.0
    evidence: list[str] = []
    if ridge_lat >= 28:
        score += 0.45
        evidence.append(f"副高北缘偏北({ridge_lat:.1f}N)")
    if strength >= 587:
        score += 0.35
        evidence.append(f"副高强度偏强({strength:.1f} gpm)")
    if z500 >= 0.8:
        score += 0.2
        evidence.append(f"500hPa正距平明显({z500:.2f}σ)")
    return min(score, 1.0), evidence


def _score_westerly_dominant(features: dict[str, Any]) -> tuple[float, list[str]]:
    jet = _f(features, "jet300_speed_kt")
    lat_shift = abs(_f(features, "jet300_lat_shift_deg"))
    trough = _f(features, "z500_trough_curvature")
    score = 0.0
    evidence: list[str] = []
    if jet >= 90:
        score += 0.5
        evidence.append(f"高空急流较强({jet:.0f}kt)")
    if lat_shift <= 4:
        score += 0.25
        evidence.append(f"西风轴摆动小({lat_shift:.1f}deg)")
    if trough <= 0.25:
        score += 0.25
        evidence.append(f"长波起伏弱(曲率{trough:.2f})")
    return min(score, 1.0), evidence


def _score_omega_block(features: dict[str, Any]) -> tuple[float, list[str]]:
    omega = _f(features, "omega_block_index")
    cutoff = _b(features, "cutoff_low_flag")
    score = 0.0
    evidence: list[str] = []
    if omega >= 0.7:
        score += 0.8
        evidence.append(f"Omega阻塞指数高({omega:.2f})")
    if cutoff:
        score += 0.2
        evidence.append("存在切断低压配合")
    return min(score, 1.0), evidence


def _score_warm_sector_prefrontal(features: dict[str, Any]) -> tuple[float, list[str]]:
    advec = _f(features, "t850_advection_k_per_6h")
    front = _f(features, "frontogenesis_index")
    vort = _f(features, "shortwave_vort_1e5")
    score = 0.0
    evidence: list[str] = []
    if advec >= 1.5:
        score += 0.45
        evidence.append(f"850hPa暖平流明显({advec:.2f}K/6h)")
    if front >= 0.6:
        score += 0.35
        evidence.append(f"锋生信号增强({front:.2f})")
    if vort >= 1.2:
        score += 0.2
        evidence.append(f"短波扰动支持({vort:.2f})")
    return min(score, 1.0), evidence


def _score_postfrontal_mixing(features: dict[str, Any]) -> tuple[float, list[str]]:
    advec = _f(features, "t850_advection_k_per_6h")
    grad = _f(features, "mslp_grad_hpa_per_100km")
    llj = _f(features, "low_level_jet_kt")
    score = 0.0
    evidence: list[str] = []
    if advec <= -1.0:
        score += 0.45
        evidence.append(f"冷平流主导({advec:.2f}K/6h)")
    if grad >= 1.8:
        score += 0.35
        evidence.append(f"压梯偏强({grad:.2f}hPa/100km)")
    if llj >= 25:
        score += 0.2
        evidence.append(f"低层动量下传条件可用({llj:.0f}kt)")
    return min(score, 1.0), evidence


def _score_cutoff_low(features: dict[str, Any]) -> tuple[float, list[str]]:
    cutoff = _b(features, "cutoff_low_flag")
    curv = _f(features, "z500_trough_curvature")
    score = 0.0
    evidence: list[str] = []
    if cutoff:
        score += 0.75
        evidence.append("500hPa切断低压标志为真")
    if curv >= 0.8:
        score += 0.25
        evidence.append(f"中层曲率较强({curv:.2f})")
    return min(score, 1.0), evidence


def _score_marine_onshore_stable(features: dict[str, Any]) -> tuple[float, list[str]]:
    marine = _f(features, "marine_onshore_index")
    cloud = _f(features, "low_cloud_fraction")
    score = 0.0
    evidence: list[str] = []
    if marine >= 0.6:
        score += 0.7
        evidence.append(f"海风/海洋层影响明显({marine:.2f})")
    if cloud >= 0.55:
        score += 0.3
        evidence.append(f"低云覆盖偏高({cloud:.2f})")
    return min(score, 1.0), evidence


def _score_cold_air_damming(features: dict[str, Any]) -> tuple[float, list[str]]:
    cad = _f(features, "cad_index")
    pressure_rise = _f(features, "mslp_tendency_24h_hpa")
    score = 0.0
    evidence: list[str] = []
    if cad >= 0.6:
        score += 0.75
        evidence.append(f"冷堆指数较高({cad:.2f})")
    if pressure_rise >= 2.0:
        score += 0.25
        evidence.append(f"地面增压支持冷堆({pressure_rise:.2f}hPa/24h)")
    return min(score, 1.0), evidence


def _score_prefrontal_convergence(features: dict[str, Any]) -> tuple[float, list[str]]:
    conv = _f(features, "convergence_index")
    front = _f(features, "frontogenesis_index")
    score = 0.0
    evidence: list[str] = []
    if conv >= 0.65:
        score += 0.6
        evidence.append(f"低层辐合增强({conv:.2f})")
    if front >= 0.5:
        score += 0.4
        evidence.append(f"锋生配合({front:.2f})")
    return min(score, 1.0), evidence


RULES: list[PatternRule] = [
    PatternRule(
        pattern="subtropical_high_control",
        scale="planetary",
        description="副热带高压控制/边缘控制",
        min_score=0.55,
        scorer=_score_subtropical_high,
    ),
    PatternRule(
        pattern="westerly_dominant",
        scale="planetary",
        description="西风带主导的平直环流",
        min_score=0.55,
        scorer=_score_westerly_dominant,
    ),
    PatternRule(
        pattern="omega_block",
        scale="planetary",
        description="Omega阻塞形势",
        min_score=0.65,
        scorer=_score_omega_block,
    ),
    PatternRule(
        pattern="warm_sector_prefrontal",
        scale="synoptic",
        description="锋前暖区形势",
        min_score=0.55,
        scorer=_score_warm_sector_prefrontal,
    ),
    PatternRule(
        pattern="postfrontal_mixing",
        scale="synoptic",
        description="锋后冷平流混合形势",
        min_score=0.55,
        scorer=_score_postfrontal_mixing,
    ),
    PatternRule(
        pattern="cutoff_low",
        scale="synoptic",
        description="切断低压控制",
        min_score=0.6,
        scorer=_score_cutoff_low,
    ),
    PatternRule(
        pattern="marine_onshore_stable",
        scale="mesoscale",
        description="海风/海洋稳定层",
        min_score=0.6,
        scorer=_score_marine_onshore_stable,
    ),
    PatternRule(
        pattern="cold_air_damming",
        scale="mesoscale",
        description="冷堆/地形阻塞冷空气",
        min_score=0.6,
        scorer=_score_cold_air_damming,
    ),
    PatternRule(
        pattern="prefrontal_convergence",
        scale="mesoscale",
        description="锋前辐合带",
        min_score=0.6,
        scorer=_score_prefrontal_convergence,
    ),
]


def classify_patterns(features: dict[str, Any]) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = {
        "planetary": [],
        "synoptic": [],
        "mesoscale": [],
    }

    for rule in RULES:
        score, evidence = rule.scorer(features)
        if score >= rule.min_score:
            grouped[rule.scale].append(
                {
                    "pattern": rule.pattern,
                    "description": rule.description,
                    "score": round(score, 3),
                    "evidence": evidence[:3],
                }
            )

    for scale in grouped:
        grouped[scale].sort(key=lambda x: x["score"], reverse=True)

    summary: dict[str, Any] = {}
    for scale in ("planetary", "synoptic", "mesoscale"):
        matches = grouped[scale]
        summary[scale] = {
            "primary": matches[0] if matches else None,
            "secondary": matches[1] if len(matches) > 1 else None,
            "candidates": matches,
        }

    return summary


SYSTEM_SCALE_HINTS = {
    "subtropical_high": "planetary",
    "westerly_belt": "planetary",
    "polar_vortex_lobe": "planetary",
    "longwave_trough": "planetary",
    "surface_low": "synoptic",
    "surface_high": "synoptic",
    "front": "synoptic",
    "shortwave_trough": "synoptic",
    "ridge": "synoptic",
    "jet_streak": "synoptic",
    "cutoff_low": "synoptic",
    "sea_breeze_front": "mesoscale",
    "outflow_boundary": "mesoscale",
    "cold_pool": "mesoscale",
}


def annotate_systems(systems: list[dict[str, Any]]) -> list[dict[str, Any]]:
    annotated = []
    for system in systems:
        system_type = str(system.get("system_type", "unknown"))
        level = str(system.get("level", ""))
        scale = str(system.get("scale", "")).strip().lower()
        if not scale:
            scale = SYSTEM_SCALE_HINTS.get(system_type, "synoptic")

        item = dict(system)
        item["scale"] = scale
        item["system_label"] = f"{system_type}@{level}" if level else system_type
        annotated.append(item)

    return annotated


def build_level_layers(systems: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    layers: dict[str, list[dict[str, Any]]] = {
        "planetary": [],
        "500": [],
        "850": [],
        "mslp": [],
        "other": [],
    }
    for system in systems:
        level = str(system.get("level", "")).lower()
        scale = str(system.get("scale", "")).lower()
        if scale == "planetary":
            layers["planetary"].append(system)
            continue
        if level in {"500", "850", "mslp"}:
            layers[level].append(system)
        else:
            layers["other"].append(system)
    return layers


def build_region_map(systems: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for system in systems:
        region = str(system.get("region_name", "unknown-region"))
        if region not in grouped:
            grouped[region] = {
                "region_name": region,
                "systems": [],
            }
        grouped[region]["systems"].append(
            {
                "label": system.get("system_label"),
                "scale": system.get("scale"),
                "trend": system.get("trend"),
                "intensity": system.get("intensity"),
            }
        )
    return list(grouped.values())


def _parse_iso(ts: str) -> datetime:
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return datetime.fromisoformat(ts)


def build_trends(
    current_time_utc: str,
    current_patterns: dict[str, Any],
    future: list[dict[str, Any]],
) -> dict[str, Any]:
    baseline = {
        scale: (current_patterns[scale]["primary"] or {}).get("pattern")
        for scale in ("planetary", "synoptic", "mesoscale")
    }

    timeline: list[dict[str, Any]] = []
    turning_points: list[dict[str, Any]] = []
    current_dt = _parse_iso(current_time_utc)

    for item in future:
        valid_time = item["valid_time_utc"]
        features = item.get("features", {})
        classified = classify_patterns(features)

        primary_by_scale = {
            scale: (classified[scale]["primary"] or {}).get("pattern")
            for scale in ("planetary", "synoptic", "mesoscale")
        }
        horizon_h = int((_parse_iso(valid_time) - current_dt).total_seconds() // 3600)
        timeline.append(
            {
                "valid_time_utc": valid_time,
                "horizon_hours": horizon_h,
                "primary_by_scale": primary_by_scale,
            }
        )

        for scale, base_pattern in baseline.items():
            new_pattern = primary_by_scale.get(scale)
            if base_pattern and new_pattern and base_pattern != new_pattern:
                turning_points.append(
                    {
                        "scale": scale,
                        "from": base_pattern,
                        "to": new_pattern,
                        "valid_time_utc": valid_time,
                        "horizon_hours": horizon_h,
                    }
                )
                baseline[scale] = new_pattern

    return {
        "timeline": timeline,
        "turning_points": turning_points,
    }


def analyze(payload: dict[str, Any]) -> dict[str, Any]:
    analysis_time_utc = payload["analysis_time_utc"]
    features = payload.get("features", {})
    systems = payload.get("systems", [])
    future = payload.get("future", [])

    patterns = classify_patterns(features)
    systems_annotated = annotate_systems(systems)
    level_layers = build_level_layers(systems_annotated)
    region_map = build_region_map(systems_annotated)
    trends = build_trends(analysis_time_utc, patterns, future)

    return {
        "analysis_time_utc": analysis_time_utc,
        "scales": patterns,
        "systems": systems_annotated,
        "level_layers": level_layers,
        "regional_picture": region_map,
        "trends": trends,
    }


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scale-aware synoptic pattern module")
    parser.add_argument("--input", required=True, help="Input JSON path")
    parser.add_argument("--output", default="", help="Output JSON path")
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    payload = load_json(Path(args.input))
    result = analyze(payload)
    text = json.dumps(result, ensure_ascii=True, indent=2)

    if args.output:
        Path(args.output).write_text(text + "\n", encoding="utf-8")
    else:
        print(text)


if __name__ == "__main__":
    main()
