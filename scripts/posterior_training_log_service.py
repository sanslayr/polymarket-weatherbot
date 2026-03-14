from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from contracts import POSTERIOR_TRAINING_LOG_SCHEMA_VERSION
from posterior_case_index_service import build_posterior_case_index


ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = ROOT / "cache" / "runtime" / "posterior_training_log"


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value or {}) if isinstance(value, dict) else {}


def _target_log_path(sample: dict[str, Any], *, root: Path = LOG_DIR) -> Path:
    node = _as_dict(sample)
    target_date = str(node.get("target_date_local") or "unknown-date")
    station_icao = str(node.get("station_icao") or "UNKNOWN")
    return root / target_date / f"{station_icao}.jsonl"


def append_posterior_learning_sample(
    sample: dict[str, Any],
    *,
    root: Path = LOG_DIR,
) -> Path:
    sample_node = _as_dict(sample)
    case_index = build_posterior_case_index(sample_node)
    path = _target_log_path(sample_node, root=root)
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "schema_version": POSTERIOR_TRAINING_LOG_SCHEMA_VERSION,
        "sample": sample_node,
        "case_index": case_index,
    }
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")
    return path


def read_posterior_learning_log(
    *,
    station_icao: str,
    target_date_local: str,
    root: Path = LOG_DIR,
) -> list[dict[str, Any]]:
    path = root / str(target_date_local or "unknown-date") / f"{str(station_icao or 'UNKNOWN')}.jsonl"
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except Exception:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows
