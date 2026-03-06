from __future__ import annotations

import os
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
RUNTIME_CACHE_DIR = ROOT / "cache" / "runtime"


def _env_flag(name: str, default: str) -> bool:
    raw = str(os.getenv(name, default) or default).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def runtime_cache_enabled() -> bool:
    # Runtime cache persistence is intentionally disabled for the live workspace.
    return False


def gfs_binary_cache_enabled() -> bool:
    # Keep GRIB downloads in temp workspaces only; do not persist under cache/runtime.
    return False
