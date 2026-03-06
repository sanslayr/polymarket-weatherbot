from __future__ import annotations

import os
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
RUNTIME_CACHE_DIR = ROOT / "cache" / "runtime"


def _env_flag(name: str, default: str) -> bool:
    raw = str(os.getenv(name, default) or default).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def runtime_cache_enabled() -> bool:
    return _env_flag("WEATHERBOT_ENABLE_RUNTIME_CACHE", "0")


def gfs_binary_cache_enabled() -> bool:
    return _env_flag("WEATHERBOT_ENABLE_GFS_BINARY_CACHE", "0")
