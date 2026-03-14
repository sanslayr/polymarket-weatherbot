from __future__ import annotations

import atexit
import fcntl
import json
import os
import tempfile
from dataclasses import asdict, is_dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
STATE_DIR = ROOT / "cache" / "runtime" / "market_alert_worker"
STATE_DIR.mkdir(parents=True, exist_ok=True)
STATE_PATH = STATE_DIR / "state.json"
PID_PATH = STATE_DIR / "worker.pid"
LOG_PATH = STATE_DIR / "worker.log"
LOCK_PATH = STATE_DIR / "worker.lock"
_LOCK_FD: int | None = None


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    finally:
        try:
            os.unlink(tmp_path)
        except FileNotFoundError:
            pass


def release_singleton_lock() -> None:
    global _LOCK_FD
    if _LOCK_FD is None:
        return
    try:
        fcntl.flock(_LOCK_FD, fcntl.LOCK_UN)
    except Exception:
        pass
    try:
        os.close(_LOCK_FD)
    except Exception:
        pass
    _LOCK_FD = None


def acquire_singleton_lock() -> None:
    global _LOCK_FD
    fd = os.open(LOCK_PATH, os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        holder = ""
        try:
            with os.fdopen(os.dup(fd), "r", encoding="utf-8", errors="ignore") as handle:
                holder = handle.read().strip()
        finally:
            os.close(fd)
        raise RuntimeError(f"market_alert_worker already running ({holder or 'lock held'})") from exc
    os.ftruncate(fd, 0)
    os.write(fd, f"{os.getpid()}\n".encode("ascii", errors="ignore"))
    _LOCK_FD = fd
    atexit.register(release_singleton_lock)


def load_worker_state() -> dict[str, Any]:
    if not STATE_PATH.exists():
        return {
            "last_alerts": {},
            "last_window_runs": {},
            "last_window_results": {},
            "last_errors": {},
            "day_disabled_events": {},
            "resident_floor_watch_states": {},
        }
    try:
        payload = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("worker state must be object")
        payload.setdefault("last_alerts", {})
        payload.setdefault("last_window_runs", {})
        payload.setdefault("last_window_results", {})
        payload.setdefault("last_errors", {})
        payload.setdefault("day_disabled_events", {})
        payload.setdefault("resident_floor_watch_states", {})
        return payload
    except Exception:
        return {
            "last_alerts": {},
            "last_window_runs": {},
            "last_window_results": {},
            "last_errors": {},
            "day_disabled_events": {},
            "resident_floor_watch_states": {},
        }


def save_worker_state(state: dict[str, Any]) -> None:
    _write_json_atomic(STATE_PATH, state)


def json_safe(value: Any) -> Any:
    if is_dataclass(value):
        return json_safe(asdict(value))
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    if isinstance(value, set):
        return [json_safe(item) for item in sorted(value, key=str)]
    return value
