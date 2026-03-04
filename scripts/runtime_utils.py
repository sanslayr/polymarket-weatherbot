from __future__ import annotations

from datetime import datetime, timedelta, timezone


def runtime_dt_from_tag(cycle_tag: str) -> datetime:
    return datetime.strptime(cycle_tag, "%Y%m%d%HZ").replace(tzinfo=timezone.utc)


def runtime_tag_from_dt(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y%m%d%HZ")


def resolve_runtime_for_valid_time(
    valid_utc: datetime,
    preferred_runtime_tag: str,
    *,
    cycle_hours: int = 6,
    max_back_cycles: int = 3,
) -> tuple[str, int]:
    """Resolve runtime/fh pair for a valid time.

    Returns (runtime_tag, fh_hours). Falls back to previous cycles when fh<0.
    """
    rt = runtime_dt_from_tag(preferred_runtime_tag)
    for _ in range(max_back_cycles + 1):
        fh = int(round((valid_utc - rt).total_seconds() / 3600.0))
        if fh >= 0:
            return runtime_tag_from_dt(rt), fh
        rt = rt - timedelta(hours=cycle_hours)
    # Return last attempt; caller may still reject negative fh.
    fh = int(round((valid_utc - rt).total_seconds() / 3600.0))
    return runtime_tag_from_dt(rt), fh
