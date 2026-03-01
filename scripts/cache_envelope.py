from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

CACHE_SCHEMA_VERSION = "runtime-cache.v1"


def utc_now_z() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def make_cache_doc(
    payload: dict[str, Any],
    *,
    source_state: str = "fresh",
    payload_schema_version: str | None = None,
    meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "cache_schema_version": CACHE_SCHEMA_VERSION,
        "updated_at": utc_now_z(),
        "source_state": source_state,
        "payload_schema_version": payload_schema_version,
        "meta": meta or {},
        "payload": payload,
    }


def extract_payload(doc: Any) -> tuple[dict[str, Any] | None, str | None, dict[str, Any] | None]:
    """Return (payload, updated_at, envelope_meta).

    Compatible with:
    - new enveloped format
    - legacy {'updated_at','payload'}
    - raw payload-only dict
    """
    if not isinstance(doc, dict):
        return None, None, None

    if isinstance(doc.get("payload"), dict):
        # new or legacy wrapper
        env = {
            "cache_schema_version": doc.get("cache_schema_version"),
            "source_state": doc.get("source_state"),
            "payload_schema_version": doc.get("payload_schema_version"),
            "meta": doc.get("meta") if isinstance(doc.get("meta"), dict) else {},
        }
        return doc.get("payload"), str(doc.get("updated_at") or ""), env

    # payload-only legacy
    return doc, None, None
