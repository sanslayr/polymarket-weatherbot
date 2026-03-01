from __future__ import annotations

"""Centralized schema/version/provider contracts.

Keeping version strings in one place prevents drift between modules and docs.
"""

FORECAST_DECISION_SCHEMA_VERSION = "forecast-decision.v4"
FORECAST_3D_BUNDLE_SCHEMA_VERSION = "forecast-3d-bundle.v1"
OBJECTS_3D_SCHEMA_VERSION = "objects-3d.v1"

# synoptic runtime cache wrapper version (for key / on-disk compatibility management)
SYNOPTIC_CACHE_SCHEMA_VERSION = "synoptic-cache.v2"
