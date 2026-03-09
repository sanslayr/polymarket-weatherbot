from __future__ import annotations

"""Centralized schema/version/provider contracts.

Keeping version strings in one place prevents drift between modules and docs.
"""

FORECAST_DECISION_SCHEMA_VERSION = "forecast-decision.v8"
FORECAST_3D_BUNDLE_SCHEMA_VERSION = "forecast-3d-bundle.v2"
OBJECTS_3D_SCHEMA_VERSION = "objects-3d.v2"
ANALYSIS_SNAPSHOT_SCHEMA_VERSION = "analysis-snapshot.v2"

# synoptic runtime cache wrapper version (for key / on-disk compatibility management)
SYNOPTIC_CACHE_SCHEMA_VERSION = "synoptic-cache.v3"
