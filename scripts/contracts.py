from __future__ import annotations

"""Centralized schema/version/provider contracts.

Keeping version strings in one place prevents drift between modules and docs.
"""

FORECAST_DECISION_SCHEMA_VERSION = "forecast-decision.v8"
FORECAST_3D_BUNDLE_SCHEMA_VERSION = "forecast-3d-bundle.v2"
OBJECTS_3D_SCHEMA_VERSION = "objects-3d.v2"
CANONICAL_RAW_STATE_SCHEMA_VERSION = "canonical-raw-state.v2"
POSTERIOR_FEATURE_VECTOR_SCHEMA_VERSION = "posterior-feature-vector.v2"
QUALITY_SNAPSHOT_SCHEMA_VERSION = "quality-snapshot.v2"
WEATHER_POSTERIOR_CORE_SCHEMA_VERSION = "weather-posterior-core.v1"
WEATHER_POSTERIOR_SCHEMA_VERSION = "weather-posterior.v1"
REPORT_FOCUS_SCHEMA_VERSION = "report-focus.v1"
ANALYSIS_SNAPSHOT_SCHEMA_VERSION = "analysis-snapshot.v6"
MARKET_IMPLIED_WEATHER_SIGNAL_SCHEMA_VERSION = "market-implied-weather-signal.v1"

# synoptic runtime cache wrapper version (for key / on-disk compatibility management)
SYNOPTIC_CACHE_SCHEMA_VERSION = "synoptic-cache.v3"
