from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from contracts import MARKET_IMPLIED_WEATHER_SIGNAL_SCHEMA_VERSION


def _to_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _to_dt(value: Any) -> datetime | None:
    try:
        text = str(value or "").strip()
        if not text:
            return None
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _bucket_lower_bound_c(bucket: dict[str, Any]) -> float | None:
    lower = _to_float(bucket.get("lower_bound_c"))
    if lower is not None:
        return lower
    return _to_float(bucket.get("threshold_c"))


def _bucket_upper_bound_c(bucket: dict[str, Any]) -> float | None:
    upper = _to_float(bucket.get("upper_bound_c"))
    if upper is not None:
        return upper
    return _to_float(bucket.get("threshold_c"))


def _bucket_display_threshold(bucket: dict[str, Any]) -> tuple[float | None, str]:
    unit = str(bucket.get("temperature_unit") or "").strip().upper() or "C"
    native = _to_float(bucket.get("threshold_native"))
    if native is not None:
        return native, unit
    return _to_float(bucket.get("threshold_c")), "C"


def _bucket_sort_key(bucket: dict[str, Any]) -> tuple[float, float]:
    lower = _bucket_lower_bound_c(bucket)
    upper = _bucket_upper_bound_c(bucket)
    return (
        float(lower) if lower is not None else float("-inf"),
        float(upper) if upper is not None else float("inf"),
    )


def _bucket_contains_observed(bucket: dict[str, Any], observed_c: float) -> bool:
    lower = _bucket_lower_bound_c(bucket)
    upper = _bucket_upper_bound_c(bucket)
    if lower is not None and observed_c < float(lower):
        return False
    if upper is not None and observed_c > float(upper):
        return False
    return True


def _bucket_live_now(bucket: dict[str, Any], *, price_floor: float) -> bool:
    bid_now = _to_float(bucket.get("best_bid"))
    return bid_now is not None and bid_now >= float(price_floor)


def _bucket_was_live(bucket: dict[str, Any], *, price_floor: float) -> bool:
    prev_bid = _to_float(bucket.get("prev_best_bid"))
    return prev_bid is not None and prev_bid >= float(price_floor)


def _collapsed_bid_threshold() -> float:
    return 0.01


def _bucket_dead_now(bucket: dict[str, Any], *, price_floor: float, ask_collapse_threshold: float) -> bool:
    bid_now = _to_float(bucket.get("best_bid"))
    return bid_now is None or bid_now <= _collapsed_bid_threshold()


def _build_live_ladder_rows(buckets: list[dict[str, Any]], *, price_floor: float) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for bucket in buckets:
        if not _bucket_live_now(bucket, price_floor=price_floor):
            continue
        label = str(bucket.get("bucket_label") or "").strip()
        if not label:
            continue
        rows.append(
            {
                "bucket_label": label,
                "best_bid": _to_float(bucket.get("best_bid")),
                "best_ask": _to_float(bucket.get("best_ask")),
            }
        )
    return rows


class MarketFloorSweepTracker:
    def __init__(
        self,
        *,
        observed_max_temp_c: float | None,
        price_floor: float = 0.02,
        ask_collapse_threshold: float = 0.01,
        initial_state: dict[str, Any] | None = None,
    ) -> None:
        self.observed_max_temp_c = _to_float(observed_max_temp_c)
        self.price_floor = float(price_floor)
        self.ask_collapse_threshold = float(ask_collapse_threshold)
        self.anchor_bucket_token: str | None = None
        self.anchor_bucket_label: str | None = None
        self.tracked_bucket_token: str | None = None
        restored_state = dict(initial_state or {})
        restored_observed = _to_float(restored_state.get("observed_max_temp_c"))
        restore_matches_observed = False
        if restored_observed is None and self.observed_max_temp_c is None:
            restore_matches_observed = True
        elif restored_observed is not None and self.observed_max_temp_c is not None:
            restore_matches_observed = abs(float(restored_observed) - float(self.observed_max_temp_c)) < 0.02
        if restore_matches_observed:
            self.anchor_bucket_token = str(restored_state.get("anchor_bucket_token") or "").strip() or None
            self.anchor_bucket_label = str(restored_state.get("anchor_bucket_label") or "").strip() or None
            self.tracked_bucket_token = str(restored_state.get("tracked_bucket_token") or "").strip() or None


    def _sorted_buckets(self, bucket_snapshots: list[dict[str, Any]]) -> list[dict[str, Any]]:
        buckets = [dict(bucket) for bucket in bucket_snapshots if isinstance(bucket, dict)]
        buckets.sort(key=_bucket_sort_key)
        return buckets


    def _choose_anchor_bucket(self, buckets: list[dict[str, Any]]) -> dict[str, Any] | None:
        if self.observed_max_temp_c is None:
            return None
        containing = [bucket for bucket in buckets if _bucket_contains_observed(bucket, float(self.observed_max_temp_c))]
        if containing:
            return min(containing, key=_bucket_sort_key)
        higher = [
            bucket
            for bucket in buckets
            if _bucket_lower_bound_c(bucket) is not None and float(_bucket_lower_bound_c(bucket) or 0.0) > float(self.observed_max_temp_c)
        ]
        if higher:
            return min(higher, key=_bucket_sort_key)
        return None


    def _find_tracked_index(self, buckets: list[dict[str, Any]]) -> int | None:
        if self.tracked_bucket_token:
            for idx, bucket in enumerate(buckets):
                if str(bucket.get("yes_token_id") or "").strip() == self.tracked_bucket_token:
                    return idx
        if self.anchor_bucket_token:
            for idx, bucket in enumerate(buckets):
                if str(bucket.get("yes_token_id") or "").strip() == self.anchor_bucket_token:
                    self.tracked_bucket_token = self.anchor_bucket_token
                    return idx
        return None


    def _initialize_anchor_state(self, buckets: list[dict[str, Any]]) -> int | None:
        anchor_bucket = self._choose_anchor_bucket(buckets)
        if anchor_bucket is None:
            return None
        self.anchor_bucket_token = str(anchor_bucket.get("yes_token_id") or "").strip() or None
        self.anchor_bucket_label = str(anchor_bucket.get("bucket_label") or "").strip() or None
        self.tracked_bucket_token = self.anchor_bucket_token
        return self._find_tracked_index(buckets)


    def _bootstrap_tracked_bucket(self, buckets: list[dict[str, Any]], start_index: int) -> None:
        for bucket in buckets[start_index:]:
            if _bucket_live_now(bucket, price_floor=self.price_floor):
                self.tracked_bucket_token = str(bucket.get("yes_token_id") or "").strip() or self.tracked_bucket_token
                return


    def evaluate(
        self,
        *,
        bucket_snapshots: list[dict[str, Any]],
        observed_at_utc: str | None,
        scheduled_report_utc: str,
        continuous_mode: bool = False,
    ) -> list[dict[str, Any]]:
        buckets = self._sorted_buckets(bucket_snapshots)
        if not buckets:
            return []
        if self.anchor_bucket_token is None:
            tracked_index = self._initialize_anchor_state(buckets)
            if tracked_index is None:
                return []
        else:
            tracked_index = self._find_tracked_index(buckets)
            if tracked_index is None:
                self.anchor_bucket_token = None
                self.anchor_bucket_label = None
                self.tracked_bucket_token = None
                tracked_index = self._initialize_anchor_state(buckets)
                if tracked_index is None:
                    return []
        tracked_bucket = buckets[tracked_index]
        if not _bucket_was_live(tracked_bucket, price_floor=self.price_floor):
            if not _bucket_live_now(tracked_bucket, price_floor=self.price_floor):
                self._bootstrap_tracked_bucket(buckets, tracked_index + 1)
            return []
        if not _bucket_dead_now(
            tracked_bucket,
            price_floor=self.price_floor,
            ask_collapse_threshold=self.ask_collapse_threshold,
        ):
            return []

        collapsed_buckets = [tracked_bucket]
        promoted_bucket: dict[str, Any] | None = None
        for bucket in buckets[tracked_index + 1 :]:
            if _bucket_live_now(bucket, price_floor=self.price_floor):
                promoted_bucket = bucket
                break
            collapsed_buckets.append(bucket)
        if promoted_bucket is None:
            return []

        self.tracked_bucket_token = str(promoted_bucket.get("yes_token_id") or "").strip() or self.tracked_bucket_token
        return [
            _build_floor_sweep_signal(
                anchor_bucket_label=self.anchor_bucket_label or str(tracked_bucket.get("bucket_label") or "").strip(),
                dead_bucket=tracked_bucket,
                collapsed_buckets=collapsed_buckets,
                promoted_bucket=promoted_bucket,
                all_buckets=buckets,
                scheduled_report_utc=scheduled_report_utc,
                observed_at_utc=observed_at_utc,
                continuous_mode=continuous_mode,
                price_floor=self.price_floor,
                ask_collapse_threshold=self.ask_collapse_threshold,
            )
        ]


    def snapshot(self) -> dict[str, Any]:
        return {
            "observed_max_temp_c": self.observed_max_temp_c,
            "anchor_bucket_token": self.anchor_bucket_token,
            "anchor_bucket_label": self.anchor_bucket_label,
            "tracked_bucket_token": self.tracked_bucket_token,
        }


def _build_floor_sweep_signal(
    *,
    anchor_bucket_label: str,
    dead_bucket: dict[str, Any],
    collapsed_buckets: list[dict[str, Any]],
    promoted_bucket: dict[str, Any],
    all_buckets: list[dict[str, Any]],
    scheduled_report_utc: str,
    observed_at_utc: str | None,
    continuous_mode: bool,
    price_floor: float,
    ask_collapse_threshold: float,
) -> dict[str, Any]:
    now_dt = _to_dt(observed_at_utc) or datetime.now(timezone.utc)
    scheduled_dt = _to_dt(scheduled_report_utc)
    delta_seconds = None if scheduled_dt is None else (now_dt - scheduled_dt).total_seconds()
    promoted_threshold_c = _to_float(promoted_bucket.get("threshold_c"))
    promoted_threshold_native, promoted_unit = _bucket_display_threshold(promoted_bucket)
    dead_threshold_c = _to_float(dead_bucket.get("threshold_c"))
    dead_threshold_native, dead_unit = _bucket_display_threshold(dead_bucket)
    promoted_index = next(
        (idx for idx, bucket in enumerate(all_buckets) if str(bucket.get("yes_token_id") or "").strip() == str(promoted_bucket.get("yes_token_id") or "").strip()),
        len(all_buckets),
    )
    live_ladder_rows = _build_live_ladder_rows(all_buckets[promoted_index:], price_floor=price_floor)
    collapsed_labels = [str(bucket.get("bucket_label") or "").strip() for bucket in collapsed_buckets if str(bucket.get("bucket_label") or "").strip()]
    collapsed_prev_bids = {
        str(bucket.get("bucket_label") or "").strip(): _to_float(bucket.get("prev_best_bid"))
        for bucket in collapsed_buckets
        if str(bucket.get("bucket_label") or "").strip()
    }
    collapsed_current_bids = {
        str(bucket.get("bucket_label") or "").strip(): _to_float(bucket.get("best_bid"))
        for bucket in collapsed_buckets
        if str(bucket.get("bucket_label") or "").strip()
    }
    collapsed_current_asks = {
        str(bucket.get("bucket_label") or "").strip(): _to_float(bucket.get("best_ask"))
        for bucket in collapsed_buckets
        if str(bucket.get("bucket_label") or "").strip()
    }
    dead_label = str(dead_bucket.get("bucket_label") or "").strip()
    promoted_label = str(promoted_bucket.get("bucket_label") or "").strip()
    confidence = "high"
    if _to_float(promoted_bucket.get("best_bid")) is not None and _to_float(promoted_bucket.get("best_bid")) < max(price_floor * 1.5, 0.03):
        confidence = "medium"
    return {
        "schema_version": MARKET_IMPLIED_WEATHER_SIGNAL_SCHEMA_VERSION,
        "signal_type": "observed_temp_floor_sweep",
        "triggered": True,
        "message": f"{dead_label} 归零，当前最低有效盘口上移至 {promoted_label}。",
        "implied_report_temp_lower_bound_c": promoted_threshold_c,
        "implied_report_temp_lower_bound_native": promoted_threshold_native,
        "target_bucket_label": promoted_label,
        "target_bucket_threshold_c": promoted_threshold_c,
        "target_bucket_threshold_native": promoted_threshold_native,
        "temperature_unit": promoted_unit,
        "confidence": confidence,
        "scheduled_report_utc": scheduled_dt.isoformat().replace("+00:00", "Z") if scheduled_dt else None,
        "observed_at_utc": now_dt.isoformat().replace("+00:00", "Z"),
        "delta_from_report_seconds": delta_seconds,
        "within_report_window": bool(continuous_mode) or (delta_seconds is not None and -15.0 <= delta_seconds <= 360.0),
        "evidence": {
            "anchor_bucket_label": anchor_bucket_label,
            "dead_bucket_label": dead_label,
            "dead_bucket_threshold_c": dead_threshold_c,
            "dead_bucket_threshold_native": dead_threshold_native,
            "dead_bucket_temperature_unit": dead_unit,
            "dead_bucket_prev_bid": _to_float(dead_bucket.get("prev_best_bid")),
            "dead_bucket_prev_ask": _to_float(dead_bucket.get("prev_best_ask")),
            "dead_bucket_bid": _to_float(dead_bucket.get("best_bid")),
            "dead_bucket_ask": _to_float(dead_bucket.get("best_ask")),
            "first_live_bucket_label": promoted_label,
            "first_live_bucket_threshold_c": promoted_threshold_c,
            "first_live_bucket_threshold_native": promoted_threshold_native,
            "first_live_bucket_bid": _to_float(promoted_bucket.get("best_bid")),
            "first_live_bucket_ask": _to_float(promoted_bucket.get("best_ask")),
            "collapsed_prefix_labels": collapsed_labels,
            "collapsed_prefix_prev_bids": collapsed_prev_bids,
            "collapsed_prefix_current_bids": collapsed_current_bids,
            "collapsed_prefix_current_asks": collapsed_current_asks,
            "live_ladder_rows": live_ladder_rows,
            "price_floor": price_floor,
            "ask_collapse_threshold": ask_collapse_threshold,
            "trigger_mode": "resident_floor_watch" if continuous_mode else "event_window_floor_watch",
        },
    }
