from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

import redis

from consumers.stats.welford import WelfordState
from shared.models import AlertRecord

Z_SCORE_THRESHOLD = 3.0
MIN_OBSERVATIONS = 30

# An EWMA-smoothed metric is autocorrelated -- consecutive readings aren't
# independent, so one real shift produces a short run of anomalous readings,
# not a single isolated one. Requiring 2 consecutive |z| > threshold
# readings before firing filters out single correlated blips without
# touching Z_SCORE_THRESHOLD/MIN_OBSERVATIONS.
CONSECUTIVE_ANOMALOUS_REQUIRED = 2

# Tried a per-metric wider threshold (success_rate/volume_per_min at 5.0)
# to cut false positives -- reverted. It reduced false positives but broke
# real spike detection too: the baseline continuously folds every value
# (including anomalous ones) into its own mean/variance, so a sustained
# incident drags the baseline toward itself and the z-score collapses
# within a couple dozen messages. Measured live: a 30s/150-message spike
# dragged Acme Store's success_rate mean from 0.899 to 0.634, erasing its
# own z-score before it could reliably clear 5.0. False-positive and
# real-spike z-scores were also found to substantially overlap (roughly
# 3.0-4.9 for noise, 3.1-5.5 for real spikes' first reading), so no single
# static threshold cleanly separates them under this self-referential
# baseline design. See the Issues log for the fuller writeup and the
# baseline-freeze approach being considered instead.

HEALTHY = "HEALTHY"
FIRING = "FIRING"

# Internal metric name (matches main.py's stats-hash fields) -> the short
# label shared/models.py's AlertRecord.metric Literal expects.
METRIC_LABELS = {
    "success_rate": "success_rate",
    "volume_per_min": "volume",
    "avg_fraud_score": "fraud_score",
    "avg_latency_ms": "latency",
}


@dataclass
class ResolveSignal:
    merchant_id: str
    merchant_name: str
    metric: str


def _describe(merchant_name: str, metric: str, value: float, baseline: float, z: float) -> str:
    if metric == "success_rate":
        verb = "dropped" if value < baseline else "rose"
        return (
            f"{merchant_name} success rate {verb} to {value:.1%} "
            f"(baseline {baseline:.1%}, z-score {z:.1f})"
        )
    if metric == "volume_per_min":
        verb = "dropped" if value < baseline else "spiked"
        return (
            f"{merchant_name} volume {verb} to {value:.0f} tx/min "
            f"(baseline {baseline:.0f}, z-score {z:.1f})"
        )
    if metric == "avg_fraud_score":
        return (
            f"{merchant_name} fraud score shifted to {value:.2f} "
            f"(baseline {baseline:.2f}, z-score {z:.1f})"
        )
    if metric == "avg_latency_ms":
        verb = "spiked" if value > baseline else "dropped"
        return (
            f"{merchant_name} latency {verb} to {value:.0f}ms "
            f"(baseline {baseline:.0f}ms, z-score {z:.1f})"
        )
    raise ValueError(f"Unknown metric: {metric}")


class AnomalyDetector:
    def __init__(self, redis_client: redis.Redis):
        self._redis = redis_client
        self._welford_cache: dict[tuple[str, str], WelfordState] = {}
        self._alert_state_cache: dict[tuple[str, str], str] = {}
        self._consecutive_anomalous: dict[tuple[str, str], int] = {}

    def _welford_key(self, merchant_id: str, metric: str) -> str:
        return f"welford:{merchant_id}:{metric}"

    def _alert_state_key(self, merchant_id: str, metric: str) -> str:
        return f"alert_state:{merchant_id}:{metric}"

    def _load_welford(self, merchant_id: str, metric: str) -> WelfordState:
        cache_key = (merchant_id, metric)
        if cache_key in self._welford_cache:
            return self._welford_cache[cache_key]

        raw = self._redis.get(self._welford_key(merchant_id, metric))
        state = WelfordState.from_dict(json.loads(raw)) if raw else WelfordState()
        self._welford_cache[cache_key] = state
        return state

    def _save_welford(self, merchant_id: str, metric: str, state: WelfordState) -> None:
        self._redis.set(self._welford_key(merchant_id, metric), json.dumps(state.to_dict()))

    def _load_alert_state(self, merchant_id: str, metric: str) -> str:
        cache_key = (merchant_id, metric)
        if cache_key in self._alert_state_cache:
            return self._alert_state_cache[cache_key]

        raw = self._redis.get(self._alert_state_key(merchant_id, metric))
        state = raw if raw else HEALTHY
        self._alert_state_cache[cache_key] = state
        return state

    def _save_alert_state(self, merchant_id: str, metric: str, state: str) -> None:
        self._alert_state_cache[(merchant_id, metric)] = state
        self._redis.set(self._alert_state_key(merchant_id, metric), state)

    def update(
        self, merchant_id: str, merchant_name: str, metric: str, value: float
    ) -> AlertRecord | ResolveSignal | None:
        state = self._load_welford(merchant_id, metric)

        z = state.z_score(value)
        is_anomalous = abs(z) > Z_SCORE_THRESHOLD and state.n >= MIN_OBSERVATIONS

        # Only fold this value into the baseline if it doesn't look
        # anomalous. Feeding anomalous readings into the baseline lets a
        # sustained real incident drag its own reference mean toward
        # itself, collapsing its own z-score the longer it lasts (measured
        # live: a 30s spike dragged a merchant's mean from 0.899 to 0.634,
        # erasing its own significance before a wider threshold could
        # reliably clear it). Freezing here also means `state.mean` at
        # firing time reflects the true pre-incident baseline, not one
        # partway chased toward the incident.
        if not is_anomalous:
            state.update(value)
            self._save_welford(merchant_id, metric, state)

        current = self._load_alert_state(merchant_id, metric)
        cache_key = (merchant_id, metric)

        if current == HEALTHY:
            if not is_anomalous:
                self._consecutive_anomalous[cache_key] = 0
                return None

            consecutive = self._consecutive_anomalous.get(cache_key, 0) + 1
            self._consecutive_anomalous[cache_key] = consecutive
            if consecutive < CONSECUTIVE_ANOMALOUS_REQUIRED:
                return None

            self._consecutive_anomalous[cache_key] = 0
            self._save_alert_state(merchant_id, metric, FIRING)
            return AlertRecord(
                alert_id=str(uuid.uuid4()),
                merchant_id=merchant_id,
                merchant_name=merchant_name,
                metric=METRIC_LABELS[metric],
                current_value=value,
                baseline_value=state.mean,
                z_score=z,
                description=_describe(merchant_name, metric, value, state.mean, z),
                fired_at=datetime.now(timezone.utc),
            )

        if current == FIRING and not is_anomalous:
            self._save_alert_state(merchant_id, metric, HEALTHY)
            return ResolveSignal(
                merchant_id=merchant_id,
                merchant_name=merchant_name,
                metric=METRIC_LABELS[metric],
            )

        return None
