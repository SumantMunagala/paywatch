import os
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import psycopg2
import redis
from confluent_kafka import Consumer
from prometheus_client import Counter, Gauge, start_http_server

from consumers.stats.detector import AnomalyDetector, ResolveSignal
from shared.config import POSTGRES_URL, REDIS_URL, get_kafka_config
from shared.models import AlertRecord, TransactionEvent

TOPIC = "transactions"
GROUP_ID = "stats-group"
VOLUME_WINDOW_SECONDS = 60
METRICS = ["success_rate", "avg_latency_ms", "avg_fraud_score", "volume_per_min"]
# Logging-only label to tell instances apart in a multi-instance run -- has
# no effect on Kafka partition assignment, which is entirely up to the
# broker's consumer group rebalance protocol.
CONSUMER_INSTANCE_ID = os.getenv("CONSUMER_INSTANCE_ID", "stats-1")
# Every instance in this consumer group runs on the same machine during
# local multi-instance testing, so a fixed port would only let the first
# instance bind it -- METRICS_PORT must be set uniquely per instance (e.g.
# 8002, 8003, 8004...) whenever running more than one at once.
METRICS_PORT = int(os.getenv("METRICS_PORT", "8002"))

STATS_BATCH_SIZE = 50
STATS_BATCH_TIMEOUT_SECONDS = 1.0
# Same reasoning as the indexer's batch rewrite: a short poll timeout lets
# the loop notice STATS_BATCH_TIMEOUT_SECONDS elapsing promptly even when
# messages are arriving slowly or not at all, instead of blocking for up to
# a full second inside a single poll() call.
POLL_TIMEOUT_SECONDS = 0.1

transactions_processed_total = Counter(
    "transactions_processed_total", "Total transactions processed", ["merchant_id"]
)
alerts_fired_total = Counter("alerts_fired_total", "Total alerts fired", ["metric"])
consumer_lag_seconds = Gauge(
    "consumer_lag_seconds", "Time between a message's Kafka timestamp and processing it"
)

# Smoothing factor for the EWMA below. Half-life is ln(0.5)/ln(1-alpha).
# avg_latency_ms/avg_fraud_score are continuous values with gentle
# per-transaction jitter, so EWMA_ALPHA=0.1 (~6.6-observation half-life)
# reacts within single-digit seconds without much noise. success_rate is a
# raw 0/1 outcome per transaction -- at that alpha, a statistically normal
# short run of failures swings the EWMA by 10+ points, easily crossing even
# a correctly-learned 3-sigma threshold. EWMA_ALPHA_SUCCESS_RATE=0.03
# (~23-observation half-life) still reacts within tens of seconds but
# smooths out that per-transaction binary noise.
EWMA_ALPHA = 0.1
EWMA_ALPHA_SUCCESS_RATE = 0.03


@dataclass
class MerchantStats:
    merchant_name: str
    transaction_count: int = 0
    success_count: int = 0
    success_rate: float = 0.0
    avg_latency_ms: float = 0.0
    avg_fraud_score: float = 0.0
    # Wall-clock timestamps of recent events, oldest first -- an in-memory
    # equivalent of the old stats:{id}:volume sorted set. len() after
    # trimming anything older than VOLUME_WINDOW_SECONDS is volume_per_min.
    volume_timestamps: deque = field(default_factory=deque)


def get_or_seed_stats(
    r: redis.Redis,
    merchant_id: str,
    merchant_name: str,
    merchant_stats: dict[str, MerchantStats],
) -> MerchantStats:
    if merchant_id in merchant_stats:
        return merchant_stats[merchant_id]

    # First time this process has seen this merchant -- load its
    # last-persisted state once so a restart doesn't reset a merchant's
    # learned EWMA baselines back to zero (the same lazy-load-once,
    # write-through-on-flush pattern AnomalyDetector already uses for
    # WelfordState).
    previous = r.hgetall(f"stats:{merchant_id}")
    if previous:
        stats = MerchantStats(
            merchant_name=merchant_name,
            transaction_count=int(previous.get("transaction_count", 0)),
            success_count=int(previous.get("success_count", 0)),
            success_rate=float(previous.get("success_rate", 0.0)),
            avg_latency_ms=float(previous.get("avg_latency_ms", 0.0)),
            avg_fraud_score=float(previous.get("avg_fraud_score", 0.0)),
        )
    else:
        stats = MerchantStats(merchant_name=merchant_name)

    # Seed the volume window from the old sorted set's scores (timestamps),
    # if any survive within the window -- keeps volume_per_min from
    # momentarily reading 0 for an already-active merchant right after a
    # restart.
    now = time.time()
    existing = r.zrangebyscore(
        f"stats:{merchant_id}:volume", now - VOLUME_WINDOW_SECONDS, now, withscores=True
    )
    stats.volume_timestamps.extend(sorted(score for _member, score in existing))

    merchant_stats[merchant_id] = stats
    return stats


def apply_event_in_memory(stats: MerchantStats, event: TransactionEvent) -> None:
    is_success = 1.0 if event.status == "success" else 0.0

    if stats.transaction_count == 0:
        # First transaction ever seen for this merchant (no prior Redis
        # state either): no prior EWMA to blend with, so bootstrap directly
        # from this observation.
        stats.success_rate = is_success
        stats.avg_latency_ms = float(event.processor_latency_ms)
        stats.avg_fraud_score = event.fraud_score
    else:
        stats.success_rate = (
            EWMA_ALPHA_SUCCESS_RATE * is_success
            + (1 - EWMA_ALPHA_SUCCESS_RATE) * stats.success_rate
        )
        stats.avg_latency_ms = (
            EWMA_ALPHA * event.processor_latency_ms + (1 - EWMA_ALPHA) * stats.avg_latency_ms
        )
        stats.avg_fraud_score = (
            EWMA_ALPHA * event.fraud_score + (1 - EWMA_ALPHA) * stats.avg_fraud_score
        )

    stats.transaction_count += 1
    stats.success_count += 1 if event.status == "success" else 0

    now = time.time()
    stats.volume_timestamps.append(now)
    cutoff = now - VOLUME_WINDOW_SECONDS
    while stats.volume_timestamps and stats.volume_timestamps[0] < cutoff:
        stats.volume_timestamps.popleft()


def record_alert_fired(r: redis.Redis, pg_conn, alert: AlertRecord) -> None:
    r.hset(
        f"alert:{alert.merchant_id}:{alert.metric}",
        mapping={
            "status": "FIRING",
            "description": alert.description,
            "fired_at": alert.fired_at.isoformat(),
            "z_score": alert.z_score,
        },
    )

    with pg_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO alerts
                (id, merchant_id, metric, current_value, baseline_value,
                 z_score, description, fired_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                alert.alert_id,
                alert.merchant_id,
                alert.metric,
                alert.current_value,
                alert.baseline_value,
                alert.z_score,
                alert.description,
                alert.fired_at,
            ),
        )

    alerts_fired_total.labels(metric=alert.metric).inc()
    print(f"ALERT FIRED: {alert.description}", flush=True)


def record_alert_resolved(r: redis.Redis, pg_conn, resolve: ResolveSignal) -> None:
    r.hset(f"alert:{resolve.merchant_id}:{resolve.metric}", "status", "RESOLVED")

    with pg_conn.cursor() as cur:
        cur.execute(
            """
            UPDATE alerts
            SET resolved_at = now()
            WHERE merchant_id = %s AND metric = %s AND resolved_at IS NULL
            """,
            (resolve.merchant_id, resolve.metric),
        )

    print(f"ALERT RESOLVED: {resolve.merchant_id} {resolve.metric}", flush=True)


def flush_batch(
    r: redis.Redis,
    pg_conn,
    detector: AnomalyDetector,
    consumer: Consumer,
    merchant_stats: dict[str, MerchantStats],
    merchants_touched: set[str],
    pending_alerts: list,
    last_msg,
    batch_size: int,
) -> None:
    pipe = r.pipeline()

    for merchant_id in merchants_touched:
        stats = merchant_stats[merchant_id]
        volume_per_min = len(stats.volume_timestamps)

        pipe.hset(
            f"stats:{merchant_id}",
            mapping={
                "transaction_count": stats.transaction_count,
                "success_count": stats.success_count,
                "success_rate": stats.success_rate,
                "avg_latency_ms": stats.avg_latency_ms,
                "avg_fraud_score": stats.avg_fraud_score,
                "volume_per_min": volume_per_min,
            },
        )

        # Rewrite the volume sorted set from the in-memory window rather
        # than incrementally zadd-ing per message -- keeps Redis a valid,
        # restart-safe snapshot of the window even though it's no longer
        # touched per message. Members just need to be unique strings;
        # nothing reads them individually (only the count/scores matter).
        volume_key = f"stats:{merchant_id}:volume"
        pipe.delete(volume_key)
        if stats.volume_timestamps:
            pipe.zadd(
                volume_key,
                {f"{ts}:{i}": ts for i, ts in enumerate(stats.volume_timestamps)},
            )

    # Same pipeline, same round trip: every WelfordState/alert_state key
    # AnomalyDetector marked dirty since the last flush.
    detector.flush_to_pipeline(pipe)

    pipe.execute()

    # Alert writes are rare and their latency doesn't affect throughput, so
    # they stay synchronous and outside the pipeline above.
    for item in pending_alerts:
        if isinstance(item, AlertRecord):
            record_alert_fired(r, pg_conn, item)
        elif isinstance(item, ResolveSignal):
            record_alert_resolved(r, pg_conn, item)

    consumer.commit(message=last_msg, asynchronous=False)

    print(
        f"[{CONSUMER_INSTANCE_ID}] Stats batch flushed: {batch_size} messages, "
        f"{len(merchants_touched)} merchants, {len(pending_alerts)} alerts fired",
        flush=True,
    )


def main() -> None:
    start_http_server(METRICS_PORT)

    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    pg_conn = psycopg2.connect(POSTGRES_URL)
    pg_conn.autocommit = True
    detector = AnomalyDetector(r, defer_redis=True)

    kafka_config = get_kafka_config()
    kafka_config.update(
        {
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer = Consumer(kafka_config)
    consumer.subscribe([TOPIC])

    print(
        f"[{CONSUMER_INSTANCE_ID}] Stats consumer started (group '{GROUP_ID}'), "
        f"subscribed to '{TOPIC}', batching up to {STATS_BATCH_SIZE} messages or every "
        f"{STATS_BATCH_TIMEOUT_SECONDS}s, metrics on :{METRICS_PORT}...",
        flush=True,
    )

    merchant_stats: dict[str, MerchantStats] = {}
    batch_size = 0
    merchants_touched: set[str] = set()
    pending_alerts: list = []
    last_msg = None
    last_flush_time = time.time()

    try:
        while True:
            msg = consumer.poll(POLL_TIMEOUT_SECONDS)

            if msg is not None:
                if msg.error():
                    print(f"Consumer error: {msg.error()}", flush=True)
                else:
                    event = TransactionEvent.model_validate_json(msg.value())

                    stats = get_or_seed_stats(
                        r, event.merchant_id, event.merchant_name, merchant_stats
                    )
                    apply_event_in_memory(stats, event)

                    current_values = {
                        "success_rate": stats.success_rate,
                        "avg_latency_ms": stats.avg_latency_ms,
                        "avg_fraud_score": stats.avg_fraud_score,
                        "volume_per_min": len(stats.volume_timestamps),
                    }
                    for metric in METRICS:
                        result = detector.update(
                            event.merchant_id, event.merchant_name, metric, current_values[metric]
                        )
                        if isinstance(result, (AlertRecord, ResolveSignal)):
                            pending_alerts.append(result)

                    transactions_processed_total.labels(merchant_id=event.merchant_id).inc()

                    timestamp_type, timestamp_ms = msg.timestamp()
                    if timestamp_type != 0:  # TIMESTAMP_NOT_AVAILABLE
                        consumer_lag_seconds.set(time.time() - timestamp_ms / 1000.0)

                    merchants_touched.add(event.merchant_id)
                    batch_size += 1
                    last_msg = msg

            size_reached = batch_size >= STATS_BATCH_SIZE
            timed_out = (time.time() - last_flush_time) >= STATS_BATCH_TIMEOUT_SECONDS

            if batch_size > 0 and (size_reached or timed_out):
                flush_batch(
                    r,
                    pg_conn,
                    detector,
                    consumer,
                    merchant_stats,
                    merchants_touched,
                    pending_alerts,
                    last_msg,
                    batch_size,
                )
                batch_size = 0
                merchants_touched = set()
                pending_alerts = []
                last_flush_time = time.time()
    except KeyboardInterrupt:
        print("\nShutting down...")
        if batch_size > 0:
            flush_batch(
                r,
                pg_conn,
                detector,
                consumer,
                merchant_stats,
                merchants_touched,
                pending_alerts,
                last_msg,
                batch_size,
            )
    finally:
        consumer.close()
        pg_conn.close()


if __name__ == "__main__":
    main()
