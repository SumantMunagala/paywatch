import sys
import time
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
METRICS_PORT = 8002

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


def update_stats(r: redis.Redis, event: TransactionEvent) -> dict:
    stats_key = f"stats:{event.merchant_id}"
    volume_key = f"stats:{event.merchant_id}:volume"

    previous = r.hgetall(stats_key)
    is_success = 1.0 if event.status == "success" else 0.0

    if previous:
        success_rate = EWMA_ALPHA_SUCCESS_RATE * is_success + (
            1 - EWMA_ALPHA_SUCCESS_RATE
        ) * float(previous["success_rate"])
        avg_latency_ms = EWMA_ALPHA * event.processor_latency_ms + (1 - EWMA_ALPHA) * float(
            previous["avg_latency_ms"]
        )
        avg_fraud_score = EWMA_ALPHA * event.fraud_score + (1 - EWMA_ALPHA) * float(
            previous["avg_fraud_score"]
        )
    else:
        # First transaction ever seen for this merchant: no prior EWMA to
        # blend with, so bootstrap directly from this observation.
        success_rate = is_success
        avg_latency_ms = float(event.processor_latency_ms)
        avg_fraud_score = event.fraud_score

    now = time.time()
    pipe = r.pipeline()
    pipe.hincrby(stats_key, "transaction_count", 1)
    pipe.hincrby(stats_key, "success_count", 1 if event.status == "success" else 0)
    pipe.zadd(volume_key, {event.transaction_id: now})
    pipe.zremrangebyscore(volume_key, 0, now - VOLUME_WINDOW_SECONDS)
    pipe.zcard(volume_key)
    transaction_count, success_count, _, _, volume_per_min = pipe.execute()

    r.hset(
        stats_key,
        mapping={
            "success_rate": success_rate,
            "avg_latency_ms": avg_latency_ms,
            "avg_fraud_score": avg_fraud_score,
            "volume_per_min": volume_per_min,
        },
    )

    return {
        "transaction_count": transaction_count,
        "success_count": success_count,
        "success_rate": success_rate,
        "avg_latency_ms": avg_latency_ms,
        "avg_fraud_score": avg_fraud_score,
        "volume_per_min": volume_per_min,
    }


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


def check_anomalies(
    detector: AnomalyDetector,
    r: redis.Redis,
    pg_conn,
    event: TransactionEvent,
    stats: dict,
) -> None:
    for metric in METRICS:
        result = detector.update(event.merchant_id, event.merchant_name, metric, stats[metric])

        if isinstance(result, AlertRecord):
            record_alert_fired(r, pg_conn, result)
        elif isinstance(result, ResolveSignal):
            record_alert_resolved(r, pg_conn, result)


def main() -> None:
    start_http_server(METRICS_PORT)

    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    pg_conn = psycopg2.connect(POSTGRES_URL)
    pg_conn.autocommit = True
    detector = AnomalyDetector(r)

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
        f"Stats consumer started (group '{GROUP_ID}'), subscribed to '{TOPIC}'...",
        flush=True,
    )

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}", flush=True)
                continue

            event = TransactionEvent.model_validate_json(msg.value())
            stats = update_stats(r, event)
            check_anomalies(detector, r, pg_conn, event, stats)

            transactions_processed_total.labels(merchant_id=event.merchant_id).inc()

            timestamp_type, timestamp_ms = msg.timestamp()
            if timestamp_type != 0:  # TIMESTAMP_NOT_AVAILABLE
                consumer_lag_seconds.set(time.time() - timestamp_ms / 1000.0)

            print(
                f"[{event.merchant_name}] "
                f"count={stats['transaction_count']} "
                f"success_rate={stats['success_rate']:.2%} "
                f"avg_latency_ms={stats['avg_latency_ms']:.1f} "
                f"avg_fraud_score={stats['avg_fraud_score']:.3f} "
                f"volume_per_min={stats['volume_per_min']}",
                flush=True,
            )

            consumer.commit(message=msg, asynchronous=False)
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        consumer.close()
        pg_conn.close()


if __name__ == "__main__":
    main()
