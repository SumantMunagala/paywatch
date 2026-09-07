import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import redis
from confluent_kafka import Consumer

from shared.config import REDIS_URL, get_kafka_config
from shared.models import TransactionEvent

TOPIC = "transactions"
GROUP_ID = "stats-group"
VOLUME_WINDOW_SECONDS = 60


def update_stats(r: redis.Redis, event: TransactionEvent) -> dict:
    stats_key = f"stats:{event.merchant_id}"
    volume_key = f"stats:{event.merchant_id}:volume"

    pipe = r.pipeline()
    pipe.hincrby(stats_key, "transaction_count", 1)
    pipe.hincrby(stats_key, "success_count", 1 if event.status == "success" else 0)
    pipe.hincrbyfloat(stats_key, "total_latency_ms", event.processor_latency_ms)
    pipe.hincrbyfloat(stats_key, "total_fraud_score", event.fraud_score)
    transaction_count, success_count, total_latency_ms, total_fraud_score = pipe.execute()

    success_rate = success_count / transaction_count
    avg_latency_ms = total_latency_ms / transaction_count
    avg_fraud_score = total_fraud_score / transaction_count

    now = time.time()
    pipe = r.pipeline()
    pipe.zadd(volume_key, {event.transaction_id: now})
    pipe.zremrangebyscore(volume_key, 0, now - VOLUME_WINDOW_SECONDS)
    pipe.zcard(volume_key)
    _, _, volume_per_min = pipe.execute()

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


def main() -> None:
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

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


if __name__ == "__main__":
    main()
