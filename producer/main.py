import argparse
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg2
from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from shared.config import POSTGRES_URL, get_kafka_config
from shared.models import TransactionEvent

TOPIC = "transactions"
# Matches the live topic's current partition count (raised from 3 to 10 via
# scripts/set_partitions.py) -- only takes effect if the topic doesn't
# already exist, but keeping it in sync avoids silently recreating a
# 3-partition topic in a fresh environment.
TOPIC_PARTITIONS = 10

PAYMENT_METHODS = ["card", "wallet", "bank_transfer"]
PAYMENT_METHOD_WEIGHTS = [0.7, 0.2, 0.1]


def load_merchants() -> list[dict]:
    # Baselines now live in Postgres (scripts/seed_merchants.py), not a
    # hardcoded dict here -- this is what lets the producer generate
    # realistic traffic for every merchant the database happens to have,
    # not just 5 known-by-name ones.
    conn = psycopg2.connect(POSTGRES_URL)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, name, baseline_success_rate, baseline_avg_latency_ms,
                       baseline_avg_fraud_score
                FROM merchants
                ORDER BY name
                """
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        raise RuntimeError(
            "No merchants found in the database. "
            "Run scripts/apply_schema.py and scripts/seed_merchants.py first."
        )

    missing_baseline = [name for _id, name, sr, _lat, _fr in rows if sr is None]
    if missing_baseline:
        raise RuntimeError(
            f"Merchants missing a baseline: {missing_baseline}. "
            "Run scripts/seed_merchants.py to backfill baseline columns."
        )

    return [
        {
            "id": str(merchant_id),
            "name": name,
            "success_rate": success_rate,
            "latency_ms": avg_latency_ms,
            "fraud_score": avg_fraud_score,
        }
        for merchant_id, name, success_rate, avg_latency_ms, avg_fraud_score in rows
    ]


def build_event(merchant: dict) -> TransactionEvent:
    success_rate = min(1.0, max(0.0, merchant["success_rate"] + random.uniform(-0.05, 0.05)))
    latency_ms = max(1, round(merchant["latency_ms"] + random.uniform(-30, 30)))
    fraud_score = min(1.0, max(0.0, merchant["fraud_score"] + random.uniform(-0.02, 0.02)))

    if random.random() < success_rate:
        status = "success"
    else:
        status = random.choice(["failed", "declined"])

    payment_method = random.choices(PAYMENT_METHODS, weights=PAYMENT_METHOD_WEIGHTS)[0]

    return TransactionEvent(
        transaction_id=str(uuid.uuid4()),
        merchant_id=merchant["id"],
        merchant_name=merchant["name"],
        amount=round(random.uniform(5, 500), 2),
        status=status,
        payment_method=payment_method,
        fraud_score=fraud_score,
        processor_latency_ms=latency_ms,
        timestamp=datetime.now(timezone.utc),
    )


def ensure_topic(kafka_config: dict) -> None:
    admin = AdminClient(kafka_config)

    existing_topics = admin.list_topics(timeout=5).topics
    if TOPIC in existing_topics:
        return

    # Redpanda would otherwise auto-create this with a single partition on
    # first produce, which makes merchant_id-based partitioning meaningless
    # (everything lands on the same partition regardless of key).
    futures = admin.create_topics(
        [NewTopic(TOPIC, num_partitions=TOPIC_PARTITIONS, replication_factor=1)]
    )
    for topic, future in futures.items():
        try:
            future.result()
            print(f"Created topic '{topic}' with {TOPIC_PARTITIONS} partitions.")
        except KafkaException as e:
            # Another producer instance may have created it in the meantime.
            if "already exists" not in str(e).lower():
                raise


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for {msg.key()}: {err}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish simulated transactions.")
    parser.add_argument(
        "--rate",
        type=int,
        default=1,
        help="Events per second, per merchant (default: 1).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    merchants = load_merchants()
    kafka_config = get_kafka_config()
    ensure_topic(kafka_config)
    producer = Producer(kafka_config)

    # Printing every event's JSON is useful at the default interactive rate
    # (1/sec/merchant), but at load-test rates the print+flush call becomes
    # the bottleneck being measured instead of real Kafka/consumer
    # throughput -- skip it whenever a non-default rate is requested.
    verbose = args.rate == 1
    interval = 1 / args.rate

    print(
        f"Publishing to '{TOPIC}' for {len(merchants)} merchants at "
        f"{args.rate}/sec/merchant ({args.rate * len(merchants)}/sec total, "
        "Ctrl+C to stop)..."
    )

    try:
        while True:
            tick_start = time.time()

            for merchant in merchants:
                event = build_event(merchant)

                producer.poll(0)
                producer.produce(
                    TOPIC,
                    key=event.merchant_id.encode("utf-8"),
                    value=event.model_dump_json().encode("utf-8"),
                    callback=delivery_report,
                )

                if verbose:
                    print(event.model_dump_json(), flush=True)

            # One event per merchant per tick, ticks spaced `interval`
            # seconds apart, gives each merchant exactly `rate` events/sec
            # regardless of how long producing this tick's events took.
            elapsed = time.time() - tick_start
            time.sleep(max(0.0, interval - elapsed))
    except KeyboardInterrupt:
        print("\nShutting down, flushing pending messages...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
