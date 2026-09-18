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
TOPIC_PARTITIONS = 3

MERCHANT_BASELINES = {
    "Acme Store": {"success_rate": 0.96, "latency_ms": 180, "fraud_score": 0.08},
    "Bravo Retail": {"success_rate": 0.94, "latency_ms": 220, "fraud_score": 0.11},
    "Cypress Fashion": {"success_rate": 0.97, "latency_ms": 150, "fraud_score": 0.07},
    "Delta Electronics": {"success_rate": 0.93, "latency_ms": 260, "fraud_score": 0.13},
    "Echo Marketplace": {"success_rate": 0.95, "latency_ms": 200, "fraud_score": 0.09},
}

PAYMENT_METHODS = ["card", "wallet", "bank_transfer"]
PAYMENT_METHOD_WEIGHTS = [0.7, 0.2, 0.1]


def load_merchants() -> list[dict]:
    conn = psycopg2.connect(POSTGRES_URL)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name FROM merchants")
            rows = cur.fetchall()
    finally:
        conn.close()

    ids_by_name = {name: str(merchant_id) for merchant_id, name in rows}

    missing = [name for name in MERCHANT_BASELINES if name not in ids_by_name]
    if missing:
        raise RuntimeError(
            f"Merchants missing from database: {missing}. "
            "Run scripts/apply_schema.py first."
        )

    return [
        {"id": ids_by_name[name], "name": name, **baseline}
        for name, baseline in MERCHANT_BASELINES.items()
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

    print(
        f"Publishing to '{TOPIC}' for {len(merchants)} merchants "
        f"at {args.rate}/sec/merchant (Ctrl+C to stop)..."
    )

    try:
        while True:
            for merchant in merchants:
                for _ in range(args.rate):
                    event = build_event(merchant)

                    producer.poll(0)
                    producer.produce(
                        TOPIC,
                        key=event.merchant_id.encode("utf-8"),
                        value=event.model_dump_json().encode("utf-8"),
                        callback=delivery_report,
                    )

                    print(event.model_dump_json(), flush=True)

            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down, flushing pending messages...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
