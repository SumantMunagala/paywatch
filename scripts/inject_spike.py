import argparse
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import redis
from confluent_kafka import Producer

from shared.config import REDIS_URL, get_kafka_config
from shared.models import TransactionEvent

TOPIC = "transactions"
PUBLISH_RATE_PER_SEC = 5
COUNTDOWN_INTERVAL_SECONDS = 5

PAYMENT_METHODS = ["card", "wallet", "bank_transfer"]
PAYMENT_METHOD_WEIGHTS = [0.7, 0.2, 0.1]

# Fallback baseline if the merchant has no live stats yet (fresh
# environment, never seen a transaction). Used only for whichever 3 fields
# aren't the one being spiked.
FALLBACK_SUCCESS_RATE = 0.95
FALLBACK_FRAUD_SCORE = 0.10
FALLBACK_LATENCY_MS = 200.0


def fetch_merchant_baseline(merchant_id: str) -> dict:
    # Read the merchant's own current EWMA values once at startup, rather
    # than using generic constants -- a fixed constant is very unlikely to
    # land exactly on any one merchant's own tightly-learned baseline, so
    # "normal" values for the 3 non-targeted fields would themselves look
    # anomalous relative to that merchant's specific history (confirmed
    # live: an earlier version of this script using generic ranges
    # triggered latency/fraud_score alerts as an unintended side effect of
    # testing success_rate).
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    stats = r.hgetall(f"stats:{merchant_id}")

    return {
        "success_rate": float(stats.get("success_rate", FALLBACK_SUCCESS_RATE)),
        "fraud_score": float(stats.get("avg_fraud_score", FALLBACK_FRAUD_SCORE)),
        "latency_ms": float(stats.get("avg_latency_ms", FALLBACK_LATENCY_MS)),
    }


def build_spike_event(
    merchant_id: str, merchant_name: str, spike_type: str, baseline: dict
) -> TransactionEvent:
    success_rate = 0.35 if spike_type == "success_rate" else baseline["success_rate"]
    fraud_score = (
        random.uniform(0.75, 0.95)
        if spike_type == "fraud_score"
        else min(1.0, max(0.0, baseline["fraud_score"] + random.uniform(-0.02, 0.02)))
    )
    latency_ms = (
        round(random.uniform(3500, 6000))
        if spike_type == "latency"
        else max(1, round(baseline["latency_ms"] + random.uniform(-30, 30)))
    )

    if random.random() < success_rate:
        status = "success"
    else:
        status = random.choice(["failed", "declined"])

    payment_method = random.choices(PAYMENT_METHODS, weights=PAYMENT_METHOD_WEIGHTS)[0]

    return TransactionEvent(
        transaction_id=str(uuid.uuid4()),
        merchant_id=merchant_id,
        merchant_name=merchant_name,
        amount=round(random.uniform(5, 500), 2),
        status=status,
        payment_method=payment_method,
        fraud_score=fraud_score,
        processor_latency_ms=latency_ms,
        timestamp=datetime.now(timezone.utc),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish anomalous transactions for one merchant to trigger an alert."
    )
    parser.add_argument("--merchant-id", required=True)
    parser.add_argument("--merchant-name", required=True)
    parser.add_argument(
        "--type",
        required=True,
        choices=["success_rate", "volume", "fraud_score", "latency"],
    )
    parser.add_argument("--duration", type=int, default=30)
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    baseline = fetch_merchant_baseline(args.merchant_id)
    producer = Producer(get_kafka_config())

    print(
        f"Injecting a '{args.type}' spike for {args.merchant_name} ({args.merchant_id}) "
        f"over {args.duration}s...",
        flush=True,
    )
    if args.type == "volume":
        print(
            "(volume spike publishes nothing for this merchant -- if the normal "
            "producer is still running for it, its traffic will keep volume_per_min "
            "up regardless; stop the producer first to see a real drop)",
            flush=True,
        )

    published = 0
    try:
        for elapsed in range(args.duration):
            remaining = args.duration - elapsed
            if remaining % COUNTDOWN_INTERVAL_SECONDS == 0:
                print(f"  {remaining}s remaining...", flush=True)

            if args.type != "volume":
                for _ in range(PUBLISH_RATE_PER_SEC):
                    event = build_spike_event(
                        args.merchant_id, args.merchant_name, args.type, baseline
                    )
                    producer.poll(0)
                    producer.produce(
                        TOPIC,
                        key=event.merchant_id.encode("utf-8"),
                        value=event.model_dump_json().encode("utf-8"),
                    )
                    published += 1

            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping early...")
    finally:
        producer.flush()

    print(f"Done. Published {published} spike transaction(s).", flush=True)


if __name__ == "__main__":
    main()
