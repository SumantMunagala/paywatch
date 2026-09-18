import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from confluent_kafka.admin import AdminClient, NewPartitions

from shared.config import get_kafka_config

TOPIC = "transactions"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Idempotently increase the transactions topic's partition count."
    )
    parser.add_argument("--partitions", type=int, default=10)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    admin = AdminClient(get_kafka_config())

    metadata = admin.list_topics(TOPIC, timeout=5)
    if TOPIC not in metadata.topics:
        raise RuntimeError(f"Topic '{TOPIC}' does not exist yet -- run the producer first.")

    current = len(metadata.topics[TOPIC].partitions)
    print(f"'{TOPIC}' currently has {current} partition(s).")

    if current >= args.partitions:
        print(f"Already at or above {args.partitions} partitions -- nothing to do.")
        return

    # Kafka/Redpanda only support increasing a topic's partition count, never
    # decreasing it (existing partitions can't be merged back together
    # without losing the key->partition mapping for whatever was on them) --
    # the current >= args.partitions check above is what makes this call
    # idempotent and safe to rerun, not a workaround for a real decrease path.
    futures = admin.create_partitions([NewPartitions(TOPIC, args.partitions)])
    for topic, future in futures.items():
        future.result()
        print(f"'{topic}': partition count increased to {args.partitions}.")


if __name__ == "__main__":
    main()
