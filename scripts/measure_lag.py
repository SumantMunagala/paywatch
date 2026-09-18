import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient

from shared.config import get_kafka_config

TOPIC = "transactions"
CONSUMER_GROUPS = ["indexer-group", "stats-group"]
DURATION_SECONDS = 30
POLL_INTERVAL_SECONDS = 2


def get_partitions(consumer: Consumer) -> list[int]:
    metadata = consumer.list_topics(TOPIC, timeout=5)
    return sorted(metadata.topics[TOPIC].partitions.keys())


def get_latest_offsets(consumer: Consumer, partitions: list[int]) -> dict[int, int]:
    latest = {}
    for partition in partitions:
        tp = TopicPartition(TOPIC, partition)
        _low, high = consumer.get_watermark_offsets(tp, timeout=5, cached=False)
        latest[partition] = high
    return latest


def get_committed_offsets(
    admin: AdminClient, group: str, partitions: list[int]
) -> dict[int, int]:
    # AdminClient.list_consumer_group_offsets reads a group's committed
    # offsets without joining it -- unlike creating a real Consumer with
    # this group.id, it never disturbs indexer-group/stats-group's actual
    # membership or triggers a rebalance of the real consumers.
    request = ConsumerGroupTopicPartitions(
        group, [TopicPartition(TOPIC, p) for p in partitions]
    )
    future = admin.list_consumer_group_offsets([request])[group]
    result = future.result()

    committed = {}
    for tp in result.topic_partitions:
        # No commit yet for this partition -> confluent-kafka reports
        # offset -1001 (OFFSET_INVALID); treat that as "0 consumed so far".
        committed[tp.partition] = tp.offset if tp.offset >= 0 else 0
    return committed


def main() -> None:
    kafka_config = get_kafka_config()
    admin = AdminClient(kafka_config)
    # group.id here is throwaway -- only used to query watermark offsets,
    # never subscribed/committed, so it can't collide with the real groups.
    watermark_consumer = Consumer(
        {**kafka_config, "group.id": "lag-measurement-throwaway"}
    )

    try:
        partitions = get_partitions(watermark_consumer)
        print(
            f"Measuring lag for {CONSUMER_GROUPS} on '{TOPIC}' "
            f"({len(partitions)} partitions) for {DURATION_SECONDS}s...\n"
        )

        start = time.time()
        while time.time() - start < DURATION_SECONDS:
            latest = get_latest_offsets(watermark_consumer, partitions)

            columns = []
            for group in CONSUMER_GROUPS:
                committed = get_committed_offsets(admin, group, partitions)
                lag = sum(latest[p] - committed.get(p, 0) for p in partitions)
                columns.append(f"{group}={lag} msgs")

            elapsed = time.time() - start
            print(f"[{elapsed:5.1f}s] " + "  ".join(columns), flush=True)

            time.sleep(POLL_INTERVAL_SECONDS)
    finally:
        watermark_consumer.close()


if __name__ == "__main__":
    main()
