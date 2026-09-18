import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from confluent_kafka import Consumer
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from prometheus_client import Counter, Gauge, start_http_server

from shared.config import ELASTICSEARCH_URL, get_kafka_config
from shared.models import TransactionEvent

TOPIC = "transactions"
GROUP_ID = "indexer-group"
INDEX_NAME = "transactions"
# Same reasoning as the stats consumer's CONSUMER_INSTANCE_ID/METRICS_PORT:
# logging-only label plus a per-instance port, both required for more than
# one instance to run on the same machine at once.
CONSUMER_INSTANCE_ID = os.getenv("CONSUMER_INSTANCE_ID", "indexer-1")
METRICS_PORT = int(os.getenv("METRICS_PORT", "8001"))

BATCH_SIZE = 100
BATCH_TIMEOUT_SECONDS = 2.0
# poll(1.0) would block up to a full second before the loop can even check
# whether BATCH_TIMEOUT_SECONDS has elapsed -- a short poll timeout lets the
# timeout-based flush fire close to on schedule even when messages are
# arriving slowly (or not at all).
POLL_TIMEOUT_SECONDS = 0.1

transactions_indexed_total = Counter(
    "transactions_indexed_total", "Total transactions indexed", ["merchant_id"]
)
indexer_errors_total = Counter("indexer_errors_total", "Total indexer errors")
consumer_lag_seconds = Gauge(
    "consumer_lag_seconds", "Time between a message's Kafka timestamp and processing it"
)

INDEX_MAPPINGS = {
    "properties": {
        "timestamp": {"type": "date"},
        "status": {"type": "keyword"},
        "payment_method": {"type": "keyword"},
        "amount": {"type": "float"},
        "fraud_score": {"type": "float"},
        "processor_latency_ms": {"type": "integer"},
        "merchant_id": {"type": "keyword"},
    }
}


def ensure_index(es: Elasticsearch) -> None:
    if not es.indices.exists(index=INDEX_NAME):
        es.indices.create(index=INDEX_NAME, mappings=INDEX_MAPPINGS)
        print(f"Created index '{INDEX_NAME}' with explicit mappings.", flush=True)


def flush_batch(
    es: Elasticsearch, consumer: Consumer, buffer: list[TransactionEvent], last_msg
) -> None:
    actions = [
        {
            "_index": INDEX_NAME,
            "_id": event.transaction_id,
            "_source": event.model_dump(mode="json"),
        }
        for event in buffer
    ]

    try:
        bulk(es, actions)
    except Exception as e:
        # Not committed - the same idempotent-redelivery design as before
        # (re-indexing the same transaction_id overwrites the same ES doc),
        # except now at batch granularity: this batch isn't retried within
        # this process, only redelivered from the last committed offset if
        # the consumer restarts.
        indexer_errors_total.inc(len(buffer))
        print(f"Bulk indexing error for {len(buffer)} transactions: {e}", flush=True)
        return

    breakdown_counts: dict[str, int] = {}
    for event in buffer:
        transactions_indexed_total.labels(merchant_id=event.merchant_id).inc()
        breakdown_counts[event.merchant_name] = breakdown_counts.get(event.merchant_name, 0) + 1
    breakdown = ", ".join(f"{name}: {count}" for name, count in breakdown_counts.items())

    # One commit for the whole batch, at the last message's offset - Kafka
    # commits are a watermark ("everything up to here is done"), so this
    # correctly covers every message in the batch without committing per
    # message.
    consumer.commit(message=last_msg, asynchronous=False)

    print(
        f"[{CONSUMER_INSTANCE_ID}] Bulk indexed {len(buffer)} transactions ({breakdown})",
        flush=True,
    )


def main() -> None:
    start_http_server(METRICS_PORT)

    es = Elasticsearch(ELASTICSEARCH_URL)
    ensure_index(es)

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
        f"[{CONSUMER_INSTANCE_ID}] Indexer consumer started (group '{GROUP_ID}'), "
        f"subscribed to '{TOPIC}', batching up to {BATCH_SIZE} messages or every "
        f"{BATCH_TIMEOUT_SECONDS}s, metrics on :{METRICS_PORT}...",
        flush=True,
    )

    buffer: list[TransactionEvent] = []
    last_msg = None
    last_flush_time = time.time()

    try:
        while True:
            msg = consumer.poll(POLL_TIMEOUT_SECONDS)

            if msg is not None:
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                else:
                    event = TransactionEvent.model_validate_json(msg.value())
                    buffer.append(event)
                    last_msg = msg

                    timestamp_type, timestamp_ms = msg.timestamp()
                    if timestamp_type != 0:  # TIMESTAMP_NOT_AVAILABLE
                        consumer_lag_seconds.set(time.time() - timestamp_ms / 1000.0)

            size_reached = len(buffer) >= BATCH_SIZE
            timed_out = (time.time() - last_flush_time) >= BATCH_TIMEOUT_SECONDS

            if buffer and (size_reached or timed_out):
                flush_batch(es, consumer, buffer, last_msg)
                buffer = []
                last_flush_time = time.time()
    except KeyboardInterrupt:
        print("\nShutting down...")
        if buffer:
            flush_batch(es, consumer, buffer, last_msg)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
