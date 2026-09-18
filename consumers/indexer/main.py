import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from confluent_kafka import Consumer
from elasticsearch import Elasticsearch
from prometheus_client import Counter, Gauge, start_http_server

from shared.config import ELASTICSEARCH_URL, get_kafka_config
from shared.models import TransactionEvent

TOPIC = "transactions"
GROUP_ID = "indexer-group"
INDEX_NAME = "transactions"
METRICS_PORT = 8001

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
        f"Indexer consumer started (group '{GROUP_ID}'), subscribed to '{TOPIC}'...",
        flush=True,
    )

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            event = TransactionEvent.model_validate_json(msg.value())

            try:
                es.index(
                    index=INDEX_NAME,
                    id=event.transaction_id,
                    document=event.model_dump(mode="json"),
                )
            except Exception as e:
                # Not committed - safe to let Kafka redeliver, since
                # re-indexing the same transaction_id is idempotent
                # (overwrites the same ES doc, per Phase 0's design).
                indexer_errors_total.inc()
                print(f"Indexing error for {event.transaction_id}: {e}", flush=True)
                continue

            transactions_indexed_total.labels(merchant_id=event.merchant_id).inc()

            timestamp_type, timestamp_ms = msg.timestamp()
            if timestamp_type != 0:  # TIMESTAMP_NOT_AVAILABLE
                consumer_lag_seconds.set(time.time() - timestamp_ms / 1000.0)

            consumer.commit(message=msg, asynchronous=False)

            print(f"Indexed {event.transaction_id} for {event.merchant_name}", flush=True)
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
