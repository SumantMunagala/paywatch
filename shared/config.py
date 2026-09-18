import json
import os

import boto3
from dotenv import load_dotenv

load_dotenv()

AWS_REGION = os.getenv("AWS_REGION")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME")
KAFKA_SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD")
ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")


def _fetch_postgres_url_from_secrets_manager() -> str:
    client = boto3.client("secretsmanager", region_name=AWS_REGION)
    response = client.get_secret_value(SecretId="paywatch/rds-credentials")
    secret = json.loads(response["SecretString"])
    return (
        f"postgresql://{secret['username']}:{secret['password']}"
        f"@{secret['host']}:{secret['port']}/{secret['dbname']}"
    )


if AWS_REGION:
    POSTGRES_URL = _fetch_postgres_url_from_secrets_manager()
else:
    POSTGRES_URL = os.getenv(
        "POSTGRES_URL", "postgresql://postgres:postgres@localhost:5432/paywatch"
    )

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")


def get_kafka_config() -> dict:
    config = {"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS}

    if KAFKA_SASL_USERNAME:
        config.update(
            {
                "security.protocol": "SASL_SSL",
                "sasl.mechanisms": "PLAIN",
                "sasl.username": KAFKA_SASL_USERNAME,
                "sasl.password": KAFKA_SASL_PASSWORD,
            }
        )

    return config
