import os
from pathlib import Path

import psycopg2

DATABASE_URL = os.getenv(
    "POSTGRES_URL", "postgresql://postgres:postgres@localhost:5435/paywatch"
)
SCHEMA_PATH = Path(__file__).resolve().parent.parent / "infra" / "schema.sql"

MERCHANT_NAMES = [
    "Acme Store",
    "Bravo Retail",
    "Cypress Fashion",
    "Delta Electronics",
    "Echo Marketplace",
]


def main() -> None:
    schema_sql = SCHEMA_PATH.read_text()

    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor() as cur:
            cur.execute(schema_sql)
            print("Applied schema.sql (merchants, transactions, alerts).")

            cur.execute("SELECT COUNT(*) FROM merchants")
            (merchant_count,) = cur.fetchone()

            if merchant_count == 0:
                cur.executemany(
                    "INSERT INTO merchants (name) VALUES (%s)",
                    [(name,) for name in MERCHANT_NAMES],
                )
                print(f"Seeded {len(MERCHANT_NAMES)} merchants.")
            else:
                print(
                    f"Merchants table already has {merchant_count} rows, skipping seed."
                )
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
