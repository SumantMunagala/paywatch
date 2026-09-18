import os
import random

import psycopg2

POSTGRES_URL = os.getenv(
    "POSTGRES_URL", "postgresql://postgres:postgres@localhost:5435/paywatch"
)

# The original 5 merchants' baselines, previously hardcoded in
# producer/main.py's MERCHANT_BASELINES -- backfilled into the new columns
# unchanged so their existing Redis/WelfordState continues to make sense
# against the same "normal" behavior it was learned from.
ORIGINAL_MERCHANT_BASELINES = {
    "Acme Store": {"success_rate": 0.96, "avg_latency_ms": 180.0, "avg_fraud_score": 0.08},
    "Bravo Retail": {"success_rate": 0.94, "avg_latency_ms": 220.0, "avg_fraud_score": 0.11},
    "Cypress Fashion": {"success_rate": 0.97, "avg_latency_ms": 150.0, "avg_fraud_score": 0.07},
    "Delta Electronics": {"success_rate": 0.93, "avg_latency_ms": 260.0, "avg_fraud_score": 0.13},
    "Echo Marketplace": {"success_rate": 0.95, "avg_latency_ms": 200.0, "avg_fraud_score": 0.09},
}

NEW_MERCHANT_COUNT = 45
SUCCESS_RATE_RANGE = (0.92, 0.98)
LATENCY_MS_RANGE = (120.0, 300.0)
FRAUD_SCORE_RANGE = (0.06, 0.15)


def ensure_baseline_columns(cur) -> None:
    cur.execute(
        """
        ALTER TABLE merchants
            ADD COLUMN IF NOT EXISTS baseline_success_rate FLOAT,
            ADD COLUMN IF NOT EXISTS baseline_avg_latency_ms FLOAT,
            ADD COLUMN IF NOT EXISTS baseline_avg_fraud_score FLOAT
        """
    )


def backfill_original_baselines(cur) -> None:
    for name, baseline in ORIGINAL_MERCHANT_BASELINES.items():
        cur.execute(
            """
            UPDATE merchants
            SET baseline_success_rate = %s,
                baseline_avg_latency_ms = %s,
                baseline_avg_fraud_score = %s
            WHERE name = %s AND baseline_success_rate IS NULL
            """,
            (
                baseline["success_rate"],
                baseline["avg_latency_ms"],
                baseline["avg_fraud_score"],
                name,
            ),
        )


def seed_new_merchants(cur) -> int:
    cur.execute("SELECT COUNT(*) FROM merchants WHERE name LIKE 'Merchant %'")
    (existing,) = cur.fetchone()
    if existing > 0:
        print(f"{existing} 'Merchant NN' row(s) already exist -- skipping seed.")
        return 0

    rows = []
    for i in range(6, 6 + NEW_MERCHANT_COUNT):
        name = f"Merchant {i:02d}"
        rows.append(
            (
                name,
                random.uniform(*SUCCESS_RATE_RANGE),
                random.uniform(*LATENCY_MS_RANGE),
                random.uniform(*FRAUD_SCORE_RANGE),
            )
        )

    cur.executemany(
        """
        INSERT INTO merchants
            (name, baseline_success_rate, baseline_avg_latency_ms, baseline_avg_fraud_score)
        VALUES (%s, %s, %s, %s)
        """,
        rows,
    )
    return len(rows)


def main() -> None:
    conn = psycopg2.connect(POSTGRES_URL)
    try:
        with conn.cursor() as cur:
            ensure_baseline_columns(cur)
            backfill_original_baselines(cur)
            added = seed_new_merchants(cur)
            if added:
                print(f"Seeded {added} new merchants (Merchant 06 through Merchant 50).")
        conn.commit()

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM merchants")
            (total,) = cur.fetchone()
        print(f"merchants table now has {total} row(s) total.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
