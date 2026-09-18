CREATE TABLE IF NOT EXISTS merchants (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    -- The producer's per-merchant "normal" behavior, read at startup so it
    -- can generate events for every seeded merchant without a hardcoded
    -- Python dict. Nullable because CREATE TABLE IF NOT EXISTS won't add
    -- these to an already-existing table -- scripts/seed_merchants.py runs
    -- an idempotent ALTER TABLE ADD COLUMN IF NOT EXISTS for that case.
    baseline_success_rate FLOAT,
    baseline_avg_latency_ms FLOAT,
    baseline_avg_fraud_score FLOAT
);

CREATE TABLE IF NOT EXISTS transactions (
    id UUID PRIMARY KEY,
    merchant_id UUID REFERENCES merchants(id),
    amount FLOAT NOT NULL,
    currency TEXT NOT NULL,
    status TEXT NOT NULL,
    payment_method TEXT NOT NULL,
    fraud_score FLOAT NOT NULL,
    processor_latency_ms INT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS alerts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    merchant_id UUID REFERENCES merchants(id),
    metric TEXT NOT NULL,
    current_value FLOAT NOT NULL,
    baseline_value FLOAT NOT NULL,
    z_score FLOAT NOT NULL,
    description TEXT NOT NULL,
    fired_at TIMESTAMPTZ DEFAULT now(),
    resolved_at TIMESTAMPTZ
);
