CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS raw.ingestion_log (
    id SERIAL PRIMARY KEY,
    file_name TEXT NOT NULL,
    file_hash TEXT NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (file_name, file_hash)
);

CREATE TABLE IF NOT EXISTS raw.transactions_raw (
    raw_id BIGSERIAL PRIMARY KEY,
    batch_no INT NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    transaction_id TEXT,
    customer_id TEXT,
    customer_name TEXT,
    merchant_id TEXT,
    transaction_ts TEXT,
    amount TEXT,
    city TEXT,
    country TEXT,
    payment_method TEXT,
    status TEXT
);

CREATE TABLE IF NOT EXISTS silver.batch_log (
    batch_no INT PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.transactions_clean (
    transaction_id TEXT PRIMARY KEY,
    batch_no INT NOT NULL,

    customer_id TEXT,
    customer_name TEXT,
    merchant_id TEXT,
    transaction_ts TIMESTAMP,
    amount NUMERIC,
    city TEXT,
    country TEXT,
    payment_method TEXT,
    status TEXT,

    is_valid BOOLEAN NOT NULL,
    validation_error TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS gold.dim_customer (
    customer_id TEXT PRIMARY KEY,
    customer_name TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS gold.dim_merchant (
    merchant_id TEXT PRIMARY KEY,
    city TEXT,
    country TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_id DATE PRIMARY KEY,
    year INT,
    month INT,
    day INT
);

CREATE TABLE IF NOT EXISTS gold.fact_transactions (
    transaction_id TEXT PRIMARY KEY,
    customer_id TEXT,
    merchant_id TEXT,
    date_id DATE,
    amount NUMERIC,
    payment_method TEXT,
    status TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.batch_control (
    batch_no INT PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('loading', 'completed')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_batch_no
    ON raw.transactions_raw(batch_no);

CREATE INDEX IF NOT EXISTS idx_raw_transaction_id
    ON raw.transactions_raw(transaction_id);


CREATE INDEX IF NOT EXISTS idx_silver_batch_no
    ON silver.transactions_clean(batch_no);

CREATE INDEX IF NOT EXISTS idx_silver_valid
    ON silver.transactions_clean(is_valid);

CREATE INDEX IF NOT EXISTS idx_fact_date_id
    ON gold.fact_transactions(date_id);

CREATE OR REPLACE VIEW gold.v_transaction_report AS
SELECT
    f.transaction_id,
    d.year,
    d.month,
    d.day,
    c.customer_name,
    m.city,
    m.country,
    f.amount,
    f.payment_method,
    f.status
FROM gold.fact_transactions f
JOIN gold.dim_customer c
    ON f.customer_id = c.customer_id
JOIN gold.dim_merchant m
    ON f.merchant_id = m.merchant_id
JOIN gold.dim_date d
    ON f.date_id = d.date_id;

CREATE SEQUENCE IF NOT EXISTS raw.batch_no_seq START 1;