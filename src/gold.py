from sqlalchemy import text


def build_gold(engine):
    with engine.begin() as conn:
        print("Building gold.dim_customer...")
        conn.execute(text("""
            INSERT INTO gold.dim_customer (
                customer_id,
                customer_name,
                updated_at
            )
            SELECT
                x.customer_id,
                x.customer_name,
                CURRENT_TIMESTAMP
            FROM (
                SELECT DISTINCT ON (s.customer_id)
                    s.customer_id,
                    s.customer_name,
                    s.batch_no,
                    s.updated_at
                FROM silver.transactions_clean s
                WHERE s.is_valid = true
                  AND s.customer_id IS NOT NULL
                  AND s.customer_id <> ''
                ORDER BY
                    s.customer_id,
                    s.batch_no DESC,
                    s.updated_at DESC
            ) x
            ON CONFLICT (customer_id) DO UPDATE
            SET
                customer_name = EXCLUDED.customer_name,
                updated_at = CURRENT_TIMESTAMP
        """))

        print("Building gold.dim_merchant...")
        conn.execute(text("""
            INSERT INTO gold.dim_merchant (
                merchant_id,
                city,
                country,
                updated_at
            )
            SELECT
                x.merchant_id,
                x.city,
                x.country,
                CURRENT_TIMESTAMP
            FROM (
                SELECT DISTINCT ON (s.merchant_id)
                    s.merchant_id,
                    s.city,
                    s.country,
                    s.batch_no,
                    s.updated_at
                FROM silver.transactions_clean s
                WHERE s.is_valid = true
                  AND s.merchant_id IS NOT NULL
                  AND s.merchant_id <> ''
                ORDER BY
                    s.merchant_id,
                    s.batch_no DESC,
                    s.updated_at DESC
            ) x
            ON CONFLICT (merchant_id) DO UPDATE
            SET
                city = EXCLUDED.city,
                country = EXCLUDED.country,
                updated_at = CURRENT_TIMESTAMP
        """))

        print("Building gold.dim_date...")
        conn.execute(text("""
            INSERT INTO gold.dim_date (
                date_id,
                year,
                month,
                day
            )
            SELECT DISTINCT
                s.transaction_ts::date AS date_id,
                EXTRACT(YEAR FROM s.transaction_ts)::int AS year,
                EXTRACT(MONTH FROM s.transaction_ts)::int AS month,
                EXTRACT(DAY FROM s.transaction_ts)::int AS day
            FROM silver.transactions_clean s
            WHERE s.is_valid = true
              AND s.transaction_ts IS NOT NULL
            ON CONFLICT (date_id) DO UPDATE
            SET
                year = EXCLUDED.year,
                month = EXCLUDED.month,
                day = EXCLUDED.day
        """))

        print("Building gold.fact_transactions...")
        conn.execute(text("""
            INSERT INTO gold.fact_transactions (
                transaction_id,
                customer_id,
                merchant_id,
                date_id,
                amount,
                payment_method,
                status,
                updated_at
            )
            SELECT
                x.transaction_id,
                x.customer_id,
                x.merchant_id,
                x.date_id,
                x.amount,
                x.payment_method,
                x.status,
                CURRENT_TIMESTAMP
            FROM (
                SELECT DISTINCT ON (s.transaction_id)
                    s.transaction_id,
                    s.customer_id,
                    s.merchant_id,
                    s.transaction_ts::date AS date_id,
                    s.amount,
                    s.payment_method,
                    s.status,
                    s.batch_no,
                    s.updated_at
                FROM silver.transactions_clean s
                WHERE s.is_valid = true
                  AND s.transaction_id IS NOT NULL
                  AND s.transaction_id <> ''
                ORDER BY
                    s.transaction_id,
                    s.batch_no DESC,
                    s.updated_at DESC
            ) x
            ON CONFLICT (transaction_id) DO UPDATE
            SET
                customer_id = EXCLUDED.customer_id,
                merchant_id = EXCLUDED.merchant_id,
                date_id = EXCLUDED.date_id,
                amount = EXCLUDED.amount,
                payment_method = EXCLUDED.payment_method,
                status = EXCLUDED.status,
                updated_at = CURRENT_TIMESTAMP
        """))

        print("Creating view gold.v_transaction_report...")
        conn.execute(text("""
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
                ON f.date_id = d.date_id
        """))

    print("GOLD complete")