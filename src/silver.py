from pyspark.sql import functions as F
from sqlalchemy import text

from src.raw import get_completed_raw_batches
from src.spark_utils import get_spark


def get_silver_batches(engine) -> set[int]:
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT batch_no
                FROM silver.batch_log
                ORDER BY batch_no
            """)
        ).fetchall()
    return {int(row[0]) for row in rows}


def build_silver_spark(engine, jdbc_url, db_properties, load_mode="incremental"):
    """
    Builds SILVER incrementally using Spark.
    Processes only completed RAW batches.
    """

    completed_raw_batches = get_completed_raw_batches(engine)
    silver_batches = get_silver_batches(engine)

    if load_mode == "incremental":
        batches_to_process = completed_raw_batches - silver_batches
    else:
        print("FULL mode → processing ALL completed batches")
        batches_to_process = completed_raw_batches

    batches_to_process = sorted(batches_to_process)

    if not batches_to_process:
        print("No new completed batches to process in SILVER (Spark)")
        return False

    spark = get_spark("silver-layer")

    for batch_no in batches_to_process:
        print(f"Processing batch {batch_no} with Spark...")

        batch_query = f"""
        (
            SELECT
                batch_no,
                transaction_id,
                customer_id,
                customer_name,
                merchant_id,
                transaction_ts,
                amount,
                city,
                country,
                payment_method,
                status
            FROM raw.transactions_raw
            WHERE batch_no = {batch_no}
        ) AS raw_batch
        """

        batch_df = spark.read.jdbc(
            url=jdbc_url,
            table=batch_query,
            properties=db_properties
        )

        df = (
            batch_df
            .withColumn("transaction_id", F.trim(F.col("transaction_id").cast("string")))
            .withColumn("customer_id", F.trim(F.col("customer_id").cast("string")))
            .withColumn("customer_name", F.initcap(F.trim(F.col("customer_name").cast("string"))))
            .withColumn("merchant_id", F.trim(F.col("merchant_id").cast("string")))
            .withColumn("city", F.initcap(F.trim(F.col("city").cast("string"))))
            .withColumn("country", F.upper(F.trim(F.col("country").cast("string"))))
            .withColumn("payment_method", F.lower(F.trim(F.col("payment_method").cast("string"))))
            .withColumn("status", F.lower(F.trim(F.col("status").cast("string"))))
            .withColumn("transaction_ts", F.try_to_timestamp(F.col("transaction_ts")))
            .withColumn(
                "amount",
                F.expr(
                    "try_cast(replace(trim(cast(amount as string)), ',', '.') as double)"
                )
            )
        )

        df = (
            df
            .withColumn(
                "validation_error",
                F.concat(
                    F.when(
                        F.col("transaction_id").isNull() |
                        (F.col("transaction_id") == "") |
                        (F.lower(F.col("transaction_id")).isin("nan", "none")),
                        F.lit("missing transaction_id; ")
                    ).otherwise(F.lit("")),

                    F.when(
                        F.col("transaction_ts").isNull(),
                        F.lit("bad date; ")
                    ).otherwise(F.lit("")),

                    F.when(
                        F.col("amount").isNull(),
                        F.lit("bad amount; ")
                    ).otherwise(F.lit("")),

                    F.when(
                        F.col("amount") < 0,
                        F.lit("negative amount; ")
                    ).otherwise(F.lit(""))
                )
            )
            .withColumn("is_valid", F.col("validation_error") == "")
            .withColumn("updated_at", F.current_timestamp())
        )

        df = df.select(
            "transaction_id",
            "batch_no",
            "customer_id",
            "customer_name",
            "merchant_id",
            "transaction_ts",
            "amount",
            "city",
            "country",
            "payment_method",
            "status",
            "is_valid",
            "validation_error",
            "updated_at"
        )

        stage_table = f"silver.transactions_clean_stage_{batch_no}"

        print(f"Writing staging table: {stage_table}")

        (
            df.write
            .mode("overwrite")
            .jdbc(
                url=jdbc_url,
                table=stage_table,
                properties=db_properties
            )
        )

        with engine.begin() as conn:
            print("Upserting into silver.transactions_clean...")

            conn.execute(text(f"""
                INSERT INTO silver.transactions_clean (
                    transaction_id,
                    batch_no,
                    customer_id,
                    customer_name,
                    merchant_id,
                    transaction_ts,
                    amount,
                    city,
                    country,
                    payment_method,
                    status,
                    is_valid,
                    validation_error,
                    updated_at
                )
                SELECT
                    transaction_id,
                    batch_no,
                    customer_id,
                    customer_name,
                    merchant_id,
                    transaction_ts,
                    amount,
                    city,
                    country,
                    payment_method,
                    status,
                    is_valid,
                    validation_error,
                    updated_at
                FROM {stage_table}
                WHERE is_valid = TRUE
                ON CONFLICT (transaction_id) DO UPDATE
                SET
                    batch_no = EXCLUDED.batch_no,
                    customer_id = EXCLUDED.customer_id,
                    customer_name = EXCLUDED.customer_name,
                    merchant_id = EXCLUDED.merchant_id,
                    transaction_ts = EXCLUDED.transaction_ts,
                    amount = EXCLUDED.amount,
                    city = EXCLUDED.city,
                    country = EXCLUDED.country,
                    payment_method = EXCLUDED.payment_method,
                    status = EXCLUDED.status,
                    is_valid = EXCLUDED.is_valid,
                    validation_error = EXCLUDED.validation_error,
                    updated_at = CURRENT_TIMESTAMP
            """))

            conn.execute(
                text("""
                    INSERT INTO silver.batch_log (batch_no)
                    VALUES (:batch_no)
                    ON CONFLICT (batch_no) DO NOTHING
                """),
                {"batch_no": int(batch_no)}
            )

            conn.execute(text(f"DROP TABLE IF EXISTS {stage_table}"))

        print(f"Batch {batch_no} processed successfully")

    spark.stop()
    print("SILVER Spark complete")
    return True