import json
import os
import time

from kafka import KafkaConsumer
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

from src.raw import create_new_batch, mark_batch_completed


DB_URL = os.getenv(
    "DB_URL",
    "postgresql+psycopg2://postgres:postgres@postgres:5432/medallion"
)

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw-transactions")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "raw-consumer")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100000"))


def get_engine():
    for _ in range(30):
        try:
            engine = create_engine(DB_URL)
            with engine.connect():
                pass
            return engine
        except OperationalError:
            print("Waiting for database...")
            time.sleep(2)
    raise RuntimeError("Could not connect to database")


def wait_for_kafka(max_retries: int = 30, sleep_seconds: int = 2):
    for attempt in range(1, max_retries + 1):
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=KAFKA_GROUP_ID,
            )
            consumer.close()
            print("Kafka is ready")
            return
        except Exception as e:
            print(f"Waiting for Kafka... attempt={attempt}/{max_retries}, error={e}")
            time.sleep(sleep_seconds)

    raise RuntimeError("Kafka is not available")


def consume_kafka_to_raw():
    engine = get_engine()

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    print("Kafka consumer started")

    current_batch_no = create_new_batch(engine)
    rows_in_batch = 0

    for msg in consumer:
        data = msg.value
        msg_type = data.get("type")

        try:
            # koniec danych → zamykamy batch
            if msg_type == "transfer_complete":
                if rows_in_batch > 0:
                    mark_batch_completed(engine, current_batch_no)
                    print(f"Final batch closed: {current_batch_no}")

                # start nowego batcha dla przyszłych danych
                current_batch_no = create_new_batch(engine)
                rows_in_batch = 0

                consumer.commit()
                continue

            # batch rotation
            if rows_in_batch > 0 and rows_in_batch % BATCH_SIZE == 0:
                mark_batch_completed(engine, current_batch_no)
                print(f"Batch completed: {current_batch_no}")

                current_batch_no = create_new_batch(engine)
                rows_in_batch = 0

            # insert
            with engine.begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO raw.transactions_raw (
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
                        ) VALUES (
                            :batch_no,
                            :transaction_id,
                            :customer_id,
                            :customer_name,
                            :merchant_id,
                            :transaction_ts,
                            :amount,
                            :city,
                            :country,
                            :payment_method,
                            :status
                        )
                    """),
                    {
                        "batch_no": current_batch_no,
                        "transaction_id": data.get("transaction_id"),
                        "customer_id": data.get("customer_id"),
                        "customer_name": data.get("customer_name"),
                        "merchant_id": data.get("merchant_id"),
                        "transaction_ts": data.get("transaction_ts"),
                        "amount": data.get("amount"),
                        "city": data.get("city"),
                        "country": data.get("country"),
                        "payment_method": data.get("payment_method"),
                        "status": data.get("status"),
                    },
                )

            rows_in_batch += 1

            print(
                f"Inserted row → batch={current_batch_no}, row={rows_in_batch}"
            )

            consumer.commit()

        except Exception as e:
            print(f"Failed to process message: {e}")


if __name__ == "__main__":
    wait_for_kafka()
    consume_kafka_to_raw()