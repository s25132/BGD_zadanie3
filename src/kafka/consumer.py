import json
import os
import time

from kafka import KafkaConsumer
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

from src.raw import (
    is_file_already_loaded,
    mark_file_as_loaded,
    get_or_create_loading_batch,
    mark_batch_completed,
)


DB_URL = os.getenv(
    "DB_URL",
    "postgresql+psycopg2://postgres:postgres@postgres:5432/medallion"
)

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw-transactions")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "raw-consumer")


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

    for msg in consumer:
        data = msg.value
        msg_type = data.get("type")

        try:
            if msg_type == "file_complete":
                file_name = data.get("source_file")
                file_hash = data.get("file_hash")

                if not file_name or not file_hash:
                    print("Skipping invalid file_complete message")
                    consumer.commit()
                    continue

                if not is_file_already_loaded(engine, file_name, file_hash):
                    mark_batch_completed(engine, file_name, file_hash)
                    mark_file_as_loaded(engine, file_name, file_hash)
                    print(f"File completed and marked as loaded: {file_name}")
                else:
                    print(f"File already marked as loaded: {file_name}")

                consumer.commit()
                continue

            if msg_type != "data":
                print(f"Skipping unknown message type: {msg_type}")
                consumer.commit()
                continue

            file_name = data.get("source_file")
            file_hash = data.get("file_hash")

            if not file_name or not file_hash:
                print("Skipping message without source_file or file_hash")
                consumer.commit()
                continue

            if is_file_already_loaded(engine, file_name, file_hash):
                print(f"Skipping already loaded file: {file_name}")
                consumer.commit()
                continue

            batch_no = get_or_create_loading_batch(engine, file_name, file_hash)

            with engine.begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO raw.transactions_raw (
                            batch_no,
                            source_file,
                            file_hash,
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
                            :source_file,
                            :file_hash,
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
                        "batch_no": batch_no,
                        "source_file": file_name,
                        "file_hash": file_hash,
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

            print(f"Inserted row into RAW: batch_no={batch_no}, source_file={file_name}")
            consumer.commit()

        except Exception as e:
            print(f"Failed to process message: {e}")


if __name__ == "__main__":
    wait_for_kafka()
    consume_kafka_to_raw()