import json
import os
import time

import pandas as pd
from kafka import KafkaProducer

from src.raw import compute_file_hash


KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw-transactions")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
DATA_FILE = os.getenv("DATA_FILE", "/opt/airflow/data/transactions.csv")


def wait_for_kafka(max_retries: int = 30, sleep_seconds: int = 2):
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            producer.close()
            print("Kafka is ready")
            return
        except Exception as e:
            print(f"Waiting for Kafka... attempt={attempt}/{max_retries}, error={e}")
            time.sleep(sleep_seconds)

    raise RuntimeError("Kafka is not available")


def produce_file_to_kafka(csv_file: str):
    if not os.path.exists(csv_file):
        raise FileNotFoundError(f"CSV file not found: {csv_file}")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    file_name = os.path.basename(csv_file)
    file_hash = compute_file_hash(csv_file)

    print(f"Producing file: {csv_file}")
    print(f"source_file={file_name}, file_hash={file_hash}")

    for chunk in pd.read_csv(csv_file, chunksize=10000):
        for _, row in chunk.iterrows():
            payload = {
                "type": "data",
                "source_file": file_name,
                "file_hash": file_hash,
                "transaction_id": None if pd.isna(row.get("transaction_id")) else str(row.get("transaction_id")),
                "customer_id": None if pd.isna(row.get("customer_id")) else str(row.get("customer_id")),
                "customer_name": None if pd.isna(row.get("customer_name")) else str(row.get("customer_name")),
                "merchant_id": None if pd.isna(row.get("merchant_id")) else str(row.get("merchant_id")),
                "transaction_ts": None if pd.isna(row.get("transaction_ts")) else str(row.get("transaction_ts")),
                "amount": None if pd.isna(row.get("amount")) else str(row.get("amount")),
                "city": None if pd.isna(row.get("city")) else str(row.get("city")),
                "country": None if pd.isna(row.get("country")) else str(row.get("country")),
                "payment_method": None if pd.isna(row.get("payment_method")) else str(row.get("payment_method")),
                "status": None if pd.isna(row.get("status")) else str(row.get("status")),
            }

            producer.send(KAFKA_TOPIC, key=file_hash.encode("utf-8"), value=payload)

    producer.send(
        KAFKA_TOPIC,
        key=file_hash.encode("utf-8"),
        value={
            "type": "file_complete",
            "source_file": file_name,
            "file_hash": file_hash,
        },
    )

    producer.flush()
    producer.close()
    print("Producer finished")


if __name__ == "__main__":
    wait_for_kafka()
    produce_file_to_kafka(DATA_FILE)