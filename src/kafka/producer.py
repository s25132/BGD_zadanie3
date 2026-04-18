import json
import os
import time

import pandas as pd
from kafka import KafkaProducer


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
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"Producing file: {csv_file}")

    for chunk in pd.read_csv(csv_file, chunksize=10000):
        for _, row in chunk.iterrows():
            payload = {
                "type": "data",
                "transaction_id": str(row.get("transaction_id")),
                "customer_id": str(row.get("customer_id")),
                "customer_name": str(row.get("customer_name")),
                "merchant_id": str(row.get("merchant_id")),
                "transaction_ts": str(row.get("transaction_ts")),
                "amount": str(row.get("amount")),
                "city": str(row.get("city")),
                "country": str(row.get("country")),
                "payment_method": str(row.get("payment_method")),
                "status": str(row.get("status")),
            }
            producer.send(KAFKA_TOPIC, value=payload)

    #sygnał końca
    producer.send(KAFKA_TOPIC, value={"type": "transfer_complete"})

    producer.flush()
    producer.close()
    print("Producer finished")


if __name__ == "__main__":
    wait_for_kafka()
    produce_file_to_kafka(DATA_FILE)