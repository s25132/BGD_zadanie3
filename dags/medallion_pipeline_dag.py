import os
import time

from airflow.sdk import dag, task, get_current_context
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

from src.raw import (
    load_raw,
    mark_file_as_loaded,
    is_file_already_loaded,
    compute_file_hash,
)
from src.silver import build_silver_spark
from src.gold import build_gold


DB_URL = os.getenv(
    "DB_URL",
    "postgresql+psycopg2://postgres:postgres@postgres:5432/medallion"
)

JDBC_URL = os.getenv(
    "JDBC_URL",
    "jdbc:postgresql://postgres:5432/medallion"
)

CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "10000"))

DB_PROPERTIES = {
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
    "driver": "org.postgresql.Driver",
}


def get_engine():
    for _ in range(30):
        try:
            engine = create_engine(DB_URL)
            with engine.connect():
                pass
            return engine
        except OperationalError:
            print("Waiting for the database...")
            time.sleep(1)
    raise Exception("Could not connect to the database")


@dag(
    dag_id="medallion_pipeline",
    description="Medallion pipeline with Airflow, Spark, and PostgreSQL",
    schedule="* * * * *",
    catchup=False,
    tags=["medallion", "airflow", "spark", "postgres"],
    default_args={"owner": "airflow", "retries": 1},
    max_active_runs=1,
    is_paused_upon_creation=False,
)
def medallion_pipeline():

    @task
    def raw_ingestion():

        context = get_current_context()
        dag_conf = context.get("dag_run").conf if context.get("dag_run") else {}

        csv_file = dag_conf.get(
            "file",
            os.getenv("DATA_FILE", None)
        )

        if not csv_file:
            print("No input file provided — skipping RAW ingestion and continuing in incremental mode")
            return {
                "should_continue": True,
                "load_mode": "incremental",
        }

        load_mode = dag_conf.get(
            "load_mode",
            os.getenv("LOAD_MODE", "incremental")
        )

        if load_mode not in ["incremental", "full"]:
            raise ValueError(f"Invalid load_mode: {load_mode}")

        engine = get_engine()

        file_name = os.path.basename(csv_file)
        file_hash = compute_file_hash(csv_file)

        print(f"Mode: {load_mode}")
        print(f"File: {csv_file}")

        # TRYB INCREMENTAL:
        # jeśli plik był już przetwarzany → pomijamy pipeline
        if load_mode == "incremental":
            if is_file_already_loaded(engine, file_name, file_hash):
                print(f"File {file_name} already loaded — skipping RAW")
                return {"should_continue": False, "load_mode": load_mode}

        
        # TRYB FULL:
        # zawsze przetwarzamy (bez sprawdzania hash)
        print(f"Starting RAW ingestion for file: {file_name}")
        load_raw(engine, csv_file, CHUNK_SIZE)
        mark_file_as_loaded(engine, file_name, file_hash)
        print("RAW ingestion complete")

        return {"should_continue": True, "load_mode": load_mode}

    @task
    def silver_build(raw_result: dict):

        should_continue = raw_result["should_continue"]
        load_mode = raw_result["load_mode"]

        # Jeśli incremental i RAW nic nie zrobił → skip
        if not should_continue and load_mode == "incremental":
            print("Skipping SILVER")
            return {"should_continue": False, "load_mode": load_mode}

        engine = get_engine()

        print(f"Starting SILVER build (mode={load_mode})...")
        processed = build_silver_spark(engine, JDBC_URL, DB_PROPERTIES, load_mode)

        if not processed:
            print("No new batches processed in SILVER — skipping GOLD")
        print("SILVER complete")

        return {
            "should_continue": processed,
            "load_mode": load_mode
        }       

    @task
    def gold_build(silver_result: dict):

        should_continue = silver_result["should_continue"]
        load_mode = silver_result["load_mode"]

        # Jeśli incremental i nic się nie zmieniło → skip
        if not should_continue and load_mode == "incremental":
            print("Skipping GOLD")
            return

        engine = get_engine()

        print(f"Starting GOLD build (mode={load_mode})...")
        build_gold(engine)
        print("GOLD complete")

    raw_result = raw_ingestion()
    silver_result = silver_build(raw_result)
    gold_build(silver_result)


dag = medallion_pipeline()