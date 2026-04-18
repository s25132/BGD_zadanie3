import hashlib
import os
import pandas as pd
from sqlalchemy import text


def compute_file_hash(file_path: str, chunk_size: int = 8192) -> str:
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(chunk_size):
            sha256.update(chunk)
    return sha256.hexdigest()


def mark_file_as_loaded(engine, file_name: str, file_hash: str):
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO raw.ingestion_log (file_name, file_hash)
                VALUES (:file_name, :file_hash)
                ON CONFLICT (file_name, file_hash) DO NOTHING
            """),
            {
                "file_name": file_name,
                "file_hash": file_hash
            }
        )


def is_file_already_loaded(engine, file_name: str, file_hash: str) -> bool:
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT 1
                FROM raw.ingestion_log
                WHERE file_name = :file_name
                  AND file_hash = :file_hash
                LIMIT 1
            """),
            {
                "file_name": file_name,
                "file_hash": file_hash
            }
        ).fetchone()

    print(
        f"Checking whether file '{file_name}' with hash '{file_hash[:12]}...' "
        f"has already been loaded: {'YES' if result else 'NO'}"
    )
    return result is not None


def get_max_raw_batch_no(engine) -> int:
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT COALESCE(MAX(batch_no), 0) FROM raw.transactions_raw")
        ).scalar()
    return int(result or 0)


def get_or_create_loading_batch(engine, file_name: str, file_hash: str) -> int:
    with engine.begin() as conn:
        existing = conn.execute(
            text("""
                SELECT batch_no
                FROM raw.batch_control
                WHERE source_file = :file_name
                  AND file_hash = :file_hash
                LIMIT 1
            """),
            {
                "file_name": file_name,
                "file_hash": file_hash
            }
        ).fetchone()

        if existing:
            return int(existing[0])

        next_batch_no = conn.execute(
            text("SELECT COALESCE(MAX(batch_no), 0) + 1 FROM raw.transactions_raw")
        ).scalar()
        next_batch_no = int(next_batch_no or 1)

        conn.execute(
            text("""
                INSERT INTO raw.batch_control (
                    batch_no,
                    source_file,
                    file_hash,
                    status
                )
                VALUES (
                    :batch_no,
                    :file_name,
                    :file_hash,
                    'loading'
                )
            """),
            {
                "batch_no": next_batch_no,
                "file_name": file_name,
                "file_hash": file_hash
            }
        )

        return next_batch_no


def mark_batch_completed(engine, file_name: str, file_hash: str):
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE raw.batch_control
                SET status = 'completed',
                    completed_at = CURRENT_TIMESTAMP
                WHERE source_file = :file_name
                  AND file_hash = :file_hash
            """),
            {
                "file_name": file_name,
                "file_hash": file_hash
            }
        )


def get_completed_raw_batches(engine) -> set[int]:
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT batch_no
                FROM raw.batch_control
                WHERE status = 'completed'
                ORDER BY batch_no
            """)
        ).fetchall()
    return {int(row[0]) for row in rows}


def load_raw(engine, csv_file: str, chunk_size: int):
    """
    Incremental, append-only RAW load from CSV.
    Entire file gets one logical batch_no stored in raw.batch_control.
    """
    file_name = os.path.basename(csv_file)
    file_hash = compute_file_hash(csv_file)

    batch_no = get_or_create_loading_batch(engine, file_name, file_hash)

    for chunk in pd.read_csv(csv_file, chunksize=chunk_size):
        print(f"Loading RAW batch {batch_no}...")

        raw_chunk = chunk.copy()
        raw_chunk["batch_no"] = batch_no
        raw_chunk["source_file"] = file_name
        raw_chunk["file_hash"] = file_hash

        raw_chunk.to_sql(
            "transactions_raw",
            engine,
            schema="raw",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000
        )

    mark_batch_completed(engine, file_name, file_hash)
    mark_file_as_loaded(engine, file_name, file_hash)
    print("RAW complete")