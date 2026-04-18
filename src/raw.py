import pandas as pd
from sqlalchemy import text
import hashlib
import os

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
                "file_hash": file_hash,
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
                "file_hash": file_hash,
            }
        ).fetchone()

    print(
        f"Checking whether file '{file_name}' with hash '{file_hash[:12]}...' "
        f"has already been loaded: {'YES' if result else 'NO'}"
    )
    return result is not None


def create_new_batch(engine) -> int:
    with engine.begin() as conn:
        batch_no = conn.execute(
            text("SELECT nextval('raw.batch_no_seq')")
        ).scalar()

        conn.execute(
            text("""
                INSERT INTO raw.batch_control (
                    batch_no,
                    status
                ) VALUES (
                    :batch_no,
                    'loading'
                )
            """),
            {"batch_no": int(batch_no)}
        )

    return int(batch_no)


def mark_batch_completed(engine, batch_no: int):
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE raw.batch_control
                SET status = 'completed',
                    completed_at = CURRENT_TIMESTAMP
                WHERE batch_no = :batch_no
            """),
            {"batch_no": int(batch_no)}
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
    current_batch_no = None
    rows_in_current_batch = 0

    for chunk in pd.read_csv(csv_file, chunksize=chunk_size):
        start_idx = 0

        while start_idx < len(chunk):
            if current_batch_no is None:
                current_batch_no = create_new_batch(engine)
                rows_in_current_batch = 0
                print(f"Created RAW batch {current_batch_no}")

            remaining_in_batch = chunk_size - rows_in_current_batch
            end_idx = min(start_idx + remaining_in_batch, len(chunk))

            raw_chunk = chunk.iloc[start_idx:end_idx].copy()
            raw_chunk["batch_no"] = current_batch_no

            raw_chunk.to_sql(
                "transactions_raw",
                engine,
                schema="raw",
                if_exists="append",
                index=False,
                method="multi",
                chunksize=5000
            )

            inserted_rows = len(raw_chunk)
            rows_in_current_batch += inserted_rows
            start_idx = end_idx

            if rows_in_current_batch >= chunk_size:
                mark_batch_completed(engine, current_batch_no)
                print(f"Marked RAW batch {current_batch_no} as completed")
                current_batch_no = None
                rows_in_current_batch = 0

    if current_batch_no is not None:
        mark_batch_completed(engine, current_batch_no)
        print(f"Marked final RAW batch {current_batch_no} as completed")

    print("RAW complete")