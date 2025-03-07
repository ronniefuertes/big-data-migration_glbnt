# services/restore_service.py
import os
import boto3
import fastavro
from sqlalchemy.orm import Session
from db import SessionLocal
from models import HiredEmployee, Department, Job

# AWS S3 Configuration
S3_BUCKET = os.getenv("S3_BACKUP_BUCKET")
if not S3_BUCKET:
    raise RuntimeError("S3_BACKUP_BUCKET environment variable is not set.")

s3_client = boto3.client("s3")

TABLE_SCHEMAS = {
    "hired_employees": (HiredEmployee, "/tmp/hired_employees_backup.avro"),
    "departments": (Department, "/tmp/departments_backup.avro"),
    "jobs": (Job, "/tmp/jobs_backup.avro"),
}

def download_from_s3(s3_key: str, local_path: str):
    """
    Downloads a file from AWS S3 to a local path.
    """
    try:
        s3_client.download_file(S3_BUCKET, s3_key, local_path)
    except Exception as e:
        raise RuntimeError(f"Error downloading from S3: {e}")

def restore_table_from_avro(table_name: str):
    """
    Restores data from an Avro backup file into the specified database table.
    """
    if table_name not in TABLE_SCHEMAS:
        raise ValueError(f"Invalid table name: {table_name}")

    model, local_file_path = TABLE_SCHEMAS[table_name]
    s3_key = f"backups/{table_name}_backup.avro"

    # Step 1: Download Avro file from S3
    download_from_s3(s3_key, local_file_path)

    # Step 2: Read Avro data
    records = []
    with open(local_file_path, "rb") as avro_file:
        avro_reader = fastavro.reader(avro_file)
        for record in avro_reader:
            records.append(record)

    if not records:
        raise RuntimeError(f"No data found in Avro backup for table {table_name}")

    # Step 3: Insert into database
    db: Session = SessionLocal()
    try:
        # Delete existing records
        db.query(model).delete()
        db.commit()

        # Insert new records in batch
        batch_size = 1000
        for i in range(0, len(records), batch_size):
            db.bulk_insert_mappings(model, records[i:i+batch_size])
            db.commit()

        return f"Successfully restored {len(records)} records into {table_name}"
    except Exception as e:
        db.rollback()
        raise RuntimeError(f"Error restoring table {table_name}: {e}")
    finally:
        db.close()
