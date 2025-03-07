# services/backup_service.py
import os
import boto3
from fastavro import writer, parse_schema
from db import SessionLocal
from models import HiredEmployee, Department, Job

# AWS S3 Configuration
S3_BUCKET = os.getenv("S3_BACKUP_BUCKET")
if not S3_BUCKET:
    raise RuntimeError("S3_BACKUP_BUCKET environment variable is not set.")

s3_client = boto3.client("s3")

# Define Avro schemas
HIRED_EMPLOYEES_SCHEMA = {
    "namespace": "backup.example",
    "type": "record",
    "name": "HiredEmployee",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
        {"name": "datetime", "type": "string"},
        {"name": "department_id", "type": "int"},
        {"name": "job_id", "type": "int"}
    ]
}

DEPARTMENTS_SCHEMA = {
    "namespace": "backup.example",
    "type": "record",
    "name": "Department",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "department", "type": "string"}
    ]
}

JOBS_SCHEMA = {
    "namespace": "backup.example",
    "type": "record",
    "name": "Job",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "job", "type": "string"}
    ]
}

def upload_to_s3(file_path: str, s3_key: str):
    """
    Uploads a file to the specified S3 bucket.
    Returns the S3 URL of the uploaded file.
    """
    try:
        s3_client.upload_file(file_path, S3_BUCKET, s3_key)
        s3_url = f"https://{S3_BUCKET}.s3.amazonaws.com/{s3_key}"
        return s3_url
    except Exception as e:
        raise RuntimeError(f"Error uploading to S3: {e}")

def backup_table_to_avro(table_name: str, model, schema: dict):
    """
    Backs up a table into an Avro file and uploads it to AWS S3.
    Returns the S3 URL of the backup file.
    """
    avro_file_path = f"/tmp/{table_name}_backup.avro"
    s3_key = f"backups/{table_name}_backup.avro"

    # Query all records from the database
    db = SessionLocal()
    try:
        records = db.query(model).all()
    finally:
        db.close()

    # Convert SQLAlchemy objects to dictionaries
    records_dict = []
    for record in records:
        rec = record.__dict__
        rec.pop("_sa_instance_state", None)
        records_dict.append(rec)

    # Write to Avro file
    parsed_schema = parse_schema(schema)
    with open(avro_file_path, "wb") as out_file:
        writer(out_file, parsed_schema, records_dict)

    # Upload to S3 and return the S3 URL
    return upload_to_s3(avro_file_path, s3_key)

def backup_all_tables():
    """
    Creates Avro backups for all tables and uploads them to AWS S3.
    Returns a dictionary with table names and their S3 URLs.
    """
    backup_files = {}
    backup_files["hired_employees"] = backup_table_to_avro("hired_employees", HiredEmployee, HIRED_EMPLOYEES_SCHEMA)
    backup_files["departments"] = backup_table_to_avro("departments", Department, DEPARTMENTS_SCHEMA)
    backup_files["jobs"] = backup_table_to_avro("jobs", Job, JOBS_SCHEMA)
    return backup_files
