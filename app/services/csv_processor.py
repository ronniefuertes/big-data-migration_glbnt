# services/csv_processor.py
import csv
import os
import tempfile
from datetime import datetime
from fastapi import HTTPException, UploadFile
import boto3

from db import SessionLocal
from models import HiredEmployee, Department, Job

# Mapping of file names to their configuration.
FILE_CONFIG = {
    "hired_employees.csv": {
         "model": HiredEmployee,
         "fields": ["id", "name", "datetime", "department_id", "job_id"],
         "num_fields": 5
    },
    "departments.csv": {
         "model": Department,
         "fields": ["id", "department"],
         "num_fields": 2
    },
    "jobs.csv": {
         "model": Job,
         "fields": ["id", "job"],
         "num_fields": 2
    }
}

# S3 configuration: ensure S3_BUCKET is set in your environment variables.
S3_BUCKET = os.getenv("S3_BUCKET")
if not S3_BUCKET:
    raise RuntimeError("S3_BUCKET environment variable is not set.")

def upload_error_file_to_s3(file_path: str, key: str):
    """Uploads the given error file to the configured S3 bucket."""
    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file(file_path, S3_BUCKET, key)
    except Exception as e:
        # In production, use proper logging instead of printing.
        print(f"Error uploading file to S3: {e}")

async def process_csv_file(file: UploadFile):
    """
    Processes the CSV file upload.
    Valid rows are inserted into the database.
    Invalid rows are logged and later uploaded to S3 as an error file.
    """
    file_name = file.filename
    if file_name not in FILE_CONFIG:
        raise HTTPException(
            status_code=400, 
            detail="Invalid file name. Expected one of: " + ", ".join(FILE_CONFIG.keys())
        )
        
    config = FILE_CONFIG[file_name]
    model = config["model"]
    expected_num_fields = config["num_fields"]

    # Read, decode, and parse the CSV file.
    contents = await file.read()
    decoded_lines = contents.decode('utf-8').splitlines()
    reader = csv.reader(decoded_lines)
    rows = list(reader)

    # Limit to 1,000 rows per request.
    if len(rows) > 1000:
        raise HTTPException(
            status_code=400, 
            detail="Maximum of 1000 rows allowed per request."
        )

    valid_objects = []
    error_rows = []
    row_number = 0

    for row in rows:
        row_number += 1

        # Validate field count.
        if len(row) != expected_num_fields:
            error_rows.append(row + [f"Row {row_number}: Expected {expected_num_fields} fields, got {len(row)}"])
            continue

        # Trim whitespace and ensure no empty fields.
        row = [field.strip() for field in row]
        if any(field == "" for field in row):
            error_rows.append(row + [f"Row {row_number}: One or more fields are empty"])
            continue

        # File-specific validations.
        if file_name == "hired_employees.csv":
            try:
                id_val = int(row[0])
                name_val = row[1]
                datetime_str = row[2]
                # Validate datetime format (adjust for the 'Z' if needed).
                try:
                    datetime.fromisoformat(datetime_str.replace("Z", "+00:00"))
                except Exception:
                    raise ValueError(f"Invalid datetime format: {datetime_str}")
                department_id_val = int(row[3])
                job_id_val = int(row[4])
                valid_objects.append(model(
                    id=id_val,
                    name=name_val,
                    datetime=datetime_str,
                    department_id=department_id_val,
                    job_id=job_id_val
                ))
            except Exception as e:
                error_rows.append(row + [f"Row {row_number}: {e}"])
                continue

        elif file_name == "departments.csv":
            try:
                id_val = int(row[0])
                department_val = row[1]
                valid_objects.append(model(
                    id=id_val,
                    department=department_val
                ))
            except Exception as e:
                error_rows.append(row + [f"Row {row_number}: {e}"])
                continue

        elif file_name == "jobs.csv":
            try:
                id_val = int(row[0])
                job_val = row[1]
                valid_objects.append(model(
                    id=id_val,
                    job=job_val
                ))
            except Exception as e:
                error_rows.append(row + [f"Row {row_number}: {e}"])
                continue

    # Insert valid rows in a batch transaction.
    db = SessionLocal()
    try:
        if valid_objects:
            db.bulk_save_objects(valid_objects)
            db.commit()
    except Exception as db_error:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {db_error}")
    finally:
        db.close()

    # Write errors to a temporary CSV file and upload it to S3.
    if error_rows:
        error_file_name = f"errors_{file_name}"
        with tempfile.NamedTemporaryFile(mode="w+", delete=False, newline='', suffix=".csv") as temp_file:
            writer = csv.writer(temp_file)
            header = config["fields"] + ["error_message"]
            writer.writerow(header)
            for error_row in error_rows:
                writer.writerow(error_row)
            temp_file_path = temp_file.name
        upload_error_file_to_s3(temp_file_path, error_file_name)

    return {"inserted_rows": len(valid_objects), "error_rows": len(error_rows)}
