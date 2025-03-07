# main.py
from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from database import get_db

from db import engine
from models import Base
from services.table_utils import check_required_tables, create_missing_tables
from services.csv_processor import process_csv_file
from services.backup_service import backup_all_tables
from services.restore_service import restore_table_from_avro
from services.query1 import fetch_hired_employees_per_quarter
from services.query2 import fetch_departments_above_mean_hires

app = FastAPI()

@app.on_event("startup")
def on_startup():
    try:
        tables_exist, missing = check_required_tables(engine)
        if not tables_exist:
            create_missing_tables(engine)
    except SQLAlchemyError as e:
        raise RuntimeError(f"Database startup error: {e}")

@app.get("/")
def read_root():
    try:
        tables_exist, missing = check_required_tables(engine)
        if not tables_exist:
            raise HTTPException(status_code=500, detail=f"Missing tables: {', '.join(missing)}")
        return {"message": "All required tables exist"}
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/upload_csv")
async def upload_csv(file: UploadFile):
    return await process_csv_file(file)

@app.get("/backup")
def backup_data():
    """
    Backs up all tables into Avro files and uploads them to AWS S3.
    Returns the S3 URLs of the stored backups.
    """
    try:
        backup_files = backup_all_tables()
        return {"message": "Backup completed", "files": backup_files}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Backup failed: {e}")

@app.post("/restore/{table_name}")
def restore_data(table_name: str):
    """
    Restores a specific table from its Avro backup stored in AWS S3.
    """
    try:
        message = restore_table_from_avro(table_name)
        return {"message": message}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/employees_hired_per_quarter")
def get_employees_hired_per_quarter(db: Session = Depends(get_db)):
    """
    Endpoint to retrieve the number of employees hired in 2021 by quarter,
    grouped by department and job, sorted alphabetically.
    """
    try:
        result = fetch_hired_employees_per_quarter(db)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving data: {str(e)}")


@app.get("/departments_above_mean_hires")
def get_departments_above_mean_hires(db: Session = Depends(get_db)):
    """
    Endpoint to retrieve departments that hired more employees than the average in 2021,
    ordered by the number of employees hired in descending order.
    """
    try:
        result = fetch_departments_above_mean_hires(db)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving data: {str(e)}")