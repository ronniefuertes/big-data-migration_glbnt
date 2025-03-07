# main.py
from fastapi import FastAPI, HTTPException
from sqlalchemy.exc import SQLAlchemyError

from db import engine
from models import Base
from services.table_utils import check_required_tables, create_missing_tables
from services.csv_processor import process_csv_file
from services.backup_service import backup_all_tables

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
