# main.py
from fastapi import FastAPI, HTTPException, UploadFile, File
from sqlalchemy.exc import SQLAlchemyError

from db import engine
from models import Base
from services.table_utils import check_required_tables, create_missing_tables
from services.csv_processor import process_csv_file

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
async def upload_csv(file: UploadFile = File(...)):
    return await process_csv_file(file)
