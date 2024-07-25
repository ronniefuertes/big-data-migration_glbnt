from fastapi import FastAPI, UploadFile, File, HTTPException
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, ForeignKey
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import inspect
import pandas as pd
from typing import List

DATABASE_URL = "mysql+pymysql://root:password@db/data_migration"

app = FastAPI()

# Database connection
engine = create_engine(DATABASE_URL)
metadata = MetaData()

@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.post("/upload-csv/")
async def upload_csv(files: List[UploadFile] = File(...)):
    try:
        for file in files:
            # Save uploaded file to disk
            file_location = f"data/{file.filename}"
            with open(file_location, "wb") as f:
                f.write(file.file.read())

            # Determine table name from file name
            table_name = determine_table_name(file.filename)
            if table_name is None:
                continue  # Skip file if table name is not recognized

            # Define column names based on table type
            column_names = get_column_names(table_name)

            # Read CSV file using pandas without headers and assign column names
            df = pd.read_csv(file_location, header=None)
            df.columns = column_names

            # Check if table exists, if not, create it
            inspector = inspect(engine)
            if not inspector.has_table(table_name):
                create_table(table_name, column_names)

            # Insert data into table
            df.to_sql(table_name, engine, if_exists='append', index=False)

        return {"message": "Files processed successfully"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def determine_table_name(filename):
    # Convert filename to lowercase for case-insensitive comparison
    filename = filename.lower()
    
    # Determine the table name based on filename
    if 'job' in filename:
        return "jobs"
    elif 'department' in filename:
        return "departments"
    elif 'hired_employee' in filename:
        return "hired_employees"
    
    return None

def get_column_names(table_name):
    if table_name == "jobs":
        return ["id", "job"]
    elif table_name == "departments":
        return ["id", "department"]
    elif table_name == "hired_employees":
        return ["id", "name", "datetime", "department_id", "job_id"]
    return []

def create_table(table_name, columns):
    table_columns = []
    for column in columns:
        if column == "id":
            table_columns.append(Column(column, Integer, primary_key=True))
        elif column.endswith("_id"):
            table_columns.append(Column(column, Integer))
        else:
            table_columns.append(Column(column, String(255)))
    
    table = Table(table_name, metadata, *table_columns, extend_existing=True)
    metadata.create_all(engine)
