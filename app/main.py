from fastapi import FastAPI, UploadFile, File, HTTPException
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import inspect
import pandas as pd
import os

DATABASE_URL = "mysql+pymysql://root:password@db/data_migration"

app = FastAPI()

# Database connection
engine = create_engine(DATABASE_URL)
metadata = MetaData()

@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.post("/upload-csv/")
async def upload_csv(file: UploadFile = File(...)):
    try:
        # Save uploaded file to disk
        file_location = f"data/{file.filename}"
        with open(file_location, "wb") as f:
            f.write(file.file.read())

        # Read CSV file using pandas
        df = pd.read_csv(file_location)

        # Determine table name from file name
        table_name = determine_table_name(file.filename)

        # Check if table exists, if not, create it
        inspector = inspect(engine)
        if not inspector.has_table(table_name):
            create_table(table_name, df)

        # Insert data into table
        df.to_sql(table_name, engine, if_exists='append', index=False)

        return {"message": "File processed successfully"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def determine_table_name(filename):
    # Convert filename to lowercase for case-insensitive comparison
    filename = filename.lower()
    
    # Check if 'job' is in the filename
    if 'job' in filename:
        return "jobs"
    
    # Add more conditions for other tables if needed
    return "unknown_table"

def create_table(table_name, df):
    columns = []
    for col in df.columns:
        if col == "id":
            columns.append(Column("id", Integer, primary_key=True))
        else:
            columns.append(Column(col, String(255)))
    
    table = Table(table_name, metadata, *columns)
    metadata.create_all(engine)
