from fastapi import FastAPI, UploadFile, File, HTTPException
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, ForeignKey
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import inspect
from sqlalchemy import func, extract, text
from sqlalchemy.orm import sessionmaker
import pandas as pd
from typing import List

DATABASE_URL = "mysql+pymysql://root:password@db/data_migration"

app = FastAPI()

# Database connection
engine = create_engine(DATABASE_URL)
metadata = MetaData()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

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

            # Validate the data
            df = validate_data(df, column_names)

            # Check if table exists, if not, create it
            inspector = inspect(engine)
            if not inspector.has_table(table_name):
                create_table(table_name, column_names)

            # Insert data into table with duplication check
            insert_data_with_check(df, table_name)

        return {"message": "Files processed successfully"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/employees-hired-2021/")
async def get_employees_hired_2021():
    try:
        session = SessionLocal()

        query = text("""
        SELECT d.department, j.job, 
               extract(quarter FROM he.datetime) AS quarter, 
               COUNT(*) AS num_employees
        FROM hired_employees he
        JOIN departments d ON he.department_id = d.id
        JOIN jobs j ON he.job_id = j.id
        WHERE extract(year FROM he.datetime) = 2021
        GROUP BY d.department, j.job, extract(quarter FROM he.datetime)
        ORDER BY d.department, j.job, extract(quarter FROM he.datetime)
        """)
        
        result = session.execute(query)
        data = result.fetchall()

        return {"data": [{"department": row[0], "job": row[1], "quarter": row[2], "num_employees": row[3]} for row in data]}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    finally:
        session.close()

@app.get("/departments-above-mean-2021/")
async def get_departments_above_mean_2021():
    try:
        session = SessionLocal()

        mean_query = text("""
        SELECT AVG(num_employees) AS mean_employees
        FROM (
            SELECT d.id, d.department, COUNT(*) AS num_employees
            FROM hired_employees he
            JOIN departments d ON he.department_id = d.id
            WHERE extract(year FROM he.datetime) = 2021
            GROUP BY d.id, d.department
        ) AS subquery
        """)
        
        mean_result = session.execute(mean_query)
        mean_employees = mean_result.scalar()
        
        departments_query = text("""
        SELECT d.id, d.department, COUNT(*) AS num_employees
        FROM hired_employees he
        JOIN departments d ON he.department_id = d.id
        WHERE extract(year FROM he.datetime) = 2021
        GROUP BY d.id, d.department
        HAVING COUNT(*) > :mean_employees
        ORDER BY num_employees DESC
        """)
        
        result = session.execute(departments_query, {"mean_employees": mean_employees})
        data = result.fetchall()

        return {"data": [{"id": row[0], "department": row[1], "num_employees": row[2]} for row in data]}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    finally:
        session.close()


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

def validate_data(df, columns):
    # Drop rows with any null or empty values
    df.dropna(inplace=True)
    df = df[df.apply(lambda x: x.str.strip() != '', axis=1)]

    # Remove duplicates based on all columns
    df.drop_duplicates(subset=columns, keep='first', inplace=True)

    return df

def insert_data_with_check(df, table_name):
    session = SessionLocal()
    table = Table(table_name, metadata, autoload_with=engine)
    
    try:
        for _, row in df.iterrows():
            # Check if the row already exists in the table
            exists = session.query(table).filter_by(**row.to_dict()).first()
            if not exists:
                session.execute(table.insert().values(**row.to_dict()))
        
        session.commit()
    except Exception as e:
        session.rollback()
        raise
    finally:
        session.close()