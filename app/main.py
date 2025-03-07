from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import inspect
from sqlalchemy.exc import SQLAlchemyError
import models
from database import engine, get_db, Base

app = FastAPI()

# Create tables on startup if they don't exist
@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(bind=engine)

@app.get("/")
def read_root():
    return {"message": "Connected to RDS MySQL!"}

@app.get("/check-tables")
def check_tables(db: Session = Depends(get_db)):
    try:
        inspector = inspect(db.get_bind())
        existing_tables = inspector.get_table_names()
        required_tables = {"hired_employees", "departments", "jobs"}
        missing_tables = [tbl for tbl in required_tables if tbl not in existing_tables]
        
        return {
            "existing_tables": existing_tables,
            "required_tables": list(required_tables),
            "missing_tables": missing_tables,
            "all_tables_exist": len(missing_tables) == 0
        }
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")