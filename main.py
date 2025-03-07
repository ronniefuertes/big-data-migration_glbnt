from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os

app = FastAPI()

# Database configuration
DB_USER = os.getenv("globant_admin")
DB_PASSWORD = os.getenv("globant_admin")
DB_HOST = os.getenv("globant-database.cxlzuyc5lzh3.us-west-2.rds.amazonaws.com")
DB_NAME = os.getenv("globant-database")
DB_PORT = os.getenv("DB_PORT", "3306")  # Default MySQL port

# Updated connection string with port
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Simple model
class Item(Base):
    __tablename__ = "items"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(50))

# Create tables if they don't exist
try:
    Base.metadata.create_all(bind=engine)
except Exception as e:
    print(f"Error connecting to database: {e}")

@app.get("/")
def read_root():
    return {"message": "Connected to RDS MySQL!"}

@app.get("/items")
def get_items():
    db = SessionLocal()
    items = db.query(Item).all()
    db.close()
    return items

@app.post("/items")
def create_item(name: str):
    db = SessionLocal()
    try:
        new_item = Item(name=name)
        db.add(new_item)
        db.commit()
        db.refresh(new_item)
        return new_item
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()