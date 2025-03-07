# models.py
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String

Base = declarative_base()

class HiredEmployee(Base):
    __tablename__ = "hired_employees"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    datetime = Column(String(255), nullable=False)  # Stored as ISO string
    department_id = Column(Integer, nullable=False)
    job_id = Column(Integer, nullable=False)

class Department(Base):
    __tablename__ = "departments"
    id = Column(Integer, primary_key=True, index=True)
    department = Column(String(255), nullable=False)

class Job(Base):
    __tablename__ = "jobs"
    id = Column(Integer, primary_key=True, index=True)
    job = Column(String(255), nullable=False)
