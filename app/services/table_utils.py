# services/table_utils.py
from sqlalchemy import inspect
from models import Base

def check_required_tables(engine):
    """
    Checks whether the required tables exist.
    Returns (True, None) if all exist, or (False, missing_tables) if not.
    """
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()
    required_tables = {"hired_employees", "departments", "jobs"}
    missing_tables = required_tables.difference(existing_tables)
    if missing_tables:
        return False, missing_tables
    return True, None

def create_missing_tables(engine):
    """Creates all tables as defined in models.py."""
    Base.metadata.create_all(bind=engine)
