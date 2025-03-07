from sqlalchemy.orm import Session
from sqlalchemy import text

def fetch_hired_employees_per_quarter(db: Session):
    """
    Fetches the number of employees hired in 2021 per quarter,
    grouped by department and job, sorted alphabetically.
    """
    query = text("""
        SELECT d.department, j.job,
               SUM(CASE WHEN QUARTER(he.datetime) = 1 THEN 1 ELSE 0 END) AS Q1,
               SUM(CASE WHEN QUARTER(he.datetime) = 2 THEN 1 ELSE 0 END) AS Q2,
               SUM(CASE WHEN QUARTER(he.datetime) = 3 THEN 1 ELSE 0 END) AS Q3,
               SUM(CASE WHEN QUARTER(he.datetime) = 4 THEN 1 ELSE 0 END) AS Q4
        FROM hired_employees he
        JOIN departments d ON he.department_id = d.id
        JOIN jobs j ON he.job_id = j.id
        WHERE YEAR(he.datetime) = 2021
        GROUP BY d.department, j.job
        ORDER BY d.department ASC, j.job ASC;
    """)

    result = db.execute(query).fetchall()

    return [
        {
            "department": row[0],
            "job": row[1],
            "Q1": row[2],
            "Q2": row[3],
            "Q3": row[4],
            "Q4": row[5],
        }
        for row in result
    ]
