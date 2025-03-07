from sqlalchemy.orm import Session
from sqlalchemy import text

def fetch_departments_above_mean_hires(db: Session):
    """
    Fetches departments that hired more employees than the mean in 2021,
    ordered by number of hires (descending).
    """
    query = text("""
        WITH department_hires AS (
            SELECT he.department_id, d.department, COUNT(*) AS total_hires
            FROM hired_employees he
            JOIN departments d ON he.department_id = d.id
            WHERE YEAR(he.datetime) = 2021
            GROUP BY he.department_id, d.department
        ),
        avg_hires AS (
            SELECT AVG(total_hires) AS mean_hires FROM department_hires
        )
        SELECT dh.department_id, dh.department, dh.total_hires
        FROM department_hires dh
        JOIN avg_hires ah ON dh.total_hires > ah.mean_hires
        ORDER BY dh.total_hires DESC;
    """)

    result = db.execute(query).fetchall()

    return [
        {
            "department_id": row[0],
            "department_name": row[1],
            "total_hires": row[2],
        }
        for row in result
    ]
