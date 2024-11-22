DIM_DATE = """
CREATE TABLE IF NOT EXISTS iceberg.chatwoot.dim_date (
    id INTEGER,
    day_of_week INTEGER,
    day_of_month INTEGER,
    day_of_year INTEGER,
    is_last_day_of_month INTEGER,
    is_weekend INTEGER,
    week_start_id INTEGER,
    week_end_id INTEGER,
    week_of_year INTEGER,
    month INTEGER,
    quarter VARCHAR(50),
    year INTEGER,
    is_holiday INTEGER,
    unix_timestamp INTEGER,
    "date" DATE
)
"""
