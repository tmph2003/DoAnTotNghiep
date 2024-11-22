DIM_USER = """
CREATE TABLE IF NOT EXISTS iceberg.chatwoot.dim_user (
    id INTEGER,
    name VARCHAR(50),
    display_name VARCHAR(50),
    email VARCHAR(50),
    type VARCHAR(50),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
"""
