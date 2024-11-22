DIM_ACCOUNT = """
CREATE TABLE IF NOT EXISTS iceberg.chatwoot.dim_account (
    id INTEGER,
    name VARCHAR(50),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
"""
