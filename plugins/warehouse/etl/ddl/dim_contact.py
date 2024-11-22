DIM_CONTACT = """
CREATE TABLE IF NOT EXISTS iceberg.chatwoot.dim_contact (
    id INTEGER,
    account_id INTEGER,
    name VARCHAR(50),
    middle_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(50),
    phone_number VARCHAR(50),
    location VARCHAR(50),
    country_code VARCHAR(50),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
"""
