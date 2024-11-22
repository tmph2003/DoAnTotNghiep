DIM_ACCOUNT_USER = """
CREATE TABLE IF NOT EXISTS iceberg.chatwoot.dim_account_user (
    account_id INTEGER,
    user_id INTEGER,
    role INTEGER, -- Xác định nhân viên hay admin
    active_at TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
"""
