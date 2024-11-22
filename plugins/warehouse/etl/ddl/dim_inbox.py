DIM_INBOX = """
CREATE TABLE IF NOT EXISTS iceberg.chatwoot.dim_inbox (
    id INTEGER,
    name VARCHAR(50),
    channel_type VARCHAR(50),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
"""
