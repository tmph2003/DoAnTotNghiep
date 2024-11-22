DIM_CHANNEL_WHATSAPP = """
CREATE TABLE IF NOT EXISTS iceberg.chatwoot.dim_channel_whatsapp (
    id INTEGER,
    account_id INTEGER,
    phone_number VARCHAR(50),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
"""
