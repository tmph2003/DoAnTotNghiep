FCT_CONVERSATION = """
CREATE TABLE IF NOT EXISTS iceberg.chatwoot.fct_conversation (
    id INTEGER,
    account_id INTEGER,
    inbox_id INTEGER,
    contact_id INTEGER,
    status INTEGER,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
"""
