FCT_MESSAGE = """
CREATE TABLE IF NOT EXISTS iceberg.chatwoot.fct_message (
    id INTEGER,
    user_id INTEGER,
    account_id INTEGER,
    inbox_id INTEGER,
    conversation_id INTEGER,
    contact_id INTEGER,
    campaign_id INTEGER,
    ws_error_code INTEGER,
    has_media INTEGER,
    ws_template_id VARCHAR(20),
    message_type INTEGER,
    status INTEGER,
    is_private INTEGER,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    partition_value DATE)
    WITH (
        partitioning = ARRAY['month(created_at)']
    )
"""
