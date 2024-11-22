FCT_CAMPAIGN_TRANSACTION = """
    CREATE TABLE IF NOT EXISTS iceberg.chatwoot.fct_campaign_transaction (
        campaign_id INTEGER,
        account_id INTEGER,
        inbox_id INTEGER,
        contact_id INTEGER,
        message_id INTEGER,
        message_status INTEGER,
        message_error_code INTEGER,
        is_ws_template INTEGER,
        ws_template_id VARCHAR(20), 
        nearest_replied_id INTEGER,
        nearest_replied_created_at TIMESTAMP,
        campaign_status INTEGER,
        is_done INTEGER,
        scheduled_at TIMESTAMP
    )
    WITH (
        partitioning = ARRAY['month(scheduled_at)']
    )
"""
