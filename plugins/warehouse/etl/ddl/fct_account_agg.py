FCT_ACCOUNT_AGG = """
    CREATE TABLE IF NOT EXISTS iceberg.chatwoot.fct_account_agg (
        account_id INTEGER,
        num_active_users INTEGER,
        num_users INTEGER,
        num_incoming_messages INTEGER,
        num_failed_outcoming_messages INTEGER,
        num_outcoming_messages INTEGER,
        num_inboxes INTEGER,
        num_new_contacts INTEGER,
        num_active_contacts INTEGER, 
        num_cared_contacts INTEGER,
        num_contacts INTEGER,
        num_active_conversations INTEGER,
        num_conversations INTEGER,
        partition_date DATE
    )
    WITH (
        partitioning = ARRAY['month(partition_date)']
    )
"""
