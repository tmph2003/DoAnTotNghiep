FCT_ACTIVE_USERS_DAILY_DWH = """
    CREATE TABLE IF NOT EXISTS clickhouse.chatwoot.fct_active_users_daily (
        user_id                 INTEGER NOT NULL,
        account_id              INTEGER NOT NULL,
        date_id                 INTEGER NOT NULL,
        month_id                INTEGER NOT NULL,
        is_active_today         INTEGER,
        num_contacts            INTEGER,
        num_messages            INTEGER,
        num_contacts_replied    INTEGER,
        etl_inserted            TIMESTAMP(0)
    )
    WITH (
        engine = 'MergeTree',
        order_by = ARRAY['user_id', 'account_id', 'date_id'],
        partition_by = ARRAY['month_id'],
        primary_key = ARRAY['user_id', 'account_id', 'date_id']
    )
"""
