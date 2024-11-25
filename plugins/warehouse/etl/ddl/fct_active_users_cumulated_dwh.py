FCT_ACTIVE_USERS_CUMULATED_DWH = """
    CREATE TABLE IF NOT EXISTS clickhouse.chatwoot.fct_active_users_cumulated
    (
        user_id                 INTEGER NOT NULL,
        account_id              INTEGER NOT NULL,
        is_daily_active         INTEGER NOT NULL,
        is_weekly_active        INTEGER NOT NULL,
        is_monthly_active       INTEGER NOT NULL,
        activity_array          VARCHAR(50),
        contact_array           VARCHAR(50),
        message_array           VARCHAR(50),
        num_contacts_7d         INTEGER,
        num_messages_7d         INTEGER,
        num_contacts_30d        INTEGER,
        num_messages_30d        INTEGER,
        etl_inserted            TIMESTAMP(0)
    )
    WITH (
        engine = 'MergeTree',
        order_by = ARRAY['user_id', 'account_id'],
        primary_key = ARRAY['user_id', 'account_id']
    )
"""