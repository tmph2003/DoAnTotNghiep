FCT_ACTIVE_USERS_DAILY = """
CREATE TABLE IF NOT EXISTS iceberg.chatwoot.fct_active_users_daily (
    user_id                 INTEGER NOT NULL,
    account_id              INTEGER NOT NULL,
    date_id                 INTEGER NOT NULL,
    is_active_today         INTEGER,
    num_contacts            INTEGER,
    num_messages            INTEGER,
    num_contacts_replied    INTEGER,
    etl_inserted            TIMESTAMP
)
"""
