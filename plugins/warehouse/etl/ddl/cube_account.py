from plugins.config import config

CUBE_ACCOUNT = """
    CREATE TABLE IF NOT EXISTS iceberg.chatwoot.cube_account (
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
        partition_date DATE NOT NULL
    )
    WITH (
        partitioning = ARRAY['month(partition_date)']
    )
"""

CUBE_ACCOUNT_BI = [
    f'USE {config.BI_CATALOG}.hebela_chat',
    """
    CALL system.execute('
    CREATE TABLE IF NOT EXISTS hebela_chat.cube_account (
        account_id Int32,
        num_active_users Int32,
        num_users Int32,
        num_incoming_messages Int32,
        num_failed_outcoming_messages Int32,
        num_outcoming_messages Int32,
        num_inboxes Int32,
        num_new_contacts Int32,
        num_active_contacts Int32,
        num_cared_contacts Int32,
        num_contacts Int32,
        num_active_conversations Int32,
        num_conversations Int32,
        partition_date Date NOT NULL
    ) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(partition_date)
    ORDER BY (branch_name, partition_date)
    ')
    """
]
