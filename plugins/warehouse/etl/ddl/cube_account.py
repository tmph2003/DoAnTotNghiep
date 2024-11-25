from plugins.config import config

CUBE_ACCOUNT_BI = [
    f'USE {config.BI_CATALOG}.chatwoot',
    """
    CALL system.execute('
    CREATE TABLE IF NOT EXISTS chatwoot.cube_account (
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
    ORDER BY (account_id, partition_date)
    ')
    """
]
