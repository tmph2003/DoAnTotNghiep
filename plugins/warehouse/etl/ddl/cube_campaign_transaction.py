from plugins.config import config

CUBE_CAMPAIGN_TRANSACTION = """
    CREATE TABLE IF NOT EXISTS iceberg.chatwoot.cube_campaign_transaction (
        campaign_id INTEGER NOT NULL, 
        inbox_id INTEGER NOT NULL, 
        account_id INTEGER NOT NULL, 
        campaign_name VARCHAR(50) NOT NULL,
        inbox_name VARCHAR(50) NOT NULL,
        branch_name VARCHAR(50) NOT NULL,
        num_contacts INTEGER,
        num_contacts_sent INTEGER,
        num_contacts_delivered INTEGER,
        num_contacts_seen INTEGER,
        num_contacts_failed INTEGER,
        num_contacts_replied INTEGER,
        error_codes VARCHAR(100),
        is_done INTEGER,
        scheduled_at TIMESTAMP(0),
        etl_inserted TIMESTAMP(0)
    )
    WITH (
        partitioning = ARRAY['month(scheduled_at)']
    )
"""

CUBE_CAMPAIGN_TRANSACTION_BI = [
    f'USE {config.BI_CATALOG}.hebela_chat',
    """
    CALL system.execute('
        CREATE TABLE IF NOT EXISTS hebela_chat.cube_campaign_transaction (
            campaign_id Int32 NOT NULL,
            inbox_id Int32 NOT NULL,
            account_id Int32 NOT NULL,
            campaign_name String NOT NULL,
            inbox_name String NOT NULL,
            branch_name String NOT NULL,
            num_contacts Int32,
            num_contacts_sent Int32,
            num_contacts_delivered Int32,
            num_contacts_seen Int32,
            num_contacts_failed Int32,
            num_contacts_replied Int32,
            error_codes String,
            is_done UInt8,
            scheduled_at DateTime,
            etl_inserted DateTime
        ) ENGINE = MergeTree()
        PRIMARY KEY campaign_id
        PARTITION BY toYYYYMM(scheduled_at)
        ORDER BY (campaign_id);
    ')
    """
]
