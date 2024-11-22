FCT_CAMPAIGN_AGG = """
    CREATE TABLE IF NOT EXISTS iceberg.chatwoot.fct_campaign_agg (
        campaign_id INTEGER,
        inbox_id INTEGER,
        account_id INTEGER,
        num_contacts INTEGER,
        num_contacts_sent INTEGER,
        num_contacts_delivered INTEGER,
        num_contacts_seen INTEGER,
        num_contacts_failed INTEGER,
        num_contacts_replied INTEGER,
        error_codes ARRAY<INTEGER>,
        is_done INTEGER,
        scheduled_at TIMESTAMP
    )
    WITH (
        partitioning = ARRAY['month(scheduled_at)']
    )
"""

