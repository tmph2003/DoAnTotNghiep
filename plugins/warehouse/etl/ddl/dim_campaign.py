DIM_CAMPAIGN = """
CREATE TABLE IF NOT EXISTS iceberg.chatwoot.dim_campaign (
    id INTEGER,
    display_id INTEGER,
    title VARCHAR(50),
    campaign_status INTEGER,
    campaign_type INTEGER,
    scheduled_at TIMESTAMP,
    created_at TIMESTAMP
)
"""
