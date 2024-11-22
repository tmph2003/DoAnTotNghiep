from plugins.config import config
from plugins.warehouse.common.trino_helper import TrinoHelper


def build_dim_campaign(logger, trino: TrinoHelper):
    logger.info('[SCD1] Building dim_campaign...')
    source_table_id = f"{config.LH_CHATWOOT_CATALOG}.{config.LH_CHATWOOT_SCHEMA}.campaigns"
    dest_table_id = f"{config.LH_CHATWOOT_CATALOG}.{config.LH_CHATWOOT_SCHEMA}.dim_campaign"
    return trino.execute(f"""
    MERGE INTO {dest_table_id} AS t
    USING (
        SELECT
            id,
            display_id,
            title,
            campaign_status,
            campaign_type,
            scheduled_at,
            created_at
        FROM
            {source_table_id}
    ) AS s
    ON (t.id = s.id)
    WHEN MATCHED
        THEN UPDATE
            SET title = s.title,
                campaign_status = s.campaign_status
    WHEN NOT MATCHED
        THEN INSERT (id, display_id, title, campaign_status, campaign_type, scheduled_at, created_at)
              VALUES(s.id, s.display_id, s.title, s.campaign_status, s.campaign_type, s.scheduled_at, s.created_at)
    """)
