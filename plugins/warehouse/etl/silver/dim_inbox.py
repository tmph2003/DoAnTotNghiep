from plugins.config import config
from plugins.warehouse.common.trino_helper import TrinoHelper


def build_dim_inbox(logger, trino: TrinoHelper):
    logger.info('[SCD1] Building dim_inbox...')
    source_table_id = f"{config.LH_CHATWOOT_CATALOG}.{config.LH_CHATWOOT_SCHEMA}.inboxes"
    dest_table_id = f"{config.LH_CHATWOOT_CATALOG}.{config.LH_CHATWOOT_SCHEMA}.dim_inbox"
    return trino.execute(f"""
    MERGE INTO {dest_table_id} AS t
    USING (
        SELECT
            id,
            name,
            channel_type,
            created_at,
            updated_at
        FROM
            {source_table_id}
    ) AS s
    ON (t.id = s.id)
    WHEN MATCHED AND t.updated_at <> s.updated_at
        THEN UPDATE
            SET name = s.name, channel_type = s.channel_type, updated_at = s.updated_at
    WHEN NOT MATCHED
        THEN INSERT (id, name, channel_type, created_at, updated_at)
              VALUES(s.id, s.name, s.channel_type, s.created_at, s.updated_at)
    """)
