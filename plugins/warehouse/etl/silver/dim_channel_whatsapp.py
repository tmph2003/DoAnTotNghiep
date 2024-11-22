from plugins.config import config
from plugins.warehouse.common.trino_helper import TrinoHelper


def build_dim_channel_whatsapp(logger, trino: TrinoHelper):
    logger.info('[SCD1] Building dim_channel_whatsapp...')
    source_table_id = f"{config.LH_CHATWOOT_CATALOG}.{config.LH_CHATWOOT_SCHEMA}.channel_whatsapp"
    dest_table_id = f"{config.LH_CHATWOOT_CATALOG}.{config.LH_CHATWOOT_SCHEMA}.dim_channel_whatsapp"
    return trino.execute(f"""
    MERGE INTO {dest_table_id} AS t
    USING (
        SELECT
            id,
            phone_number,
            created_at,
            updated_at
        FROM
            {source_table_id}
    ) AS s
    ON (t.id = s.id)
    WHEN MATCHED
        THEN UPDATE
            SET phone_number = s.phone_number, updated_at = s.updated_at
    WHEN NOT MATCHED
        THEN INSERT (id, phone_number, created_at, updated_at)
              VALUES(s.id, s.phone_number, s.created_at, s.updated_at)
    """)
