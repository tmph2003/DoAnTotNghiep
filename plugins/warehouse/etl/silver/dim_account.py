from plugins.config import config
from plugins.warehouse.common.trino_helper import TrinoHelper


def build_dim_account(logger, trino: TrinoHelper):
    logger.info('[SCD1] Building dim_account...')
    source_table_id = f"{config.LH_CHATWOOT_CATALOG}.{config.LH_CHATWOOT_SCHEMA}.accounts"
    dest_table_id = f"{config.LH_CHATWOOT_CATALOG}.{config.LH_CHATWOOT_SCHEMA}.dim_account"
    
    return trino.execute(f"""
    MERGE INTO {dest_table_id} AS t
    USING (
        SELECT
            id,
            name,
            created_at,
            updated_at
        FROM
            {source_table_id}
    ) AS s
    ON (t.id = s.id)
    WHEN MATCHED AND t.updated_at <> s.updated_at
        THEN UPDATE
            SET name = s.name, updated_at = s.updated_at
    WHEN NOT MATCHED
        THEN INSERT (id, name, created_at, updated_at)
              VALUES(s.id, s.name, s.created_at, s.updated_at)
    """)
