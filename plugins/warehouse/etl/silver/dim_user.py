from plugins.config import config
from plugins.warehouse.common.trino_helper import TrinoHelper


def build_dim_user(logger, trino: TrinoHelper):
    logger.info('[SCD1] Building dim_user...')
    source_table_id = f"{config.LH_CHATWOOT_CATALOG}.{config.LH_CHATWOOT_SCHEMA}.users"
    dest_table_id = f"{config.LH_CHATWOOT_CATALOG}.{config.LH_CHATWOOT_SCHEMA}.dim_user"
    return trino.execute(f"""
    MERGE INTO {dest_table_id} AS t
    USING (
        SELECT
            id,
            name,
            display_name,
            email,
            type,
            created_at,
            updated_at
        FROM
            {source_table_id}
    ) AS s
    ON (t.id = s.id)
    WHEN MATCHED AND t.updated_at <> s.updated_at THEN
        UPDATE SET
            name = s.name,
            display_name = s.display_name,
            email = s.email,
            type = s.type,
            updated_at = s.updated_at
    WHEN NOT MATCHED THEN
        INSERT (
            id,
            name,
            display_name,
            email,
            type,
            created_at,
            updated_at
        )
        VALUES (
            s.id,
            s.name,
            s.display_name,
            s.email,
            s.type,
            s.created_at,
            s.updated_at
        )
    """)
