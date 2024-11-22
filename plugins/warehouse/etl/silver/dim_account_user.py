from plugins.config import config
from plugins.warehouse.common.trino_helper import TrinoHelper


def build_dim_account_user(logger, trino: TrinoHelper):
    logger.info('[SCD1] Building dim_account_user...')
    source_table_id = f"{config.LH_CHATWOOT_CATALOG}.{config.LH_CHATWOOT_SCHEMA}.account_users"
    dest_table_id = f"{config.LH_CHATWOOT_CATALOG}.{config.LH_CHATWOOT_SCHEMA}.dim_account_user"
    
    return trino.execute(f"""
    MERGE INTO {dest_table_id} AS t
    USING (
        SELECT
            account_id,
            user_id,
            role,
            active_at,
            created_at,
            updated_at
        FROM
            {source_table_id}
    ) AS s
    ON (t.account_id = s.account_id AND t.user_id = s.user_id)
    WHEN MATCHED AND t.updated_at <> s.updated_at
        THEN UPDATE SET
            role = s.role,
            updated_at = s.updated_at
    WHEN NOT MATCHED THEN
        INSERT (
            account_id,
            user_id,
            role,
            active_at,
            created_at,
            updated_at
        )
        VALUES (
            s.account_id,
            s.user_id,
            s.role,
            s.active_at,
            s.created_at,
            s.updated_at
        )
    """)
