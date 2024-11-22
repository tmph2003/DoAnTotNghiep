from plugins.config import config
from plugins.warehouse.common.trino_helper import TrinoHelper


def build_fct_conversation(logger, trino: TrinoHelper):
    logger.info('Building fct_conversation...')
    source_table_id = f"{config.LH_CHATWOOT_CATALOG}.{config.LH_CHATWOOT_SCHEMA}.conversations"
    dest_table_id = f"{config.LH_CHATWOOT_CATALOG}.{config.LH_CHATWOOT_SCHEMA}.fct_conversation"
    return trino.execute(f"""
    MERGE INTO {dest_table_id} AS t
    USING (
        SELECT
            id,
            account_id,
            inbox_id,
            contact_id,
            status,
            created_at,
            updated_at
        FROM
            {source_table_id}
    ) AS s
    ON (t.id = s.id)
    WHEN MATCHED AND t.status <> s.status THEN
        UPDATE SET
            status = s.status,
            updated_at = s.updated_at
    WHEN NOT MATCHED THEN
        INSERT (
           id,
            account_id,
            inbox_id,
            contact_id,
            status,
            created_at,
            updated_at
        )
        VALUES (
            s.id, 
            s.account_id,
            s.inbox_id,
            s.contact_id,
            s.status,
            s.created_at,
            s.updated_at
        )
    """)
