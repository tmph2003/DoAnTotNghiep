from plugins.config import config
from plugins.warehouse.common.trino_helper import TrinoHelper


def build_dim_contact(logger, trino: TrinoHelper):
    logger.info('[SCD1] Building dim_contact...')
    source_table_id = f"{config.LH_CHATWOOT_CATALOG}.{config.LH_CHATWOOT_SCHEMA}.contacts"
    dest_table_id = f"{config.LH_CHATWOOT_CATALOG}.{config.LH_CHATWOOT_SCHEMA}.dim_contact"
    return trino.execute(f"""
    MERGE INTO {dest_table_id} AS t
    USING (
        SELECT
            id,
            account_id,
            name,
            middle_name,
            last_name,
            email,
            phone_number,
            location,
            country_code,
            created_at,
            updated_at
        FROM
            {source_table_id}
    ) AS s
    ON (t.id = s.id)
    WHEN MATCHED AND t.updated_at <> s.updated_at THEN
        UPDATE SET
            account_id = s.account_id,
            name = s.name,
            middle_name = s.middle_name,
            last_name = s.last_name,
            email = s.email,
            phone_number = s.phone_number,
            location = s.location,
            country_code = s.country_code,
            updated_at = s.updated_at
    WHEN NOT MATCHED THEN
        INSERT (
            id,
            account_id,
            name,
            middle_name,
            last_name,
            email,
            phone_number,
            location,
            country_code,
            created_at,
            updated_at
        )
        VALUES (
            s.id,
            s.account_id,
            s.name,
            s.middle_name,
            s.last_name,
            s.email,
            s.phone_number,
            s.location,
            s.country_code,
            s.created_at,
            s.updated_at
        )
    """)
