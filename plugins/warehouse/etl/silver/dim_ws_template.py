from plugins.config import config
from plugins.warehouse.common.trino_helper import TrinoHelper


def build_dim_ws_template(logger, trino: TrinoHelper):
    logger.info('[SCD1] Building dim_ws_template...')
    source_table_id = f"{config.LH_CHATWOOT_CATALOG}.{config.LH_CHATWOOT_SCHEMA}.channel_whatsapp"
    inboxes_table_id = f"{config.LH_CHATWOOT_CATALOG}.{config.LH_CHATWOOT_SCHEMA}.inboxes"
    dest_table_id = f"{config.LH_CHATWOOT_CATALOG}.{config.LH_CHATWOOT_SCHEMA}.dim_ws_template"

    return trino.execute(f"""
    MERGE INTO {dest_table_id} AS t
    USING (
        WITH ws_template AS (
        SELECT
            json_extract_scalar(item, '$.id') AS id,
            json_extract_scalar(item, '$.name') AS name,
            json_extract_scalar(item, '$.status') AS status,
            json_extract_scalar(item, '$.category') AS category,
            json_extract_scalar(item, '$.language') AS language,
            CASE WHEN json_extract_scalar(json_extract(item, '$.components[0]'), '$.format') != 'TEXT'
                THEN json_extract_scalar(json_extract(item, '$.components[0]'), '$.format')
                ELSE NULL END AS header_type,
            ROW_NUMBER() OVER (
                PARTITION BY 
                    ibx.id,
                    json_extract_scalar(item, '$.name'),
                    json_extract_scalar(item, '$.status'),
                    json_extract_scalar(item, '$.category'),
                    json_extract_scalar(item, '$.language')
                ORDER BY json_extract_scalar(item, '$.id') DESC
            ) AS rn
        
        FROM
            {source_table_id} cw
        CROSS JOIN UNNEST(cast(json_extract(message_templates, '$') as array(json))) AS t(item)
        INNER JOIN {inboxes_table_id} ibx ON cw.account_id = ibx.account_id AND channel_type like 'Channel::Whatsapp' AND cw.id = ibx.channel_id
        )
        SELECT
            id,
            name,
            status,
            category,
            language,
            header_type
        FROM ws_template
        WHERE rn = 1
    ) AS s
    ON (t.id = s.id)
    WHEN MATCHED THEN
        UPDATE SET
            name = s.name,
            status = s.status,
            category = s.category,
            language = s.language,
            header_type = s.header_type
    WHEN NOT MATCHED THEN
        INSERT (
            id,
            name,
            status,
            category,
            language,
            header_type
        )
        VALUES (
            s.id,
            s.name,
            s.status,
            s.category,
            s.language,
            s.header_type
        )
    """)
