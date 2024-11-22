from plugins.config import config
from plugins.warehouse.common.trino_helper import TrinoHelper


def build_fct_message(logger, trino: TrinoHelper):
    logger.info('[SCD1] Building fct_message...')
    source_table_id = f"{config.LH_CHATWOOT_CATALOG}.{config.LH_CHATWOOT_SCHEMA}"
    dest_table_id = f"{config.LH_CHATWOOT_CATALOG}.{config.LH_CHATWOOT_SCHEMA}.fct_message"

    handle_missing_templates(logger=logger, trino=trino)
    return trino.execute(f"""
    MERGE INTO {dest_table_id} AS t
    USING (
        WITH messages_has_media AS (
        SELECT DISTINCT 
            message_id, 
            account_id, 
            TRUE as has_media
        FROM {source_table_id}.attachments
        )
        SELECT
            a.id,
            a.sender_id AS "user_id",
            a.account_id,
            a.inbox_id,
            a.conversation_id,
            b.contact_id,
            CAST(json_extract_scalar(a.additional_attributes, '$.campaign_id') AS INTEGER) AS "campaign_id",
            e.code as "ws_error_code",
            e.error_type,
            e.description,
            d.has_media,
            c.id AS "ws_template_id",
            a.message_type,
            a.status,
            a.private AS "is_private",
            a.created_at,
            a.updated_at,
            a.partition_value
        FROM
            {source_table_id}.messages a
            JOIN {source_table_id}.conversations b ON a.conversation_id = b.id
            LEFT JOIN {source_table_id}.dim_ws_template c ON json_extract_scalar(a.additional_attributes, '$.template_params.id') = c.id
            LEFT JOIN messages_has_media d ON a.id = d.message_id
            LEFT JOIN {source_table_id}.dim_ws_error_code e ON
                CAST(regexp_extract(json_extract_scalar(json_extract_scalar(json_parse(content_attributes), '$'), '$.external_error'), '(\d+):', 1) AS INTEGER) = e.code
            WHERE json_extract_scalar(a.additional_attributes, '$.template_params.id') IS NOT NULL
        UNION
        SELECT
            a.id,
            a.sender_id AS "user_id",
            a.account_id,
            a.inbox_id,
            a.conversation_id,
            b.contact_id,
            CAST(json_extract_scalar(a.additional_attributes, '$.campaign_id') AS INTEGER) AS "campaign_id",
            e.code as "ws_error_code",
            e.error_type,
            e.description,
            d.has_media,
            c.id AS "ws_template_id",
            a.message_type,
            a.status,
            a.private AS "is_private",
            a.created_at,
            a.updated_at,
            a.partition_value
        FROM
            {source_table_id}.messages a
            JOIN {source_table_id}.conversations b ON a.conversation_id = b.id
            LEFT JOIN {source_table_id}.dim_ws_template c ON 
                (
                    a.inbox_id = c.inbox_id AND
                    json_extract_scalar(a.additional_attributes, '$.template_params.name') = c.name AND
                    json_extract_scalar(a.additional_attributes, '$.template_params.category') = c.category AND
                    json_extract_scalar(a.additional_attributes, '$.template_params.language') = c.language
                )
            LEFT JOIN messages_has_media d ON a.id = d.message_id
            LEFT JOIN {source_table_id}.dim_ws_error_code e ON
                CAST(regexp_extract(json_extract_scalar(json_extract_scalar(json_parse(content_attributes), '$'), '$.external_error'), '(\d+):', 1) AS INTEGER) = e.code
        WHERE json_extract_scalar(a.additional_attributes, '$.template_params.id') IS NULL
    ) AS s
    ON (t.id = s.id)
    WHEN MATCHED AND t.partition_value = s.partition_value AND t.updated_at <> s.updated_at
        THEN UPDATE SET
            campaign_id = s.campaign_id,
            ws_template_id = s.ws_template_id,
            status = s.status,
            updated_at = s.updated_at
    WHEN NOT MATCHED
        THEN INSERT (id, user_id, account_id, inbox_id, conversation_id, contact_id, campaign_id, ws_error_code, has_media, ws_template_id, message_type, status, is_private, created_at, updated_at, partition_value)
            VALUES (s.id, s.user_id, s.account_id, s.inbox_id, s.conversation_id, s.contact_id, s.campaign_id, s.ws_error_code, s.has_media, s.ws_template_id, s.message_type, s.status, s.is_private, s.created_at, s.updated_at, s.partition_value)
    """)


def handle_missing_templates(logger, trino: TrinoHelper):
    logger.info('Handle missing templates from new messages...')
    source_table_id = f"{config.LH_CHATWOOT_CATALOG}.{config.LH_CHATWOOT_SCHEMA}"
    dest_table_id = f"{config.LH_CHATWOOT_CATALOG}.{config.LH_CHATWOOT_SCHEMA}.dim_ws_template"
    
    return trino.execute(f"""
    INSERT INTO {dest_table_id}
    WITH ws_template AS (
        SELECT 
            m.inbox_id,
            json_extract_scalar(m.additional_attributes, '$.template_params.id') AS id,
            json_extract_scalar(m.additional_attributes, '$.template_params.name') AS name,
            'APPROVED' AS status,
            coalesce(json_extract_scalar(m.additional_attributes, '$.template_params.category'), 'UTILITY') AS category,
            coalesce(json_extract_scalar(m.additional_attributes, '$.template_params.language'), 'en') AS language,
            CASE
                WHEN cardinality(cast(json_extract(m.additional_attributes, '$.template_params.header_params') AS array(json))) > 0
                THEN upper(cast(json_extract(m.additional_attributes, '$.template_params.header_params[0].type') AS VARCHAR))
                ELSE NULL
            END AS header_type,
            ROW_NUMBER() OVER (
                PARTITION BY 
                    m.inbox_id,
                    json_extract_scalar(m.additional_attributes, '$.template_params.name'),
                    json_extract_scalar(m.additional_attributes, '$.template_params.category'),
                    json_extract_scalar(m.additional_attributes, '$.template_params.language')
                ORDER BY m.id DESC
            ) AS rn
        FROM 
            {source_table_id}.messages AS m
        WHERE 
            json_extract_scalar(m.additional_attributes, '$.template_params.name') IS NOT NULL
    ),
    ws_template_latest AS (
        SELECT
            account_id,
            inbox_id,
            id,
            name,
            status,
            category,
            language,
            header_type
        FROM
            ws_template
        WHERE
            rn = 1
    ),
    ws_template_latest_no_id AS (
        SELECT *
        FROM ws_template_latest
        WHERE id IS NULL
    ),
    ws_template_latest_has_id AS (
        SELECT *
        FROM ws_template_latest
        WHERE id IS NOT NULL
    )
    SELECT 
        COALESCE(wt.id, CAST(uuid() AS VARCHAR)) AS id,
        wt.inbox_id,
        wt.name,
        wt.status,
        wt.category,
        wt.language,
        wt.header_type
    FROM
        ws_template_latest_no_id AS wt
    LEFT JOIN {dest_table_id} AS dwt
        ON (
            wt.inbox_id = dwt.inbox_id AND
            wt.name = dwt.name AND
            wt.category = dwt.category AND
            wt.language = dwt.language
        )
    JOIN {source_table_id}.inboxes AS ibx ON
        ibx.id = wt.inbox_id
    WHERE dwt.name IS NULL
    UNION
    SELECT 
        COALESCE(wt.id, CAST(uuid() AS VARCHAR)) AS id,
        wt.inbox_id,
        wt.name,
        wt.status,
        wt.category,
        wt.language,
        wt.header_type
    FROM
        ws_template_latest_has_id AS wt
    LEFT JOIN {dest_table_id} AS dwt
        ON (wt.id = dwt.id)
    JOIN {source_table_id}.inboxes AS ibx ON
        ibx.id = wt.inbox_id
    WHERE dwt.name IS NULL
    """)
