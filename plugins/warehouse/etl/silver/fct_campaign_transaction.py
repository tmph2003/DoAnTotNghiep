import json
import pandas as pd

from datetime import datetime, timedelta
from plugins.warehouse.common.trino_helper import TrinoHelper


def build_fct_campaign_transaction(logger, trino: TrinoHelper, partition_value=None):
    logger.info('Building fct_campaign_transaction...')

    # Lấy danh sách các campaign chưa được xử lý.
    results = trino.execute("""
    SELECT
        c.id AS campaign_id,
        c.account_id AS account_id,
        c.inbox_id AS inbox_id,
        json_extract(c.trigger_rules, '$.audience_conditions') AS audience_conditions_1,
        transform(cast(json_extract(c.audience, '$') AS array(json)), x -> json_extract_scalar(x, '$.id')) AS audience_conditions_2,
        d.id AS ws_template_id,
        c.campaign_status,
        COALESCE(fct.is_done, 0) AS is_done,
        c.scheduled_at
    FROM iceberg.chatwoot.campaigns c
    LEFT JOIN iceberg.chatwoot.fct_campaign_transaction fct ON c.id = fct.campaign_id
    LEFT JOIN iceberg.chatwoot.dim_ws_template d ON json_extract_scalar(c.trigger_rules, '$.template_params.id') = d.id
    WHERE fct.campaign_status IS NULL OR fct.campaign_status = 0
    """)
    campaign_df = pd.DataFrame(results,
                               columns=['campaign_id', 'account_id', 'inbox_id', 'audience_conditions_1',
                                        'audience_conditions_2', 'ws_template_id', 'campaign_status', 'is_done', 'scheduled_at'])

    for idx, row in campaign_df.iterrows():
        logger.info(f"============{idx}/{campaign_df.shape[0]}============")
        results = trino.execute(f"""
        SELECT
            l.id AS label_id,
            coalesce(t.id, -1) AS tag_id
        FROM iceberg.chatwoot.labels l
        LEFT JOIN iceberg.chatwoot.tags t ON l.title = t.name
        WHERE l.account_id = {row['account_id']}
        """)
        label_to_tag_dict = dict(results)

        label_conditions = []
        audience_conditions_1 = json.loads(row['audience_conditions_1']) if isinstance(row['audience_conditions_1'],
                                                                                       str) else row[
            'audience_conditions_1']
        audience_conditions_2 = json.loads(row['audience_conditions_2']) if isinstance(row['audience_conditions_2'],
                                                                                       str) else row[
            'audience_conditions_2']

        # Lấy định nghĩa labels cho contact được gửi
        if isinstance(audience_conditions_1, list) and len(audience_conditions_1):
            label_conditions = audience_conditions_1
        elif isinstance(audience_conditions_2, list) and len(audience_conditions_2):
            label_conditions = audience_conditions_2

        # Cập nhật danh sách các contact thỏa mãn điều kiện gửi mới nhất.
        ids_or = [str(label_to_tag_dict.get(int(item), -1)) for item in label_conditions if
                  not isinstance(item, dict) and str(item)]
        ids_and = [json.loads(item.get('values')) for item in label_conditions if
                   isinstance(item, dict) and item.get('operator', '') == 'and' and json.loads(item.get('values'))]

        ids_and_sql = ''
        for ids_item in ids_and:  # [[[1], [903]], [[904], [905]]]
            ids_item_sql = ''
            for id_list in ids_item:
                if len(id_list) == 0:
                    continue
                elif len(id_list) == 1:
                    item_sql = f"t.tag_id = {id_list[0]}"
                else:
                    item_sql = f"(t.tag_id in ({', '.join([str(label_to_tag_dict.get(int(i), -1)) for i in id_list])}))"
                ids_item_sql = f"{ids_item_sql} AND {item_sql}" if ids_item_sql else item_sql
            ids_and_sql = f"{ids_and_sql} OR ({ids_item_sql})" if ids_and_sql else f"({ids_item_sql})"

        labels_sql = ''
        if len(ids_or):
            labels_sql = f"t.tag_id in ({', '.join(ids_or)})" if len(ids_or) > 1 else f"t.tag_id = {ids_or[0]}"

        if len(ids_and):
            labels_sql = f"{labels_sql} OR ({ids_and_sql})" if labels_sql else f"({ids_and_sql})"

        if labels_sql:
            labels_sql = f"({labels_sql}) AND"

        if not labels_sql:
            continue

        merge_into_sql = f"""
        MERGE INTO iceberg.chatwoot.fct_campaign_transaction AS t
        USING (
            WITH contact_matched AS (
                SELECT DISTINCT t.taggable_id AS contact_id
                FROM iceberg.chatwoot.taggings t
                WHERE t.taggable_type = 'Contact' AND {labels_sql} t.created_at < timestamp '{row['scheduled_at']}'
            )
            SELECT
                {row['campaign_id']} AS campaign_id,
                {row['account_id']} AS account_id,
                {row['inbox_id']} AS inbox_id,
                contact_id AS contact_id,
                {row['campaign_status']} AS campaign_status,
                {'NULL' if not row['ws_template_id'] else "'" + str(row['ws_template_id']) + "'"} AS ws_template_id,
                {row['is_done']} AS is_done,
                timestamp '{row['scheduled_at']}' AS scheduled_at
            FROM contact_matched
        )  AS s
        ON (t.campaign_id = s.campaign_id AND t.contact_id = s.contact_id)
        WHEN MATCHED AND t.campaign_status <> s.campaign_status
        THEN UPDATE
            SET campaign_status = s.campaign_status
        WHEN NOT MATCHED
            THEN INSERT (campaign_id, account_id, inbox_id, contact_id, campaign_status, ws_template_id, is_done, scheduled_at)
                  VALUES(s.campaign_id, s.account_id, s.inbox_id, s.contact_id, s.campaign_status, s.ws_template_id, s.is_done, s.scheduled_at)
        """
        trino.execute(merge_into_sql)

    # Cập nhật các tin nhắn và trạng thái sau khi đã gửi.
    current_date = datetime.utcnow() + timedelta(hours=7)
    if partition_value is None:
        partition_value = current_date.strftime('%Y-%m-%d')

    update_sql = f"""
    MERGE INTO iceberg.chatwoot.fct_campaign_transaction AS d
    USING (
        WITH camaign_need_updated AS (
            SELECT
                *
            FROM iceberg.chatwoot.fct_campaign_transaction fct
            WHERE fct.is_done = 0
        ),
        messages_campaign AS (
            SELECT
                a.campaign_id,
                b.inbox_id,
                a.contact_id,
                a.message_id,
                b.status AS message_status,
                CAST(regexp_extract(json_extract_scalar(json_extract_scalar(json_parse(content_attributes), '$'), '$.external_error'), '(\d+):', 1) AS INTEGER) AS message_error_code,
                CASE WHEN json_extract(b.additional_attributes, '$.template_params') IS NOT NULL THEN 1 ELSE 0 END AS is_ws_template
            FROM (
                SELECT
                    CAST(json_extract_scalar(m.additional_attributes, '$.campaign_id') AS INTEGER) as campaign_id,
                    c.contact_id,
                    max(m.id) as message_id
                FROM iceberg.chatwoot.messages m
                JOIN iceberg.chatwoot.conversations c on m.conversation_id = c.id
                WHERE m.partition_value = DATE '{partition_value}'
                AND message_type = 1
                AND CAST(json_extract_scalar(m.additional_attributes, '$.campaign_id') AS INTEGER) IS NOT NULL
                GROUP BY 1, 2
            ) a
            JOIN iceberg.chatwoot.messages b ON a.message_id = b.id
            WHERE b.partition_value = DATE '{partition_value}'
        ),
        messages_incoming_min AS (
            SELECT
                a.campaign_id,
                b.inbox_id,
                a.contact_id,
                a.nearest_replied_id,
                b.created_at AS nearest_replied_created_at
            FROM (
                SELECT
                    mc.campaign_id,
                    mc.contact_id AS contact_id,
                    min(m.id) as nearest_replied_id
                FROM messages_campaign mc
                LEFT JOIN iceberg.chatwoot.messages m ON mc.inbox_id = m.inbox_id AND mc.contact_id = m.sender_id AND mc.message_id < m.id
                WHERE m.partition_value = DATE '{partition_value}'
                AND m.message_type = 0
                GROUP BY 1, 2
            ) a
            JOIN iceberg.chatwoot.messages b ON a.nearest_replied_id = b.id
            WHERE b.partition_value = DATE '{partition_value}'
        )
        SELECT
            a.campaign_id,
            a.account_id,
            a.inbox_id,
            a.contact_id,
            COALESCE(a.message_id, b.message_id) AS message_id,
            COALESCE(a.message_status, b.message_status) AS message_status,
            COALESCE(a.message_error_code, b.message_error_code) AS message_error_code,
            COALESCE(a.is_ws_template, b.is_ws_template) AS is_ws_template,
            COALESCE(a.nearest_replied_id, c.nearest_replied_id) AS nearest_replied_id,
            COALESCE(a.nearest_replied_created_at, c.nearest_replied_created_at) AS nearest_replied_created_at,
            CASE WHEN date_diff('day', date(a.scheduled_at), DATE '{partition_value}') >= 7 THEN 1 ELSE 0 END AS is_done,
            a.scheduled_at
        FROM camaign_need_updated a
        LEFT JOIN messages_campaign b on a.campaign_id = b.campaign_id AND a.contact_id = b.contact_id
        LEFT JOIN messages_incoming_min c on a.campaign_id = c.campaign_id AND a.contact_id = c.contact_id
    ) AS s
    ON s.campaign_id = d.campaign_id AND s.contact_id = d.contact_id
    WHEN MATCHED
        THEN UPDATE SET
            message_id = s.message_id,
            message_status = s.message_status,
            message_error_code = s.message_error_code,
            is_ws_template = s.is_ws_template,
            nearest_replied_id = s.nearest_replied_id,
            nearest_replied_created_at = s.nearest_replied_created_at,
            is_done = s.is_done
    """
    trino.execute(update_sql)
