from datetime import datetime, timedelta

from plugins.warehouse.common.trino_helper import TrinoHelper


def build_fct_account_agg(logger, trino: TrinoHelper, partition_value=None):
    logger.info('Building fct_account_agg...')

    # Cập nhật các tin nhắn và trạng thái sau khi đã gửi.
    current_date = datetime.utcnow() + timedelta(hours=7)
    if partition_value is None:
        partition_value = current_date.strftime('%Y-%m-%d')

    logger.info(f'Truncate current partition: {partition_value}...')
    trino.execute(
        f"""DELETE FROM iceberg.chatwoot.fct_account_agg WHERE partition_date = DATE '{partition_value}'""")
    logger.info('Refresh data for current partition...')
    return trino.execute(f"""
    INSERT INTO iceberg.chatwoot.fct_account_agg 
    WITH account_users_metrics AS (
	SELECT
		account_id,
		COUNT(DISTINCT CASE WHEN DATE(active_at) = DATE '{partition_value}' THEN user_id ELSE 0 END) -1 AS num_active_users,
		COUNT(DISTINCT user_id) AS num_users
	FROM iceberg.chatwoot.account_users
	GROUP BY 1
    ),
    contacts_metrics AS (
        SELECT
            account_id,
            SUM(CASE WHEN DATE(created_at) = DATE '{partition_value}' THEN 1 ELSE 0 END) AS num_new_contacts,
            COUNT(*) AS num_contacts
        FROM iceberg.chatwoot.contacts
        GROUP BY 1
    ),
    inboxes_metrics AS (
        SELECT
            account_id,
            COUNT(*)  AS num_inboxes
        FROM iceberg.chatwoot.inboxes
        GROUP BY 1
    ),
    messages_metrics AS (
        SELECT
            m.account_id,
            SUM(CASE WHEN message_type = 0 and DATE(m.created_at) = DATE '{partition_value}' THEN 1 ELSE 0 END) AS num_incoming_messages,
            SUM(CASE WHEN message_type = 1 and DATE(m.created_at) = DATE '{partition_value}' AND json_extract_scalar(json_extract_scalar(json_parse(m.content_attributes), '$'), '$.external_error') IS NOT NULL THEN 1 ELSE 0 END) AS num_failed_outcoming_messages,
            SUM(CASE WHEN message_type = 1 and DATE(m.created_at) = DATE '{partition_value}' THEN 1 ELSE 0 END) AS num_outcoming_messages,
            COUNT(DISTINCT CASE WHEN message_type = 0 and DATE(m.created_at) = DATE '{partition_value}' THEN sender_id ELSE 0 END) -1 AS num_active_contacts,
            COUNT(DISTINCT CASE WHEN message_type = 1 and DATE(m.created_at) = DATE '{partition_value}' THEN c.contact_id ELSE 0 END) -1 AS num_cared_contacts,
            COUNT(DISTINCT CASE WHEN DATE(m.created_at) = DATE '{partition_value}' THEN conversation_id ELSE 0 END) - 1 AS num_active_conversations
        FROM iceberg.chatwoot.messages m
        JOIN iceberg.chatwoot.conversations c on m.account_id = c.account_id and m.conversation_id = c.id
        GROUP BY 1
    ),
    conversations_metrics AS (
        SELECT
            account_id,
            COUNT(*) AS num_conversations
        FROM iceberg.chatwoot.conversations
        GROUP BY 1
    )
    SELECT
        aum.account_id,
        aum.num_active_users,
        aum.num_users,
        mm.num_incoming_messages,
        mm.num_failed_outcoming_messages,
        mm.num_outcoming_messages,
        im.num_inboxes,
        cm.num_new_contacts,
        mm.num_active_contacts,
        mm.num_cared_contacts,
        cm.num_contacts,
        mm.num_active_conversations,
        com.num_conversations,
        CURRENT_DATE AS partition_date
    FROM account_users_metrics aum
    JOIN contacts_metrics cm ON aum.account_id = cm.account_id
    JOIN inboxes_metrics im ON aum.account_id = im.account_id
    JOIN messages_metrics mm ON aum.account_id = mm.account_id
    JOIN conversations_metrics com ON aum.account_id = com.account_id
    """)
