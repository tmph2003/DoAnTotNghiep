from plugins.warehouse.common.trino_helper import TrinoHelper


def build_fct_campaign_agg(logger, trino: TrinoHelper):
    logger.info('Building fct_campaign_agg...')

    return trino.execute(f"""
    MERGE INTO iceberg.chatwoot.fct_campaign_agg AS t
    USING (
        SELECT 
            a.campaign_id,
            a.inbox_id,
            a.account_id,
            a.is_done,
            a.scheduled_at,
            COUNT(contact_id) AS num_contacts,
            SUM(CASE WHEN a.message_status = 0 THEN 1 ELSE 0 END) AS num_contacts_sent,
            SUM(CASE WHEN a.message_status = 1 THEN 1 ELSE 0 END) AS num_contacts_delivered,
            SUM(CASE WHEN a.message_status = 2 THEN 1 ELSE 0 END) AS num_contacts_seen,
            SUM(CASE WHEN a.message_status = 3 THEN 1 ELSE 0 END) AS num_contacts_failed,
            SUM(CASE WHEN a.nearest_replied_id IS NOT NULL THEN 1 ELSE 0 END) AS num_contacts_replied,
            ARRAY_AGG(DISTINCT a.message_error_code) AS error_codes
        FROM iceberg.chatwoot.fct_campaign_transaction a
        LEFT JOIN iceberg.chatwoot.fct_campaign_agg b ON a.campaign_id = b.campaign_id
        WHERE b.is_done IS NULL OR b.is_done = 0
        GROUP BY 1, 2, 3, 4, 5
    ) AS s
    ON t.campaign_id = s.campaign_id
    WHEN MATCHED THEN
        UPDATE SET
            num_contacts = s.num_contacts,
            num_contacts_sent = s.num_contacts_sent,
            num_contacts_delivered = s.num_contacts_delivered,
            num_contacts_seen = s.num_contacts_seen,
            num_contacts_failed = s.num_contacts_failed,
            num_contacts_replied = s.num_contacts_replied,
            error_codes = s.error_codes,
            is_done = s.is_done,
            scheduled_at = s.scheduled_at
    WHEN NOT MATCHED
        THEN INSERT (campaign_id, inbox_id, account_id, num_contacts, num_contacts_sent, num_contacts_delivered, num_contacts_seen, num_contacts_failed, num_contacts_replied, error_codes, is_done, scheduled_at)
            VALUES(s.campaign_id, s.inbox_id, s.account_id, s.num_contacts, s.num_contacts_sent, s.num_contacts_delivered, s.num_contacts_seen, s.num_contacts_failed, s.num_contacts_replied, s.error_codes, s.is_done, s.scheduled_at)
    """)
