from plugins.config import config
from plugins.warehouse.common.trino_helper import TrinoHelper


def build_fct_active_users_daily(logger, trino: TrinoHelper):
    logger.info('Building fct_active_users_daily...')
    source_table_id = f"{config.LH_CHATWOOT_CATALOG}.{config.LH_CHATWOOT_SCHEMA}"
    dest_table_id = f"{config.WH_CHATWOOT_CATALOG}.{config.WH_CHATWOOT_SCHEMA}.fct_active_users_daily"
    return trino.execute(f"""
    INSERT INTO {dest_table_id}
    WITH all_account_user AS (SELECT a.account_id,
                                    a.user_id,
                                    a.role,
                                    b.id AS date_id
                            FROM {source_table_id}.dim_account_user a
                                    CROSS JOIN {source_table_id}.dim_date b
                            WHERE CAST(DATE_FORMAT(a.created_at, '%Y%m%d') AS INTEGER) < b.id
                                and b.id < CAST(DATE_FORMAT(CAST(CURRENT_TIMESTAMP + INTERVAL '7' HOUR AS DATE), '%Y%m%d') AS INTEGER)),
        contacts_sent AS (SELECT a.user_id,
                                a.account_id,
                                CAST(DATE_FORMAT(a.created_at, '%Y%m%d') AS INTEGER) AS date_id,
                                COUNT(DISTINCT a.contact_id)                         AS num_contacts_sent
                        FROM {source_table_id}.fct_message a
                        WHERE a.message_type = 1
                        GROUP BY a.user_id, a.account_id,
                                    CAST(DATE_FORMAT(a.created_at, '%Y%m%d') AS INTEGER)),
        contacts_replied AS (SELECT b.assignee_id,
                                    a.account_id,
                                    CAST(DATE_FORMAT(a.created_at, '%Y%m%d') AS INTEGER) AS date_id,
                                    count(DISTINCT a.contact_id)                         AS num_contacts_replied
                            FROM {source_table_id}.fct_message a
                                    JOIN {source_table_id}.conversations b ON a.conversation_id = b.id
                            WHERE a.message_type = 0
                            GROUP BY b.assignee_id, a.account_id,
                                    CAST(DATE_FORMAT(a.created_at, '%Y%m%d') AS INTEGER))
    select aau.user_id,
        aau.account_id,
        aau.date_id,
        aau.date_id / 100 AS month_id,
        CASE
            WHEN EXISTS(SELECT 1
                        FROM {source_table_id}.fct_message f
                        WHERE f.user_id = aau.user_id
                            AND f.account_id = aau.account_id
                            AND CAST(DATE_FORMAT(f.created_at, '%Y%m%d') AS INTEGER) = aau.date_id) THEN 1
            ELSE 0
            END                             AS is_active_today,
        COALESCE(b.num_contacts_sent, 0)    AS num_contacts,
        COUNT(a.id)                         AS num_messages,
        COALESCE(c.num_contacts_replied, 0) AS num_contacts_replied,
        current_timestamp + INTERVAL '7' hour
    FROM all_account_user aau
            LEFT JOIN {source_table_id}.fct_message a
                    ON aau.user_id = a.user_id
                        AND aau.account_id = a.account_id
                        AND aau.date_id = CAST(DATE_FORMAT(a.created_at, '%Y%m%d') AS INTEGER)
            LEFT JOIN contacts_sent b
                    ON aau.user_id = b.user_id
                        AND aau.account_id = b.account_id
                        AND aau.date_id = b.date_id
            LEFT JOIN contacts_replied c
                    ON aau.user_id = c.assignee_id
                        AND aau.account_id = c.account_id
                        AND aau.date_id = c.date_id
    WHERE aau.user_id is not null
    AND aau.account_id is not null
    GROUP BY aau.user_id,
            aau.account_id,
            aau.date_id,
            COALESCE(b.num_contacts_sent, 0),
            COALESCE(c.num_contacts_replied, 0)
    """)
