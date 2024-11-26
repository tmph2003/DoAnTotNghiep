from plugins.config import config
from plugins.warehouse.common.trino_helper import TrinoHelper


def build_fct_active_users_cumulated_dwh(logger, trino: TrinoHelper):
    logger.info('Building fct_active_users_cumulated_dwh...')
    source_table_id = f"{config.LH_CHATWOOT_CATALOG}.{config.LH_CHATWOOT_SCHEMA}"
    dest_table_id = f"{config.WH_CHATWOOT_CATALOG}.{config.WH_CHATWOOT_SCHEMA}.fct_active_users_cumulated"
    return trino.execute(f"""
    INSERT INTO {dest_table_id}
    WITH cte_weekly AS (SELECT  user_id,
                                account_id,
                                1 AS is_active_weekly
                            FROM {source_table_id}.fct_active_users_daily
                            WHERE date_id <= CAST(DATE_FORMAT(DATE_ADD('hour', 7, CURRENT_TIMESTAMP), '%Y%m%d') AS INTEGER)
                            AND date_id >= CAST(DATE_FORMAT(DATE_ADD('hour', 7, CURRENT_TIMESTAMP) - INTERVAL '7' DAY,
                                                            '%Y%m%d') AS INTEGER)
                            GROUP BY user_id, account_id
                            HAVING bool_or(is_active_today = 1)),
            cte_monthly AS (SELECT user_id,
                                    account_id,
                                    1 AS is_active_monthly
                            FROM {source_table_id}.fct_active_users_daily
                            WHERE date_id <= CAST(DATE_FORMAT(DATE_ADD('hour', 7, CURRENT_TIMESTAMP), '%Y%m%d') AS INTEGER)
                            AND date_id >= CAST(DATE_FORMAT(date_trunc('month', CURRENT_TIMESTAMP + INTERVAL '7' hour),
                                                            '%Y%m%d') AS INTEGER)
                            GROUP BY user_id, account_id
                            HAVING bool_or(is_active_today = 1)),
            user_dates AS (SELECT u.user_id, -- cross join mỗi user, account 30 ngày
                                u.account_id,
                                dd.id AS date_id
                            FROM (SELECT DISTINCT user_id, account_id FROM {source_table_id}.fct_active_users_daily) u -- Danh sách tất cả user
                                    CROSS JOIN
                                {source_table_id}.dim_date dd
                            WHERE dd.id >= CAST(DATE_FORMAT(DATE_ADD('day', -30, CURRENT_DATE), '%Y%m%d') AS INTEGER)
                            AND dd.id <=
                                CAST(DATE_FORMAT(DATE_ADD('hour', 7, CURRENT_TIMESTAMP), '%Y%m%d') AS INTEGER)),
            user_activity AS (SELECT ud.user_id,
                                    ud.account_id,
                                    ud.date_id,
                                    COALESCE(faud.is_active_today, 0) AS is_active_today,
                                    COALESCE(faud.num_contacts, 0)    AS num_contacts,
                                    COALESCE(faud.num_messages, 0)    AS num_messages
                            FROM user_dates ud
                                        LEFT JOIN {source_table_id}.fct_active_users_daily faud ON
                                ud.user_id = faud.user_id AND faud.account_id = ud.account_id AND
                                ud.date_id = faud.date_id
                            WHERE ud.date_id >= CAST(DATE_FORMAT(DATE_ADD('day', -30, CURRENT_DATE), '%Y%m%d') AS INTEGER)
                                AND ud.date_id <=
                                    CAST(DATE_FORMAT(DATE_ADD('hour', 7, CURRENT_TIMESTAMP), '%Y%m%d') AS INTEGER)),
            cte_active_array AS (SELECT ua.user_id,
                                        ua.account_id,
                                        array_agg(COALESCE(ua.is_active_today, 0) ORDER BY ua.date_id DESC) AS activity_array,
                                        array_agg(COALESCE(ua.num_contacts, 0) ORDER BY ua.date_id DESC)    AS contact_array,
                                        array_agg(COALESCE(ua.num_messages, 0) ORDER BY ua.date_id DESC)    AS message_array
                                FROM user_activity ua
                                GROUP BY ua.user_id, ua.account_id)
        select faud.user_id,
            faud.account_id,
            faud.is_active_today,
            COALESCE(cw.is_active_weekly, 0)                        AS is_active_weekly,
            COALESCE(cm.is_active_monthly, 0)                       AS is_active_monthly,
            array_join(caa.activity_array, ', ') AS activity_array,
            array_join(caa.contact_array, ', ') AS contact_array,
            array_join(caa.message_array, ', ') AS message_array,
            reduce(slice(contact_array, 1, 7), 0, (s, x) -> s + x, s -> s) AS num_contacts_7d,
            reduce(slice(message_array, 1, 7), 0, (s, x) -> s + x, s -> s) AS num_messages_7d,
            reduce(contact_array, 0, (s, x) -> s + x, s -> s)              AS num_contacts_30d,
            reduce(message_array, 0, (s, x) -> s + x, s -> s)              AS num_messages_30d,
            CURRENT_TIMESTAMP + INTERVAL '7' hour                          AS etl_inserted
        FROM {source_table_id}.fct_active_users_daily faud
                LEFT JOIN cte_weekly cw ON faud.user_id = cw.user_id AND faud.account_id = cw.account_id
                LEFT JOIN cte_monthly cm ON faud.user_id = cm.user_id AND faud.account_id = cm.account_id
                JOIN cte_active_array caa ON faud.user_id = caa.user_id AND faud.account_id = caa.account_id
        WHERE faud.date_id = CAST(DATE_FORMAT(DATE_ADD('hour', 7, CURRENT_TIMESTAMP), '%Y%m%d') AS INTEGER)
    """)
