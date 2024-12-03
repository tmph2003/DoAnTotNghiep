WITH import_fct_active_users_cumulated AS ( -- noqa: 
  SELECT *
  FROM {{ source('hc_gold', 'fct_active_users_cumulated') }}
),

final_cte AS (
  SELECT
    fauc.user_id,
    fauc.account_id,
    fauc.is_daily_active,
    fauc.is_weekly_active,
    fauc.is_monthly_active,
    fauc.activity_array,
    fauc.contact_array,
    fauc.message_array,
    fauc.num_contacts_7d,
    fauc.num_messages_7d,
    fauc.num_contacts_30d,
    fauc.num_messages_30d
  FROM (
    SELECT *
    FROM import_fct_active_users_cumulated
    WHERE etl_inserted = (
        SELECT MAX(etl_inserted) AS max_etl_inserted
        FROM import_fct_active_users_cumulated
      )
  ) fauc
)
SELECT * FROM final_cte