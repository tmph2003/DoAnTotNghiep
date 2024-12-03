WITH import_fct_active_users_daily AS ( -- noqa: 
  SELECT *
  FROM {{ source('hc_gold', 'fct_active_users_daily') }}
),

final_cte AS (
  SELECT
    faud.user_id,
    faud.account_id,
    faud.date_id,
    faud.month_id,
    faud.is_active_today,
    faud.num_contacts,
    faud.num_messages,
    faud.num_contacts_replied
  FROM (
    SELECT *
    FROM import_fct_active_users_daily
    WHERE (
      date_id, etl_inserted) = (
      SELECT
        MAX(date_id) AS max_date_id,
        MAX(etl_inserted) AS max_etl_inserted
      FROM import_fct_active_users_daily
    )
  ) faud
)
SELECT * FROM final_cte