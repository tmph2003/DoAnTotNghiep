{{
    config(
        materialized='table',
        partition_by='month(partition_date)',
        unique_key=['account_id', 'partition_date']
    )
}}

WITH import_dim_account AS (
  SELECT *
  FROM {{ source('hc_silver', 'dim_account') }}
),

import_fct_account_agg AS (
  SELECT *
  FROM {{ source('hc_silver', 'fct_account_agg') }}
  WHERE partition_date = DATE(CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Bangkok')
),

final_cte AS (
  SELECT
    faa.account_id,
    da.name AS branch_name,
    faa.num_active_users,
    faa.num_users,
    faa.num_incoming_messages,
    faa.num_failed_outcoming_messages,
    faa.num_outcoming_messages,
    faa.num_inboxes,
    faa.num_new_contacts,
    faa.num_active_contacts,
    faa.num_cared_contacts,
    faa.num_contacts,
    faa.num_active_conversations,
    faa.num_conversations,
    faa.partition_date
  FROM import_fct_account_agg AS faa
  INNER JOIN
    import_dim_account AS da ON
  faa.account_id = da.id
)
SELECT * FROM final_cte
