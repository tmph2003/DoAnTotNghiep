WITH import_dim_ws_template_price AS (
  SELECT * FROM {{ source('hc_silver', 'dim_ws_template_price') }}
),

import_fct_message AS (
  SELECT * FROM {{ source('hc_silver', 'fct_message') }}
),

import_dim_ws_template AS (
  SELECT * FROM {{ source('hc_silver', 'dim_ws_template') }}
),

import_dim_contact AS (
  SELECT * FROM {{ source('hc_silver', 'dim_contact') }}
),

logical_formatted_phone_codes AS (
  SELECT
    marketing,
    utility,
    authentication,
    authentication_international,
    service,
    ARRAY[phone_code] AS phone_code_array -- noqa: CP02
  FROM import_dim_ws_template_price
),

logical_total_cost_country AS (
  SELECT
    a.ws_template_id,
    CAST(DATE_FORMAT(a.created_at, '%Y%m%d') AS INTEGER)        AS date_id,
    CAST(DATE_FORMAT(a.created_at, '%Y%m%d') AS INTEGER) / 100  AS month_id,
    COUNT(CASE WHEN a.status = 0 THEN 1 END)                    AS num_sent,
    COUNT(CASE WHEN a.status = 1 THEN 1 END)                    AS num_delivered,
    COUNT(CASE WHEN a.status = 2 THEN 1 END)                    AS num_read,
    COUNT(CASE WHEN a.ws_error_code IN (131053) THEN 1 END)     AS num_failed_due_sys,
    COUNT(CASE WHEN a.ws_error_code NOT IN (131053) THEN 1 END) AS num_failed_due_ws,
    ROUND(
      COALESCE(
        CASE
          WHEN b.category = 'MARKETING'
            THEN
              COUNT(CASE WHEN a.status IN (1, 2) THEN 1 END) * d.marketing
          WHEN b.category = 'UTILITY'
            THEN
              COUNT(CASE WHEN a.status IN (1, 2) THEN 1 END) * d.utility
          WHEN b.category = 'AUTHENTICATION'
            THEN
              COUNT(CASE WHEN a.status IN (1, 2) THEN 1 END) * d.authentication
          WHEN b.category = 'AUTHENTICATION-INTERNATIONAL'
            THEN
              COUNT(CASE WHEN a.status IN (1, 2) THEN 1 END) * d.authentication_international
          WHEN b.category = 'SERVICE' THEN
            COUNT(CASE WHEN a.status IN (1, 2) THEN 1 END) * d.service
        END, 0
      ), 5
    )                                                           AS total_cost
  FROM import_fct_message a
  INNER JOIN import_dim_ws_template b ON a.ws_template_id = b.id
  INNER JOIN import_dim_contact c ON a.contact_id = c.id
  INNER JOIN logical_formatted_phone_codes d ON ANY_MATCH(d.phone_code_array, code -> c.phone_number LIKE CONCAT(code, '%')) -- noqa: 
  WHERE a.ws_template_id IS NOT NULL
  GROUP BY
    a.ws_template_id, CAST(DATE_FORMAT(a.created_at, '%Y%m%d') AS INTEGER),
    b.category,
    d.marketing,
    d.utility,
    d.authentication,
    d.authentication_international,
    d.service
),

logical_metrics AS (
  SELECT
    ws_template_id,
    date_id,
    month_id,
    SUM(num_sent)           AS num_sent,
    SUM(num_delivered)      AS num_delivered,
    SUM(num_read)           AS num_read,
    SUM(num_failed_due_sys) AS num_failed_due_sys,
    SUM(num_failed_due_ws)  AS num_failed_due_ws,
    SUM(total_cost)         AS total_cost
  FROM logical_total_cost_country
  GROUP BY ws_template_id, date_id, month_id
),

final_cte AS (
  SELECT
    ws_template_id,
    date_id,
    month_id,
    num_sent,
    num_delivered,
    num_read,
    num_failed_due_sys,
    num_failed_due_ws,
    total_cost,
    CAST(CURRENT_TIMESTAMP + INTERVAL '7' HOUR AS TIMESTAMP(0)) AS etl_inserted
  FROM logical_metrics
)

SELECT * FROM final_cte
