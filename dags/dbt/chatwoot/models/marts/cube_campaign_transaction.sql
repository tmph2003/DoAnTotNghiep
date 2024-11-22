{{
    config(
        materialized='incremental',
        partition_by='month(scheduled_at)',
        unique_key='campaign_id',
        incremental_strategy='merge',
        post_hook=[
            "USE {{ var('catalog_bi') }}.{{ this.schema }}; CALL system.execute('ALTER TABLE {{ this.schema }}.{{ this.name }} DELETE WHERE is_done = 0')",
            "INSERT INTO {{ var('catalog_bi') }}.{{ this.schema }}.{{ this.name }} SELECT * FROM {{ this }} WHERE etl_inserted = (SELECT max(etl_inserted) FROM {{ this }})"
        ]
    )
}}

WITH import_dim_campaign AS (
    SELECT *
    FROM {{ source('hc_silver', 'dim_campaign') }}
),

import_dim_inbox AS (
    SELECT *
    FROM {{ source('hc_silver', 'dim_inbox') }}
),

import_dim_account AS (
    SELECT *
    FROM {{ source('hc_silver', 'dim_account') }}
),

import_fct_campaign_agg as (
    SELECT *
    FROM {{ source('hc_silver', 'fct_campaign_agg') }}
),

import_cube_campaign_transaction as (
    SELECT *
    FROM {{ source('hc_gold', 'cube_campaign_transaction') }}
),

final_cte AS (
    SELECT
        fca.campaign_id,
        fca.inbox_id,
        fca.account_id,
        dc.title AS campaign_name,
        di.name AS inbox_name,
        da.name AS branch_name,
        fca.num_contacts,
        fca.num_contacts_sent,
        fca.num_contacts_delivered,
        fca.num_contacts_seen,
        fca.num_contacts_failed,
        fca.num_contacts_replied,
        array_join(fca.error_codes, ',') AS error_codes,
        fca.is_done,
        cast(fca.scheduled_at AS TIMESTAMP(0)) AS scheduled_at,
        cast(current_timestamp AT TIME ZONE 'Asia/Bangkok' AS TIMESTAMP(0)) AS etl_inserted
    FROM import_fct_campaign_agg AS fca
    LEFT JOIN import_cube_campaign_transaction cct ON
        cct.campaign_id = fca.campaign_id
    JOIN import_dim_campaign AS dc ON
        dc.id = fca.campaign_id
    JOIN import_dim_inbox AS di ON
        di.id = fca.inbox_id
    JOIN import_dim_account AS da ON
        da.id = fca.account_id
    WHERE cct.is_done IS NULL OR cct.is_done = 0
)
SELECT *
FROM final_cte
