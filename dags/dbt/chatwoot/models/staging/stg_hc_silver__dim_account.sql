{{
    config(
        alias='dim_account'
    )
}}
WITH source AS (
  SELECT * FROM {{ source('hc_silver', 'dim_account') }}
),
renamed AS (
  SELECT
    {{ adapter.quote("id") }},
    {{ adapter.quote("name") }}
  FROM source
)
SELECT * FROM renamed