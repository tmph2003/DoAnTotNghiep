{{
    config(
        alias='dim_user'
    )
}}

WITH source AS (
  SELECT * FROM {{ source('hc_silver', 'dim_user') }}
),
renamed AS (
  SELECT
    {{ adapter.quote("id") }},
    {{ adapter.quote("name") }},
    {{ adapter.quote("display_name") }},
    {{ adapter.quote("email") }},
    {{ adapter.quote("type") }}

  FROM source
)
SELECT * FROM renamed