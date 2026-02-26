{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='HASH_KEY'
    )
}}

WITH CTE AS (
    SELECT *
    FROM {{ source('btc','btc') }}
)

SELECT *
FROM CTE 
{% if is_incremental() %}
where BLOCK_TIMESTAMP > (select max(BLOCK_TIMESTAMP) from {{this}})
{% endif %}