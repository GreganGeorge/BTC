{{
    config(
        materialized='incremental',
        incremental_strategy='append'
    )
}}

WITH flattened_data AS (
    SELECT
    tx.HASH_KEY,
    tx.BLOCK_NUMBER,
    tx.BLOCK_TIMESTAMP,
    tx.IS_COINBASE,
    f.value:address::STRING AS output_address,
    f.value:value::FLOAT AS output_value
    FROM {{ ref('stg_btc') }} tx,
    LATERAL FLATTEN (input => outputs) f
    WHERE f.value:address is not null
    {% if is_incremental() %}
    AND tx.BLOCK_TIMESTAMP > (SELECT MAX(BLOCK_TIMESTAMP) FROM {{this}})
    {% endif %}
)

SELECT 
HASH_KEY,
BLOCK_NUMBER,
BLOCK_TIMESTAMP,
IS_COINBASE,
output_address,
output_value
FROM 
flattened_data
