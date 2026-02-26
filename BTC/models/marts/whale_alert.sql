WITH WHALES AS (
    SELECT
    output_address,
    sum(output_value) AS total_sent,
    count(*) as tx_count
    FROM
    {{ ref('stg_btc_transactions') }}
    WHERE output_value > 10
    GROUP BY output_address
    ORDER BY total_sent desc
)

SELECT 
w.output_address,
w.total_sent,
w.tx_count,
{{ convert_to_usd('w.total_sent') }} as total_send
FROM WHALES w
ORDER BY total_send desc
