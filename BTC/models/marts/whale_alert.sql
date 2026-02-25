SELECT
output_address,
sum(output_value) AS total_sent,
count(*) as tx_count
FROM
{{ ref('stg_btc_transactions') }}
WHERE output_value > 10
GROUP BY output_address
ORDER BY total_sent desc