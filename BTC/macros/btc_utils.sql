{% macro convert_to_usd(column_name)%}

{{column_name}} * (SELECT close as latest_price from 
    {{ ref('bitcoin_historical_data') }}
    WHERE DATEADD(day,1,TO_DATE(timestamp))=current_date())

{% endmacro %}