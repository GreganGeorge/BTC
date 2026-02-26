{% macro log_row_count() %}

{% if execute %}
    {% set query %}
        select count(*) from {{this}}
    {% endset %}
    {% set results = run_query(query) %}
    {% set row_count = results.columns[0][0] %}
    {{ log("Model " ~ this.name ~ " Finished. Row count: " ~ row_count, info=True) }}
{% endif %}

{% endmacro %}