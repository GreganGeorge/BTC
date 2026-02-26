{% macro log_dbt_run() %}

{% set query %}
    INSERT INTO BTC.AUDIT.DBT_AUDIT (
        invocation_id,
        run_started_at,
        dbt_command,
        target_profile,
        target_name,
        target_user,
        dbt_version 
    ) VALUES (
        '{{invocation_id}}',
        '{{run_started_at}}',
        '{{invocation_args_dict["invocation_command"]}}',
        '{{target.profile_name}}',
        '{{target.name}}',
        '{{target.user}}',
        '{{dbt_version}}' 
    );
commit;
{% endset %}

{% do run_query(query) %}

{% endmacro %}