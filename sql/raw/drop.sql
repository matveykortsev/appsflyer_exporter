ALTER TABLE appsflyer.{{ params.table_type }} ON CLUSTER {{ var.value.CH_CLUSTER }}
    {%- if params.table_type == 'uninstalls' %}
DROP PARTITION '{{ (data_interval_end - macros.timedelta(days=12)) | ds }}',
    DROP PARTITION '{{ (data_interval_end - macros.timedelta(days=3)) | ds }}'
{%- elif params.table_type == 'installs' %}
    DROP PARTITION '{{ data_interval_start | ds }}'
{%- elif params.table_type == 'in_app_events' %}
    DROP PARTITION '{{ data_interval_start | ds }}'
{%- endif %}