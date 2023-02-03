insert into appsflyer.{{ params.table_type }}
(
{%- if params.organic_type == 'organic_' %}
    {%- if params.platform == 'apple' %}
        {{ ti.xcom_pull(task_ids='get_apple_organic_csv')['header'] }}
    {% elif params.platform == 'android' %}
        {{ ti.xcom_pull(task_ids='get_android_organic_csv')['header'] }}
    {%- endif %}
{%- elif params.organic_type == '' %}
    {%- if params.platform == 'apple' %}
        {{ ti.xcom_pull(task_ids='get_apple_csv')['header'] }}
    {% elif params.platform == 'android' %}
        {{ ti.xcom_pull(task_ids='get_android_csv')['header'] }}
    {%- endif %}
{%- endif %}
    , pdate)
select
{%- if params.organic_type == 'organic_' %}
    {%- if params.platform == 'apple' %}
        {{ ti.xcom_pull(task_ids='get_apple_organic_csv')['header'] }}
    {% elif params.platform == 'android' %}
        {{ ti.xcom_pull(task_ids='get_android_organic_csv')['header'] }}
    {%- endif %}
{%- elif params.organic_type == '' %}
    {%- if params.platform == 'apple' %}
        {{ ti.xcom_pull(task_ids='get_apple_csv')['header'] }}
    {% elif params.platform == 'android' %}
        {{ ti.xcom_pull(task_ids='get_android_csv')['header'] }}
    {%- endif %}
{%- endif %}
  , toDate(event_time) as pdate
from s3(
    '{{ var.value.AF_BUCKET }}{%- if params.organic_type == "organic_" %}{%- if params.platform == "apple" %}{{ ti.xcom_pull(task_ids="get_apple_organic_csv")["file_name"] }}{% elif params.platform == "android" %}{{ ti.xcom_pull(task_ids="get_android_organic_csv")["file_name"] }}{%- endif %}{%- elif params.organic_type == "" %}{%- if params.platform == "apple" %}{{ ti.xcom_pull(task_ids="get_apple_csv")["file_name"] }}{% elif params.platform == "android" %}{{ ti.xcom_pull(task_ids="get_android_csv")["file_name"] }}{%- endif %}{%- endif %}',
    '{{ conn.s3_repo.extra_dejson.aws_access_key_id }}',
    '{{ conn.s3_repo.extra_dejson.aws_secret_access_key }}',
    'CSVWithNames',
    '{%- if params.organic_type == "organic_" %}{%- if params.platform == "apple" %} {{ ti.xcom_pull(task_ids="prepare_csv_schema_organic_apple") }} {% elif params.platform == "android" %} {{ ti.xcom_pull(task_ids="prepare_csv_schema_organic_android") }} {%- endif %}{%- elif params.organic_type == "" %} {%- if params.platform == "apple" %} {{ ti.xcom_pull(task_ids="prepare_csv_schema_apple") }} {% elif params.platform == "android" %} {{ ti.xcom_pull(task_ids="prepare_csv_schema_android") }}{%- endif %}{%- endif %}')