insert into cohort_data.on_day_user_acquisition_{{ params.table_type }}
({%- if params.platform == 'apple' %} {{ ti.xcom_pull(task_ids='get_apple_csv')['header'] }} {% elif params.platform == 'android' %} {{ ti.xcom_pull(task_ids='get_android_csv')['header'] }} {%- endif %}, platform)
select {%- if params.platform == 'apple' %} {{ ti.xcom_pull(task_ids='get_apple_csv')['header'] }} {% elif params.platform == 'android' %} {{ ti.xcom_pull(task_ids='get_android_csv')['header'] }} {%- endif %}, '{{ params.platform }}'
from s3(
    '{{ var.value.AF_BUCKET }}{%- if params.platform == "apple" %}{{ ti.xcom_pull(task_ids="get_apple_csv")["file_name"] }}{% elif params.platform == "android" %}{{ ti.xcom_pull(task_ids="get_android_csv")["file_name"] }}{%- endif %}',
    '{{ conn.s3_repo.extra_dejson.aws_access_key_id }}',
    '{{ conn.s3_repo.extra_dejson.aws_secret_access_key }}',
    'CSVWithNames',
    '{%- if params.platform == "apple" %} {{ ti.xcom_pull(task_ids="prepare_csv_schema_apple") }} {% elif params.platform == "android" %} {{ ti.xcom_pull(task_ids="prepare_csv_schema_android") }} {%- endif %}')