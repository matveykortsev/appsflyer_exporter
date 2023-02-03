
from airflow import DAG
from datetime import datetime, timedelta

from airflow.providers.http.operators.http import SimpleHttpOperator
from custom_slack_operator import on_failure_slack_callback, sla_miss_callback
from airflow.operators.python import PythonOperator
from clickhouse_operator import ClickHouseOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator, S3DeleteObjectsOperator
from raw_af_api import handle_raw_api_response

doc_md = '''
# appsflyer_raw_uninstalls DAG:

Dag daily reloads uninstalls from [Appsflyer raw API](https://support.appsflyer.com/hc/en-us/articles/360007530258-Using-Pull-API-raw-data)
<p></p>


**Creating two similar pipelines for android and apple platforms, organic and non-organic reports:**
```python
for platform in ('android', 'apple'):
    for organic_type in ('organic_', ''):
```
### get_android/apple_csv
We are requesting data by endpoint using the request body passed to `data` parameter.
Endpoint differs by Android/IOS `app_id` and `report_type` which is different for organic and non-organic report types.
Parameters `from` and `to` are generated dynamically via [Airflow Macroses](https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html).
We are requesting data during Appsflyer API [limitations](https://support.appsflyer.com/hc/en-us/articles/360007530258-Using-Pull-API-raw-data#traits-and-limitations):
- **For apple platform offset 12 days ago**
- **For android platform offset 3 days ago**

Offset set like this because of data delivery time in Appsflyer. Interval of data requested is set by 24 hours.

Each report for each platform and organic type processed via method [handle_raw_api_response](https://gitlab.infra.prql.dev/bi/airflow-plugins/-/blob/main/raw_api.py#L4-36) in the `response_filter` parameter of `SimpleHttpOperator`.
Method universal for all types of raw reports so consists of all logic needed for processing each of them.
In general method:
- Generates file name depending on report type, platform, organic type and report date, returns file name for later usage.
- Formats and replace the header of the file, and return the header for later usage.
- Saves API response in CSV format in the directory `/mnt/airflow_s3_data/data/` which is [mounted](https://gitlab.infra.prql.dev/infra/helm/-/blob/master/airflow/airflow-prod-dataset.yml) with s3 bucket `prql-airflow-prod`.
<p></p>

### copy_to_target_bucket
After data was loaded to `prql-airflow-prod` we copy the files by his names in target bucket `af-bis3prql-prod`.
### delete_temp_csv
Deleting the CSV's from `prql-airflow-prod`.
### drop_data_part
Drops partition depends on platform, generated dynamically via [Airflow Macroses](https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html) in [drop.sql](https://gitlab.infra.prql.dev/bi/etl/-/blob/main/dags/sql/raw/drop.sql)
- **For apple platform offset 12 days ago**
- **For android platform offset 3 days ago**
### prepare_csv_schema
We prepare CSV schema for inserting data. We're using the header of CSV file for [mapping](https://gitlab.infra.prql.dev/bi/airflow-plugins/-/blob/main/raw_api.py#L39-45) columns with their types and returning the schema of file via string for using in the insert query.
### insert_to_target_apple/android
Execute insert queries which is generated dynamically in [insert.sql](https://gitlab.infra.prql.dev/bi/etl/-/blob/main/dags/sql/raw/insert.sql) script, depends on os organic type and platform.
Schema generated in `prepare_csv_schema` task is used for correct parsing of CSV file and passed as s3 function last parameter.
'''


default_args = {
    'owner': 'infra',
    'depends_on_past': False,
    'start_date': datetime(2022, 11, 22, 0, 0),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': on_failure_slack_callback,
}


with DAG('appsflyer_raw_uninstalls',
         default_args=default_args,
         description='Upload raw uninstalls',
         max_active_runs=1,
         catchup=False,
         schedule='@daily',
         doc_md=doc_md,
         tags=['appsflyer', 'raw', 'uninstalls'],
         sla_miss_callback=sla_miss_callback,
         template_searchpath=['/opt/airflow/dags/sql/raw']
         ):

    def prepare_schema(header, **kwargs):
        from raw_af_api import uninstalls_mapper
        header_elems = header.split(',')
        elems_with_data_types = list(map(uninstalls_mapper, header_elems))
        return '\n'.join(elems_with_data_types)[:-1]


    drop_part = ClickHouseOperator(
        task_id=f'drop_data_part',
        sql='drop.sql',
        clickhouse_conn_id='{{ conn.clickhouse }}',
        params={
            'table_type': 'uninstalls',
        }
    )

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    for platform in ('android', 'apple'):
        for organic_type in ('organic_', ''):

            get_csv = SimpleHttpOperator(
                task_id=f'get_{platform}_{organic_type}csv',
                http_conn_id='raw_reports_api',
                method='GET',
                endpoint=f'/api/raw-data/export/app/{{{{ var.value.{platform.upper()}_APP_ID }}}}/{organic_type}uninstall_events_report/v5',
                data={
                    "from": "{{ (data_interval_end - macros.timedelta(days=12)) | ds }}" if platform == 'apple' else "{{ (data_interval_end - macros.timedelta(days=3)) | ds }}",
                    "to": "{{ (data_interval_end - macros.timedelta(days=12)) | ds }}" if platform == 'apple' else "{{ (data_interval_end - macros.timedelta(days=3)) | ds }}",
                    "maximum_rows": 1000000,
                    "additional_fields": 'deeplink_url',
                },
                response_filter=lambda response, data_interval_start, data_interval_end, params: handle_raw_api_response(response, data_interval_start, data_interval_end, params),
                params={
                    'platform': platform,
                    'report_type': 'uninstalls',
                    'organic_type': organic_type,
                },
                headers={
                    'Authorization': 'Bearer {{ conn.raw_reports_api.password }}',
                },
            )

            copy_to_target_bucket = S3CopyObjectOperator(
                task_id=f'copy_to_target_bucket_{organic_type}{platform}',
                source_bucket_name='{{ var.value.AIRFLOW_S3_BUCKET }}',
                dest_bucket_name='af-bis3prql-prod',
                source_bucket_key=f'data/{{{{ ti.xcom_pull(task_ids="get_{platform}_{organic_type}csv")["file_name"] }}}}',
                dest_bucket_key=f'{{{{ ti.xcom_pull(task_ids="get_{platform}_{organic_type}csv")["file_name"] }}}}',
                aws_conn_id='s3_repo'
            )

            delete_temp_csv = S3DeleteObjectsOperator(
                task_id=f'delete_temp_csv_{organic_type}{platform}',
                bucket='{{ var.value.AIRFLOW_S3_BUCKET }}',
                keys=f'data/{{{{ ti.xcom_pull(task_ids="get_{platform}_{organic_type}csv")["file_name"] }}}}',
                aws_conn_id='s3_repo'
            )

            prepare_csv_schema = PythonOperator(
                task_id=f'prepare_csv_schema_{organic_type}{platform}',
                python_callable=prepare_schema,
                op_kwargs={
                    'header': f'{{{{ ti.xcom_pull(task_ids="get_{platform}_{organic_type}csv")["header"] }}}}'
                }
            )

            insert_to_target = ClickHouseOperator(
                task_id=f'insert_to_target_{organic_type}{platform}',
                sql='insert.sql',
                clickhouse_conn_id='{{ conn.clickhouse }}',
                params={
                    'platform': platform,
                    'table_type': 'uninstalls',
                    'organic_type': organic_type
                }
            )

            start >> get_csv
            get_csv >> copy_to_target_bucket >> [delete_temp_csv, drop_part] >> insert_to_target
            get_csv >> prepare_csv_schema >> insert_to_target
            insert_to_target >> end