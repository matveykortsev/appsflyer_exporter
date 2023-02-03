from airflow import DAG
from datetime import datetime, timedelta

from airflow.providers.http.operators.http import SimpleHttpOperator
from custom_slack_operator import on_failure_slack_callback, sla_miss_callback
from recursive_render import recursive_render
from cohort_api import handle_cohort_on_day_api_response
from airflow.operators.python import PythonOperator
from clickhouse_operator import ClickHouseOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator, S3DeleteObjectsOperator


doc_md = '''
# on_day_user_acquisition_revenue DAG:

<p>Dag fully reload on-day revenue cohort, every day for last 60 days.</p>

### Main logic of cohort request and processing in `get_csv` task:

We are requesting data by endpoint using the request body. Endpoint differs only by Android/IOS `app_id`. Request body and app_id set as airflow Variable in Helm chart of Airflow [here](https://gitlab.infra.prql.dev/infra/helm/-/blob/master/airflow/prod_values.yml#L99-105).
Interval of data requested is installed via [Airflow Macroses](https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html).
```json
{"from": "{{ "{{" }} (data_interval_start - macros.timedelta(days=60)).strftime("%Y-%m-%d") {{ "}}" }}", "aggregation_type": "on_day", "partial_data": false, "to": "{{ "{{" }} data_interval_start.strftime("%Y-%m-%d") {{ "}}" }}", "min_cohort_size": 1, "filters": { "period": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9,10, 11, 12, 13, 14, 15, 16, 17, 18, 19,20, 21, 22, 23, 24, 25, 26, 27, 28, 29,30, 31, 32, 33, 34, 35, 36, 37, 38, 39,40, 41, 42, 43, 44, 45, 46, 47, 48, 49,50]},"groupings": ["pid","date","af_ad","af_adset","c"],"kpis": ["{{ "{{" }} params.table_type {{ "}}" }}"],"cohort_type": "user_acquisition"}
```
Most of the request parameters are static for on-day cohorts, except:
- `kpis` - parameterized from `params.table_type` = `revenue`
- `from`/`to` - parametrized dynamically via Airflow Macroses that's allow us to reload cohort data using [BackFill](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dag-run.html#backfill) or just rerun existing tasks.

After data was requested we processed the request's response via method [handle_cohort_on_day_api_response](https://gitlab.infra.prql.dev/bi/airflow-plugins/-/blob/main/cohort_api.py#L4-27) in the `response_filter` parameter of `SimpleHttpOperator`.
Method universal for all types of on-day cohorts so consists of all logic needed for processing on-day cohort response.
Next, we save API response in CSV format in the directory `/mnt/airflow_s3_data/data/` which is [mounted](https://gitlab.infra.prql.dev/infra/helm/-/blob/master/airflow/airflow-prod-dataset.yml) with s3 bucket `prql-airflow-prod`.
By the end, the method returns the saved file's name and header for later usage.

### Rest of process:

After data was loaded to `prql-airflow-prod` we copy the file by his name in target bucket `af-bis3prql-prod` in task `copy_to_target_bucket` and then deletes data from `prql-airflow-prod`.
Also, target table was truncated in task `drop_data`.
<p></p>

At the same time, we prepare CSV schema for inserting data in task `prepare_csv_schema`. We're using the header of CSV file for [mapping](https://gitlab.infra.prql.dev/bi/airflow-plugins/-/blob/main/cohort_api.py#L57-85) columns with their types and returning the schema of file via string for using in the insert query.
<p></p>

At the end of the process, we insert data in the target table using parameterized insert [query](https://gitlab.infra.prql.dev/bi/etl/-/blob/main/dags/sql/cohorts/on_day/insert.sql).

Here we insert data natively from s3 using the built-in ClickHouse function and specifying column names using the header of the file.
Prepared schema from task `prepare_csv_schema` used as the last parameter in s3 function for correct parsing of CSV.
<p></p>

**Note: here we must use the header of the saved file because AppsFlyer API may change the order of columns from time to time.**
'''

default_args = {
    'owner': 'infra',
    'depends_on_past': False,
    'start_date': datetime(2022, 11, 22, 0, 0),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': on_failure_slack_callback,
}


with DAG('on_day_user_acquisition_revenue',
         default_args=default_args,
         description='Daily reload revenue cohorts for last 60 days',
         max_active_runs=1,
         catchup=False,
         doc_md=doc_md,
         tags=['cohort', 'on_day', 'revenue', 'appsflyer'],
         schedule_interval='0 */8 * * *',
         sla_miss_callback=sla_miss_callback,
         template_searchpath=['/opt/airflow/dags/sql/cohorts/on_day'],
         user_defined_filters={'recursive_render': recursive_render}
         ) as dag:

    def prepare_schema(header, **kwargs):
        from cohort_api import revenue_mapper
        header_elems = header.split(',')
        elems_with_data_types = list(map(revenue_mapper, header_elems))
        return '\n'.join(elems_with_data_types)[:-1]

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    drop_part = ClickHouseOperator(
        task_id=f'drop_data',
        sql='drop.sql',
        clickhouse_conn_id='{{ conn.clickhouse }}',
        params={
            'table_type': 'revenue',
        }
    )

    for platform in ('apple', 'android'):

        get_csv = SimpleHttpOperator(
            task_id=f'get_{platform}_csv',
            http_conn_id='api_cohorts',
            endpoint=f'/api/cohorts/v1/data/app/{{{{ var.value.{platform.upper()}_APP_ID }}}}?format=csv',
            data='{{ var.value.on_day_user_acquisition_req_body | recursive_render(1) }}',
            headers={
                'Authorization': 'Bearer {{ conn.api_cohorts.password }}',
            },
            response_filter=lambda response, data_interval_start, params: handle_cohort_on_day_api_response(response, data_interval_start, params),
            params={
                'platform': platform,
                'table_type': 'revenue'
            }
        )

        copy_to_target_bucket = S3CopyObjectOperator(
            task_id=f'copy_to_target_bucket_{platform}',
            source_bucket_name='{{ var.value.AIRFLOW_S3_BUCKET }}',
            dest_bucket_name='af-bis3prql-prod',
            source_bucket_key=f'data/{{{{ ti.xcom_pull(task_ids="get_{platform}_csv")["file_name"] }}}}',
            dest_bucket_key=f'{{{{ ti.xcom_pull(task_ids="get_{platform}_csv")["file_name"] }}}}',
            aws_conn_id='s3_repo'
        )

        insert_to_target = ClickHouseOperator(
            task_id=f'insert_to_target_{platform}',
            sql='insert.sql',
            clickhouse_conn_id='{{ conn.clickhouse }}',
            params={
                'platform': platform,
                'table_type': 'revenue',
            }
        )

        prepare_csv_schema = PythonOperator(
            task_id=f'prepare_csv_schema_{platform}',
            python_callable=prepare_schema,
            op_kwargs={
                'header': f'{{{{ ti.xcom_pull(task_ids="get_{platform}_csv")["header"] }}}}'
            }
        )

        delete_temp_csv = S3DeleteObjectsOperator(
            task_id=f'delete_temp_csv_{platform}',
            bucket='{{ var.value.AIRFLOW_S3_BUCKET }}',
            keys=f'data/{{{{ ti.xcom_pull(task_ids="get_{platform}_csv")["file_name"] }}}}',
            aws_conn_id='s3_repo'
        )

        start >> get_csv
        get_csv >> copy_to_target_bucket >> [drop_part, delete_temp_csv] >> insert_to_target
        get_csv >> prepare_csv_schema >> insert_to_target
        insert_to_target >> end