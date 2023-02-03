from airflow import DAG, XComArg
from datetime import datetime, timedelta

from airflow.decorators import task
from airflow.providers.http.operators.http import SimpleHttpOperator
from custom_slack_operator import on_failure_slack_callback, sla_miss_callback
from recursive_render import recursive_render
from airflow.operators.python import PythonOperator
from clickhouse_operator import ClickHouseOperator
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator, S3DeleteObjectsOperator
from cohort_api import handle_cohort_cumulative_api_response

doc_md = '''
# cumulative_user_acquisition_uninstalls DAG:

<p>Dag reloads cumulative uninstalls cohort, every 9 hours 15 min for last 180 days.</p>

In appsflyer data for previous dates might have changed that's why we're requesting last 180 days.

### get_data_intervals
Intervals of data requested generates dynamically for every execution of DAG:

```python
data_range_end = data_interval_start.strftime("%Y-%m-%d")
data_range_start = (data_interval_start - timedelta(days=180)).replace(day=1).strftime("%Y-%m-%d")
logging.info(f'Start interval of data: {data_range_start}, end interval {data_range_end}')
dtrange = pd.date_range(start=data_range_start, end=data_range_end, freq='d')
months = pd.Series(dtrange .month)
starts, ends = months.ne(months.shift(1)), months.ne(months.shift(-1))
df = pd.DataFrame({'month_starting_date': dtrange[starts].strftime('%Y-%m-%d'),
                   'month_ending_date': dtrange[ends].strftime('%Y-%m-%d')})
list_of_intervals = df.values.tolist()
logging.info(f'List of data intervals are {list_of_intervals}')
```
**Algorithm subtract 180 days from current execution date and replace day of month for 1,
because we reload data by months, even if we have last day of the month we should request all month to avoid data inconsistency while dropping month partition.**
<p></p>
Next, algorithm generates list of intervals from start date till end date, every interval from the begging of month till end of month, except the last interval because we might be in a middle of month.

### get_drop_queries_list
Generates `DROP PARTITION` queries for every month from the date intervals list.
### prepare_req_body
Generates request body's for every date interval to pass in `get_android/apple_csv` task. Separate for android and apple platforms because of different KPI's names:
- `Start Trial Auth` for apple
- `start trial auth` for android

<p></p>

**Next we're creating two similar pipelines for android and apple platforms:**

### get_csv
We are requesting data by endpoint using the request body passed to `data` parameter. Endpoint differs only by Android/IOS `app_id`.
We [expand](https://airflow.apache.org/docs/apache-airflow/2.3.0/concepts/dynamic-task-mapping.html) the task by the request's body which was generated in `prepare_req_body` task.
After data was requested for each interval we processed the request's responses via method [handle_cohort_cumulative_api_response](https://gitlab.infra.prql.dev/bi/airflow-plugins/-/blob/main/cohort_api.py#L30-54) in the `response_filter` parameter of `SimpleHttpOperator`.
Method universal for all types of cumulative cohorts so consists of all logic needed for processing cumulative cohort response.
Next, we save API response in CSV format in the directory `/mnt/airflow_s3_data/data/` which is [mounted](https://gitlab.infra.prql.dev/infra/helm/-/blob/master/airflow/airflow-prod-dataset.yml) with s3 bucket `prql-airflow-prod`.
By the end, the method returns the saved file's name and header for later usage.
<p></p>

### generate_source_dest_pairs
Reducing all file names from `get_csv` tasks and generates source-destination pairs for each downloaded file in `get_csv` tasks.
### copy_to_target_bucket
After data was loaded to `prql-airflow-prod` we copy the files by his names in target bucket `af-bis3prql-prod`.
### delete_temp_csv
Deleting the CSV's from `prql-airflow-prod`.
### drop_data_part
Drops partition by months based on date intervals received in `get_data_intervals` and queries generated in `get_drop_queries_list`
### get_apple/android_queries
Reducing all file names and headers from `get_csv` tasks and generates insert queries:
```python
for elem in files_list:
    header = elem['header']
    file_name = elem['file_name']
    year = file_name.split('.')[0].split('_')[-2]
    month = file_name.split('.')[0].split('_')[-1]
    header_elems = header.split(',')
    elems_with_data_types = list(map(uninstalls_mapper, header_elems))
    csv_schema = '\n'.join(elems_with_data_types)[:-1]

    query = f"""
                insert into cohort_data.cumulative_user_acquisition_{table_type} ({header}, platform, year, month)
                select {header}, '{platform}', '{year}', '{month}'
                from s3(
                    '{bucket_name}{file_name}',
                    '{s3_conn.extra_dejson['aws_access_key_id']}',
                    '{s3_conn.extra_dejson['aws_secret_access_key']}',
                    'CSVWithNames',
                    '{csv_schema}')
                """
    insert_queies_list.append(query)
```
**Note: here we must use the header of the saved file because AppsFlyer API may change the order of columns from time to time.**
### insert_to_target_apple/android
Execute insert queries which is generated in `get_apple/android_queries` tasks.
'''

default_args = {
    'owner': 'infra',
    'depends_on_past': False,
    'start_date': datetime(2022, 11, 22, 0, 0),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': on_failure_slack_callback,
}


with DAG('cumulative_user_acquisition_start_trial_auth',
         default_args=default_args,
         description='Every 8 hours reloads cumulative cohorts for last 6 months',
         max_active_runs=1,
         catchup=False,
         doc_md=doc_md,
         tags=['cohort', 'cumulative', 'start_trial_auth', 'appsflyer'],
         schedule_interval='30 */9 * * *',
         sla_miss_callback=sla_miss_callback,
         template_searchpath=['/opt/airflow/dags/sql/cohorts/cumulative'],
         render_template_as_native_obj=True,
         user_defined_filters={'recursive_render': recursive_render}
         ) as dag:

    def get_data_intervals(data_interval_start, **kwargs):
        import logging
        import pandas as pd
        data_range_end = data_interval_start.strftime("%Y-%m-%d")
        data_range_start = (data_interval_start - timedelta(days=180)).replace(day=1).strftime("%Y-%m-%d")
        logging.info(f'Start interval of data: {data_range_start}, end interval {data_range_end}')
        dtrange = pd.date_range(start=data_range_start, end=data_range_end, freq='d')
        months = pd.Series(dtrange .month)
        starts, ends = months.ne(months.shift(1)), months.ne(months.shift(-1))
        df = pd.DataFrame({'month_starting_date': dtrange[starts].strftime('%Y-%m-%d'),
                           'month_ending_date': dtrange[ends].strftime('%Y-%m-%d')})
        list_of_intervals = df.values.tolist()
        logging.info(f'List of data intervals are {list_of_intervals}')
        return list_of_intervals


    def prepare_req_body(interval_list, table_type):
        import logging
        import json
        result_req_list = []
        for interval in interval_list:
            date_from, date_to = interval
            logging.info(f'Date from: {date_from}, date to {date_to}')
            req_body = {"from": date_from, "aggregation_type": "cumulative", "partial_data": True, "to": date_to, "min_cohort_size": 1, "filters": { "period": [0,1,3,10,17,24,31,180]},"groupings": ["pid","geo","af_ad","af_adset","c"],"kpis": [str(table_type)],"cohort_type": "user_acquisition"}
            result_req_list.append(json.dumps(req_body))
        logging.info(result_req_list)
        return result_req_list


    get_intervals = PythonOperator(
        task_id='get_data_intervals',
        python_callable=get_data_intervals,
    )

    @task(task_id=f'get_drop_queries_list')
    def get_queries_list(data_intervals, table_type):
        from airflow.models import Variable
        cluster_name = Variable.get('CH_CLUSTER')
        queries_list = []
        for elem in data_intervals:
            start_interval = datetime.strptime(elem[0], "%Y-%m-%d")
            month = start_interval.month
            year = start_interval.year
            quire = f"ALTER TABLE cohort_data.cumulative_user_acquisition_{table_type} ON CLUSTER {cluster_name} DROP PARTITION ('{year}','{month}')"
            queries_list.append(quire)
        return queries_list


    queries_list = get_queries_list(
        XComArg(get_intervals),
        table_type='start_trial_auth'
    )

    drop_part = ClickHouseOperator.partial(
        task_id=f'drop_data_part',
        clickhouse_conn_id='clickhouse',
    ).expand(sql=queries_list)

    for platform in ('apple', 'android'):

        prepare_body = PythonOperator(
            task_id=f'prepare_{platform}_req_body',
            python_callable=prepare_req_body,
            op_kwargs={
                'interval_list': '{{ ti.xcom_pull(task_ids="get_data_intervals") }}',
                'table_type': 'start trial auth' if platform == 'android' else 'Start Trial Auth'
            }
        )

        get_csv = SimpleHttpOperator.partial(
            task_id=f'get_{platform}_csv',
            http_conn_id='api_cohorts',
            endpoint=f'/api/cohorts/v1/data/app/{{{{ var.value.{platform.upper()}_APP_ID }}}}?format=csv',
            headers={
                'Authorization': 'Bearer {{ conn.api_cohorts.password }}',
            },
            response_filter=lambda response, params, task: handle_cohort_cumulative_api_response(response, params, task),
            params={
                'platform': platform,
                'table_type': 'start trial auth' if platform == 'android' else 'Start Trial Auth'
            }
        ).expand(
            data=XComArg(prepare_body),
        )

        @task(task_id=f'generate_{platform}_source_dest_pairs')
        def generate_source_dest_pairs(files_list):
            source_dest_pairs = []
            for file in files_list:
                file_name = file["file_name"]
                source_dest_pairs.append(
                    {
                        'source_bucket_key': f'data/{file_name}',
                        'dest_bucket_key': file_name
                    }
                )
            return source_dest_pairs

        source_dest_pairs = generate_source_dest_pairs(
            XComArg(get_csv)
        )

        copy_to_target_bucket = S3CopyObjectOperator.partial(
            task_id=f'copy_to_target_bucket_{platform}',
            source_bucket_name='{{ var.value.AIRFLOW_S3_BUCKET }}',
            dest_bucket_name='af-bis3prql-prod',
            aws_conn_id='s3_repo'
        ).expand_kwargs(source_dest_pairs)

        @task(task_id=f'get_{platform}_file_names')
        def get_file_names(files_list):
            file_names = []
            for elem in files_list:
                file_name = elem['file_name']
                file_names.append(f'data/{file_name}')
            return file_names

        file_names = get_file_names(
            XComArg(get_csv)
        )

        delete_temp_csv = S3DeleteObjectsOperator.partial(
            task_id=f'delete_temp_csv_{platform}',
            bucket='{{ var.value.AIRFLOW_S3_BUCKET }}',
            aws_conn_id='s3_repo'
        ).expand(keys=file_names)

        @task(task_id=f'get_{platform}_insert_queries')
        def get_insert_queries(files_list, platform, table_type):
            import logging
            from cohort_api import start_trial_auth_mapper
            from airflow.hooks.base import BaseHook
            from airflow.models import Variable
            bucket_name = Variable.get('AF_BUCKET')
            s3_conn = BaseHook.get_connection('s3_repo')
            insert_queies_list = []
            for elem in files_list:
                header = elem['header']
                file_name = elem['file_name']
                year = file_name.split('.')[0].split('_')[-2]
                month = file_name.split('.')[0].split('_')[-1]
                header_elems = header.split(',')
                elems_with_data_types = list(map(start_trial_auth_mapper, header_elems))
                csv_schema = '\n'.join(elems_with_data_types)[:-1]

                query = f"""
                insert into cohort_data.cumulative_user_acquisition_{table_type} ({header}, platform, year, month)
                select {header}, '{platform}', '{year}', '{month}'
                from s3(
                    '{bucket_name}{file_name}',
                    '{s3_conn.extra_dejson['aws_access_key_id']}',
                    '{s3_conn.extra_dejson['aws_secret_access_key']}',
                    'CSVWithNames',
                    '{csv_schema}')
                """
                logging.info(query)
                insert_queies_list.append(query)
            return insert_queies_list

        insert_queries_list = get_insert_queries(
            XComArg(get_csv),
            platform=platform,
            table_type='start_trial_auth'
        )

        insert_to_target = ClickHouseOperator.partial(
            task_id=f'insert_to_target_{platform}',
            clickhouse_conn_id='clickhouse',
        ).expand(sql=insert_queries_list)

        get_intervals >> prepare_body >> get_csv >> copy_to_target_bucket >> [delete_temp_csv, drop_part] >> insert_to_target