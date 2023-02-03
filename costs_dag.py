from datetime import datetime, timedelta

from airflow import DAG
from airflow import XComArg
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.task_group import TaskGroup
from clickhouse_operator import ClickHouseOperator
from custom_slack_operator import on_failure_slack_callback, sla_miss_callback
from prometheus_push_gateway_operator import PrometheusPushGateway


doc_md = """"
# Costs dag

Appsflyer drops batches in s3 bucket by their own schedule every 6 hours:

1. Batch 1: 02:00AM UTC
2. Batch 2: 08:00AM UTC
3. Batch 3: 02:00PM UTC
4. Batch 4: 08:00PM UTC
5. All Cost Geo Batch: 02:00PM UTC

**<p>Note: Each time data is written, it includes data for the current day and the previous 6 days (referred to as 7 days in this article), as well as days 14, 29, and 88.
By that reason in case of DAG/task failure you should restart DAG Runs from failed one till last one, from first batch till fourth one.
Otherwise, you might get inconsistency data.</p>**
More about that in [documentation](https://support.appsflyer.com/hc/en-us/articles/360008849238-Cost-ETL-granular-campaign-cost-in-a-bucket?query=costs#directory-and-filename-structure).

To keep batch order for every task added `priority_weight = - batch_number` and pool `costs_pool`

## Tasks:

* `check_channel_batch_<1/2/3/4/all_cost_geo>` - checks for existing of the specified batch in the s3 bucket.
  Checks if file _SUCCESS is presented in the bucket folder for a specified date and specified batch. If batch exists continue to the next task,
  otherwise wait 21600 seconds (6 hours) or 43200 seconds (12 hours) for all_cost_geo batch. Timeout is 64800 for every batch.
* `insert_channel_<1/2/3/4/all_cost_geo>` - inserts batch to service table `service.appsflyer_<cost_batch_<1/2/3/4/>|all_cost_batch>` directly from s3 bucket.
* `get_partitions_task` - get distinct dates from service table for each batch.
* `drop_parttions` - drops partition in source table for every distinct date in batch.
* `insert_<channel/all_cost_to>_target_table` - insert batch from service table to source tables `appsflyer.channel` or `appsflyer.all_cost_geo`
* `truncate_service_table` - truncates service table
* `trigger_cost_mart_<1/2/3/4>` - after every batch was inserted into source table we should update `bi_mart.cost_by_channel` which is depends on `appsflyer.channel` table.
"""

default_args = {
    'owner': 'infra',
    'depends_on_past': False,
    'start_date': datetime(2022, 7, 22, 0, 0),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': on_failure_slack_callback,
    'weight_rule': 'absolute'
}


with DAG('costs',
         default_args=default_args,
         description='Daily load costs',
         max_active_runs=1,
         catchup=False,
         doc_md=doc_md,
         tags=['costs', 'appsflyer.channel', 'appsflyer'],
         schedule_interval='00 2 * * *',
         template_searchpath=['/opt/airflow/dags/sql/costs'],
         sla_miss_callback=sla_miss_callback
         ) as dag:

    def get_partitions_task(clickhouse_conn_id, **context):
        from clickhouse_hook import ClickHouseHook
        hook = ClickHouseHook(clickhouse_conn_id)
        partitions_list = hook.execute_query(f'SELECT DISTINCT "date" FROM {context["params"]["service_schema"]}.{context["params"]["service_table"]}')[0][-1].split('\n')[:-1]
        result = list(map(lambda x: [x], partitions_list))
        return result

    def drop_partitions_task(partition, clickhouse_conn_id, cluster, **context):
        from clickhouse_hook import ClickHouseHook
        hook = ClickHouseHook(clickhouse_conn_id)
        hook.execute_query(f"ALTER TABLE {context['params']['schema']}.{context['params']['table']} ON CLUSTER {cluster} DROP PARTITION '{partition}'")


    prometheus_push_gateway_tasks = PrometheusPushGateway(
        task_id=f'prometheus_push_gateway_channel_task',
        clickhouse_conn_id='{{ conn.clickhouse }}',
        metric_group={'job': 'today_costs'},
        metric_name='costs_count',
        sql=f'SELECT count(*) FROM appsflyer.channel',
        table='appsflyer.channel',
        prometheus_url='{{ conn.PROMETHEUS.get_uri() }}',
        sla=timedelta(hours=24)
    )

    for batch in [1, 2, 3, 4]:
        check_channel_batch = S3KeySensor(
            task_id=f'check_channel_batch_{batch}',
            poke_interval=21600,
            timeout=64800,
            priority_weight=-batch,
            pool='costs_pool',
            aws_conn_id='s3',
            mode='reschedule',
            soft_fail=False,
            bucket_key=f'{{{{ var.value.COSTS_S3_SOURCE }}}}dt={{{{ data_interval_end | ds }}}}/b={batch}/channel/_SUCCESS'
        )
        with TaskGroup(group_id=f'process_batch_{batch}') as process_batch:
            insert_channel_from_s3_to_service_table_task = ClickHouseOperator(
                task_id=f'insert_channel_{batch}_from_s3_to_service_table',
                sql='insert_from_s3.sql',
                clickhouse_conn_id='{{ conn.clickhouse }}',
                pool='costs_pool',
                priority_weight=-batch,
                task_group=process_batch,
                params={
                    'service_schema': 'service',
                    'service_table': f'appsflyer_cost_batch_{batch}',
                    'bucket_address': 'https://af-xpend-cost-etl-acc-l3mblwzn-prql-xsdfadd-stg.s3.eu-central-1.amazonaws.com/cost_etl/v1/',
                    'batch': batch,
                    'table_path': 'channel',
                }
            )

            partitions_channel = PythonOperator(
                task_id=f'get_partitions_task_{batch}',
                python_callable=get_partitions_task,
                priority_weight=-batch,
                pool='costs_pool',
                task_group=process_batch,
                params={
                    'service_schema': 'service',
                    'service_table': f'appsflyer_cost_batch_{batch}',
                },
                op_kwargs={
                    'clickhouse_conn_id': '{{ conn.clickhouse }}'
                }
            )

            drop_partitions_channel = PythonOperator.partial(
                task_id=f'drop_parttions_{batch}',
                python_callable=drop_partitions_task,
                priority_weight=-batch,
                pool='costs_pool',
                task_group=process_batch,
                params={
                    'schema': 'appsflyer',
                    'table': 'channel'
                },
                op_kwargs={
                    'clickhouse_conn_id': '{{ conn.clickhouse }}',
                    'cluster': '{{ var.value.CH_CLUSTER }}'
                }
            ).expand(
                op_args=XComArg(partitions_channel)
            )

            insert_channel_to_target_table_task = ClickHouseOperator(
                task_id=f'insert_channel_{batch}_to_target_table',
                sql='insert_to_target.sql',
                priority_weight=-batch,
                pool='costs_pool',
                task_group=process_batch,
                clickhouse_conn_id='{{ conn.clickhouse }}',
                params={
                    'service_schema': 'service',
                    'service_table': f'appsflyer_cost_batch_{batch}',
                    'schema': 'appsflyer',
                    'table': 'channel'
                }
            )

            truncate_service_table_task_channel = ClickHouseOperator(
                task_id=f'truncate_service_table_{batch}',
                sql='TRUNCATE TABLE IF EXISTS {{ params.service_schema }}.{{params.service_table}} ON CLUSTER {{ var.value.CH_CLUSTER }}',
                clickhouse_conn_id='{{ conn.clickhouse }}',
                priority_weight=-batch,
                pool='costs_pool',
                task_group=process_batch,
                params={
                    'service_schema': 'service',
                    'service_table': f'appsflyer_cost_batch_{batch}',
                }
            )
            insert_channel_from_s3_to_service_table_task >> partitions_channel >> drop_partitions_channel
            drop_partitions_channel >> insert_channel_to_target_table_task >> truncate_service_table_task_channel

        trigger_reload_cost_mart = TriggerDagRunOperator(
            task_id=f'trigger_cost_mart_dag_{batch}',
            trigger_dag_id='cost_mart',
            priority_weight=-batch,
            pool='costs_pool'
        )

        check_channel_batch >> process_batch >> trigger_reload_cost_mart >> prometheus_push_gateway_tasks


    check_all_cost_geo_sensor = S3KeySensor(
        task_id='check_all_cost_geo',
        poke_interval=43200,
        timeout=64800,
        aws_conn_id='s3',
        pool='costs_pool',
        priority_weight=10000,
        mode='reschedule',
        soft_fail=False,
        bucket_key='{{ var.value.COSTS_S3_SOURCE }}dt={{ data_interval_end | ds }}/b=3/all-cost/geo/_SUCCESS'
    )

    with TaskGroup(group_id='process_all_cost_batch') as process_all_cost_batch:
        insert_all_cost_from_s3_to_service_table_task = ClickHouseOperator(
            task_id='insert_all_cost_from_s3_to_service_table',
            sql='insert_from_s3.sql',
            clickhouse_conn_id='{{ conn.clickhouse }}',
            priority_weight=10000,
            pool='costs_pool',
            task_group=process_all_cost_batch,
            params={
                'service_schema': 'service',
                'service_table': 'appsflyer_all_cost_batch',
                'bucket_address': 'https://af-xpend-cost-etl-acc-l3mblwzn-prql-xsdfadd-stg.s3.eu-central-1.amazonaws.com/cost_etl/v1/',
                'batch': 3,
                'table_path': 'all-cost/geo',
            }
        )

        insert_all_cost_to_target_table_task = ClickHouseOperator(
            task_id='insert_all_cost_to_target_table',
            sql='insert_to_target.sql',
            clickhouse_conn_id='{{ conn.clickhouse }}',
            priority_weight=10000,
            pool='costs_pool',
            task_group=process_all_cost_batch,
            params={
                'service_schema': 'service',
                'service_table': 'appsflyer_all_cost_batch',
                'schema': 'appsflyer',
                'table': 'all_cost_geo'
            }
        )

        truncate_service_table_task = ClickHouseOperator(
            task_id='truncate_service_table',
            sql='TRUNCATE TABLE IF EXISTS {{ params.service_schema }}.{{params.service_table}} ON CLUSTER {{ var.value.CH_CLUSTER }}',
            clickhouse_conn_id='{{ conn.clickhouse }}',
            priority_weight=10000,
            pool='costs_pool',
            task_group=process_all_cost_batch,
            params={
                'service_schema': 'service',
                'service_table': 'appsflyer_all_cost_batch',
            }
        )

        partitions = PythonOperator(
            task_id='get_partitions_task',
            python_callable=get_partitions_task,
            priority_weight=10000,
            pool='costs_pool',
            task_group=process_all_cost_batch,
            params={
                'service_schema': 'service',
                'service_table': 'appsflyer_all_cost_batch'
            },
            op_kwargs={
                'clickhouse_conn_id': '{{ conn.clickhouse }}'
            }
        )

        drop_partitions = PythonOperator.partial(
            task_id='drop_parttions',
            python_callable=drop_partitions_task,
            priority_weight=10000,
            pool='costs_pool',
            task_group=process_all_cost_batch,
            params={
                'schema': 'appsflyer',
                'table': 'all_cost_geo'
            },
            op_kwargs={
                'clickhouse_conn_id': '{{ conn.clickhouse }}',
                'cluster': '{{ var.value.CH_CLUSTER }}'
            }
        ).expand(
            op_args=XComArg(partitions)
        )

        insert_all_cost_from_s3_to_service_table_task >> partitions
        partitions >> drop_partitions >> insert_all_cost_to_target_table_task >> truncate_service_table_task

    check_all_cost_geo_sensor >> process_all_cost_batch >> prometheus_push_gateway_tasks