from datetime import datetime, timedelta

from airflow import DAG
from custom_slack_operator import on_failure_slack_callback, sla_miss_callback
from overwrite_clickhouse_table_operator import OverwriteClickHouseOperator
from prometheus_push_gateway_operator import PrometheusPushGateway

default_args = {
    'owner': 'infra',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1, 0, 0),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': on_failure_slack_callback,
    'sla_miss_callback': sla_miss_callback
}

doc_md = """
# cost_mart DAG:

Dag have not his own schedule and triggered by `costs` DAG after each batch of costs was uploaded.
Source table: `appsflyer.channel`
Target: `bi_mart.cost_by_channel`
"""


with DAG('cost_mart',
         default_args=default_args,
         description='Daily reload cost_mart',
         max_active_runs=1,
         catchup=False,
         doc_md=doc_md,
         tags=['appsflyer', 'appsflyer.channel', 'costs'],
         schedule_interval=None,
         template_searchpath=['/opt/airflow/dags/sql/cost_mart'],
         sla_miss_callback=sla_miss_callback
         ) as dag:

    overwrite_click_tables_tasks = OverwriteClickHouseOperator(
        task_id=f'overwrite_ch_cost_mart_task',
        clickhouse_conn_id='{{ conn.clickhouse }}',
        sql='cost_mart.sql',
        sql_truncate='TRUNCATE TABLE IF EXISTS {{ params.schema }}.{{ params.table }} ON CLUSTER {{ var.value.CH_CLUSTER }}',
        params={
            'schema': 'bi_mart',
            'table': 'cost_by_channel',
            'source_schema': 'appsflyer',
            'source_table': 'channel',
            'cluster': 'click'
        }
    )

    prometheus_push_gateway_tasks = PrometheusPushGateway(
        task_id=f'prometheus_push_gateway_cost_mart_task',
        clickhouse_conn_id='{{ conn.clickhouse }}',
        metric_group={'job': 'cost_mart_etl'},
        metric_name='cost_mart_count',
        sql='SELECT count(*) FROM bi_mart.cost_by_channel',
        table='bi_mart.cost_by_channel',
        prometheus_url='{{ conn.PROMETHEUS.get_uri() }}',
        sla=timedelta(minutes=10)
    )

overwrite_click_tables_tasks >> prometheus_push_gateway_tasks
