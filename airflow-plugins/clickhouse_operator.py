from airflow.models import BaseOperator
from clickhouse_hook import ClickHouseHook


class ClickHouseOperator(BaseOperator):
    template_fields = ('sql', 'clickhouse_conn_id')
    template_ext = ('.sql')

    def __init__(self,
                 sql,
                 clickhouse_conn_id='clickhouse',
                 **kwargs):

        super().__init__(**kwargs)

        self.sql = sql
        self.clickhouse_conn_id = clickhouse_conn_id

    def get_hook(self):
        self.log.info(self.clickhouse_conn_id)
        return ClickHouseHook(self.clickhouse_conn_id)

    def execute(self, context):
        hook = self.get_hook()
        return [hook.execute_query(each.strip()) if each.strip() != '' else ''
                for each in self.sql.split(';')]