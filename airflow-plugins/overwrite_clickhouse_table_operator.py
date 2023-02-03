from airflow.models import BaseOperator
from clickhouse_hook import ClickHouseHook


class OverwriteClickHouseOperator(BaseOperator):
    template_fields = ('sql', 'clickhouse_conn_id', 'sql_truncate')
    template_ext = ('.sql',)

    def __init__(self,
                 sql,
                 clickhouse_conn_id='clickhouse',
                 sql_truncate: str = None,
                 **kwargs):

        super().__init__(**kwargs)

        self.sql = sql
        self.clickhouse_conn_id = clickhouse_conn_id
        self.sql_truncate = sql_truncate
        self.result = []
        self.hook: ClickHouseHook = None

    def truncate_clickhouse_table(self):
        self.log.info(f'Truncating table {self.params["schema"]}.{self.params["table"]}')
        self.result.append(
            [
                self.hook.execute_query(each.strip()) if each.strip() != '' else ''
                for each in self.sql_truncate.split(';')
            ]
        )

    def execute(self, context):
        self.log.info(self.clickhouse_conn_id)
        self.hook = ClickHouseHook(self.clickhouse_conn_id)
        self.truncate_clickhouse_table()
        self.result.append(self.hook.execute_query(self.sql))
        return self.result