import json
import requests

from datetime import datetime as dt

from airflow.providers.http.hooks.http import HttpHook

from airflow.exceptions import AirflowException


class ClickHouseHook(HttpHook):

    def parse_response(self, response):
        self.log.info(f"status code: {response.status_code}, response text: {response.text}")

        if response.text == '':
            return []
        else:
            parse_functions = {
                'String': lambda x: (x,),
                'Date': lambda x: (dt.strptime(x, '%Y-%m-%d'),),
            }

            result = []
            try:
                if response.status_code != 200 or 'DB::Exception' in response.text:
                    raise AirflowException(response.text)
                result.append((response.status_code, response.text))
            except ValueError:
                try:
                    response_json = response.json()
                except json.decoder.JSONDecodeError:
                    return []
                if 'meta' in response_json:
                    for value in response_json['data']:
                        value_type = response_json['meta'][0]['type']
                        result.append(
                            parse_functions.get(
                                value_type,
                                lambda x: tuple(x)
                            )(value[0]))

            self.log.info(result)

            return result

    def __init__(self, clickhouse_conn_id, timeout=30000):
        super().__init__()
        self.timeout = timeout
        self.clickhouse_conn_id = clickhouse_conn_id

        click_conn = self.get_connection(self.clickhouse_conn_id)

        self.host = f"http://{click_conn.host}:{click_conn.port}/?receive_timeout=30000&send_timeout=30000&http_send_timeout=30000&http_receive_timeout=30000"

        self.request_params = {
            'user': click_conn.login,
            'password': click_conn.password,
            'receive_timeout': 30000,
            'send_timeout': 300000,
            'http_send_timeout': 30000,
            'http_receive_timeout': 30000,
        }

    def execute_query(self, sql, **kwargs):
        sql = sql.encode('utf-8')
        response = requests.post(
            self.host,
            timeout=self.timeout,
            params={
                **self.request_params,
            },
            data=sql
        )
        self.log.info(sql)
        return self.parse_response(response)