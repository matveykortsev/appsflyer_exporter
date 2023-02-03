from airflow.hooks.base import BaseHook
from airflow.providers.slack.operators.slack_webhook \
    import SlackWebhookOperator

INFRA_SLACK_CONN_ID = 'slack'
ANALYTICS_SLACK_CONN_ID = 'slack_analytics'


class SlackOperator(SlackWebhookOperator):

    def __init__(self,
                 *args,
                 **kwargs):
        super(SlackOperator, self).__init__(*args, **kwargs)


def on_failure_slack_callback(context):
    webhook_token = BaseHook.get_connection(INFRA_SLACK_CONN_ID).password
    msg = f"""
            :red_circle: Task Failed. 
            *Task*: {context.get('task_instance').task_id}  
            *Dag*: {context.get('task_instance').dag_id} 
            *Execution Time*: {context.get('execution_date')}  
            *Log Url*: {context.get('task_instance').log_url} 
            """
    failed_alert = SlackOperator(
        task_id='on_failure_slack_callback',
        http_conn_id=INFRA_SLACK_CONN_ID,
        webhook_token=webhook_token,
        message=msg,
        username='airflow')
    return failed_alert.execute(context=context)


def on_success_slack_callback_analytics(context):
    webhook_token = BaseHook.get_connection(ANALYTICS_SLACK_CONN_ID).password
    msg = f"""
            :white_check_mark: Dag Successfully finished.   
            *Dag*: {context.get('task_instance').dag_id} 
            *Execution Time*: {context.get('execution_date')}  
            *Log Url*: {context.get('task_instance').log_url} 
            """
    failed_alert = SlackOperator(
        task_id='on_success_slack_callback_analytics',
        http_conn_id=ANALYTICS_SLACK_CONN_ID,
        webhook_token=webhook_token,
        message=msg,
        username='airflow')
    return failed_alert.execute(context=context)


def on_failure_slack_callback_analytics(context):
    webhook_token = BaseHook.get_connection(ANALYTICS_SLACK_CONN_ID).password
    msg = f"""
            :red_circle: Task Failed. 
            *Task*: {context.get('task_instance').task_id}  
            *Dag*: {context.get('task_instance').dag_id} 
            *Execution Time*: {context.get('execution_date')}  
            *Log Url*: {context.get('task_instance').log_url} 
            """
    failed_alert = SlackOperator(
        task_id='on_failure_slack_callback_analytics',
        http_conn_id=ANALYTICS_SLACK_CONN_ID,
        webhook_token=webhook_token,
        message=msg,
        username='airflow')
    return failed_alert.execute(context=context)


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    webhook_token = BaseHook.get_connection(INFRA_SLACK_CONN_ID).password
    task_instance = blocking_tis[0]
    msg = f"""
            :sos: Task Missed SLA. 
            *Task*: {task_instance.task_id}  
            *Dag*: {dag.dag_id} 
            *Execution Time*: {task_instance.execution_date}
            *SLA Time* {task_instance.task.sla}
            *Blocking Task List* {blocking_task_list}
            *Log Url* {task_instance.log_url}  
        """
    sla_alert = SlackOperator(
        task_id='sla_slack_callback',
        http_conn_id=INFRA_SLACK_CONN_ID,
        webhook_token=webhook_token,
        message=msg,
        username='airflow')
    return sla_alert.execute()
