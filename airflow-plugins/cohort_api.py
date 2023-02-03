import json


def handle_cohort_on_day_api_response(response, data_interval_start, params):
    from datetime import timedelta
    import logging
    data_range_start = data_interval_start - timedelta(days=60)
    year = data_range_start.year
    month = data_range_start.month
    platform = params['platform']
    table_type = 'start_trial_auth' if (params['table_type'] == 'start trial auth' or params['table_type'] == 'Start Trial Auth') else params['table_type']
    response_text = response.text
    header = response_text.split('\n')[0].replace(params['table_type'], f'{table_type}') if table_type == 'start_trial_auth' else response_text.split('\n')[0]
    if table_type == 'start_trial_auth':
        list_of_rows = response_text.split('\n')
        list_of_rows[0] = header
        csv_with_replaced_header = '\n'.join(list_of_rows)
    file_name = f'appsflyer_cohort_{platform}_on_day_user_acquisition_{table_type}_{year}_{month}.csv'
    logging.info(f'Saving {file_name} to S3 bucket...')
    with open(f'/mnt/airflow_s3_data/data/{file_name}', 'w', encoding='utf-8') as f:
        if table_type == 'start_trial_auth':
            f.write(csv_with_replaced_header)
        else:
            f.write(response_text)
    logging.info('Successfully saved')
    logging.info(f'Header of file: {header}')
    return {'header': header, 'file_name': file_name}


def handle_cohort_cumulative_api_response(response, params, task):
    from datetime import timedelta, datetime
    import logging
    req_body = json.loads(task.data)
    data_range_start = datetime.strptime(req_body["from"], "%Y-%m-%d")
    year = data_range_start.year
    month = data_range_start.month
    platform = params['platform']
    table_type = 'start_trial_auth' if (params['table_type'] == 'start trial auth' or params['table_type'] == 'Start Trial Auth') else params['table_type']
    response_text = response.text
    header = response_text.split('\n')[0].replace(params['table_type'], f'{table_type}') if table_type == 'start_trial_auth' else response_text.split('\n')[0]
    if table_type == 'start_trial_auth':
        list_of_rows = response_text.split('\n')
        list_of_rows[0] = header
        csv_with_replaced_header = '\n'.join(list_of_rows)
    file_name = f'appsflyer_cohort_{platform}_cumulative_acquisition_{table_type}_{year}_{month}.csv'
    logging.info(f'Saving {file_name} to S3 bucket...')
    with open(f'/mnt/airflow_s3_data/data/{file_name}', 'w', encoding='utf-8') as f:
        if table_type == 'start_trial_auth':
            f.write(csv_with_replaced_header)
        else:
            f.write(response_text)
    logging.info('Successfully saved')
    logging.info(f'Header of file: {header}')
    return {'header': header, 'file_name': file_name}


def start_trial_auth_mapper(elem):
    if elem in ('pid', 'geo', 'af_ad', 'af_adset', 'c', 'users'):
        return elem + ' String,'
    if elem in ('cost', 'ecpi') or elem.startswith('start_trial_auth'):
        return elem + ' Float32,'
    if elem == 'date':
        return elem + ' Date,'


def revenue_mapper(elem):
    if elem in ('pid', 'geo', 'af_ad', 'af_adset', 'c', 'users'):
        return elem + ' String,'
    if elem in ('cost', 'ecpi') or elem.startswith('revenue_sum'):
        return elem + ' Float32,'
    if elem.startswith('revenue_count'):
        return elem + ' Int32,'
    if elem == 'date':
        return elem + ' Date,'


def uninstalls_mapper(elem):
    if elem in ('pid', 'geo', 'af_ad', 'af_adset', 'c', 'users'):
        return elem + ' String,'
    if elem in ('cost', 'ecpi'):
        return elem + ' Float32,'
    if elem.startswith('uninstalls'):
        return elem + ' Int32,'
    if elem == 'date':
        return elem + ' Date,'