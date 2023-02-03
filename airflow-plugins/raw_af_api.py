from airflow.exceptions import AirflowFailException


def handle_raw_api_response(response, data_interval_start, data_interval_end, params):
    import logging
    from datetime import timedelta
    platform = params['platform']
    report_type = params['report_type']
    organic_type = params['organic_type']
    if report_type == 'uninstalls':
        if platform == 'apple':
            report_date = (data_interval_end - timedelta(days=12)).strftime('%Y_%m_%d')
        elif platform == 'android':
            report_date = (data_interval_end - timedelta(days=3)).strftime('%Y_%m_%d')
    elif report_type == 'in_app_events':
        report_date = (data_interval_start - timedelta(days=1)).strftime('%Y_%m_%d_%H_%M')
    else:
        report_date = data_interval_start.strftime('%Y_%m_%d')
    logging.info(f'Loading {platform} {report_type} for day {report_date}')
    response_text = response.text
    rows_count = int(response.headers["Report-Rows"])
    logging.info(f'Rows in report: {rows_count}')
    if rows_count >= 1000000:
        logging.error(f'There are more 1000000 rows in response, please decrease interval and repeat request!')
        raise AirflowFailException
    header = response_text.split('\n')[0].lower().replace(' ', '_')
    list_of_rows = response_text.split('\n')
    list_of_rows[0] = header
    csv_with_replaced_header = '\n'.join(list_of_rows)
    file_name = f'appsflyer_raw_{platform}_{organic_type}{report_type}_{report_date}.csv'
    logging.info(f'Saving {file_name} to S3 bucket...')
    with open(f'/mnt/airflow_s3_data/data/{file_name}', 'w', encoding='utf-8') as f:
        f.write(csv_with_replaced_header)
    logging.info('Successfully saved')
    logging.info(f'Header of file: {header}')
    return {'header': header, 'file_name': file_name}


def installs_mapper(elem):
    if elem in ('attributed_touch_time', 'install_time', 'event_time'):
        return elem + ' DateTime64(3),'
    if elem == 'pdate':
        return elem + ' Date,'
    else:
        return elem + ' String,'


def uninstalls_mapper(elem):
    if elem in ('attributed_touch_time', 'install_time', 'event_time'):
        return elem + ' DateTime64(3),'
    if elem == 'pdate':
        return elem + ' Date,'
    else:
        return elem + ' String,'


def in_app_events_mapper(elem):
    if elem in ('attributed_touch_time', 'install_time', 'event_time'):
        return elem + ' DateTime64(3),'
    if elem == 'pdate':
        return elem + ' Date,'
    else:
        return elem + ' String,'