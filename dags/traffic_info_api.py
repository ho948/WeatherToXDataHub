from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import xml.etree.ElementTree as ET
import csv
import os
import logging

cur_path = os.path.dirname(os.path.realpath(__file__))
API_NAME = 'TrafficInfo'
DAG_NAME = 'traffic_info'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    f'{DAG_NAME}_dag',
    default_args=default_args,
    description='서울시 실시간 도로 소통 정보 API',
    schedule_interval='@hourly',
    catchup=False
)

def get_csv_data(**context):
    data = []
    column_name = 'link_id'
    file_name = 'link_info'
    file_path = os.path.join(cur_path, f'output/master_files/{file_name}.csv')

    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            data.append(row[column_name])

    return data

def fetch(link_id, ts):
    start_index = 1
    end_index = 1000
    api_key = Variable.get('SEOUL_API_KEY')
    api_url = f"http://openapi.seoul.go.kr:8088/{api_key}/xml/{API_NAME}/{start_index}/{end_index}/{link_id}"
    logging.info(f'API 주소: {api_url}')
    response = requests.get(api_url)
    
    if response.status_code == 200:
        try:
            root = ET.fromstring(response.content)
            data = []
            for row in root.findall('row'):
                link_id = row.find('link_id').text
                prcs_spd = row.find('prcs_spd').text
                prcs_trv_time = row.find('prcs_trv_time').text
                data.append([link_id, prcs_spd, prcs_trv_time, ts])
            return data
        
        except ET.ParseError as e:
            logging.error(f"XML Parse Error: {e}")
            logging.error(f"Response content: {response.content}")
            return []
    else:
        logging.error(f"API call failed with status code: {response.status_code}")
        logging.error(f"Response content: {response.content}")
        return []

def save_to_csv(data, dir_path, ts_str):
    csv_path = f'{dir_path}/{DAG_NAME}_{ts_str}.csv'
    try:
        with open(csv_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['spot_num', 'ymd', 'hh', 'io_type', 'lane_num', 'vol', 'ts'])
            writer.writerows(data)
            logging.info(f'{csv_path}가 저장되었습니다.')
    except Exception as e:
        logging.error(f"Error: {e}")

def fetch_and_save(**context):
    link_ids = context["task_instance"].xcom_pull(key="return_value", task_ids="get_csv_data")
    execution_ts = datetime.strptime(context['ts_nodash'], '%Y%m%dT%H%M%S') + timedelta(hours=9)
    year = execution_ts.year
    month = execution_ts.month
    day = execution_ts.day
    hour = execution_ts.hour
    dir_path = os.path.join(cur_path, f'output/transaction_files/{DAG_NAME}/{year}/{month}/{day}/{hour}')
    
    try:
        os.makedirs(dir_path)
        logging.info(f'{dir_path}가 생성되었습니다.')
    except Exception as e:
        logging.error(f'Error: {e}')

    all_data = []
    ts_str = execution_ts.strftime("%Y%m%d_%H%M")
    
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = []
        for link_id in link_ids:
            futures.append(executor.submit(fetch, link_id, execution_ts))

        for future in as_completed(futures):
            result = future.result()
            if result:
                all_data.extend(result)

        save_to_csv(all_data, dir_path, ts_str)

get_csv_data_task = PythonOperator(
    task_id='get_csv_data',
    python_callable=get_csv_data,
    dag=dag
)

fetch_and_save_task = PythonOperator(
    task_id='fetch_and_save',
    python_callable=fetch_and_save,
    dag=dag
)

get_csv_data_task >> fetch_and_save_task
