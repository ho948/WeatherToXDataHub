from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
import requests
import xml.etree.ElementTree as ET
import csv
import os
import logging

cur_path = os.path.dirname(os.path.realpath(__file__))
API_NAME = 'AccInfo'
DAG_NAME = 'acc_info'

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
    description='서울시 실시간 돌발 정보 API',
    schedule_interval='@hourly',
    catchup=False
)

def fetch(ts):
    start_index = 1
    end_index = 1000
    api_key = Variable.get('SEOUL_API_KEY')
    api_url = f"http://openapi.seoul.go.kr:8088/{api_key}/xml/{API_NAME}/{start_index}/{end_index}/"
    logging.info(f'API 주소: {api_url}')
    response = requests.get(api_url)
    
    if response.status_code == 200:
        try:
            root = ET.fromstring(response.content)
            data = []
            for row in root.findall('row'):
                acc_id = row.find('acc_id').text
                occr_date = row.find('occr_date').text
                occr_time = row.find('occr_time').text
                exp_clr_date = row.find('exp_clr_date').text
                exp_clr_time = row.find('exp_clr_time').text
                acc_type = row.find('acc_type').text
                acc_dtype = row.find('acc_dtype').text
                link_id = row.find('link_id').text
                grs80tm_x = row.find('grs80tm_x').text
                grs80tm_y = row.find('grs80tm_y').text
                acc_info = row.find('acc_info').text
                acc_road_code = row.find('acc_road_code').text
                data.append([acc_id, occr_date, occr_time, exp_clr_date, exp_clr_time, acc_type, acc_dtype, link_id, grs80tm_x, grs80tm_y, acc_info, acc_road_code, ts])
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
            writer.writerow(['acc_id', 'occr_date', 'occr_time', 'exp_clr_date', 'exp_clr_time', 'acc_type', 'acc_dtype', 'link_id', 'grs80tm_x', 'grs80tm_y', 'acc_info', 'acc_road_code', 'ts'])
            writer.writerows(data)
            logging.info(f'{csv_path}가 저장되었습니다.')
    except Exception as e:
        logging.error(f"Error: {e}")

def fetch_and_save(**context):
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

    ts_str = execution_ts.strftime("%Y%m%d_%H%M")
    data = fetch(execution_ts)

    save_to_csv(data, dir_path, ts_str)

fetch_and_save_task = PythonOperator(
    task_id='fetch_and_save',
    python_callable=fetch_and_save,
    dag=dag
)

fetch_and_save_task
