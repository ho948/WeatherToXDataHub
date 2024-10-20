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
API_NAME = 'VolInfo'
DAG_NAME = 'vol_info'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    f'{DAG_NAME}_dag',
    default_args=default_args,
    description='서울시 교통량 이력 정보 API',
    schedule_interval='@daily',
    catchup=True
)

def get_csv_data(**context):
    data = []
    column_name = 'spot_num'
    file_name = 'spot_info'
    file_path = os.path.join(cur_path, f'output/master_files/{file_name}.csv')

    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            data.append(row[column_name])

    return data

def fetch(spot_num, date_str, hour):
    start_index = 1
    end_index = 1000
    api_key = Variable.get('SEOUL_API_KEY')
    api_url = f"http://openapi.seoul.go.kr:8088/{api_key}/xml/{API_NAME}/{start_index}/{end_index}/{spot_num}/{date_str}/{hour}/"
    logging.info(f'API 주소: {api_url}')
    response = requests.get(api_url)
    
    if response.status_code == 200:
        try:
            root = ET.fromstring(response.content)
            data = []
            for row in root.findall('row'):
                spot_num = row.find('spot_num').text
                ymd = row.find('ymd').text
                hh = row.find('hh').text
                io_type = row.find('io_type').text
                lane_num = row.find('lane_num').text
                vol = row.find('vol').text
                data.append([spot_num, ymd, hh, io_type, lane_num, vol])
            return data
        
        except ET.ParseError as e:
            logging.error(f"XML Parse Error: {e}")
            logging.error(f"Response content: {response.content}")
            return []
    else:
        logging.error(f"API call failed with status code: {response.status_code}")
        logging.error(f"Response content: {response.content}")
        return []

def save_to_csv(data, dir_path, date_str):
    csv_path = f'{dir_path}/{DAG_NAME}_{date_str}.csv'
    try:
        with open(csv_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['spot_num', 'ymd', 'hh', 'io_type', 'lane_num', 'vol'])
            writer.writerows(data)
            logging.info(f'{csv_path}가 저장되었습니다.')
    except Exception as e:
        logging.error(f"Error: {e}")

def fetch_and_save(**context):
    spot_nums = context["task_instance"].xcom_pull(key="return_value", task_ids="get_csv_data")
    execution_date = datetime.strptime(context['ds_nodash'], '%Y%m%d') - timedelta(days=1)
    
    year = execution_date.year
    month = execution_date.month
    day = execution_date.day
    dir_path = os.path.join(cur_path, f'output/transaction_files/{DAG_NAME}/{year}/{month}/{day}')
    
    try:
        os.makedirs(dir_path)
        logging.info(f'{dir_path}가 생성되었습니다.')
    except Exception as e:
        logging.error(f'Error: {e}')

    all_data = []
    date_str = execution_date.strftime("%Y%m%d")
    
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = []
        for hour in range(24):
            hour_str = f"{hour:02d}"
            for spot_num in spot_nums:
                futures.append(executor.submit(fetch, spot_num, date_str, hour_str))

        for future in as_completed(futures):
            result = future.result()
            if result:
                all_data.extend(result)

        save_to_csv(all_data, dir_path, date_str)

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
