import requests
import xml.etree.ElementTree as ET
import csv
from dotenv import load_dotenv
import os

dotenv_path = os.path.join('..', 'resources', 'secret.env')
load_dotenv(dotenv_path)

api_key = os.getenv('SEOUL_API_KEY')

FILE_TYPE = "xml"
SERVICE_NAME = "SpotInfo"

def get_total_count():
    start_index = 1
    end_index = 1
    api_url = (
        f"http://openapi.seoul.go.kr:8088/{api_key}/"
        f"{FILE_TYPE}/{SERVICE_NAME}/{start_index}/{end_index}/"
    )
    response = requests.get(api_url)

    if response.status_code == 200:
        root = ET.fromstring(response.content)
        total_count = int(root.find('list_total_count').text)
        return total_count
    else:
        print(f"API 호출 실패: {response.status_code}")
        return None

def fetch_data(total_count):
    all_data = []
    start_index = 1
    end_index = total_count
    api_url = (
        f"http://openapi.seoul.go.kr:8088/{api_key}/"
        f"{FILE_TYPE}/{SERVICE_NAME}/{start_index}/{end_index}/"
    )
    response = requests.get(api_url)

    if response.status_code == 200:
        root = ET.fromstring(response.content)
        for row in root.findall('row'):
            spot_num = row.find('spot_num').text
            spot_nm = row.find('spot_nm').text
            grs80tm_x = row.find('grs80tm_x').text
            grs80tm_y = row.find('grs80tm_y').text
            all_data.append([spot_num, spot_nm, grs80tm_x, grs80tm_y])
    else:
        print(f"API 호출 실패: {response.status_code}")
    
    return all_data

def save_to_csv(data):
    output_path = './output/spot_info.csv'
    master_file_path = '../dags/output/master_files/spot_info.csv'
    
    with open(output_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['spot_num', 'spot_nm', 'grs80tm_x', 'grs80tm_y'])
        writer.writerows(data)
    
    with open(master_file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['spot_num', 'spot_nm', 'grs80tm_x', 'grs80tm_y'])
        writer.writerows(data)

def main():
    total_count = get_total_count()

    if total_count:
        print(f"총 데이터 개수: {total_count}")
        data = fetch_data(total_count)
        save_to_csv(data)
        print("CSV 파일 변환 완료!")
    else:
        print("데이터를 가져오는 데 실패했습니다.")

if __name__ == "__main__":
    main()
