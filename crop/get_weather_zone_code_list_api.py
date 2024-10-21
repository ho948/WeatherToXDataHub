from dotenv import load_dotenv
import requests
import xml.etree.ElementTree as ET
import csv
import os

# API에서 XML 데이터를 가져오는 함수
def fetch(api_url, params):
    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status()
        root = ET.fromstring(response.content)  # XML 파싱
        return root
    except requests.exceptions.RequestException as e:
        print(f"API 요청 중 오류가 발생했습니다: {e}")
        return None
    except ET.ParseError as e:
        print(f"XML 파싱 중 오류가 발생했습니다: {e}")
        return None

# XML 데이터를 CSV 형식의 리스트로 변환하는 함수
def transform(root):
    data_list = []
    try:
        for item in root.findall('.//item'):
            zone_code = item.find('zone_Code').text if item.find('zone_Code') is not None else ''
            zone_name = item.find('zone_Name').text if item.find('zone_Name') is not None else ''
            
            # 관측지점 목록 순회
            spot_list = item.find('zone_Spot_List')
            if spot_list is not None:
                for spot in spot_list.findall('.//item'):
                    obsr_spot_code = spot.find('obsr_Spot_Code').text if spot.find('obsr_Spot_Code') is not None else ''
                    obsr_spot_name = spot.find('obsr_Spot_Nm').text if spot.find('obsr_Spot_Nm') is not None else ''
                    
                    # 각 행을 리스트로 저장
                    data_list.append([zone_code, zone_name, obsr_spot_code, obsr_spot_name])
    except AttributeError as e:
        print(f"XML 구조를 읽는 중 오류가 발생했습니다: {e}")
    
    return data_list

# 데이터를 CSV 파일로 저장하는 함수
def save_to_csv(data_list, filename=None, columns=None):
    try:
        output_dir = './output'
        master_file_dir = '../dags/output/master_files'
        
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(master_file_dir, exist_ok=True)

        output_path = os.path.join(output_dir, f'{filename}.csv')
        master_file_path = os.path.join(master_file_dir, f'{filename}.csv')
        
        with open(output_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(columns)
            writer.writerows(data_list)
        
        with open(master_file_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(columns)
            writer.writerows(data_list)

        print(f"CSV 파일이 성공적으로 생성되었습니다: {output_path}")
        print(f"CSV 파일이 성공적으로 생성되었습니다: {master_file_path}")
    
    except IOError as e:
        print(f"CSV 파일 생성 중 오류가 발생했습니다: {e}")

# 메인 실행 로직
if __name__ == "__main__":
    api_url = "http://apis.data.go.kr/1390802/AgriWeather/WeatherObsrInfo/GrdlInfo/getWeatherZoneCodeList"
    columns = ['zone_code', 'zone_name', 'obsr_spot_code', 'obsr_spot_name']
    
    dotenv_path = os.path.join('..', 'resources', 'secret.env')
    load_dotenv(dotenv_path)
    service_key = os.getenv('DATA_GO_API_KEY')
    
    params = {
        'serviceKey': service_key,
    }

    fetched_root = fetch(api_url=api_url, params=params)
    
    if fetched_root is not None:
        data_list = transform(root=fetched_root)
        filename = 'weather_zone_code_list'
        save_to_csv(data_list=data_list, filename=filename, columns=columns)
