import requests
import xml.etree.ElementTree as ET
import csv
from dotenv import load_dotenv
import os

# .env 파일 경로 설정 및 로드
dotenv_path = os.path.join('..', 'resources', 'secret.env')
load_dotenv(dotenv_path)

# .env에서 SEOUL_API_KEY 값 읽기
api_key = os.getenv('SEOUL_API_KEY')

# 파일 타입 및 서비스명
file_type = "xml"
service_name = "SpotInfo"

# 첫 번째 API 호출로 list_total_count 값 확인
def get_total_count():
    # 1/1 호출해서 총 개수를 가져옴
    start_index = 1
    end_index = 1
    api_url = f"http://openapi.seoul.go.kr:8088/{api_key}/{file_type}/{service_name}/{start_index}/{end_index}/"
    response = requests.get(api_url)

    if response.status_code == 200:
        # XML 데이터 파싱
        root = ET.fromstring(response.content)
        # <list_total_count> 태그에서 값 추출
        total_count = int(root.find('list_total_count').text)
        return total_count
    else:
        print(f"API 호출 실패: {response.status_code}")
        return None

# 전체 데이터 가져오기
def fetch_data(total_count):
    all_data = []
    start_index = 1
    end_index = total_count
    api_url = f"http://openapi.seoul.go.kr:8088/{api_key}/{file_type}/{service_name}/{start_index}/{end_index}/"
    response = requests.get(api_url)

    if response.status_code == 200:
        # XML 데이터 파싱
        root = ET.fromstring(response.content)
        # 각 row 데이터 추출 후 저장
        for row in root.findall('row'):
            spot_num = row.find('spot_num').text
            spot_nm = row.find('spot_nm').text
            grs80tm_x = row.find('grs80tm_x').text
            grs80tm_y = row.find('grs80tm_y').text
            all_data.append([spot_num, spot_nm, grs80tm_x, grs80tm_y])
    else:
            print(f"API 호출 실패: {response.status_code}")
    
    return all_data

# CSV 파일로 저장
def save_to_csv(data):
    with open('./output/spot_info.csv', mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        # CSV 헤더 작성
        writer.writerow(['spot_num', 'spot_nm', 'grs80tm_x', 'grs80tm_y'])
        # 데이터 저장
        writer.writerows(data)

# 메인 실행 함수
def main():
    # 1. 첫 번째 호출로 총 데이터 개수 가져오기
    total_count = get_total_count()

    if total_count:
        print(f"총 데이터 개수: {total_count}")

        # 2. 전체 데이터를 가져와 리스트에 저장
        data = fetch_data(total_count)

        # 3. 데이터를 CSV로 저장
        save_to_csv(data)
        print("CSV 파일 변환 완료!")
    else:
        print("데이터를 가져오는 데 실패했습니다.")

# 실행
if __name__ == "__main__":
    main()
