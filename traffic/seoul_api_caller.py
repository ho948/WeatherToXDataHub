import requests
import xml.etree.ElementTree as ET
import csv
from dotenv import load_dotenv
import os

class SeoulAPICaller:
    def __init__(self, service_name, output_filename, columns, additionals=None):
        dotenv_path = os.path.join('..', 'resources', 'secret.env')
        load_dotenv(dotenv_path)
        
        self.api_key = os.getenv('SEOUL_API_KEY')
        self.file_type = "xml"
        self.service_name = service_name
        self.output_filename = output_filename
        self.columns = columns
        self.additionals = additionals

    def get_total_count(self, additional=None):
        start_index = 1
        end_index = 1

        if additional is None:
            api_url = (
                f"http://openapi.seoul.go.kr:8088/{self.api_key}/"
                f"{self.file_type}/{self.service_name}/{start_index}/{end_index}/"
            )
        else:
            api_url = (
                f"http://openapi.seoul.go.kr:8088/{self.api_key}/"
                f"{self.file_type}/{self.service_name}/{start_index}/{end_index}/{additional}/"
            )
        response = requests.get(api_url)

        if response.status_code == 200:
            root = ET.fromstring(response.content)
            total_count = int(root.find('list_total_count').text)
            return total_count
        else:
            print(f"API 호출 실패: {response.status_code}")
            return None

    def fetch_data(self, total_count, additional=None):
        all_data = []
        start_index = 1
        end_index = total_count
        
        if additional is None:
            api_url = (
                f"http://openapi.seoul.go.kr:8088/{self.api_key}/"
                f"{self.file_type}/{self.service_name}/{start_index}/{end_index}/"
            )
        else:
            api_url = (
                f"http://openapi.seoul.go.kr:8088/{self.api_key}/"
                f"{self.file_type}/{self.service_name}/{start_index}/{end_index}/{additional}/"
            )
        response = requests.get(api_url)

        if response.status_code == 200:
            root = ET.fromstring(response.content)
            for row in root.findall('row'):
                data_row = [row.find(col).text for col in self.columns]
                all_data.append(data_row)
        else:
            print(f"API 호출 실패: {response.status_code}")
        
        return all_data

    def save_to_csv(self, data):
        output_dir = './output'
        master_file_dir = '../dags/output/master_files'
        
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(master_file_dir, exist_ok=True)
        
        output_path = os.path.join(output_dir, f'{self.output_filename}.csv')
        master_file_path = os.path.join(master_file_dir, f'{self.output_filename}.csv')
        
        with open(output_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(self.columns)
            writer.writerows(data)
        
        with open(master_file_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(self.columns)
            writer.writerows(data)

    def process(self):
        total_count = self.get_total_count()

        if total_count:
            print(f"총 데이터 개수: {total_count}")
            data = self.fetch_data(total_count)
            self.save_to_csv(data)
            print(f"{self.output_filename} CSV 파일 변환 완료!")
        else:
            print("데이터를 가져오는 데 실패했습니다.")

    def get_csv_data(self, column_name):
        data = []
        csv_path = f'./output/{self.output_filename}.csv'

        with open(csv_path, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                data.append(row[column_name])
        
        print(f"{'*'*10} {self.output_filename}.csv을 읽었습니다. {'*'*10}")
        return data

    def process_with_additionals(self):
        all_data = []

        for additional in self.additionals:
            total_count = self.get_total_count(additional)
            print(f"총 데이터 개수: {total_count}")
            data = self.fetch_data(total_count, additional)
            all_data.extend(data)

        self.save_to_csv(all_data)
        print(f"{self.output_filename} CSV 파일 변환 완료!")


if __name__ == "__main__":
    # 마스터 데이터 API
    acc_main_api_caller = SeoulAPICaller("AccMainCode", "acc_main_code", ['acc_type', 'acc_type_nm'])
    acc_main_api_caller.process()
    acc_sub_client_api_caller = SeoulAPICaller("AccSubCode", "acc_sub_code", ['acc_dtype', 'acc_dtype_nm'])
    acc_sub_client_api_caller.process()
    region_info_client_api_caller = SeoulAPICaller("RegionInfo", "region_info", ['reg_cd', 'reg_name'])
    region_info_client_api_caller.process()
    road_div_client_api_caller = SeoulAPICaller("RoadDivInfo", "road_div_info", ['road_div_cd', 'road_div_nm'])
    road_div_client_api_caller.process()
    spot_info_client_api_caller = SeoulAPICaller("SpotInfo", "spot_info", ['spot_num', 'spot_nm', 'grs80tm_x', 'grs80tm_y'])
    spot_info_client_api_caller.process()

    # 마스터 to 마스터 데이터 API
    road_div_cds = road_div_client_api_caller.get_csv_data('road_div_cd')
    road_info_api_caller = SeoulAPICaller("RoadInfo", "road_info", ['road_div_cd', 'axis_cd', 'axis_name'], road_div_cds)
    road_info_api_caller.process_with_additionals()