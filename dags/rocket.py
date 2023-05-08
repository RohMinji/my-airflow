# 로켓 발사 데터 다운로드 및 처리를 위한 DAG 작성
import json
import pathlib

import airflow
import requests
import requests.exceptions as requests_exceptions

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# DAG 객체 인스턴스 생성
dag = DAG(
    dag_id='download_rocket_launches',
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval=None,
)

download_launches = BashOperator(
    task_id='download_launches',
    #bash_command="/usr/bin/curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming' ", 
    bash_command='env',
    dag=dag,
)

#json 결괏값을 파싱하고 모든 로켓 사진을 다운로드하는 함수
def _get_pictures():
    # 경로가 존재하는지 확인
    pathlib.Path('/tmp/images').mkdir(parents=True,exist_ok=True)

    #launches.json 파일에 있는 모든 그림 파일 다운로드
    with open('/tmp/launches.json') as f:
        launches = json.loads(f)
        image_urls = [launch['image'] for launch in launches["results"]]
        for image_url in image_urls:
            try:
                response = requests.get(image_url)
                image_filename = image_url.split('/')[-1]
                target_file = f"/tmp/images/{image_filename}"
                with open(target_file, 'wb') as f:
                    f.write(response.content)
                print(f"Downloaded {image_url} to {target_file}")
            except requests_exceptions.MissingSchema:
                print(f"{image_url} appears to be an invalid URL.")
            except requests_exceptions.ConnectionError:
                print(f"Could not download image {image_url}") 
                
get_pictures=PythonOperator(
    task_id='get_pictures',
    python_callable=_get_pictures,
    dag=dag,
)

notify = BashOperator(
    task_id='notify',
    bash_command='echo "There are now $(ls /tmp/images | wc -l) images."',
    dag=dag,
)

download_launches >> get_pictures >> notify
