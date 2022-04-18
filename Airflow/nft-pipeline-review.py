# 아파치 에어플로우 복습
import json
from pandas import json_normalize # json을 판다스로 만들어줌
from airflow import DAG
from datetime import datetime
from airflow.providers.sqlite.operators.sqlite import SqliteOperator 
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'start_date' : datetime(2021,1,1),
}

def _processed_nft(task_instance):
    assets = task_instance.xcom_pull(task_ids=["extract_nft"])
    if not len(assets):
        raise ValueError("assets is empty")
    nft = assets[0]['assets'][0]

    # json을 pandas 객체로
    json_to_pandas = json_normalize(
        data = {
            'token_id':nft['token_id'],
            'name' : nft['name'],
            'image_url' : nft['image_url']
        }
    )

    # pandas => csv
    json_to_pandas.to_csv('/tmp/processed_nft.csv', header=False, index=False)


# DAG Skeleton
with DAG(
    dag_id="pipeline-review",
    schedule_interval='@daily',
    tags='nft',
    default_args=default_args,
    catchup=False
    ) as dag :

    # 테이블 생성 Operator
    creating_nfts_table = SqliteOperator(
        task_id="creating_nfts_table",
        sqlite_conn_id="sqlite_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS nfts(
                token_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                image_url TEXT NOT NULL
            )
        """
    )

    # API 연결 확인 SENSOR
    is_api_available = HttpSensor(
        task_id="is_api_available",
        http_conn_id="githubcontent_api",
        endpoint="keon/data-engineering/main/02-airflow/nftresponse.json"
    )

    # 연결 확인 -> 데이터 가지고 오기 HttpOperator
    extract_nft = SimpleHttpOperator(
        task_id="extract_nft",
        http_conn_id="githubcontent_api",
        endpoint="keon/data-engineering/main/02-airflow/nftresponse.json",
        method="GET",
        response_filter= lambda res: json.loads(res.text), # text파밍, json 객체로 받아옴
        log_response=True
        )

    # 데이터 가공하기
    processe_nft = PythonOperator(
        task_id="processe_nft",
        python_callable=_processed_nft
    )

    # bash operator 로 데이터 저장하기
    store_nft = BashOperator(
        task_id="store_nft",
        bash_command='echo -e ".separator ","\n.import /tmp/processed_nft.csv nfts" | sqlite3 /Users/devkhk/airflow/airflow.db'
    )

    # 의존성 만들기
    creating_nfts_table >> is_api_available >> extract_nft >> processe_nft >> store_nft
    