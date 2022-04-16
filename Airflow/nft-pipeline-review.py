# DAG Skeleton

from datetime import datetime
from airflow import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensors.http import HttpSensor

default_arg = {
    'start_date' : datetime(2021, 1, 1),
}

with DAG(dag_id='nft-pipeline',
         schedule_interval='@daily',
         default_args=default_arg,
         tags=['nft'],
         catchup=False ) as dag:
    
    # 테스크 추가하기
    creating_table = SqliteOperator(
        task_id="creating_table",
        sqlite_conn_id="db_sqlite", # ui 웹에서 추가
        sql="""
            CREATE TABLE IF NOT EXISTS nfts(
                token_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                image_url TEXT NOT NULL
            )
        """
    )

    # sensor task 추가
    is_api_available = HttpSensor(
        task_id='is_api_available',
        headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.4 Safari/605.1.15',
        'referrer':'api/v1/assets?collection=doodles-official&limit=1',
        'X-Api-Key': ""
        },
        http_conn_id='opensea_api',
        endpoint='api/v1/assets?collection=doodles-official&limit=1'
    )
