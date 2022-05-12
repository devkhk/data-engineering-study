# sql로 카프카 스트림 테이블 만들기

import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# env.set_runtime_mode(execution_mode=RuntimeExecutionMode.STREAMING) 과 같음
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = StreamTableEnvironment.create(env, env_settings)

## 카프카 connector jar 경로 설정
kafka_jar_path = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), "../",
    "flink-sql-connector-kafka-1.15.0.jar"
)
t_env.get_config().get_configuration().set_string(
    "pipeline.jars", f"file://{kafka_jar_path}"
)

# source 테이블 만들기
source_query = f"""
    CREATE TABLE source (
        framework STRING,
        chapter INT
    ) with (
        'connector' = 'kafka',
        'topic' = 'example-source',
        'properties.bootstrap.servers' = 'localhost:9092',
        'properties.group.id' = 'test-group',
        'format' = 'json',
        'scan.startup.mode' = 'earliest-offset'
    )
"""

sink_query = """
    CREATE TABLE sink(
        framework STRING,
        chapter INT
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'example-destination',
        'properties.bootstrap.servers' = 'localhost:9092',
        'format' = 'json',
        'scan.startup.mode' = 'earliest-offset'
    )
"""
t_env.execute_sql(source_query) # 소스 테이블 생성
t_env.execute_sql(sink_query)   # 싱크 테이블 생성

t_env.from_path("source").execute_insert("sink").wait() # insert
