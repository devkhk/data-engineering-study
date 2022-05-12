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
        'topic' = 'flink-test',
        'properties.bootstrap.servers' = 'localhost:9092',
        'properties.group.id' = 'test-group',
        'format' = 'csv',
        'scan.startup.mode' = 'earliest-offset'
    )
"""
# 쿼리 등록 : source 테이블을 만들고 카프카 컨넥터로부터 데이터를 전달 받는다.
t_env.execute_sql(source_query)

sink_query = """
    CREATE TABLE blackhole(
        framework STRING,
        chapter INT
    ) WITH (
        'connector' = 'blackhole'
    )
"""
t_env.execute_sql(sink_query) # 블랙홀 테이블 생성


# 아랫줄과 같은 내용이고 , execute_sql()은 쿼리문을 통해서 필터링 등등, 원하는 작업을 직접 수행할 수 있다.
t_env.execute_sql("INSERT INTO blackhole SELECT framework, chapter FROM source").wait()
# t_env.from_path("source").execute_insert("blackhole").wait()

