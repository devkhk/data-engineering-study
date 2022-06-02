#
# 트위터 스트림을 통해 카프카로 전송한 데이터의 글자수를 파악하는 모듈
#
import os
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, DataTypes
from pyflink.table.udf import udf

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
# env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime)
setttings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = StreamTableEnvironment.create(env, environment_settings=setttings)

## 카프카 connector jar 경로 설정
kafka_jar_path = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), "../",
    "flink-sql-connector-kafka-1.15.0.jar"
)
t_env.get_config().get_configuration().set_string(
    "pipeline.jars", f"file://{kafka_jar_path}"
)

# CONVERT_TZ(created_at, 'UTC', 'Asia/Seoul')
# WATERMARK FOR ts AS ts - INTERVAL '5' SECOND

source_query = """
    CREATE TABLE tweets (
        text STRING,
        created_at TIMESTAMP(3),
        WATERMARK FOR created_at AS created_at - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'korean-tweets',
        'properties.bootstrap.servers' = 'localhost:9092',
        'properties.group.id' = 'tweet-group',
        'format' = 'json',
        'json.timestamp-format.standard' = 'ISO-8601',
        'scan.startup.mode' = 'latest-offset'
    )
"""

sink_query = """
    CREATE TABLE sink (
        word_count STRING,
        w_start TIMESTAMP(3),
        w_end TIMESTAMP(3)
    ) WITH (
        'connector' = 'print'
    )
"""

# 소스, 싱크 테이블 생성
t_env.execute_sql(source_query)
t_env.execute_sql(sink_query)

# t_env.from_path("tweets").execute_insert("sink").wait()

# windowed = t_env.sql_query("""
#     SELECT
#         text,
#         HOP_START(created_at, INTERVAL '5' SECONDS, INTERVAL '10' SECONDS) AS w_start,
#         HOP_END(created_at, INTERVAL '5' SECONDS, INTERVAL '10' SECONDS) AS w_end
#     FROM tweets
#     GROUP BY
#         HOP(created_at, INTERVAL '5' SECONDS, INTERVAL '10' SECONDS),
#         text
# """)

windowed = t_env.sql_query("""
    SELECT
        text,
        TUMBLE_START(created_at, INTERVAL '10' SECONDS) AS w_start,
        TUMBLE_END(created_at, INTERVAL '10' SECONDS) AS w_end
    FROM tweets
    GROUP BY
        TUMBLE(created_at, INTERVAL '10' SECONDS),
        text
""")
counter = {}
@udf(result_type=DataTypes.STRING())
def word_count(data):
    # data : text
    word_list = data.split()
    for word in word_list:
        if word not in counter:
            counter[word] = 1
        else:
            counter[word] += 1
    # max value -> key print
    sorted_di = sorted(counter.items(), key = lambda item : item[1], reverse=True)
    return str(sorted_di[:5])

res = windowed.select(word_count(windowed.text).alias('word_count'), windowed.w_start, windowed.w_end)
# res = windowed.select(windowed.text, windowed.w_start, windowed.w_end)
print("ready")
res.execute_insert("sink").wait()