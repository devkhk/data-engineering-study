import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (
    StreamTableEnvironment,
    EnvironmentSettings,
    DataTypes
)
from pyflink.table.udf import udf


env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = StreamTableEnvironment.create(env, environment_settings=settings)

# 카프카 connector jar 경로 설정
kafka_jar_path = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), "../",
    "flink-sql-connector-kafka-1.15.0.jar"
)
t_env.get_config().get_configuration().set_string(
    "pipeline.jars", f"file://{kafka_jar_path}"
)

source_query = """
    CREATE TABLE trips (
        VendorID INT,
        tpep_pickup_datetime STRING,
        tpep_dropoff_datetime STRING,
        passenger_count INT,
        trip_distance DOUBLE,
        RatecodeID INT,
        store_and_fwd_flag STRING,
        PULocationID INT,
        DOLocationID INT,
        payment_type INT,
        fare_amount DOUBLE,
        extra DOUBLE,
        mta_tax DOUBLE,
        tip_amount DOUBLE,
        tolls_amount DOUBLE,
        improvement_surcharge DOUBLE,
        total_amount DOUBLE,
        congestion_surcharge DOUBLE
    ) with (
        'connector' = 'kafka',
        'topic' = 'taxi-trips',
        'properties.bootstrap.servers' = 'localhost:9092',
        'properties.group.id' = 'taxi-group',
        'format' = 'csv',
        'scan.startup.mode' = 'latest-offset'
    )
"""

sink_query = """
    CREATE TABLE sink (
        tpep_pickup_datetime STRING
    ) with (
        'connector' = 'print'
    )
"""

# 테이블 생성
t_env.execute_sql(source_query)
t_env.execute_sql(sink_query)

t_env.from_path("trips").select("tpep_pickup_datetime").execute_insert("sink").wait()

