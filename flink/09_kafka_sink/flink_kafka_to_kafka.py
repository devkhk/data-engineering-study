# 
# 카프카로부터 들어오는 스트림 데이터를 플링크에서 다시 카프카로 전달 하는 모듈
# 


import os
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.execution_mode import RuntimeExecutionMode


# env 스트리밍 모드 정의
env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(execution_mode=RuntimeExecutionMode.STREAMING)

# 스트리밍 enviroment는 항상 runtime모드가 disable이기 때문에 체크포인트를 enable해준다.
env.enable_checkpointing(1000) # interval
env.get_checkpoint_config().set_max_concurrent_checkpoints(1) # 동시에 생기는 체크포인트는 최대 1개로 설정


## 카프카 connector jar 경로 설정
kafka_jar_path = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), "../",
    "flink-sql-connector-kafka-1.15.0.jar"
)
env.add_jars(f"file://{kafka_jar_path}")

in_schema = SimpleStringSchema()
# 플링크 카프카 컨슈머 : 카프카로부터 데이터를 받는 컨슈머, 플링크에서 전달받은 데이터를 플링크에서 카프카프로듀서로 전달 할 것.
kafka_consumer = FlinkKafkaConsumer(
    topics="example-source",
    deserialization_schema=in_schema,
    properties={
        "bootstrap.servers" : "localhost:9092",
        "group.id": "test_group"
    }
)

out_schema = SimpleStringSchema()
# 플링크 카프카 프로듀서 생성 : 전달받은 데이터를 플링크-카프카 프로듀서를 통해 카프카로 내보낸다.
# 플링크에서는 목적지를 sink로 이해하고, 카프카 프로듀서는 전달하는 곳으로 topic인 exapmle-destination 으로 이해한다.
kafka_producer = FlinkKafkaProducer(
    topic="example-destination",
    serialization_schema=out_schema,
    producer_config={
        "bootstrap.servers" : "localhost:9092"
    }
)

ds = env.add_source(kafka_consumer)
ds.add_sink(kafka_producer)

env.execute("kafka_to_kafka")