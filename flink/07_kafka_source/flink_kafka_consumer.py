# kafka Source 테이블 만들기

import os
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.datastream.execution_mode import RuntimeExecutionMode
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table import TableDescriptor
from pyflink.table import Schema

# env 스트리밍 모드 정의
env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(execution_mode=RuntimeExecutionMode.STREAMING)

# 스트리밍 enviroment는 항상 runtime모드가 disable이기 때문에 체크포인트를 enable해준다.
env.enable_checkpointing(1000) # interval
env.get_checkpoint_config().set_max_concurrent_checkpoints(1) # 동시에 생기는 체크포인트는 최대 1개로 설정

# 환경설정이 끝났으면 table env를 만든다.
t_env = StreamTableEnvironment.create(env)

## 카프카 connector jar 경로 설정
kafka_jar_path = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), "../",
    "flink-sql-connector-kafka-1.15.0.jar"
)
t_env.get_config().get_configuration().set_string(
    "pipeline.jars", f"file://{kafka_jar_path}"
)


schema = SimpleStringSchema()
kafka_consumer = FlinkKafkaConsumer(
    topics="flink-test2",
    deserialization_schema=schema,
    properties={
        "bootstrap-server" : "localhost:9092",
        "group.id": "test_group"
    }
)

ds = env.add_source(kafka_consumer) # 카프카 컨슈머를 소스로 사용할 수 있게 해준다.

# 간단한 sink 만들기 

# table 생성
t_env.execute_sql(
    """
    CREATE TABLE blackhole(
        data STRING
    ) WITH (
        'connector' = 'blackhole'
    )
    """
)

# 데이터 스트림이 올때 테이블을 만들고 insert 해준다.
table = t_env.from_data_stream(ds) 
# table.execute_insert("blackhole")


## flink 1.15v 부터 사용되지 않는 메서드가 많이 생김. 아래 코드는 확실치 않고 후에 확인 해 봐야함.
# https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=134745878


t_stmt_set = t_env.create_statement_set()

sink_descriptor = TableDescriptor.for_connector("blackhole")\
                                 .schema(Schema.new_builder()\
                                                .column("data", DataTypes.STRING())\
                                                .build())\
                                 .build()
t_stmt_set.add_insert(sink_descriptor, table)

result = t_stmt_set.execute()
print(result.print())
