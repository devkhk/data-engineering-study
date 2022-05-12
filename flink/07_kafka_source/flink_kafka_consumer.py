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
    topics="flink-test",
    deserialization_schema=schema,
    properties={
        "bootstrap.servers" : "localhost:9092",
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
table.execute_insert("blackhole").wait()

# v1.15 릴리즈에서 테이블 환경에서 execute , insertInto 메서드를 삭제했다.
# https://nightlies.apache.org/flink/flink-docs-release-1.15/release-notes/flink-1.15/#remove-pre-flip-84-methods
# 이유는 테이블 환경과 다른 설정된 환경이 같이 구축되어 있을 때 execute 메서드를 실행했을 경우,
# table환경과 다른 환경중 어떤 환경에서 execute가 되는지 불분명해지는 문제가 있다.
# 
# https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=134745878
# [번역]
# 불분명한 플링크 테이블 프로그램 트리거 포인트. 
# 'TableEnvironment.execute()`와 'StreamExecutionEnvironment.execute()` 
# 모두 Flink 테이블 프로그램 실행을 트리거할 수 있습니다. 
# 그러나 TableEnvironment를 사용하여 Flink 테이블 프로그램을 구축하는 경우, 
# StreamExecutionEnvironment 인스턴스를 가져올 수 없기 때문에 
# 'TableEnvironment.execute()`를 사용하여 실행을 트리거해야 합니다. 
# StreamTableEnvironment를 사용하여 Flink 테이블 프로그램을 구축하는 경우,
#  둘 다 실행을 트리거할 수 있습니다. 
# 테이블 프로그램을 DataStream 프로그램으로 변환하는 경우(StreamExecutionEnvironment.toAppendStream/toRetractStream 사용),
#  둘 다 사용하여 실행을 트리거할 수도 있습니다. 
# 그래서 어떤 '실행' 방법을 사용해야 하는지 설명하기 어렵다. 
# StreamTableEnvironment와 마찬가지로, BatchTableEnvironment도 같은 문제가 있다.
