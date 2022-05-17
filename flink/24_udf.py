#
#  udf를 이용해 json데이터를 읽어오고 조작하고 다시 리턴하는 모듈
#

import json
from pyflink.table import (
    EnvironmentSettings, 
    TableEnvironment,
    DataTypes,
    TableDescriptor,
    Schema
)
from pyflink.table.udf import udf

t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())

table = t_env.from_elements(
    elements=[
        (1, '{"name":"Spark", "score": 5}'),
        (2, '{"name":"Airflow", "score": 7}'),
        (3, '{"name":"Kafka", "score": 9}'),
        (4, '{"name":"Flink", "score": 8}'),
    ],
    schema = ['id', 'data']
)

# sink table
t_env.create_temporary_table(
    "sink",
    TableDescriptor.for_connector("print")
                   .schema(Schema.new_builder()
                                 .column("id", DataTypes.BIGINT())
                                 .column("data", DataTypes.STRING())
                                 .build()
                    ).build()
)

# udf 정의
# 결과값을 내보낼 타입을 지정할 수 있다.
@udf(result_type=DataTypes.STRING())
def update_score(data):
    json_data = json.loads(data)
    json_data["score"] += 1
    return json.dumps(json_data)

table = table.select(table.id, update_score(table.data))
table.execute_insert("sink").wait()