# Stream Operation
# sliding window

from pyflink.common.time import Instant # 데이터에 밀리초 단위 시간 개념 추가.
from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (
    DataTypes, TableDescriptor, Schema, StreamTableEnvironment
)
from pyflink.table.expressions import lit, col
from pyflink.table.window import Slide

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
t_env = StreamTableEnvironment.create(env)

# 데이터 소스
ds = env.from_collection(
    collection = [
        (Instant.of_epoch_milli(1000), "Alice", 110.1),
        (Instant.of_epoch_milli(4000), "Bob", 30.2),
        (Instant.of_epoch_milli(3000), "Alice", 20.0),
        (Instant.of_epoch_milli(2000), "Bob", 53.1),
        (Instant.of_epoch_milli(5000), "Alice", 13.1),
        (Instant.of_epoch_milli(3000), "Bob", 3.1),
        (Instant.of_epoch_milli(7000), "bob", 16.1),
        (Instant.of_epoch_milli(10000), "Alice", 20.1),
    ],
    type_info=Types.ROW([Types.INSTANT(), Types.STRING(), Types.FLOAT()])
)

table = t_env.from_data_stream(
    ds,
    Schema.new_builder()
          .column_by_expression("ts", "CAST(f0 AS TIMESTAMP(3))")
          .column("f1", DataTypes.STRING())
          .column("f2", DataTypes.FLOAT())
          .watermark("ts", "ts - INTERVAL '3' SECOND")
          .build()
).alias("ts, name, price")

# sink
t_env.create_temporary_table(
    "sink",
    TableDescriptor.for_connector("print")
                    .schema(Schema.new_builder()
                                  .column("name", DataTypes.STRING())
                                  .column("tatal_price", DataTypes.FLOAT())
                                  .column("w_start", DataTypes.TIMESTAMP(3))
                                  .column("w_end", DataTypes.TIMESTAMP(3))
                                  .build()
                    ).build()
)

# tumble과 큰 차이 없지만 slide 윈도우의 특징인 time interval의 주기보다 더 짧은
# 업데이트 주기를 갖고 있기 때문에 업데이트 주기를 정의해 주어야 한다 => every()를 추가해준다.
table = table.window(
                Slide.over(lit(5).seconds)
                     .every(lit(2).seconds)
                     .on(col("ts")).alias("w"))\
             .group_by(table.name, col("w"))\
             .select(table.name, table.price.sum, col("w").start, col("w").end)

table.execute_insert("sink").wait()