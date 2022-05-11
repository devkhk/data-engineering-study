# stream 환경에서 CSV Source 테이블을 만들기

from pyflink.table import (
    EnvironmentSettings, TableEnvironment,
    Schema, DataTypes, TableDescriptor
)

# TableDescriptor : 데이터를 가지고올 때 외부의 테이블과 연결 해 주는 역할

env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

# 테이블 환경에서 스트림을 사용할 땐 parralism 설정을 해주어야 한다.
t_env.get_config().get_configuration().set_string("parallelism.default", "1")

input_path = "./sample.csv"

# 테이블 생성 - Table API 를 사용해서 생성
t_env.create_temporary_table(
    "source",
    TableDescriptor.for_connector("filesystem")
                   .schema(Schema.new_builder()
                                 .column("framework", DataTypes.STRING())
                                 .column("chapter", DataTypes.INT())
                                 .build())
                    .option("path", input_path)
                    .format("csv")
                    .build()
)

src = t_env.from_path("source")
print(src.to_pandas())