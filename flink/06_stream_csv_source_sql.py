# 05번과 같지만 sql문을 사용해 테이블을 생성

from pyflink.table import EnvironmentSettings, TableEnvironment

t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
t_env.get_config().get_configuration().set_string("parallelism.default", "1") # 여기까지 같음

input_path = "./sample.csv"


# sql 문 - with에 기존에 TableDescriptor에서 작성했던 내용들을 적어준다.
source_ddl = f"""
    CREATE TABLE source (
        framework STRING,
        chapter INT
    ) with (
        'connector' = 'filesystem',
        'format' = 'csv',
        'path' = '{input_path}'
    )
"""

# sql문 실행
t_env.execute_sql(source_ddl) # 테이블이 만들어진다.

src = t_env.from_path("source")
print(src.to_pandas())
