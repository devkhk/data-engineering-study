# pyflink를 이용해 데이터 플로우를 구현할 떄 가장 먼저 source 테이블을 만든다.
# source 테이블을 만드는 방법
# 
# 1. 개발/디버깅 용으로 하드코딩 생성
# 2. 외부 파일, 데이터 베이스, 메시지 큐로부터 테이블 생성

# 하드코딩

from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes

settings = EnvironmentSettings.new_instance().in_batch_mode().build()
table_env = TableEnvironment.create(settings)

sample_data = [
    ("Spark", 1),
    ("Airflow", 2),
    ("Kafka", 3),
    ("Flink", 4)
]

src1 = table_env.from_elements(sample_data) # 소스 테이블 생성
print(src1)
src1.print_schema()

df = src1.to_pandas()
print(df)

# 커스터마이즈
col_names = ["framework", "chapter"]
src2 = table_env.from_elements(sample_data, col_names)
print(src2.to_pandas())

schema = DataTypes.ROW([
    DataTypes.FIELD("framework", DataTypes.STRING()),
    DataTypes.FIELD("chapter", DataTypes.BIGINT())
])

src3 = table_env.from_elements(sample_data, schema)
print(src3.to_pandas())