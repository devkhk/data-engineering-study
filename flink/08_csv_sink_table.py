# csv Source 테이블 -> sink 테이블만들기

from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes
from pyflink.table import CsvTableSource, CsvTableSink, WriteMode

settings = EnvironmentSettings.new_instance().in_batch_mode().build()
t_env = TableEnvironment.create(settings)

in_field_names = ["framework", "chapter"]
in_field_types = [DataTypes.STRING(),DataTypes.BIGINT()]

source = CsvTableSource(
    "./sample.csv",
    in_field_names,
    in_field_types,
    ignore_first_line=False
)

t_env.register_table_source("chapters", source)
table = t_env.from_path("chapters")

print("--- print schema ---")
table.print_schema()

out_field_names = ["framework", "chapter"]
out_field_types = [DataTypes.STRING(),DataTypes.BIGINT()]

# 싱크 테이블 생성
sink = CsvTableSink(
    out_field_names,
    out_field_types,
    "./sample_copy.csv",
    num_files=1,
    write_mode=WriteMode.OVERWRITE
)

# 싱크 테이블 레지스터
t_env.register_table_sink("out_sink", sink)

table.execute_insert("out_sink").wait()
