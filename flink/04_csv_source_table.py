# csv Source 테이블 만들기

from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes
from pyflink.table import CsvTableSource

settings = EnvironmentSettings.new_instance().in_batch_mode().build()
table_env = TableEnvironment.create(settings)

field_names= ["framework", "chapter"]
field_types = [DataTypes.STRING(),DataTypes.BIGINT()]
source = CsvTableSource(
    source_path="./sample.csv",
    field_names=field_names,
    field_types=field_types,
    ignore_first_line=False
)

table_env.register_table_source("chapters", source)
table = table_env.from_path("chapters")

print(table.to_pandas())
