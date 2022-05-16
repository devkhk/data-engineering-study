# flink explain 함수 확인

from pyflink.table import EnvironmentSettings, TableEnvironment

env_settgins = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settgins)

col_names = ["id", "language"]
datas = [
    (1, "php"),
    (2, "python"),
    (3, "c++"),
    (4, "java"),
]
t1 = t_env.from_elements(datas, col_names)
t2 = t_env.from_elements(datas, col_names)

table = t1.where(t1.language.like("p%")).union_all(t2)
print(table.explain())
