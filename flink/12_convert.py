# 
# 데이터 스트림과 테이블 같이 쓰기
# 


from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.common.typeinfo import Types

env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)

# ds : datastream 
ds = env.from_collection(["c", "python", "php", "java"], Types.STRING())

# --- 데이터 스트림을 테이블로 가지고 오기 ---
t = t_env.from_data_stream(ds)

# 스파크때 개념과 같음 temp에 저장해야 쿼리를 사용할 수 있다.
t_env.create_temporary_view("lang", t)
res_table = t_env.sql_query("SELECT * FROM lang WHERE f0 like 'p%'")

# --- res_table을 데이터 스트림으로 내보내기 --- 
res_ds = t_env.to_data_stream(res_table)

res_ds.print()
env.execute()