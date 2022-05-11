# 
# 데이터 스트림 환경에서 테이블을 만드는 방법
# 
# 데이터 스트림 위에서 테이블을 만들어야 하기 때문에 데이터 스트림 환경을 먼저 만들어 주어야 한다.
# 데이터 스트림 환경을 테이블에 넣어주고 테이블 환경을 사용하면 된다.
#  

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

datastream_env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(datastream_env)
