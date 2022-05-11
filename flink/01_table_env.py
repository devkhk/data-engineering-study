# flink 테이블 환경 만드는 방법

from pyflink.table import EnvironmentSettings, TableEnvironment

# 배치 환경
batch_settings = EnvironmentSettings.new_instance()\
                                    .in_batch_mode()\
                                    .build() # 쿼리플래너 lagucy VS blink => blink가 새로 출시
batch_table_env = TableEnvironment.create(batch_settings)

# stream 환경
stream_settings = EnvironmentSettings.new_instance().in_streaming_mode()\
                                     .build()
stream_table_env = TableEnvironment.create(stream_settings)