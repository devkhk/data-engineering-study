#
# keyed stream을 만들고
# keyed stream 안에서 state에 접근해 데이터를 process하는 방법
#

from pyflink.common import Time
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor, StateTtlConfig


# KeyedProcessFunction을 상속받아 state에 접근해 k, v 형태의 데이터를 가공 한다. 
class Sum(KeyedProcessFunction):
    def __init__(self):
        self.state = None # state 초기화

    def open(self, runtime_context: RuntimeContext):
        state_descriptor = ValueStateDescriptor("state", Types.FLOAT())
        
        # ttl : time to live -> 스테이트가 언제까지 존재하는지 정보를 담고있는 config 설정
        # Time.seconds(1) : 얼마동안 살아있을지
        # set_update_type(StateTtlConfig.UpdateType.OnReadAndWrite) : 읽고 쓸때 업데이트
        # disable_cleanup_in_background() : background에서 
        state_ttl_config = StateTtlConfig.new_builder(Time.seconds(1))\
            .set_update_type(StateTtlConfig.UpdateType.OnReadAndWrite)\
            .disable_cleanup_in_background()\
            .build()
        state_descriptor.enable_time_to_live(state_ttl_config)
        self.state = runtime_context.get_state(state_descriptor)

    # process_element는 제너레이터기 때문에 yield를 해줄 수 있다.
    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        current = self.state.value()
        if current is None:
            current = 0
        
        current += value[1]
        self.state.update(current)
        yield value[0], current
# [k, v] -> process_element[k , v] -> [k, v]

env = StreamExecutionEnvironment.get_execution_environment()

ds = env.from_collection(
    collection=[
        ("Alice", 110.1),
        ("Bob", 30.1),
        ("Alice", 20.1),
        ("Bob", 53.1),
        ("Alice", 13.1),
        ("Bob", 3.1),
        ("Bob", 16.1),
        ("Alice", 20.1),
    ],
    type_info=Types.TUPLE([Types.STRING(), Types.FLOAT()])
)

# key_by함수를 이용해 keyed stream을 만들면 process함수를 사용할 수 있다.
# process함수 안에는 사용자가 정의하는 process func가 들어간다.
ds.key_by(lambda x: x[0]).process(Sum()).print() # print sink
env.execute()