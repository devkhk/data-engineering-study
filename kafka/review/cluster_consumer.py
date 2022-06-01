import json
import logging
import time
import traceback
from kafka import KafkaConsumer
from kafka.structs import TopicPartition

brokers = ["localhost:9092"]
TOPIC = "korean-tweets"

consumer = KafkaConsumer(
                         bootstrap_servers=brokers,
                         group_id = "parser2",
                         value_deserializer=lambda v: json.loads(v.decode('utf-8')),
)

# 파티션 모두 가지고 오기
partitions = []
for pt in consumer.partitions_for_topic(TOPIC):
    partition = TopicPartition(TOPIC, pt)
    partitions.append(partition)

# consumer topic 파티션 설정
consumer.assign(partitions=partitions)

# offset 설정
# consumer.seek(TopicPartition(TOPIC, 0), 5)
# consumer.seek(TopicPartition(TOPIC, 0), 7)

# off-set 초기화
# consumer.seek_to_beginning(*partitions)

# consumer poll을 사용하는 방법
try:
    while True:
        # 여기에서 poll을 한다. 
        max_records = 10

        for _, records in consumer.poll(max_records=max_records).items():
            for record in records:
                try:
                    print(record.value)
                except:
                    logging.error(traceback.format_exc())
                consumer.commit_async()
                # time.sleep(0.5)
finally:
    consumer.commit()
    consumer.close()


# for message in consumer:
#     print(message.value)
