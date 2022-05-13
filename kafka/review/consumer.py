# 카프카 기본 컨슈머 생성 복습

from kafka import KafkaConsumer
import json

brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]
topic_name = "trips"

consumer = KafkaConsumer(topic_name, bootstrap_servers=brokers)

# 무한 루프
for c in consumer :
    message = json.loads(c.value.decode("utf-8"))
    
    # trip_distance > 5km 알람
    if float(message[4]) >= 5:
        print("trip_distance over 5km")
    print(message)

