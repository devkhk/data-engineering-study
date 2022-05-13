# 정상 거래로 확인되어 들어오는 데이터

from kafka import KafkaConsumer
import json

TOPIC = "legit_payments"

brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]
consumer = KafkaConsumer(TOPIC, bootstrap_servers=brokers) # consumer

for messages in consumer:
    print("정상 거래로 등록 " , json.loads(messages.value.decode("utf-8")))
    # code ...