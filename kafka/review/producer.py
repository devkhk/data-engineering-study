# 카프카 기본 프로듀서 생성 복습

from kafka import KafkaProducer
import csv
import json
import time

brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]
producer = KafkaProducer(bootstrap_servers=brokers)
topic_name = "trips"

with open("../trips/yellow_tripdata_2021-01.csv", "r") as f:
    reader = csv.reader(f)
    header = next(reader)

    for row in reader:
        producer.send(topic_name,json.dumps(row).encode("utf-8"))
        print(row)
        time.sleep(1)

