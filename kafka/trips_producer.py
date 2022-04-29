#
# 뉴욕 yellow_trip_data csv파일을 스트림으로 바꿔주는 프로듀서
#

from kafka import KafkaProducer
import csv
import json
import time

brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]
producer = KafkaProducer(bootstrap_servers=brokers)
topic_name = "trips"

with open("./trips/yellow_tripdata_2021-01.csv", "r") as file:
    reader = csv.reader(file)
    headings = next(reader) # csv.reader 가 제너레이터기 때문에 첫줄을 읽으면 그게 헤더가 된다.

    # 그 다음 줄 부터는 모두 row
    for row in reader:
        # 1, 2, 3 = > [1, 2, 3] 
        producer.send(topic_name, json.dumps(row).encode("utf-8"))
        print(row)
        time.sleep(1)