# 트립 데이터 시뮬레이션
# trips 프로듀서 만들기

import time
from kafka import KafkaProducer


TAXI_TRIPS_TOPIC = "taxi-trips"
producer = KafkaProducer(bootstrap_servers=["localhost:9092"])

with open("./trips/yellow_tripdata_2021-01.csv", "r") as f:
    next(f) # 헤더 스킵
    for row in f:
        print(row)
        producer.send(TAXI_TRIPS_TOPIC, row.encode("utf-8"))
        time.sleep(1) # 시뮬레이션이기 때문에 데이터가 계속해서 들어오는것으로 설정

# 종료되면 producer 종료
producer.flush()
