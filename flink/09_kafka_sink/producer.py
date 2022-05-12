#
# 플링크에 데이터를 전달하기 위한 kafka producer
#

from kafka import KafkaProducer

brokers = ["localhost:9092"]
producer = KafkaProducer(bootstrap_servers=brokers)

producer.send("example-source", b"testing out flink and kafka!")
producer.flush()