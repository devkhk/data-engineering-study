# 3개의 브로커를 사용할 수 있는 프로듀서

from kafka import KafkaProducer

brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]
topic_name = "first-cluster-topic"

producer = KafkaProducer(bootstrap_servers=brokers)

producer.send(topic_name, b"hello cluster world")

# 버퍼 클리어
producer.flush()