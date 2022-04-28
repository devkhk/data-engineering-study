# 만들어둔 클러스터를 사용할 수 있는 컨슈머

from kafka import KafkaConsumer

brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]

consumer = KafkaConsumer("first-cluster-topic", bootstrap_servers=brokers)

for message in consumer:
    print(message)
