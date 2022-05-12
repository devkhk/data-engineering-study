#
# kafka producer - json
#

from kafka import KafkaProducer
import json

# Spark,1
# Airflow,2
# Kafka,3
# Flink,4

data = [
    {"framework":"Spark", "chapter":"1"},
    {"framework":"Airflow", "chapter":"2"},
    {"framework":"Kafka", "chapter":"3"},
    {"framework":"Flink", "chapter":"4"},
]

brokers = ["localhost:9092"]
producer = KafkaProducer(bootstrap_servers=brokers)

for d in data:
    producer.send("example-source", json.dumps(d).encode("utf-8"))
producer.flush()