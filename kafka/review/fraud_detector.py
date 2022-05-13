#
# 이상 거래 탐지 마이크로 서비스 
# 
# 카프카에 들어오는 메시지 중 이상 거래가 의심되는 메시지를 확인해 알림을 보낸다.
# 컨슈머로 카프카에 들어오는 데이터를 확인하고, 확인된 데이터를 분류된 카프카의 토픽으로 전송한다.

from kafka import KafkaConsumer
from kafka import KafkaProducer
import json

PAYMENTS_TOPIC = "payments"
FRAUD_TOPIC = "fraud_payments"
LEGIT_TOPIC = "legit_payments"

brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]
consumer = KafkaConsumer(PAYMENTS_TOPIC, bootstrap_servers=brokers) # consumer
producer = KafkaProducer(bootstrap_servers=brokers) # producer


def is_suspicious(message):
    if message['method'] == "BITCOIN" or message['to'] == "stranger":
        return True
    return False

# 의심 여부에 따른 카프카 토픽을 변경해 전송.
for messages in consumer:
    message = json.loads(messages.value.decode("utf-8"))
    # message => {'date': '2022-05-13', 'time': '16:45:54', 'method': 'ACCOUNT', 'to': 'dad', 'amount': 210}

    fraud = is_suspicious(message)

    topic_name = FRAUD_TOPIC if fraud else LEGIT_TOPIC
    producer.send(topic_name, json.dumps(message).encode("utf-8"))
    
    if fraud:
        print("의심스러운 거래 확인", message)
    else:
        print("정상 거래 확인", message)