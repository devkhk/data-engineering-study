#
# [ 수상한 결제를 감지하는 Detector 모듈 ]
#
# 비트코인 결제는 수상한 것으로 판단하고 fraud_payments 토픽으로 보내고 
# 나머지는 정상 결제로 legit_payments 토픽으로 메시지를 보낸다. 
# 

from kafka import KafkaConsumer
from kafka import KafkaProducer
import json


PAYMENT_TOPIC = "payments"
FRAUD_TOPIC = "fraud_payments"
LEGIT_TOPIC = "legit_payments"

brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]

consumer = KafkaConsumer(PAYMENT_TOPIC, bootstrap_servers=brokers)
producer = KafkaProducer(bootstrap_servers=brokers)

def is_suspicious(transations):
    # and transations["TO"] == "stranger"
    if transations["PAYMENT_TYPE"] == "BITCOIN":
        return True
    return False

for message in consumer :
    # {'DATE': '04/30/2022', 'TIME': '21:57:46', 'PAYMENT_TYPE': 'BITCOIN', 'AMOUNT': 100, 'TO': 'me'}
    msg = json.loads(message.value.decode())

    # true가 수상한 결제 내역(비트코인), false 정상 결제내역 
    topic = FRAUD_TOPIC if is_suspicious(msg) else LEGIT_TOPIC
    producer.send(topic, value=json.dumps(msg).encode("utf-8"))
    print(topic, is_suspicious(msg), msg["PAYMENT_TYPE"])