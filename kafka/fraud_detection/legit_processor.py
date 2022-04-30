#
# 정상 데이터 처리를 위한 컨슈머 모듈
#

from kafka import KafkaConsumer
import json

LEGIT_TOPIC = "legit_payments"

brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]
consumer = KafkaConsumer(LEGIT_TOPIC, bootstrap_servers=brokers)

for message in consumer:
    msg = json.loads(message.value.decode())
    to = msg["TO"]
    amount = msg["AMOUNT"]
    payment_type = msg["PAYMENT_TYPE"]

    if payment_type == "VISA":
        print(f"[VISA]  payment to : {to} - {amount}")
    elif payment_type == "MASTERCARD":
        print(f"[MASTERCARD]  payment to : {to} - {amount}")
    else:
        print("[ALERT] unable to process payments")

    
