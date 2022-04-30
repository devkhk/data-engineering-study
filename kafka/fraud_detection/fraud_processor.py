# 
# fraud detector 모듈에서 걸러 받은 비트코인 결제 내역을 받는 컨슈머
# 비트코인 결제에서 이상이 있는 경우 알림을 보내는 모듈
# 

from kafka import KafkaConsumer
import json

FRAUD_TOPIC = "fraud_payments"

brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]
consumer = KafkaConsumer(FRAUD_TOPIC, bootstrap_servers=brokers)

#   data = {
#     'DATE' : d,
#     'TIME' : t,
#     'PAYMENT_TYPE' : payment_type,
#     'AMOUNT' : amount,
#     'TO' : to
# }
for message in consumer:
    msg = json.loads(message.value.decode())
    to = msg["TO"]
    amount = msg["AMOUNT"]
    if to == "stranger":
        print(f"[ALERT] fraud detected payment to : {to} - {amount}" )
    else:
        print(f"[PROCESSING BITCOIN] payment to : {to} - {amount}")
    


