# 비정상 거래로 확인되어 들어오는 데이터

from kafka import KafkaConsumer
import json

TOPIC = "fraud_payments"

brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]
consumer = KafkaConsumer(TOPIC, bootstrap_servers=brokers) # consumer

for messages in consumer:
    message = json.loads(messages.value.decode("utf-8"))

    # data = {
    #     "date" : d,
    #     "time" : t,
    #     "method" : method, 
    #     "to" : to,
    #     "amount" : amount
    # }

    date = message['date']
    time = message['time']
    method = message['method']
    to = message['to']
    amount = message['amount']

    msg = "의심 정황 목록 :"

    if method == "BITCOIN":
        msg += " 비트코인 거래 /"
    
    if to == "stranger":
        msg += " 미등록 계좌와 거래"

    print(f"비정상 거래로 의심됩니다. / {date} {time} / 거래승인{method} /{to}->{amount} ")
    print(msg)
    # code ...
