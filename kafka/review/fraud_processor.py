# 비정상 거래로 확인되어 들어오는 데이터

from kafka import KafkaConsumer
from fraud_alarmbot import post_slack_message
import json

TOPIC = "fraud_payments"

brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]
consumer = KafkaConsumer(TOPIC, bootstrap_servers=brokers) # consumer

for messages in consumer:
    message = json.loads(messages.value.decode("utf-8"))

    date = message['date']
    time = message['time']
    method = message['method']
    to = message['to']
    amount = message['amount']

    msg = "의심 정황 목록 :"

    if method == "BITCOIN":
        msg += " 비트코인 거래 /"
    
    if to == "stranger":
        msg += " 미등록 계좌와 거래 /"

    slack_message = f"""
        비정상 거래로 의심되는 알람 / {date} {time} / 거래승인 {method} /{to}->{amount}\n
        {msg}
    """
    print(f"비정상 거래로 의심됩니다. / {date} {time} / 거래승인{method} /{to}->{amount} ")
    print(msg)
    post_slack_message("#이상거래-알림봇", slack_message)

