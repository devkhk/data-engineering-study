# Fraud Detection : 컨슈머와 프로듀서를 응용해 비정상 거래를 탐지하는 마이크로서비스
# 특징 : 프로듀서와 컨슈머가 공존 하는 프로그램
# 
# 프로듀서 : 가상으로 거래 정보를 만들어 브로커에게 보내준다.
#
#

from kafka import KafkaProducer
import datetime
import time
import pytz
import random
import json

TOPIC_NAME = "payments"
brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]
producer = KafkaProducer(bootstrap_servers=brokers)

def get_time_data():
    utc_now = pytz.utc.localize(datetime.datetime.utcnow())
    kst_now = utc_now.astimezone(pytz.timezone("ASIA/Seoul"))
    d = kst_now.strftime("%m/%d/%Y")
    t = kst_now.strftime("%H:%M:%S")

    return d, t

def generate_payment_data():
    payment_type = random.choice(["VISA", "MASTERCARD", "BITCOIN"])
    amount = random.randint(0, 100)
    to = random.choice(["me", "mom", "dad", "friend", "stranger"])

    return payment_type, amount, to

while True:
    d, t = get_time_data()
    payment_type, amount, to = generate_payment_data()

    data = {
        'DATE' : d,
        'TIME' : t,
        'PAYMENT_TYPE' : payment_type,
        'AMOUNT' : amount,
        'TO' : to
    }

    producer.send(TOPIC_NAME, json.dumps(data).encode("utf-8"))
    print(data)
    time.sleep(1)