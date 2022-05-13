#
# 결제 내용을 보내는 프로듀서
#
#

import datetime
import time
import json
import pytz
import random
from kafka import KafkaProducer


topic_name = "payments"
brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]

def get_time_data():
    utc_now = pytz.utc.localize(datetime.datetime.utcnow())
    seoul_now = utc_now.astimezone(pytz.timezone("Asia/Seoul"))

    date = seoul_now.strftime("%Y-%m-%d")
    time = seoul_now.strftime("%H:%M:%S")

    return date, time

# 결제 내용을 만들어 주는 함수
def get_random_payments():
    method = random.choice(["CREDITCARD", "BITCOIN", "ACCOUNT", "NAVERPAY", "KAKAOPAY", "TOSS"]) 
    to = random.choice(["mom", "dad", "friend", "stranger"])
    amount = random.randint(1, 300)

    return method, to, amount

def payments():
    d, t = get_time_data()
    method, to, amount = get_random_payments()

    data = {
        "date" : d,
        "time" : t,
        "method" : method, 
        "to" : to,
        "amount" : amount
    }

    return data


# 프로듀서 생성
producer = KafkaProducer(bootstrap_servers=brokers)

# 30초간 데이터 보내기
t_end = time.time() + 30
while time.time() < t_end :
    data = payments()
    producer.send(topic_name, json.dumps(data).encode("utf-8"))
    print("거래 승인 ", data)
    time.sleep(1)