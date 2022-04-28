# 파이썬으로 프로듀서 만들기

from kafka import KafkaProducer

# 프로듀서를 인스턴스화 해서 프로듀서를 만들 수 있다.

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

producer.send('first-topic',b'hello world from python') # b => 바이트 메시지
producer.flush()