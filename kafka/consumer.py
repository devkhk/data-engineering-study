# 파이썬으로 컨슈머 만들기

from kafka import KafkaConsumer

consumer = KafkaConsumer('first-topic', bootstrap_servers=['localhost:9092'])

# 컨슈머는 파이썬의 제너레이터 형태이기 때문에 무한 루프가 가능하다.
for message in consumer: # 컨슈머에 메시지가 도착하면 메시지를 계속해서 프린트 해줄 수 있다.
    print(message)