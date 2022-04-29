#
# trips json 데이터를 읽는 consumer
#

from kafka import KafkaConsumer
import json

brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]
topic_name = "trips"

consumer = KafkaConsumer(topic_name, bootstrap_servers=brokers)

# consumer header
# ['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count',\
# 'trip_distance', 'RatecodeID', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID', \
# 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', \
# 'improvement_surcharge', 'total_amount', 'congestion_surcharge']

# 인코딩을 디코드 해줘야 한다.
for message in consumer:
    row = json.loads(message.value.decode())
    # fare amount가 10불 이상인 경우 알람
    if float(row[10]) > 10: 
        # print("-- over 10 --")
        print(f"payment_type : {row[9]} - fare_amount : {row[10]}")