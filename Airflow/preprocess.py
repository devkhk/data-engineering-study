#
# 저장된 데이터를 pyspark로 필요한 정보 가공
# Train/Test 데이터를 나눠서 airflow/data/ 저장
#

from pyspark.sql import SparkSession

# parquet 압축 코덱 선택, defalut:"snappy"
MAX_MEMORY = "5g"
spark = SparkSession.builder.appName("taxi-fare-prediction")\
            .config("spark.executor.memory", MAX_MEMORY)\
            .config("spark.driver.memory", MAX_MEMORY)\
            .config("spark.sql.parquet.compression.codec", None)\
            .getOrCreate()

trip_files = "/Users/devkhk/Documents/data-engineering-study/data/trips/*"
trips_df = spark.read.csv(f"file:///{trip_files}", inferSchema=True, header=True)

trips_df.createOrReplaceTempView("trips")
query = """
SELECT
    passenger_count,
    PULocationID as pickup_location_id,
    DOLocationID as dropoff_location_id,
    trip_distance,
    HOUR(tpep_pickup_datetime) as pickup_time,
    DATE_FORMAT(TO_DATE(tpep_pickup_datetime), 'EEEE') as day_of_week,
    total_amount
FROM
    trips
WHERE
    total_amount < 5000
    AND total_amount > 0
    AND trip_distance > 0
    AND trip_distance < 500
    AND passenger_count < 4
    AND TO_DATE(tpep_pickup_datetime) >= '2021-01-01'
    AND TO_DATE(tpep_pickup_datetime) < '2021-08-01'
"""
data_df = spark.sql(query)

train_df, test_df = data_df.randomSplit([.8, .2], seed=1)
data_dir = "/Users/devkhk/Documents/data-engineering-study/airflow/data/"
train_df.write.format("parquet").mode('overwrite').save(f"{data_dir}/train/")
test_df.write.format("parquet").mode('overwrite').save(f"{data_dir}/test/")
