#
# 하이퍼 파라미터를 불러와 모델을 트레이닝하고 트레이닝된 모델을 저장한다.
#

from pyspark.sql import SparkSession
from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml import Pipeline
from pyspark.ml.regression import LinearRegression

import pandas as pd

MAX_MEMORY = "5g"
spark = SparkSession.builder.appName("taxi-fare-prediction")\
            .config("spark.executor.memory", MAX_MEMORY)\
            .config("spark.driver.memory", MAX_MEMORY)\
            .getOrCreate()


# 저장된 분석 데이터 불러오기
data_dir = "/Users/devkhk/Documents/data-engineering-study/airflow/data/"
train_df = spark.read.parquet(f"{data_dir}/train/")
test_df = spark.read.parquet(f"{data_dir}/test/")

# 저장된 하이퍼 튜닝 데이터 불러오고 alpha, reg_param 변수 저장.
hyper_df = pd.read_csv(f"{data_dir}hyperparameter.csv")
alpha = float(hyper_df.iloc[0]['alpha'])
reg_param = float(hyper_df.iloc[0]['reg_param'])

# 튜닝했던 것처럼 파이프라인 설계는 같고, sample데이터가 아닌 실제 train 데이터를 넣어 모델을 학습시킨다.
stages = []

cat_feats = [
    "pickup_location_id",
    "dropoff_location_id",
    "day_of_week"
]

for c in cat_feats:
    cat_indexer = StringIndexer(inputCol=c, outputCol= c + "_idx").setHandleInvalid("keep")
    onehot_encoder = OneHotEncoder(inputCols=[cat_indexer.getOutputCol()], outputCols=[c + "_onehot"])
    stages += [cat_indexer, onehot_encoder]


num_feats = [
    "passenger_count",
    "trip_distance",
    "pickup_time"
]

for n in num_feats:
    num_assembler = VectorAssembler(inputCols=[n], outputCol= n + "_vector")
    num_scalar = StandardScaler(inputCol=num_assembler.getOutputCol(), outputCol= n + "_scaled")
    stages += [num_assembler, num_scalar]

assembler_input = [c + "_onehot" for c in cat_feats] + [n + "_scaled" for n in num_feats]
assembler = VectorAssembler(inputCols=assembler_input, outputCol="feature_vector")
stages += [assembler]

# Training
transform_stages = stages
pipeline = Pipeline(stages=transform_stages)
fitted_transformer = pipeline.fit(train_df)

vtrain_df = fitted_transformer.transform(train_df)

# 튜닝해 불러온 reg_param과 alpha를 추가 해준다.
lr = LinearRegression(
    maxIter=30,
    solver="normal",
    labelCol="total_amount",
    featuresCol="feature_vector",
    elasticNetParam=alpha,
    regParam=reg_param
)

# 모델 학습
model = lr.fit(vtrain_df)

# test_df 또한 파이프라인을 통과해야한다.
vtest_df = fitted_transformer.transform(test_df)
predictions = model.transform(vtest_df)

predictions.cache()
predictions.select(["day_of_week","trip_distance", "total_amount", "prediction"]).show()

# 학습 모델을 저장한다.
model_dir = "/Users/devkhk/Documents/data-engineering-study/airflow/data/model/"
model.write().overwrite().save(model_dir)