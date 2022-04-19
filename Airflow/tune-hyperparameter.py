#
# 가공된 학습용 데이터를 불러와 Hyperparameter Tuning 
# 튜닝된 hyperparameter 정보를 csv 파이로 저장
# 로그 확인을 위해 print 함수를 사용
#

from pyspark.sql import SparkSession
from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml import Pipeline
from pyspark.ml.regression import LinearRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator

import pandas as pd

MAX_MEMORY = "5g"
spark = SparkSession.builder.appName("taxi-fare-prediction")\
            .config("spark.executor.memory", MAX_MEMORY)\
            .config("spark.driver.memory", MAX_MEMORY)\
            .getOrCreate()

data_dir = "/Users/devkhk/Documents/data-engineering-study/airflow/data/"

# 저장된 parquet 불러오기
train_df = spark.read.parquet(f"{data_dir}/train/")

# toy_df 샘플링
toy_df = train_df.sample(False, fraction=0.1,seed=1)

# stages 만들기
# feature => 카테고리 / 넘버릭 구분
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

# Hyperparameter Tunig
lr = LinearRegression(
    maxIter=30,
    solver="normal",
    labelCol="total_amount",
    featuresCol="feature_vector"
)

cv_stages = stages + [lr] # CrossValidator_stages

cv_pipeline = Pipeline(stages=cv_stages)
param_grid = ParamGridBuilder()\
                .addGrid(lr.elasticNetParam, [0.1, 0.2, 0.3,])\
                .addGrid(lr.regParam, [0.01, 0.02, 0.03,])\
                .build()

# crossValidator를 실행할 인스턴스 생성
cross_val = CrossValidator( estimator=cv_pipeline,
                            estimatorParamMaps=param_grid,
                            evaluator=RegressionEvaluator(labelCol="total_amount"),
                            numFolds=5
                          )

cv_model = cross_val.fit(toy_df)
alpha = cv_model.bestModel.stages[-1]._java_obj.getElasticNetParam()
reg_param = cv_model.bestModel.stages[-1]._java_obj.getRegParam()

# 하이퍼 파라미터 결과값 csv 파일로 저장
hyperparam = {
    "alpha" : [alpha],
    "reg_param" : [reg_param]
}

hyper_df = pd.DataFrame(hyperparam).to_csv(f"{data_dir}hyperparameter.csv")
print(hyper_df) # 로그 확인용