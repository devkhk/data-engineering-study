# data-engineering-study
### 데이터 엔지니어링 학습 포트폴리오

## 학습 목표
- ETL 방식에서 ELT 흐름으로 넘어가는 모던 데이터 엔지니어링 아키텍쳐 이해
- 배치 파이프라인과 스트림 파이프라인을 동시에 사용하는 ML 데이터 학습 & 서빙 파이프라인 설계

## 학습 내용
- **Spark** : 데이터 병렬-분산 처리
- **Airflow** : 데이터 오케스트레이션
- **Kafka** : 이벤트 스트리밍
- **Flink** : 분산 스트림 프로세싱

## 학습 아키텍쳐
<img src="/images/architecture.png" width="900" height="400">

## Spark
- 데이터 분석
- Data Preprocessing
- Hyper Parmeter 파이프라인
- ML 예측 모델 학습 파이프라인    

<p align="center">
  데이터 분석 - 날짜별 택시 이용
  <br>
  <img src="/images/pickup_date.png" width="900" height="300">
</p>
<p align="center">
  데이터 분석 - 요일별 택시 이용
  <br>
  <img src="/images/pickup_weeks.png" width="900" height="300">
</p>
<p align="center">
  학습 모델 예측 결과 값
  <br>
  <img src="/images/prediction.png" alt="text"/>
</p>

## Airflow
- Data Preprocessing -> Train/Test 데이터 저장
- Hyper Parameter 학습 -> 파라미터 csv 파일로 저장
- Train Model -> 학습 된 모델을 저장
- 위 과정을 에어플로우 DAG의 작업화(Task) 하고 의존성 추가