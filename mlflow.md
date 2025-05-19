## Azure Databricks에서 MLflow 사용하기

이 실습에서는 Azure Databricks에서 MLflow를 사용하여 머신러닝 모델을 학습시키고 배포하는 방법을 탐색합니다.

이 실습을 완료하는 데 약 45분이 소요됩니다.

**참고:** Azure Databricks 사용자 인터페이스는 지속적으로 개선되고 있습니다. 이 실습의 지침이 작성된 이후 사용자 인터페이스가 변경되었을 수 있습니다.

### 시작하기 전에

관리자 수준 액세스 권한이 있는 Azure 구독이 필요합니다.

### Azure Databricks Workspace 프로비저닝하기

**참고:** 이 실습에서는 모델 서빙(model serving)이 지원되는 지역의 **Premium** Azure Databricks workspace가 필요합니다. 지역별 Azure Databricks 기능에 대한 자세한 내용은 [Azure Databricks 지역](https://azure.microsoft.com/global-infrastructure/services/?products=databricks)을 참조하십시오. 적합한 지역에 이미 Premium 또는 평가판 Azure Databricks workspace가 있는 경우 이 절차를 건너뛰고 기존 workspace를 사용할 수 있습니다.

이 실습에는 새 Azure Databricks workspace를 프로비저닝하는 스크립트가 포함되어 있습니다. 이 스크립트는 Azure 구독에서 이 실습에 필요한 컴퓨팅 코어에 대한 충분한 할당량이 있는 지역에 Premium 계층 Azure Databricks workspace 리소스를 만들려고 시도합니다. 또한 사용자 계정에 구독에서 Azure Databricks workspace 리소스를 만들 수 있는 충분한 권한이 있다고 가정합니다. 할당량 또는 권한 부족으로 스크립트가 실패하는 경우 Azure Portal에서 대화형으로 Azure Databricks workspace를 만들어 볼 수 있습니다.

1.  웹 브라우저에서 Azure Portal(https://portal.azure.com)에 로그인합니다.
2.  페이지 상단의 검색 창 오른쪽에 있는 **[>_]** 버튼을 사용하여 Azure Portal에서 새 Cloud Shell을 만들고, **PowerShell** 환경을 선택합니다. Cloud Shell은 다음 그림과 같이 Azure Portal 하단 창에 명령줄 인터페이스를 제공합니다.

    *(이미지: Azure portal with a cloud shell pane)*

    **참고:** 이전에 Bash 환경을 사용하는 Cloud Shell을 만든 경우 PowerShell로 전환하십시오.

    Cloud Shell 창 상단의 구분선을 드래그하거나 창 오른쪽 상단의 —, ⤢, X 아이콘을 사용하여 창 크기를 조정하거나 최소화, 최대화, 닫을 수 있습니다. Azure Cloud Shell 사용에 대한 자세한 내용은 [Azure Cloud Shell 설명서](https://docs.microsoft.com/azure/cloud-shell/overview)를 참조하십시오.

3.  PowerShell 창에서 다음 명령을 입력하여 이 저장소를 복제합니다.

    ```powershell
    rm -r mslearn-databricks -f
    git clone https://github.com/MicrosoftLearning/mslearn-databricks
    ```
    *   `rm -r mslearn-databricks -f`: 기존에 `mslearn-databricks` 디렉토리가 있다면 `-r` (recursive, 하위 디렉토리 포함) 옵션과 `-f` (force, 강제) 옵션을 사용하여 삭제합니다.
    *   `git clone ...`: GitHub에서 `mslearn-databricks`라는 공개 저장소를 현재 디렉토리로 복제합니다. 이 저장소에는 실습에 필요한 파일들이 들어있습니다.

4.  저장소가 복제된 후 다음 명령을 입력하여 `setup.ps1` 스크립트를 실행합니다. 이 스크립트는 사용 가능한 지역에 Azure Databricks workspace를 프로비저닝합니다.

    ```powershell
    ./mslearn-databricks/setup.ps1
    ```
    *   `./mslearn-databricks/setup.ps1`: 복제된 `mslearn-databricks` 디렉토리 내의 `setup.ps1` PowerShell 스크립트를 실행합니다. 이 스크립트는 Azure Databricks workspace를 자동으로 생성해줍니다.

5.  메시지가 표시되면 사용할 구독을 선택합니다(여러 Azure 구독에 액세스할 수 있는 경우에만 해당).
6.  스크립트가 완료될 때까지 기다립니다. 일반적으로 약 5분 정도 걸리지만 경우에 따라 더 오래 걸릴 수 있습니다. 기다리는 동안 Azure Databricks 설명서의 [MLflow 가이드](https://docs.databricks.com/mlflow/index.html) 문서를 검토하십시오.

### Cluster 생성하기

Azure Databricks는 Apache Spark cluster를 사용하여 여러 노드에서 데이터를 병렬로 처리하는 분산 처리 플랫폼입니다. 각 cluster는 작업을 조정하는 드라이버 노드(driver node)와 처리 작업을 수행하는 작업자 노드(worker nodes)로 구성됩니다. 이 실습에서는 리소스가 제한될 수 있는 랩 환경에서 사용되는 컴퓨팅 리소스를 최소화하기 위해 단일 노드 cluster(single-node cluster)를 만듭니다. 프로덕션 환경에서는 일반적으로 여러 작업자 노드가 있는 cluster를 만듭니다.

**팁:** Azure Databricks workspace에 13.3 LTS ML 이상의 런타임 버전을 사용하는 cluster가 이미 있는 경우 해당 cluster를 사용하여 이 실습을 완료하고 이 절차를 건너뛸 수 있습니다.

1.  Azure Portal에서 스크립트에 의해 생성된 `msl-xxxxxxx` 리소스 그룹(또는 기존 Azure Databricks workspace가 포함된 리소스 그룹)으로 이동합니다.
2.  Azure Databricks Service 리소스(설정 스크립트를 사용하여 만든 경우 `databricks-xxxxxxx`로 이름 지정됨)를 선택합니다.
3.  workspace의 **Overview** 페이지에서 **Launch Workspace** 버튼을 사용하여 새 브라우저 탭에서 Azure Databricks workspace를 엽니다. 메시지가 표시되면 로그인합니다.

    **팁:** Databricks Workspace 포털을 사용하면 다양한 팁과 알림이 표시될 수 있습니다. 이를 무시하고 제공된 지침에 따라 이 실습의 작업을 완료하십시오.

4.  왼쪽 사이드바에서 **(+) New** 작업을 선택한 다음 **Cluster**를 선택합니다.
5.  **New Cluster** 페이지에서 다음 설정으로 새 cluster를 만듭니다.
    *   **Cluster name**: *사용자 이름*'s cluster (기본 cluster 이름)
    *   **Policy**: Unrestricted
    *   **Cluster mode**: Single Node
    *   **Access mode**: Single user (사용자 계정 선택됨)
    *   **Databricks runtime version**: 다음 조건을 충족하는 최신 비-베타 버전의 **ML** 에디션 선택 (Standard 런타임 버전 아님):
        *   GPU를 사용하지 않음
        *   Scala > 2.11 포함
        *   Spark > 3.4 포함
        *   *(설명: ML 런타임에는 머신러닝에 필요한 라이브러리들이 미리 설치되어 있어 편리합니다.)*
    *   **Use Photon Acceleration**: 선택 해제
        *   *(설명: Photon은 Databricks의 고성능 쿼리 엔진이지만, 이 실습에서는 필요하지 않습니다.)*
    *   **Node type**: Standard\_D4ds\_v5
    *   **Terminate after 20 minutes of inactivity**: 20분 동안 비활성 상태이면 종료
6.  Cluster가 생성될 때까지 기다립니다. 1~2분 정도 걸릴 수 있습니다.

    **참고:** Cluster 시작에 실패하는 경우 Azure Databricks workspace가 프로비저닝된 지역에서 구독의 할당량이 부족할 수 있습니다. 자세한 내용은 [CPU 코어 제한으로 인한 cluster 생성 불가](https://docs.microsoft.com/azure/databricks/kb/clusters/azure-core-limit)를 참조하십시오. 이 경우 workspace를 삭제하고 다른 지역에 새 workspace를 만들어 볼 수 있습니다. `./mslearn-databricks/setup.ps1 eastus`와 같이 설정 스크립트에 대한 매개변수로 지역을 지정할 수 있습니다.

### Notebook 생성하기

Spark MLLib 라이브러리를 사용하여 머신러닝 모델을 학습시키는 코드를 실행할 것이므로 첫 번째 단계는 workspace에 새 notebook을 만드는 것입니다.

1.  사이드바에서 **(+) New** 링크를 사용하여 **Notebook**을 만듭니다.
2.  기본 notebook 이름(`Untitled Notebook [날짜]`)을 **MLflow**로 변경하고 **Connect** 드롭다운 목록에서 cluster가 아직 선택되지 않은 경우 cluster를 선택합니다. Cluster가 실행 중이 아니면 시작하는 데 1분 정도 걸릴 수 있습니다.

### 데이터 수집 및 준비하기

이 실습의 시나리오는 남극 펭귄 관찰을 기반으로 하며, 관찰된 펭귄의 위치와 신체 측정값을 기반으로 펭귄 종을 예측하는 머신러닝 모델을 학습시키는 것을 목표로 합니다.

**인용:** 이 실습에 사용된 펭귄 데이터 세트는 Kristen Gorman 박사와 Long Term Ecological Research Network의 회원인 Palmer Station, Antarctica LTER에서 수집하고 제공한 데이터의 일부입니다.

1.  Notebook의 첫 번째 셀에 다음 코드를 입력합니다. 이 코드는 셸 명령을 사용하여 GitHub에서 펭귄 데이터를 cluster에서 사용하는 파일 시스템으로 다운로드합니다.

    ```python
    %sh
    rm -r /dbfs/mlflow_lab -f
    mkdir /dbfs/mlflow_lab
    wget -O /dbfs/mlflow_lab/penguins.csv https://raw.githubusercontent.com/MicrosoftLearning/mslearn-databricks/main/data/penguins.csv
    ```
    *   `%sh`: 이 셀의 코드를 셸(shell) 명령으로 실행하도록 하는 Databricks 매직 명령어입니다.
    *   `rm -r /dbfs/mlflow_lab -f`: `/dbfs/mlflow_lab` 디렉토리가 이미 존재하면 강제로 삭제합니다. `/dbfs`는 Databricks File System의 약자입니다.
    *   `mkdir /dbfs/mlflow_lab`: `/dbfs/mlflow_lab` 디렉토리를 생성합니다.
    *   `wget -O /dbfs/mlflow_lab/penguins.csv [URL]`: 지정된 URL에서 파일을 다운로드하여 `/dbfs/mlflow_lab/penguins.csv` 라는 이름으로 저장합니다.

2.  셀 왼쪽의 **▸ Run Cell** 메뉴 옵션을 사용하여 실행합니다. 그런 다음 코드로 실행된 Spark 작업이 완료될 때까지 기다립니다.

3.  이제 머신러닝을 위해 데이터를 준비합니다. 기존 코드 셀 아래에 **+** 아이콘을 사용하여 새 코드 셀을 추가합니다. 그런 다음 새 셀에 다음 코드를 입력하고 실행하여 다음을 수행합니다.
    *   불완전한 행 제거
    *   적절한 데이터 타입 적용
    *   데이터의 무작위 샘플 보기
    *   데이터를 두 개의 데이터 세트로 분할: 하나는 학습용(training), 다른 하나는 테스트용(testing).

    ```python
    from pyspark.sql.types import *
    from pyspark.sql.functions import *
       
    # CSV 파일을 DataFrame으로 읽어옵니다. header 옵션은 첫 번째 줄을 컬럼 이름으로 사용하도록 합니다.
    data = spark.read.format("csv").option("header", "true").load("/mlflow_lab/penguins.csv")
    
    # 결측치가 있는 행을 제거하고, 각 컬럼을 적절한 데이터 타입으로 변환합니다.
    data = data.dropna().select(col("Island").astype("string"),
                                col("CulmenLength").astype("float"), # 부리 길이
                                col("CulmenDepth").astype("float"),  # 부리 두께
                                col("FlipperLength").astype("float"),# 지느러미 길이
                                col("BodyMass").astype("float"),   # 몸무게
                                col("Species").astype("int")      # 펭귄 종 (목표 변수)
                              )
    # 데이터의 20%를 무작위로 샘플링하여 표시합니다.
    display(data.sample(0.2))
       
    # 데이터를 학습 데이터(70%)와 테스트 데이터(30%)로 무작위 분할합니다.
    splits = data.randomSplit([0.7, 0.3])
    train = splits[0]
    test = splits[1]
    print ("Training Rows:", train.count(), " Testing Rows:", test.count())
    ```
    *   `from pyspark.sql.types import *`: PySpark SQL의 데이터 타입을 사용하기 위해 `*` (모든 것)을 가져옵니다. (예: `StringType`, `FloatType`)
    *   `from pyspark.sql.functions import *`: PySpark SQL의 내장 함수들(예: `col`, `avg`, `sum`)을 사용하기 위해 가져옵니다.
    *   `spark.read.format("csv").option("header", "true").load(...)`: Spark 세션(`spark`)을 사용하여 CSV 파일을 읽습니다. `option("header", "true")`는 CSV 파일의 첫 번째 줄을 컬럼 이름으로 사용하도록 지정합니다. `load(...)`는 지정된 경로의 파일을 DataFrame으로 로드합니다.
    *   `dropna()`: DataFrame에서 하나 이상의 NULL 값을 포함하는 모든 행을 제거합니다.
    *   `select(col("ColumnName").astype("newType"))`: 특정 컬럼들을 선택하고, `astype()`을 사용하여 데이터 타입을 변환합니다. `col()` 함수는 컬럼 이름을 나타내는 객체를 반환합니다.
    *   `display()`: Databricks에서 DataFrame이나 시각화를 보기 좋게 표시하는 함수입니다.
    *   `data.sample(0.2)`: DataFrame `data`에서 약 20%의 데이터를 무작위로 추출합니다.
    *   `data.randomSplit([0.7, 0.3])`: 데이터를 지정된 비율(여기서는 70%와 30%)로 무작위 분할하여 두 개의 DataFrame 리스트로 반환합니다.
    *   `train.count()`, `test.count()`: 각 DataFrame의 행 수를 계산하여 출력합니다.

### MLflow 실험 실행하기

MLflow를 사용하면 모델 학습 프로세스를 추적하고 평가 메트릭을 기록하는 실험(experiments)을 실행할 수 있습니다. 모델 학습 실행의 세부 정보를 기록하는 이 기능은 효과적인 머신러닝 모델을 만드는 반복적인 프로세스에서 매우 유용할 수 있습니다.

일반적으로 모델을 학습하고 평가하는 데 사용하는 것과 동일한 라이브러리와 기술(이 경우 Spark MLLib 라이브러리 사용)을 사용할 수 있지만, 프로세스 중에 중요한 메트릭과 정보를 기록하기 위한 추가 명령이 포함된 MLflow 실험의 컨텍스트 내에서 수행합니다.

1.  새 셀을 추가하고 다음 코드를 입력합니다.

    ```python
    import mlflow
    import mlflow.spark # MLflow에서 Spark ML 모델을 로깅하기 위한 모듈
    from pyspark.ml import Pipeline # 여러 단계를 순차적으로 실행하는 파이프라인
    from pyspark.ml.feature import StringIndexer, VectorAssembler, MinMaxScaler # 특성 공학 도구들
    from pyspark.ml.classification import LogisticRegression # 분류 알고리즘
    from pyspark.ml.evaluation import MulticlassClassificationEvaluator # 다중 클래스 분류 평가 도구
    import time # 고유한 모델 이름을 생성하기 위해 사용
       
    # MLflow 실행(run) 시작
    with mlflow.start_run():
        catFeature = "Island" # 범주형 특성 이름
        numFeatures = ["CulmenLength", "CulmenDepth", "FlipperLength", "BodyMass"] # 수치형 특성 이름 리스트
         
        # 하이퍼파라미터 설정
        maxIterations = 5  # 로지스틱 회귀의 최대 반복 횟수
        regularization = 0.5 # 로지스틱 회귀의 정규화 파라미터
       
        # 특성 공학 및 모델 단계 정의
        # 1. StringIndexer: 범주형 특성 "Island"를 숫자 인덱스로 변환 ("IslandIdx" 컬럼 생성)
        catIndexer = StringIndexer(inputCol=catFeature, outputCol=catFeature + "Idx")
        # 2. VectorAssembler: 수치형 특성들을 단일 벡터 "numericFeatures"로 결합
        numVector = VectorAssembler(inputCols=numFeatures, outputCol="numericFeatures")
        # 3. MinMaxScaler: "numericFeatures" 벡터를 정규화하여 "normalizedFeatures" 컬럼 생성
        numScaler = MinMaxScaler(inputCol = numVector.getOutputCol(), outputCol="normalizedFeatures")
        # 4. VectorAssembler: 인덱싱된 범주형 특성과 정규화된 수치형 특성을 최종 "Features" 벡터로 결합
        featureVector = VectorAssembler(inputCols=["IslandIdx", "normalizedFeatures"], outputCol="Features")
        # 5. LogisticRegression: 로지스틱 회귀 모델 정의
        algo = LogisticRegression(labelCol="Species", featuresCol="Features", maxIter=maxIterations, regParam=regularization)
       
        # 단계들을 파이프라인(Pipeline)의 스테이지(stages)로 연결
        pipeline = Pipeline(stages=[catIndexer, numVector, numScaler, featureVector, algo])
       
        # 학습 파라미터 값 로깅
        print ("Training Logistic Regression model...")
        mlflow.log_param('maxIter', algo.getMaxIter()) # 최대 반복 횟수 로깅
        mlflow.log_param('regParam', algo.getRegParam()) # 정규화 파라미터 로깅
        
        # 파이프라인을 학습 데이터(train)에 적합(fit)시켜 모델 생성
        model = pipeline.fit(train)
          
        # 모델 평가 및 메트릭 로깅
        prediction = model.transform(test) # 테스트 데이터로 예측 수행
        metrics = ["accuracy", "weightedRecall", "weightedPrecision"] # 평가할 메트릭 리스트
        for metric in metrics:
            evaluator = MulticlassClassificationEvaluator(labelCol="Species", predictionCol="prediction", metricName=metric)
            metricValue = evaluator.evaluate(prediction) # 예측 결과 평가
            print("%s: %s" % (metric, metricValue))
            mlflow.log_metric(metric, metricValue) # 평가 메트릭 로깅
       
               
        # 모델 자체를 로깅
        unique_model_name = "classifier-" + str(time.time()) # 고유한 모델 이름 생성
        # MLflow에 Spark 모델을 로깅합니다. 모델, 아티팩트 경로 내 이름, conda 환경을 지정합니다.
        mlflow.spark.log_model(model, unique_model_name, mlflow.spark.get_default_conda_env())
        # DBFS 경로에 모델 저장 (선택 사항, log_model로도 충분히 MLflow에 기록됨)
        modelpath = "/model/%s" % (unique_model_name) 
        mlflow.spark.save_model(model, modelpath)
           
        print("Experiment run complete.")
    ```
    *   `import mlflow`: MLflow 라이브러리를 가져옵니다.
    *   `with mlflow.start_run():`: 새로운 MLflow 실행(run)을 시작합니다. 이 `with` 블록 내에서 기록되는 파라미터, 메트릭, 아티팩트 등은 이 특정 실행에 연결되어 관리됩니다.
    *   `StringIndexer(inputCol=..., outputCol=...)`: 문자열 값을 갖는 `inputCol`을 숫자 인덱스로 변환하여 `outputCol`에 저장합니다. 머신러닝 알고리즘은 보통 숫자 입력을 필요로 하므로 범주형 데이터를 변환할 때 사용됩니다.
    *   `VectorAssembler(inputCols=..., outputCol=...)`: `inputCols`에 지정된 여러 컬럼들을 하나의 벡터(vector)로 합쳐 `outputCol`에 저장합니다. Spark ML 알고리즘은 특성들을 단일 벡터 형태로 입력받는 경우가 많습니다.
    *   `MinMaxScaler(inputCol=..., outputCol=...)`: `inputCol`의 값들을 특정 범위(기본값 0~1)로 조정(scaling)하여 `outputCol`에 저장합니다. 특성 스케일링은 모델 성능을 향상시키는 데 도움이 될 수 있습니다.
    *   `LogisticRegression(...)`: 로지스틱 회귀 분류 모델을 설정합니다.
        *   `labelCol="Species"`: 예측 대상이 되는 레이블(종속 변수) 컬럼입니다.
        *   `featuresCol="Features"`: 모델 학습에 사용될 특성(독립 변수) 벡터 컬럼입니다.
        *   `maxIter`: 학습 알고리즘의 최대 반복 횟수입니다.
        *   `regParam`: 정규화(regularization) 파라미터로, 과적합(overfitting)을 방지하는 데 사용됩니다.
    *   `Pipeline(stages=...)`: 여러 데이터 변환 단계(transformers)와 모델 학습 단계(estimator)를 순서대로 실행하는 파이프라인을 만듭니다. 이렇게 하면 전체 워크플로우를 하나의 객체로 관리할 수 있습니다.
    *   `mlflow.log_param('param_name', value)`: MLflow에 하이퍼파라미터 `param_name`과 그 값 `value`를 기록합니다.
    *   `pipeline.fit(train)`: 학습 데이터 `train`을 사용하여 파이프라인의 모든 단계를 실행하고 모델을 학습시킵니다.
    *   `model.transform(test)`: 학습된 모델(파이프라인)을 사용하여 테스트 데이터 `test`에 대한 예측을 수행합니다.
    *   `MulticlassClassificationEvaluator(...)`: 다중 클래스 분류 모델의 성능을 평가하는 도구입니다.
        *   `labelCol="Species"`: 실제 레이블이 있는 컬럼입니다.
        *   `predictionCol="prediction"`: 모델이 예측한 레이블이 있는 컬럼입니다.
        *   `metricName`: 평가할 메트릭의 이름입니다 (예: `accuracy`, `f1`, `weightedPrecision`, `weightedRecall`).
    *   `evaluator.evaluate(prediction)`: 주어진 예측 결과에 대해 지정된 메트릭 값을 계산합니다.
    *   `mlflow.log_metric('metric_name', value)`: MLflow에 평가 메트릭 `metric_name`과 그 값 `value`를 기록합니다.
    *   `mlflow.spark.log_model(model, artifact_path, conda_env=...)`: 학습된 Spark ML `model`을 MLflow 실행의 `artifact_path`에 아티팩트(artifact)로 기록합니다. `conda_env`는 모델을 실행하는 데 필요한 Python 환경을 지정하여 재현성을 높입니다.
    *   `mlflow.spark.save_model(model, path)`: Spark ML 모델을 지정된 `path`(보통 DBFS 경로)에 저장합니다. `log_model`은 MLflow 추적 서버에 모델을 기록하는 반면, `save_model`은 파일 시스템에 직접 저장합니다.

2.  실험 실행이 완료되면 코드 셀 아래에 필요한 경우 **▸** 토글을 사용하여 MLflow 실행 세부 정보를 확장합니다. 그런 다음 여기에 표시된 **experiment** 하이퍼링크를 사용하여 실험 실행 목록을 보여주는 MLflow 페이지를 엽니다. 각 실행에는 고유한 이름이 할당됩니다.
3.  가장 최근 실행을 선택하고 세부 정보를 봅니다. 섹션을 확장하여 기록된 **Parameters**와 **Metrics**를 볼 수 있으며, 학습되고 저장된 모델의 세부 정보도 볼 수 있습니다.

    **팁:** 이 notebook 오른쪽의 사이드바 메뉴에 있는 **MLflow experiments** 아이콘을 사용하여 실험 실행의 세부 정보를 볼 수도 있습니다.

### 함수 만들기

머신러닝 프로젝트에서 데이터 과학자들은 종종 다른 파라미터로 모델을 학습시키고 매번 결과를 기록합니다. 이를 위해 학습 프로세스를 캡슐화하는 함수를 만들고 시도하려는 파라미터로 호출하는 것이 일반적입니다.

1.  새 셀에서 다음 코드를 실행하여 이전에 사용한 학습 코드를 기반으로 함수를 만듭니다.

    ```python
    def train_penguin_model(training_data, test_data, maxIterations, regularization):
        import mlflow
        import mlflow.spark
        from pyspark.ml import Pipeline
        from pyspark.ml.feature import StringIndexer, VectorAssembler, MinMaxScaler
        from pyspark.ml.classification import LogisticRegression
        from pyspark.ml.evaluation import MulticlassClassificationEvaluator
        import time
       
        # MLflow 실행(run) 시작
        with mlflow.start_run():
       
            catFeature = "Island"
            numFeatures = ["CulmenLength", "CulmenDepth", "FlipperLength", "BodyMass"]
       
            # 특성 공학 및 모델 단계 정의 (이전 셀과 동일)
            catIndexer = StringIndexer(inputCol=catFeature, outputCol=catFeature + "Idx")
            numVector = VectorAssembler(inputCols=numFeatures, outputCol="numericFeatures")
            numScaler = MinMaxScaler(inputCol = numVector.getOutputCol(), outputCol="normalizedFeatures")
            featureVector = VectorAssembler(inputCols=["IslandIdx", "normalizedFeatures"], outputCol="Features")
            algo = LogisticRegression(labelCol="Species", featuresCol="Features", maxIter=maxIterations, regParam=regularization)
       
            # 파이프라인 구성 (이전 셀과 동일)
            pipeline = Pipeline(stages=[catIndexer, numVector, numScaler, featureVector, algo])
       
            # 학습 파라미터 값 로깅
            print ("Training Logistic Regression model...")
            mlflow.log_param('maxIter', algo.getMaxIter())
            mlflow.log_param('regParam', algo.getRegParam())
            model = pipeline.fit(training_data) # 함수 인자로 받은 training_data 사용
       
            # 모델 평가 및 메트릭 로깅
            prediction = model.transform(test_data) # 함수 인자로 받은 test_data 사용
            metrics = ["accuracy", "weightedRecall", "weightedPrecision"]
            for metric in metrics:
                evaluator = MulticlassClassificationEvaluator(labelCol="Species", predictionCol="prediction", metricName=metric)
                metricValue = evaluator.evaluate(prediction)
                print("%s: %s" % (metric, metricValue))
                mlflow.log_metric(metric, metricValue)
       
            # 모델 자체를 로깅
            unique_model_name = "classifier-" + str(time.time())
            mlflow.spark.log_model(model, unique_model_name, mlflow.spark.get_default_conda_env())
            modelpath = "/model/%s" % (unique_model_name)
            mlflow.spark.save_model(model, modelpath)
       
            print("Experiment run complete.")
    ```
    *   `def train_penguin_model(training_data, test_data, maxIterations, regularization):`: 펭귄 모델을 학습시키는 `train_penguin_model`이라는 함수를 정의합니다. 이 함수는 학습 데이터(`training_data`), 테스트 데이터(`test_data`), 로지스틱 회귀의 최대 반복 횟수(`maxIterations`), 그리고 정규화 파라미터(`regularization`)를 입력으로 받습니다. 이전 셀의 코드를 재사용하여 함수화함으로써 코드의 재사용성을 높이고, 다른 하이퍼파라미터 값으로 쉽게 실험을 반복할 수 있게 합니다.

2.  새 셀에서 다음 코드를 사용하여 함수를 호출합니다.

    ```python
    train_penguin_model(train, test, 10, 0.2)
    ```
    *   앞서 정의한 `train_penguin_model` 함수를 호출합니다. 학습 데이터 `train`, 테스트 데이터 `test`를 전달하고, `maxIterations`는 10으로, `regularization` 파라미터는 0.2로 설정하여 모델 학습 및 평가를 수행합니다.

3.  두 번째 실행에 대한 MLflow 실험의 세부 정보를 봅니다.

### MLflow로 모델 등록 및 배포하기

학습 실험 실행의 세부 정보를 추적하는 것 외에도 MLflow를 사용하여 학습한 머신러닝 모델을 관리할 수 있습니다. 이미 각 실험 실행에서 학습된 모델을 기록했습니다. 또한 모델을 등록(register)하고 배포(deploy)하여 클라이언트 애플리케이션에 제공(serve)할 수 있습니다.

**참고:** 모델 서빙(Model serving)은 Azure Databricks Premium workspace에서만 지원되며 특정 지역으로 제한됩니다.

1.  가장 최근 실험 실행의 세부 정보 페이지를 봅니다.
2.  **Register Model** 버튼을 사용하여 해당 실행에서 기록된 모델을 등록하고 메시지가 표시되면 **Penguin Predictor**라는 새 모델을 만듭니다.
    *   *(설명: MLflow Model Registry는 모델의 버전 관리, 스테이지(Staging, Production 등) 관리, 모델 배포 등을 체계적으로 할 수 있게 해주는 중앙 저장소입니다.)*
3.  모델이 등록되면 (왼쪽 탐색 모음에서) **Models** 페이지를 보고 **Penguin Predictor** 모델을 선택합니다.
4.  **Penguin Predictor** 모델 페이지에서 **Use model for inference** 버튼을 사용하여 다음 설정으로 새 실시간 엔드포인트(real-time endpoint)를 만듭니다.
    *   **Model**: Penguin Predictor
    *   **Model version**: 1
    *   **Endpoint**: predict-penguin
    *   **Compute size**: Small
    *   *(설명: 모델 서빙 엔드포인트는 등록된 모델을 API 형태로 제공하여 다른 애플리케이션에서 예측 요청을 보낼 수 있도록 합니다. 이를 위해 내부적으로 별도의 컴퓨팅 리소스가 할당됩니다.)*
    서빙 엔드포인트는 새 cluster에서 호스팅되며, 생성하는 데 몇 분 정도 걸릴 수 있습니다.

5.  엔드포인트가 생성되면 오른쪽 상단의 **Query endpoint** 버튼을 사용하여 엔드포인트를 테스트할 수 있는 인터페이스를 엽니다. 그런 다음 테스트 인터페이스의 **Browser** 탭에서 다음 JSON 요청을 입력하고 **Send Request** 버튼을 사용하여 엔드포인트를 호출하고 예측을 생성합니다.

    ```json
     {
       "dataframe_records": [
       {
          "Island": "Biscoe",
          "CulmenLength": 48.7,
          "CulmenDepth": 14.1,
          "FlipperLength": 210,
          "BodyMass": 4450
       }
       ]
     }
    ```
    *   `dataframe_records`: Spark ML 모델을 서빙할 때 Databricks 모델 서빙 엔드포인트가 기대하는 표준 입력 형식 중 하나입니다. 리스트 안에 각 예측 요청에 해당하는 딕셔너리(레코드)를 포함합니다. 각 딕셔너리는 모델 학습 시 사용된 특성 이름(컬럼 이름)을 키로, 해당 특성 값을 값으로 가집니다.

6.  펭귄 특성에 대해 몇 가지 다른 값으로 실험하고 반환되는 결과를 관찰합니다. 그런 다음 테스트 인터페이스를 닫습니다.

### 엔드포인트 삭제하기

엔드포인트가 더 이상 필요하지 않으면 불필요한 비용을 피하기 위해 삭제해야 합니다.

1.  **predict-penguin** 엔드포인트 페이지의 **⁝** (케밥) 메뉴에서 **Delete**를 선택합니다.

### 정리하기

1.  Azure Databricks 포털의 **Compute** 페이지에서 cluster를 선택하고 **■ Terminate**를 선택하여 종료합니다.

Azure Databricks 탐색을 마쳤다면 불필요한 Azure 비용을 피하고 구독의 용량을 확보하기 위해 만든 리소스를 삭제할 수 있습니다.

