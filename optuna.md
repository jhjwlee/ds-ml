
**Azure Databricks에서 기계 학습을 위한 Hyperparameters 최적화**

이 연습에서는 Optuna 라이브러리를 사용하여 Azure Databricks에서 machine learning model training을 위한 hyperparameters를 최적화합니다.

이 연습을 완료하는 데 약 30분이 소요됩니다.

참고: Azure Databricks 사용자 인터페이스는 지속적으로 개선되고 있습니다. 이 연습의 지침이 작성된 이후 사용자 인터페이스가 변경되었을 수 있습니다.

**시작하기 전에**

관리자 수준 액세스 권한이 있는 Azure subscription이 필요합니다.

**Azure Databricks workspace 프로비저닝**

팁: 이미 Azure Databricks workspace가 있다면 이 절차를 건너뛰고 기존 workspace를 사용할 수 있습니다.

이 연습에는 새 Azure Databricks workspace를 프로비저닝하는 스크립트가 포함되어 있습니다. 이 스크립트는 Azure subscription에서 이 연습에 필요한 compute cores에 대한 충분한 quota가 있는 지역에 Premium tier Azure Databricks workspace resource를 만들려고 시도합니다. 또한 사용자 계정이 subscription에서 Azure Databricks workspace resource를 만들 수 있는 충분한 권한을 가지고 있다고 가정합니다. quota 또는 권한 부족으로 스크립트가 실패하면 Azure portal에서 대화형으로 Azure Databricks workspace를 만들어 볼 수 있습니다.

1.  웹 브라우저에서 Azure portal (https://portal.azure.com)에 로그인합니다.
2.  페이지 상단 검색창 오른쪽의 **[>_]** 버튼을 사용하여 Azure portal에서 새 Cloud Shell을 만들고, **PowerShell** 환경을 선택합니다. Cloud Shell은 Azure portal 하단 창에 명령줄 인터페이스를 제공합니다. 아래 그림과 같습니다:

    *Azure portal과 Cloud Shell 창*

    참고: 이전에 Bash 환경을 사용하는 Cloud Shell을 만들었다면 PowerShell로 전환하십시오.

    Cloud Shell 창 상단의 구분선을 드래그하거나 창 오른쪽 상단의 —, ⤢, X 아이콘을 사용하여 창 크기를 조절하거나 최소화, 최대화, 닫을 수 있습니다. Azure Cloud Shell 사용에 대한 자세한 내용은 Azure Cloud Shell 설명서를 참조하십시오.

3.  PowerShell 창에 다음 명령을 입력하여 이 저장소를 복제합니다:

    ```powershell
    rm -r mslearn-databricks -f
    git clone https://github.com/MicrosoftLearning/mslearn-databricks
    ```
    *   `rm -r mslearn-databricks -f`: 이 명령은 `mslearn-databricks`라는 디렉토리가 이미 존재할 경우, 해당 디렉토리와 그 안의 모든 내용을 강제로(-f) 재귀적으로(-r) 삭제합니다. 스크립트를 여러 번 실행할 때 이전 실행으로 인한 충돌을 방지하기 위함입니다.
    *   `git clone ...`: 이 명령은 지정된 GitHub 주소에서 `mslearn-databricks`라는 프로젝트를 현재 디렉토리로 복제(다운로드)합니다. 이 저장소에는 연습에 필요한 파일들이 포함되어 있습니다.

4.  저장소가 복제된 후 다음 명령을 입력하여 `setup.ps1` 스크립트를 실행합니다. 이 스크립트는 사용 가능한 지역에 Azure Databricks workspace를 프로비저닝합니다:

    ```powershell
    ./mslearn-databricks/setup.ps1
    ```
    *   `./mslearn-databricks/setup.ps1`: 복제된 `mslearn-databricks` 디렉토리 내의 `setup.ps1` PowerShell 스크립트를 실행합니다. 이 스크립트는 Azure Databricks workspace를 자동으로 생성하고 설정하는 작업을 수행합니다.

5.  메시지가 표시되면 사용할 subscription을 선택합니다 (여러 Azure subscription에 액세스할 수 있는 경우에만 해당됩니다).
6.  스크립트가 완료될 때까지 기다립니다 - 일반적으로 약 5분 정도 걸리지만 경우에 따라 더 오래 걸릴 수 있습니다. 기다리는 동안 Azure Databricks 설명서의 [Hyperparameter tuning](https://docs.databricks.com/machine-learning/automl-hyperparam-tuning/index.html) 문서를 검토하십시오.

**Cluster 생성**

Azure Databricks는 Apache Spark cluster를 사용하여 여러 node에서 데이터를 병렬로 처리하는 분산 처리 플랫폼입니다. 각 cluster는 작업을 조정하는 driver node와 처리 작업을 수행하는 worker node로 구성됩니다. 이 연습에서는 랩 환경(resource가 제한될 수 있음)에서 사용되는 compute resource를 최소화하기 위해 single-node cluster를 만듭니다. 프로덕션 환경에서는 일반적으로 여러 worker node가 있는 cluster를 만듭니다.

팁: Azure Databricks workspace에 13.3 LTS ML 이상의 runtime version을 가진 cluster가 이미 있다면, 해당 cluster를 사용하여 이 연습을 완료하고 이 절차를 건너뛸 수 있습니다.

1.  Azure portal에서 스크립트에 의해 생성된 `msl-xxxxxxx` resource group (또는 기존 Azure Databricks workspace가 포함된 resource group)으로 이동합니다.
2.  Azure Databricks Service resource (설정 스크립트를 사용하여 생성한 경우 `databricks-xxxxxxx` 이름)를 선택합니다.
3.  workspace의 **Overview** 페이지에서 **Launch Workspace** 버튼을 사용하여 새 브라우저 탭에서 Azure Databricks workspace를 엽니다. 메시지가 표시되면 로그인합니다.

    팁: Databricks Workspace portal을 사용하면 다양한 팁과 알림이 표시될 수 있습니다. 이를 무시하고 이 연습의 작업을 완료하기 위한 지침을 따르십시오.

4.  왼쪽 사이드바에서 **(+) New** 작업을 선택한 다음 **Cluster**를 선택합니다.
5.  **New Cluster** 페이지에서 다음 설정으로 새 cluster를 만듭니다:
    *   **Cluster name**: *사용자 이름*'s cluster (기본 클러스터 이름)
    *   **Policy**: Unrestricted
    *   **Cluster mode**: Single Node
        *   *설명*: Single Node cluster는 driver node만 있고 worker node가 없는 cluster입니다. 소규모 데이터셋이나 개발/테스트 목적에 적합하며, 이 연습에서는 리소스 사용을 최소화하기 위해 선택합니다.
    *   **Access mode**: Single user (사용자 계정 선택됨)
    *   **Databricks runtime version**: 다음 조건을 만족하는 최신 non-beta 버전의 **ML** edition을 선택합니다 (Standard runtime version이 아님):
        *   GPU를 사용하지 않음
        *   Scala > 2.11 포함
        *   Spark > 3.4 포함
        *   *설명*: `Databricks runtime version`은 cluster에서 실행될 소프트웨어 스택을 정의합니다. **ML** (Machine Learning) edition은 scikit-learn, TensorFlow, PyTorch, Optuna와 같은 일반적인 ML 라이브러리가 사전 설치되어 있어 편리합니다.
    *   **Use Photon Acceleration**: 선택 해제
        *   *설명*: Photon Acceleration은 Spark API와 호환되는 Databricks 네이티브 벡터화된 쿼리 엔진으로, SQL 및 DataFrame 워크로드의 성능을 향상시킵니다. 이 연습에서는 필요하지 않아 선택 해제합니다.
    *   **Node type**: Standard_D4ds_v5
    *   **Terminate after** `20` **minutes of inactivity**
        *   *설명*: 일정 시간 동안 cluster가 비활성 상태일 때 자동으로 종료되도록 설정하여 불필요한 비용 발생을 방지합니다.
6.  cluster가 생성될 때까지 기다립니다. 1~2분 정도 걸릴 수 있습니다.

    참고: cluster 시작에 실패하면 Azure Databricks workspace가 프로비저닝된 지역에서 subscription의 quota가 부족할 수 있습니다. 자세한 내용은 [CPU core limit prevents cluster creation](https://learn.microsoft.com/azure/databricks/kb/clusters/cpu-core-limit-prevents-cluster-creation)을 참조하십시오. 이 경우 workspace를 삭제하고 다른 지역에 새 workspace를 만들어 볼 수 있습니다. 다음과 같이 `setup.ps1` 스크립트에 지역을 매개변수로 지정할 수 있습니다: `./mslearn-databricks/setup.ps1 eastus`

**Notebook 생성**

Spark MLLib library를 사용하여 machine learning model을 학습시키는 코드를 실행할 것이므로, 첫 번째 단계는 workspace에 새 notebook을 만드는 것입니다.

1.  사이드바에서 **(+) New** 링크를 사용하여 **Notebook**을 만듭니다.
2.  기본 notebook 이름(Untitled Notebook [날짜])을 **Hyperparameter Tuning**으로 변경하고 **Connect** 드롭다운 목록에서 아직 선택되지 않았다면 cluster를 선택합니다. cluster가 실행 중이 아니면 시작하는 데 1분 정도 걸릴 수 있습니다.

**데이터 수집 (Ingest data)**

이 연습의 시나리오는 남극 펭귄 관찰을 기반으로 하며, 관찰된 펭귄의 위치와 신체 측정값을 기반으로 펭귄의 종을 예측하는 machine learning model을 학습시키는 것이 목표입니다.

인용: 이 연습에 사용된 펭귄 데이터셋은 Kristen Gorman 박사와 Palmer Station, Antarctica LTER (Long Term Ecological Research Network의 회원)이 수집하고 제공한 데이터의 일부입니다.

1.  notebook의 첫 번째 셀에 다음 코드를 입력합니다. 이 코드는 셸 명령을 사용하여 GitHub에서 펭귄 데이터를 cluster에서 사용하는 파일 시스템으로 다운로드합니다.

    ```python
    %sh
    rm -r /dbfs/hyperparam_tune_lab -f
    mkdir /dbfs/hyperparam_tune_lab
    wget -O /dbfs/hyperparam_tune_lab/penguins.csv https://raw.githubusercontent.com/MicrosoftLearning/mslearn-databricks/main/data/penguins.csv
    ```
    *   `%sh`: Databricks notebook에서 셸 명령을 실행할 수 있게 해주는 magic command입니다.
    *   `rm -r /dbfs/hyperparam_tune_lab -f`: `/dbfs/hyperparam_tune_lab` 디렉토리가 이미 존재하면 해당 디렉토리와 그 내용을 강제로(-f) 재귀적으로(-r) 삭제합니다. `/dbfs/`는 Databricks File System (DBFS)의 루트 경로입니다.
    *   `mkdir /dbfs/hyperparam_tune_lab`: `/dbfs/` 경로 아래에 `hyperparam_tune_lab`이라는 새 디렉토리를 만듭니다.
    *   `wget -O /dbfs/hyperparam_tune_lab/penguins.csv <URL>`: 지정된 URL에서 파일을 다운로드하여 `/dbfs/hyperparam_tune_lab/penguins.csv`라는 이름으로 저장합니다.

2.  셀 왼쪽의 **▸ Run Cell** 메뉴 옵션을 사용하여 실행합니다. 그런 다음 코드가 실행하는 Spark job이 완료될 때까지 기다립니다.
3.  이제 machine learning을 위해 데이터를 준비합니다. 기존 코드 셀 아래에 **+** 아이콘을 사용하여 새 코드 셀을 추가합니다. 그런 다음 새 셀에 다음 코드를 입력하고 실행하여 다음을 수행합니다:
    *   불완전한 행 제거
    *   적절한 데이터 타입 적용
    *   데이터의 무작위 샘플 보기
    *   데이터를 두 개의 데이터셋으로 분할: 하나는 training용, 다른 하나는 testing용.

    ```python
    from pyspark.sql.types import *
    from pyspark.sql.functions import *
       
    # CSV 파일을 Spark DataFrame으로 읽어옵니다. header 옵션은 CSV 파일의 첫 줄을 컬럼 이름으로 사용하도록 합니다.
    data = spark.read.format("csv").option("header", "true").load("/hyperparam_tune_lab/penguins.csv")
    
    # 결측값이 있는 행을 제거하고(dropna), 각 컬럼을 적절한 데이터 타입으로 변환합니다.
    data = data.dropna().select(col("Island").astype("string"),
                              col("CulmenLength").astype("float"), # 부리 길이
                              col("CulmenDepth").astype("float"),  # 부리 깊이
                              col("FlipperLength").astype("float"),# 지느러미 길이
                              col("BodyMass").astype("float"),   # 몸무게
                              col("Species").astype("int")       # 펭귄 종 (레이블)
                              )
    # 데이터의 20%를 무작위로 샘플링하여 Databricks notebook에 시각적으로 표시합니다.
    display(data.sample(0.2))
       
    # 데이터를 training set (70%)과 test set (30%)으로 무작위로 분할합니다.
    splits = data.randomSplit([0.7, 0.3])
    train = splits[0]
    test = splits[1]
    print ("Training Rows:", train.count(), " Testing Rows:", test.count())
    ```
    *   `from pyspark.sql.types import *`: PySpark SQL에서 사용되는 다양한 데이터 타입(예: `StringType`, `FloatType`, `IntegerType` 등)을 가져옵니다.
    *   `from pyspark.sql.functions import *`: PySpark SQL에서 사용되는 내장 함수(예: `col`, `lit`, `when` 등)를 가져옵니다.
    *   `spark.read.format("csv").option("header", "true").load(...)`: CSV 파일을 읽어 Spark DataFrame으로 만듭니다. `option("header", "true")`는 CSV 파일의 첫 번째 줄을 컬럼 이름으로 사용하도록 지정합니다.
    *   `data.dropna()`: DataFrame에서 하나 이상의 `null` 값을 포함하는 모든 행을 제거합니다.
    *   `select(col("ColumnName").astype("newType"), ...)`: 특정 컬럼들을 선택하고, `astype()` 함수를 사용하여 각 컬럼의 데이터 타입을 지정된 타입으로 변환합니다. `col("ColumnName")`은 해당 이름의 컬럼을 참조합니다.
    *   `display(data.sample(0.2))`: DataFrame의 20%를 무작위로 추출하여 Databricks notebook 환경에서 테이블 또는 차트 형태로 시각화하여 보여줍니다.
    *   `data.randomSplit([0.7, 0.3])`: 데이터를 지정된 비율(여기서는 70%와 30%)로 무작위로 분할하여 두 개의 DataFrame 리스트를 반환합니다. 이는 머신러닝 모델 학습 시 training set과 test set을 나누는 일반적인 방법입니다.
    *   `train.count()`, `test.count()`: 각 DataFrame에 있는 행의 수를 계산합니다.

**model training을 위한 hyperparameter 값 최적화**

machine learning model은 features를 알고리즘에 맞춰 학습시켜 가장 가능성이 높은 label을 계산합니다. 알고리즘은 training data를 매개변수로 사용하고 features와 labels 간의 수학적 관계를 계산하려고 시도합니다. 데이터 외에도 대부분의 알고리즘은 관계 계산 방식에 영향을 미치는 하나 이상의 hyperparameters를 사용합니다. 최적의 hyperparameter 값을 결정하는 것은 반복적인 model training process의 중요한 부분입니다.

최적의 hyperparameter 값을 결정하는 데 도움을 주기 위해 Azure Databricks에는 Optuna가 포함되어 있습니다. Optuna는 여러 hyperparameter 값을 시도하고 데이터에 가장 적합한 조합을 찾을 수 있게 해주는 라이브러리입니다.

Optuna 사용의 첫 번째 단계는 다음을 수행하는 함수를 만드는 것입니다:

1.  함수에 매개변수로 전달되는 하나 이상의 hyperparameter 값을 사용하여 model을 학습합니다.
2.  손실(model이 완벽한 예측 성능에서 얼마나 멀리 떨어져 있는지)을 측정하는 데 사용할 수 있는 성능 지표를 계산합니다.
3.  손실 값을 반환하여 다른 hyperparameter 값을 시도함으로써 반복적으로 최적화(최소화)될 수 있도록 합니다.

새 셀을 추가하고 다음 코드를 사용하여 hyperparameter에 사용할 값의 범위를 정의하고 펭귄 데이터를 사용하여 위치와 측정값을 기반으로 펭귄의 종을 예측하는 분류 모델을 학습시키는 함수를 만듭니다:

```python
import optuna
import mlflow # 실험을 로깅하고 싶다면 사용합니다.
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, MinMaxScaler
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
   
def objective(trial):
    # Hyperparameter 값 제안 (maxDepth 및 maxBins):
    # "MaxDepth"라는 이름의 hyperparameter에 대해 0에서 9 사이의 정수 값을 제안합니다.
    max_depth = trial.suggest_int("MaxDepth", 0, 9) 
    # "MaxBins"라는 이름의 hyperparameter에 대해 [10, 20, 30] 리스트 중 하나의 범주형 값을 제안합니다.
    max_bins = trial.suggest_categorical("MaxBins", [10, 20, 30])

    # Pipeline 구성 요소 정의
    cat_feature = "Island" # 범주형 feature 컬럼 이름
    num_features = ["CulmenLength", "CulmenDepth", "FlipperLength", "BodyMass"] # 수치형 feature 컬럼 이름 리스트
    
    # StringIndexer: 범주형 문자열 컬럼("Island")을 숫자 인덱스 컬럼("IslandIdx")으로 변환합니다.
    catIndexer = StringIndexer(inputCol=cat_feature, outputCol=cat_feature + "Idx")
    # VectorAssembler: 여러 수치형 feature 컬럼들을 단일 벡터 컬럼("numericFeatures")으로 결합합니다.
    numVector = VectorAssembler(inputCols=num_features, outputCol="numericFeatures")
    # MinMaxScaler: "numericFeatures" 벡터의 값들을 0과 1 사이로 정규화하여 "normalizedFeatures" 컬럼을 만듭니다.
    numScaler = MinMaxScaler(inputCol=numVector.getOutputCol(), outputCol="normalizedFeatures")
    # VectorAssembler: 인덱싱된 범주형 feature와 정규화된 수치형 feature들을 최종 "Features" 벡터 컬럼으로 결합합니다.
    featureVector = VectorAssembler(inputCols=[cat_feature + "Idx", "normalizedFeatures"], outputCol="Features")

    # DecisionTreeClassifier: 의사결정 트리 분류기를 설정합니다.
    # labelCol: 예측 대상 컬럼 (펭귄 종)
    # featuresCol: 학습에 사용될 feature 컬럼
    # maxDepth, maxBins: Optuna가 제안한 hyperparameter 값 사용
    dt = DecisionTreeClassifier(
        labelCol="Species",
        featuresCol="Features",
        maxDepth=max_depth,
        maxBins=max_bins
    )

    # Pipeline: 정의된 모든 변환 단계(Indexer, Assembler, Scaler)와 분류기(dt)를 순서대로 실행하는 파이프라인을 만듭니다.
    pipeline = Pipeline(stages=[catIndexer, numVector, numScaler, featureVector, dt])
    # 모델 학습: training data(train)를 사용하여 파이프라인을 학습시킵니다.
    model = pipeline.fit(train)

    # 정확도를 사용하여 모델 평가
    # 학습된 모델을 사용하여 test data에 대한 예측을 생성합니다.
    predictions = model.transform(test)
    # MulticlassClassificationEvaluator: 다중 클래스 분류 모델의 성능을 평가합니다.
    # metricName="accuracy": 평가 지표로 정확도를 사용합니다.
    evaluator = MulticlassClassificationEvaluator(
        labelCol="Species",
        predictionCol="prediction", # 모델이 생성한 예측값 컬럼
        metricName="accuracy"
    )
    # 예측 결과에 대한 정확도를 계산합니다.
    accuracy = evaluator.evaluate(predictions)

    # Optuna는 목적 함수를 최소화하므로, 정확도의 음수 값을 반환합니다.
    # 이렇게 하면 Optuna가 정확도를 최대화하는 방향으로 최적화를 수행하게 됩니다.
    return -accuracy
```
*   `import optuna`: Hyperparameter 최적화를 위한 Optuna 라이브러리를 가져옵니다.
*   `import mlflow`: 실험 추적 및 로깅을 위한 MLflow 라이브러리를 가져옵니다. Optuna와 함께 사용하면 각 `trial`의 hyperparameter와 결과를 자동으로 로깅할 수 있습니다.
*   `from pyspark.ml import Pipeline`: 여러 ML 단계를 하나의 워크플로우로 묶는 `Pipeline`을 가져옵니다.
*   `from pyspark.ml.feature import StringIndexer, VectorAssembler, MinMaxScaler`:
    *   `StringIndexer`: 범주형 문자열 컬럼을 숫자 인덱스로 변환합니다. ML 알고리즘은 숫자 입력을 선호하기 때문입니다.
    *   `VectorAssembler`: 여러 feature 컬럼을 단일 벡터 컬럼으로 결합합니다. 대부분의 ML 알고리즘은 feature를 단일 벡터 형태로 받습니다.
    *   `MinMaxScaler`: feature 값을 특정 범위(기본값 0~1)로 조정(scale)합니다. 서로 다른 범위의 feature들이 모델 학습에 미치는 영향을 균등하게 만듭니다.
*   `from pyspark.ml.classification import DecisionTreeClassifier`: 분류 문제에 사용되는 의사결정 트리 알고리즘입니다.
*   `from pyspark.ml.evaluation import MulticlassClassificationEvaluator`: 다중 클래스 분류 모델의 성능(예: 정확도, F1-score)을 평가합니다.
*   `def objective(trial):`: Optuna가 최적화할 목적 함수입니다. `trial` 객체는 각 hyperparameter 최적화 시도를 나타냅니다.
    *   `trial.suggest_int("MaxDepth", 0, 9)`: "MaxDepth"라는 hyperparameter에 대해 0부터 9까지의 정수 값을 제안하도록 Optuna에 요청합니다. Optuna는 이 범위 내에서 다양한 값을 시도합니다.
    *   `trial.suggest_categorical("MaxBins", [10, 20, 30])`: "MaxBins"라는 hyperparameter에 대해 제공된 리스트 `[10, 20, 30]` 중에서 하나의 값을 선택하도록 Optuna에 요청합니다.
    *   `Pipeline(stages=[...])`: 데이터 전처리 단계(StringIndexer, VectorAssembler, MinMaxScaler)와 모델 학습 단계(DecisionTreeClassifier)를 순차적으로 실행하는 ML Pipeline을 구성합니다.
    *   `pipeline.fit(train)`: `train` 데이터셋을 사용하여 Pipeline 전체를 학습시킵니다.
    *   `model.transform(test)`: 학습된 모델을 사용하여 `test` 데이터셋에 대한 예측을 수행합니다.
    *   `evaluator.evaluate(predictions)`: `test` 데이터셋에 대한 예측 정확도를 계산합니다.
    *   `return -accuracy`: Optuna는 기본적으로 목적 함수의 반환값을 최소화하려고 합니다. 정확도는 높을수록 좋으므로, 정확도에 음수를 취한 값을 반환하여 Optuna가 정확도를 최대화하도록 만듭니다.

새 셀을 추가하고 다음 코드를 사용하여 최적화 실험을 실행합니다:

```python
# 5번의 trial로 최적화 실행:
# Optuna study 객체를 생성합니다. study는 최적화 과정을 관리합니다.
study = optuna.create_study() 
# objective 함수를 사용하여 최적화를 시작합니다. n_trials=5는 5번의 다른 hyperparameter 조합을 시도하라는 의미입니다.
study.optimize(objective, n_trials=5)

print("최적화 실행에서 찾은 최상의 매개변수 값:")
# study.best_params는 최적화 과정에서 가장 좋은 성능(가장 낮은 손실 값, 즉 가장 높은 정확도)을 보인 hyperparameter 조합을 반환합니다.
print(study.best_params)
```
*   `optuna.create_study()`: 새로운 Optuna `study`를 생성합니다. `study` 객체는 전체 최적화 과정을 조정하고 결과를 저장합니다.
*   `study.optimize(objective, n_trials=5)`: 이전에 정의한 `objective` 함수를 사용하여 hyperparameter 최적화를 시작합니다. `n_trials=5`는 Optuna가 `objective` 함수를 5번 호출하여 서로 다른 hyperparameter 조합을 시도하고 최상의 조합을 찾는다는 의미입니다.
*   `study.best_params`: 최적화가 완료된 후, `study` 객체에서 가장 좋은 결과를 낸 hyperparameter들의 딕셔너리를 가져옵니다.

코드가 `n_trials` 설정에 따라 손실을 최소화하려고 시도하면서 학습 함수를 5번 반복적으로 실행하는 것을 관찰하십시오. 각 trial은 MLflow에 의해 기록되며, 코드 셀 아래의 MLflow 실행 결과에서 **▸** 토글을 확장하고 실험 하이퍼링크를 선택하여 확인할 수 있습니다. 각 실행에는 임의의 이름이 할당되며, MLflow 실행 뷰어에서 각 실행을 확인하여 기록된 매개변수 및 메트릭의 세부 정보를 볼 수 있습니다.

모든 실행이 완료되면 코드가 찾은 최상의 hyperparameter 값(가장 적은 손실을 초래한 조합)의 세부 정보를 표시하는 것을 관찰하십시오. 이 경우 `MaxBins` 매개변수는 세 가지 가능한 값(10, 20, 30) 목록에서 선택으로 정의됩니다. 최상의 값은 Optuna가 선택한 실제 값을 보여줍니다. `MaxDepth` 매개변수는 0과 9 사이의 임의의 정수로 정의되며, 최상의 결과를 제공한 정수 값이 표시됩니다.

**정리**

Azure Databricks portal의 **Compute** 페이지에서 cluster를 선택하고 **■ Terminate**를 선택하여 종료합니다.

Azure Databricks 탐색을 마쳤다면, 불필요한 Azure 비용을 피하고 subscription의 용량을 확보하기 위해 생성한 resource를 삭제할 수 있습니다.

