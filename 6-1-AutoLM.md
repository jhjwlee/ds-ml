

**AutoML을 사용하여 모델 학습**

AutoML은 Azure Databricks의 기능으로, 데이터를 사용하여 여러 알고리즘과 매개변수를 시도하여 최적의 machine learning model을 학습합니다.

이 연습을 완료하는 데 약 45분이 소요됩니다.

참고: Azure Databricks 사용자 인터페이스는 지속적으로 개선되고 있습니다. 이 연습의 지침이 작성된 이후 사용자 인터페이스가 변경되었을 수 있습니다.

**시작하기 전에**

관리자 수준 액세스 권한이 있는 Azure subscription이 필요합니다.

**Azure Databricks workspace 프로비저닝**

참고: 이 연습에서는 model serving이 지원되는 지역의 Premium Azure Databricks workspace가 필요합니다. 지역별 Azure Databricks 기능에 대한 자세한 내용은 [Azure Databricks regions](https://azure.microsoft.com/global-infrastructure/services/?products=databricks)를 참조하십시오. 적합한 지역에 Premium 또는 Trial Azure Databricks workspace가 이미 있다면 이 절차를 건너뛰고 기존 workspace를 사용할 수 있습니다.

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
6.  스크립트가 완료될 때까지 기다립니다 - 일반적으로 약 5분 정도 걸리지만 경우에 따라 더 오래 걸릴 수 있습니다. 기다리는 동안 Azure Databricks 설명서의 [What is AutoML?](https://docs.databricks.com/machine-learning/automl/index.html) 문서를 검토하십시오.

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
        *   *설명*: `Databricks runtime version`은 cluster에서 실행될 소프트웨어 스택을 정의합니다. **ML** (Machine Learning) edition은 AutoML 실행에 필요한 라이브러리들이 사전 설치되어 있어 편리합니다.
    *   **Use Photon Acceleration**: 선택 해제
        *   *설명*: Photon Acceleration은 Spark API와 호환되는 Databricks 네이티브 벡터화된 쿼리 엔진으로, SQL 및 DataFrame 워크로드의 성능을 향상시킵니다. 이 연습에서는 필요하지 않아 선택 해제합니다.
    *   **Node type**: Standard_D4ds_v5
    *   **Terminate after** `20` **minutes of inactivity**
        *   *설명*: 일정 시간 동안 cluster가 비활성 상태일 때 자동으로 종료되도록 설정하여 불필요한 비용 발생을 방지합니다.
6.  cluster가 생성될 때까지 기다립니다. 1~2분 정도 걸릴 수 있습니다.

    참고: cluster 시작에 실패하면 Azure Databricks workspace가 프로비저닝된 지역에서 subscription의 quota가 부족할 수 있습니다. 자세한 내용은 [CPU core limit prevents cluster creation](https://learn.microsoft.com/azure/databricks/kb/clusters/cpu-core-limit-prevents-cluster-creation)을 참조하십시오. 이 경우 workspace를 삭제하고 다른 지역에 새 workspace를 만들어 볼 수 있습니다. 다음과 같이 `setup.ps1` 스크립트에 지역을 매개변수로 지정할 수 있습니다: `./mslearn-databricks/setup.ps1 eastus`

**SQL Warehouse에 training data 업로드**

AutoML을 사용하여 machine learning model을 학습하려면 training data를 업로드해야 합니다. 이 연습에서는 펭귄의 위치와 신체 측정값을 포함한 관찰을 기반으로 펭귄을 세 가지 종 중 하나로 분류하는 모델을 학습합니다. 종 레이블이 포함된 training data를 Azure Databricks data warehouse의 테이블에 로드합니다.

1.  workspace의 Azure Databricks portal에서 사이드바의 **SQL** 아래에 있는 **SQL Warehouses**를 선택합니다.
    *   *설명*: **SQL Warehouses** (이전에는 SQL Endpoints로 알려짐)는 Databricks SQL 쿼리를 실행하기 위한 컴퓨팅 리소스입니다. 데이터 분석 및 BI 워크로드에 최적화되어 있습니다.
2.  workspace에 이미 **Serverless Starter Warehouse**라는 SQL Warehouse가 있는지 확인합니다.
3.  SQL Warehouse의 **Actions** (⁝) 메뉴에서 **Edit**을 선택합니다. 그런 다음 **Cluster size** 속성을 **2X-Small**로 설정하고 변경 사항을 저장합니다.
    *   *설명*: **Serverless Starter Warehouse**는 신속하게 시작하고 SQL 쿼리를 실행할 수 있도록 제공되는 기본 웨어하우스입니다. **Cluster size**는 웨어하우스의 컴퓨팅 용량을 결정합니다. 이 연습에서는 최소 크기인 `2X-Small`로 설정하여 비용을 절감합니다.
4.  **Start** 버튼을 사용하여 SQL Warehouse를 시작합니다 (1~2분 정도 걸릴 수 있습니다).

    참고: SQL Warehouse 시작에 실패하면 Azure Databricks workspace가 프로비저닝된 지역에서 subscription의 quota가 부족할 수 있습니다. 자세한 내용은 [Required Azure vCPU quota](https://learn.microsoft.com/azure/databricks/sql/admin/quotas-azure)를 참조하십시오. 이 경우 웨어하우스 시작 실패 시 오류 메시지에 자세히 설명된 대로 quota 증가를 요청해 볼 수 있습니다. 또는 workspace를 삭제하고 다른 지역에 새 workspace를 만들어 볼 수 있습니다. 다음과 같이 `setup.ps1` 스크립트에 지역을 매개변수로 지정할 수 있습니다: `./mslearn-databricks/setup.ps1 eastus`

5.  로컬 컴퓨터에 `https://raw.githubusercontent.com/MicrosoftLearning/mslearn-databricks/main/data/penguins.csv`에서 `penguins.csv` 파일을 다운로드하여 `penguins.csv`로 저장합니다.
6.  Azure Databricks workspace portal의 사이드바에서 **(+) New**를 선택한 다음 **Add or upload data**를 선택합니다. **Add data** 페이지에서 **Create or modify table**을 선택하고 컴퓨터에 다운로드한 `penguins.csv` 파일을 업로드합니다.
7.  **Create or modify table from file upload** 페이지에서 기본 `schema`를 선택하고 `table name`을 `penguins`로 설정합니다. 그런 다음 **Create table**을 선택합니다.
    *   *설명*: 여기서 `schema`는 데이터베이스 내에서 테이블을 구성하는 논리적 컨테이너를 의미합니다 (일반적으로 `default` 또는 사용자가 생성한 `schema` 이름). `table name`은 생성될 테이블의 이름입니다.
8.  테이블이 생성되면 세부 정보를 검토합니다.

**AutoML experiment 생성**

이제 데이터가 있으므로 AutoML과 함께 사용하여 모델을 학습할 수 있습니다.

1.  왼쪽 사이드바에서 **Experiments**를 선택합니다.
    *   *설명*: **Experiments**는 MLflow의 구성 요소로, machine learning 모델 학습 실행을 구성하고 추적하는 데 사용됩니다. AutoML 실험도 MLflow를 통해 관리됩니다.
2.  **Experiments** 페이지에서 **Classification** 타일을 찾아 **Start training**을 선택합니다.
    *   *설명*: AutoML은 문제 유형(Classification, Regression, Forecasting)에 따라 다른 접근 방식을 사용합니다. 펭귄 종을 예측하는 것은 **Classification** 문제입니다.
3.  다음 설정으로 AutoML experiment를 구성합니다:
    *   **Cluster**: 사용자의 cluster를 선택합니다.
        *   *설명*: AutoML 실험을 실행할 컴퓨팅 리소스입니다.
    *   **Input training dataset**: 기본 `database` (일반적으로 `default` 또는 `hive_metastore` 내의 사용자 `schema`)로 이동하여 `penguins` 테이블을 선택합니다.
        *   *설명*: 모델 학습에 사용될 데이터셋입니다. 이전 단계에서 업로드한 `penguins` 테이블입니다.
    *   **Prediction target**: `Species`
        *   *설명*: 모델이 예측하고자 하는 컬럼(종속 변수)입니다.
    *   **Experiment name**: `Penguin-classification`
        *   *설명*: 이 AutoML 실행을 식별하기 위한 고유한 이름입니다.
    *   **Advanced configuration** (고급 구성):
        *   **Evaluation metric**: `Precision`
            *   *설명*: 모델 성능을 평가하는 기준입니다. `Precision` (정밀도)은 모델이 Positive로 예측한 것들 중에서실제로 Positive인 샘플의 비율입니다. 분류 문제에서 중요한 지표 중 하나입니다.
        *   **Training frameworks**: `lightgbm`, `sklearn`, `xgboost`
            *   *설명*: AutoML이 모델 학습에 사용할 machine learning 라이브러리(프레임워크)입니다. 선택된 프레임워크 내의 다양한 알고리즘과 하이퍼파라미터를 AutoML이 자동으로 시도합니다. `LightGBM`, `Scikit-learn (sklearn)`, `XGBoost`는 매우 인기 있고 성능 좋은 라이브러리입니다.
        *   **Timeout**: `5` (분)
            *   *설명*: AutoML 실험의 최대 실행 시간입니다. 지정된 시간이 지나면 AutoML은 그때까지 찾은 최상의 모델을 반환하고 실험을 중지합니다.
        *   **Time column for training/validation/testing split**: 비워 둡니다.
            *   *설명*: 데이터에 시간 순서가 있고, 이를 기준으로 학습/검증/테스트 세트를 분할해야 할 경우 사용합니다. 이 예제에서는 사용하지 않습니다.
        *   **Positive label**: 비워 둡니다.
            *   *설명*: 이진 분류 문제에서 '긍정' 클래스를 명시적으로 지정할 때 사용합니다. 이 펭귄 분류 문제는 다중 클래스 분류이므로 비워둡니다.
        *   **Intermediate data storage location**: `MLflow Artifact`
            *   *설명*: AutoML 실행 중 생성되는 중간 데이터(예: 전처리된 데이터셋, 생성된 노트북 등)가 저장될 위치입니다. 기본적으로 MLflow 아티팩트 저장소에 저장됩니다.
4.  **Start AutoML** 버튼을 사용하여 실험을 시작합니다. 표시되는 정보 대화 상자를 닫습니다.
5.  실험이 완료될 때까지 기다립니다. **Runs** 탭 아래에서 생성된 실행의 세부 정보를 볼 수 있습니다.
    *   *설명*: AutoML은 여러 모델을 병렬 또는 순차적으로 학습시키며, 각 학습 시도를 'run'이라고 합니다. 각 run은 MLflow에 기록됩니다.
6.  5분 후 실험이 종료됩니다. 실행을 새로 고침하면 선택한 `precision` 메트릭을 기준으로 최상의 성능을 보인 모델의 실행이 목록 맨 위에 표시됩니다.

**최상의 성능을 보이는 모델 배포**

AutoML 실험을 실행했으므로 생성된 최상의 성능 모델을 살펴볼 수 있습니다.

1.  **Penguin-classification** 실험 페이지에서 **View notebook for best model**을 선택하여 모델 학습에 사용된 notebook을 새 브라우저 탭에서 엽니다.
    *   *설명*: AutoML은 최상의 모델을 생성하는 데 사용된 전체 코드(데이터 로딩, 전처리, 모델 학습, 평가)를 포함하는 notebook을 자동으로 생성합니다. 이를 통해 AutoML이 수행한 작업을 투명하게 확인하고 필요에 따라 코드를 수정하거나 재사용할 수 있습니다.
2.  notebook의 셀을 스크롤하면서 모델 학습에 사용된 코드를 확인합니다.
3.  notebook이 포함된 브라우저 탭을 닫고 **Penguin-classification** 실험 페이지로 돌아갑니다.
4.  실행 목록에서 첫 번째 실행(최상의 모델을 생성한 실행)의 이름을 선택하여 엽니다.
5.  **Artifacts** 섹션에서 모델이 MLflow artifact로 저장되었음을 확인합니다. 그런 다음 **Register model** 버튼을 사용하여 모델을 `Penguin-Classifier`라는 새 모델로 등록합니다.
    *   *설명*: **Artifacts**에는 모델 파일, 환경 파일, 생성된 notebook 등 실행과 관련된 파일들이 저장됩니다. **Register model**을 클릭하면 이 학습된 모델을 MLflow Model Registry에 등록할 수 있습니다. Model Registry는 모델 버전 관리, 스테이징, 프로덕션 배포를 용이하게 합니다.
6.  왼쪽 사이드바에서 **Models** 페이지로 전환합니다. 그런 다음 방금 등록한 **Penguin-Classifier** 모델을 선택합니다.
7.  **Penguin-Classifier** 페이지에서 **Use model for inference** 버튼을 사용하여 다음 설정으로 새 실시간 엔드포인트를 만듭니다:
    *   **Model**: `Penguin-Classifier`
    *   **Model version**: `1`
    *   **Endpoint**: `classify-penguin`
        *   *설명*: 모델을 실시간으로 예측 요청에 사용할 수 있도록 하는 API 엔드포인트입니다.
    *   **Compute size**: `Small`
        *   *설명*: 엔드포인트에 할당될 컴퓨팅 리소스의 크기입니다. 트래픽 양에 따라 선택합니다. `Small`은 테스트 또는 낮은 트래픽에 적합합니다.
    *   serving endpoint는 새 cluster에서 호스팅되며, 생성하는 데 몇 분 정도 걸릴 수 있습니다.

8.  엔드포인트가 생성되면 오른쪽 상단의 **Query endpoint** 버튼을 사용하여 엔드포인트를 테스트할 수 있는 인터페이스를 엽니다. 그런 다음 테스트 인터페이스의 **Browser** 탭에서 다음 JSON 요청을 입력하고 **Send Request** 버튼을 사용하여 엔드포인트를 호출하고 예측을 생성합니다.

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
    *   *설명*: `dataframe_records`는 모델에 입력으로 전달될 데이터 레코드의 리스트입니다. 각 레코드는 feature 이름과 해당 값을 가진 딕셔너리 형태여야 합니다. 이 JSON 형식은 Databricks Model Serving에서 일반적으로 사용됩니다.

9.  펭귄 feature에 대해 몇 가지 다른 값으로 실험하고 반환되는 결과를 관찰합니다. 그런 다음 테스트 인터페이스를 닫습니다.

**엔드포인트 삭제**

엔드포인트가 더 이상 필요하지 않으면 불필요한 비용을 피하기 위해 삭제해야 합니다.

1.  `classify-penguin` 엔드포인트 페이지의 **⁝** 메뉴에서 **Delete**를 선택합니다.

**정리**

Azure Databricks portal의 **Compute** 페이지에서 cluster를 선택하고 **■ Terminate**를 선택하여 종료합니다.

Azure Databricks 탐색을 마쳤다면, 불필요한 Azure 비용을 피하고 subscription의 용량을 확보하기 위해 생성한 resource를 삭제할 수 있습니다.

