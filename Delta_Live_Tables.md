
---

**Delta Live Tables로 데이터 파이프라인 생성**

Delta Live Tables는 신뢰할 수 있고 유지 관리가 가능하며 테스트 가능한 데이터 처리 파이프라인을 구축하기 위한 선언적 프레임워크입니다. Pipeline은 Delta Live Tables로 데이터 처리 워크플로우를 구성하고 실행하는 주요 단위입니다. Python 또는 SQL로 선언된 Directed Acyclic Graph (DAG)를 통해 데이터 소스를 대상 데이터셋에 연결합니다.

이 실습을 완료하는 데 약 40분이 소요됩니다.

참고: Azure Databricks 사용자 인터페이스는 지속적으로 개선되고 있습니다. 이 연습의 지침이 작성된 이후 사용자 인터페이스가 변경되었을 수 있습니다.

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
6.  스크립트가 완료될 때까지 기다립니다 - 일반적으로 약 5분 정도 걸리지만 경우에 따라 더 오래 걸릴 수 있습니다. 기다리는 동안 Azure Databricks 설명서의 [What is Delta Live Tables?](https://docs.databricks.com/delta-live-tables/index.html) 문서를 검토하십시오.

**Cluster 생성**

Azure Databricks는 Apache Spark cluster를 사용하여 여러 node에서 데이터를 병렬로 처리하는 분산 처리 플랫폼입니다. 각 cluster는 작업을 조정하는 driver node와 처리 작업을 수행하는 worker node로 구성됩니다. 이 연습에서는 랩 환경(resource가 제한될 수 있음)에서 사용되는 compute resource를 최소화하기 위해 single-node cluster를 만듭니다. 프로덕션 환경에서는 일반적으로 여러 worker node가 있는 cluster를 만듭니다.

팁: Azure Databricks workspace에 13.3 LTS 이상의 runtime version을 가진 cluster가 이미 있다면, 해당 cluster를 사용하여 이 연습을 완료하고 이 절차를 건너뛸 수 있습니다.

1.  Azure portal에서 스크립트에 의해 생성된 `msl-xxxxxxx` resource group (또는 기존 Azure Databricks workspace가 포함된 resource group)으로 이동합니다.
2.  Azure Databricks Service resource (설정 스크립트를 사용하여 생성한 경우 `databricks-xxxxxxx` 이름)를 선택합니다.
3.  workspace의 **Overview** 페이지에서 **Launch Workspace** 버튼을 사용하여 새 브라우저 탭에서 Azure Databricks workspace를 엽니다. 메시지가 표시되면 로그인합니다.

    팁: Databricks Workspace portal을 사용하면 다양한 팁과 알림이 표시될 수 있습니다. 이를 무시하고 이 연습의 작업을 완료하기 위한 지침을 따르십시오.

4.  왼쪽 사이드바에서 **(+) New** 작업을 선택한 다음 **Cluster**를 선택합니다 (More 하위 메뉴에서 찾아야 할 수도 있습니다).
5.  **New Cluster** 페이지에서 다음 설정으로 새 cluster를 만듭니다:
    *   **Cluster name**: *사용자 이름*'s cluster (기본 클러스터 이름)
    *   **Policy**: Unrestricted
    *   **Cluster mode**: Single Node
        *   *설명*: Single Node cluster는 driver node만 있고 worker node가 없는 cluster입니다. 소규모 데이터셋이나 개발/테스트 목적에 적합하며, 이 연습에서는 리소스 사용을 최소화하기 위해 선택합니다.
    *   **Access mode**: Single user (사용자 계정 선택됨)
    *   **Databricks runtime version**: 13.3 LTS (Spark 3.4.1, Scala 2.12) 또는 그 이상
        *   *설명*: `Databricks runtime version`은 cluster에서 실행될 소프트웨어 스택을 정의합니다. Delta Live Tables는 특정 runtime version 이상을 요구할 수 있습니다.
    *   **Use Photon Acceleration**: Selected
        *   *설명*: Photon Acceleration은 Spark API와 호환되는 Databricks 네이티브 벡터화된 쿼리 엔진으로, SQL 및 DataFrame 워크로드의 성능을 향상시킵니다. Delta Live Tables 파이프라인의 성능을 높이는 데 도움이 될 수 있습니다.
    *   **Node type**: Standard_D4ds_v5
    *   **Terminate after** `20` **minutes of inactivity**
        *   *설명*: 일정 시간 동안 cluster가 비활성 상태일 때 자동으로 종료되도록 설정하여 불필요한 비용 발생을 방지합니다.
6.  cluster가 생성될 때까지 기다립니다. 1~2분 정도 걸릴 수 있습니다.

    참고: cluster 시작에 실패하면 Azure Databricks workspace가 프로비저닝된 지역에서 subscription의 quota가 부족할 수 있습니다. 자세한 내용은 [CPU core limit prevents cluster creation](https://learn.microsoft.com/azure/databricks/kb/clusters/cpu-core-limit-prevents-cluster-creation)을 참조하십시오. 이 경우 workspace를 삭제하고 다른 지역에 새 workspace를 만들어 볼 수 있습니다. 다음과 같이 `setup.ps1` 스크립트에 지역을 매개변수로 지정할 수 있습니다: `./mslearn-databricks/setup.ps1 eastus`

**Notebook 생성 및 데이터 수집**

1.  사이드바에서 **(+) New** 링크를 사용하여 **Notebook**을 만듭니다.
2.  기본 notebook 이름(Untitled Notebook [날짜])을 **Create a pipeline with Delta Live tables**로 변경하고 **Connect** 드롭다운 목록에서 아직 선택되지 않았다면 cluster를 선택합니다. cluster가 실행 중이 아니면 시작하는 데 1분 정도 걸릴 수 있습니다.
3.  notebook의 첫 번째 셀에 다음 코드를 입력합니다. 이 코드는 셸 명령을 사용하여 GitHub에서 데이터 파일을 cluster에서 사용하는 파일 시스템으로 다운로드합니다.

    ```python
    %sh
    rm -r /dbfs/delta_lab -f
    mkdir /dbfs/delta_lab
    wget -O /dbfs/delta_lab/covid_data.csv https://github.com/MicrosoftLearning/mslearn-databricks/raw/main/data/covid_data.csv
    ```
    *   `%sh`: Databricks notebook에서 셸 명령을 실행할 수 있게 해주는 magic command입니다.
    *   `rm -r /dbfs/delta_lab -f`: `/dbfs/delta_lab` 디렉토리가 이미 존재하면 해당 디렉토리와 그 내용을 강제로(-f) 재귀적으로(-r) 삭제합니다. `/dbfs/`는 Databricks File System (DBFS)의 루트 경로입니다.
    *   `mkdir /dbfs/delta_lab`: `/dbfs/` 경로 아래에 `delta_lab`이라는 새 디렉토리를 만듭니다. 이 디렉토리에 원본 데이터를 저장합니다.
    *   `wget -O /dbfs/delta_lab/covid_data.csv <URL>`: 지정된 URL에서 `covid_data.csv` 파일을 다운로드하여 `/dbfs/delta_lab/covid_data.csv`라는 이름으로 저장합니다.

4.  셀 왼쪽의 **▸ Run Cell** 메뉴 옵션을 사용하여 실행합니다. 그런 다음 코드가 실행하는 Spark job이 완료될 때까지 기다립니다.

**SQL을 사용하여 Delta Live Tables Pipeline 생성**

1.  새 notebook을 만들고 이름을 **Pipeline Notebook**으로 변경합니다.
2.  notebook 이름 옆에서 **Python**을 선택하고 기본 언어를 **SQL**로 변경합니다.
    *   *설명*: Delta Live Tables 파이프라인은 SQL 또는 Python을 사용하여 정의할 수 있습니다. 이 연습에서는 SQL을 사용합니다.
3.  첫 번째 셀에 다음 코드를 실행하지 않고 입력합니다. 모든 셀은 파이프라인이 생성된 후 실행됩니다. 이 코드는 이전에 다운로드한 원시 데이터로 채워질 Delta Live Table을 정의합니다:

    ```sql
    CREATE OR REFRESH LIVE TABLE raw_covid_data
    COMMENT "COVID sample dataset. This data was ingested from the COVID-19 Data Repository by the Center for Systems Science and Engineering (CSSE) at Johns Hopkins University."
    AS
    SELECT
      Last_Update,
      Country_Region,
      Confirmed,
      Deaths,
      Recovered
    FROM read_files('dbfs:/delta_lab/covid_data.csv', format => 'csv', header => true)
    ```
    *   `CREATE OR REFRESH LIVE TABLE raw_covid_data`: `raw_covid_data`라는 이름의 `LIVE TABLE`을 생성하거나 이미 존재하면 새로 고칩니다. `LIVE TABLE`은 Delta Live Tables의 핵심 구성 요소로, 스트리밍 또는 배치 데이터를 처리하고 결과를 Delta Lake 테이블에 저장합니다.
    *   `COMMENT "..."`: 테이블에 대한 설명을 추가합니다.
    *   `AS SELECT ...`: 테이블의 내용을 정의하는 SQL 쿼리입니다.
    *   `FROM read_files('dbfs:/delta_lab/covid_data.csv', format => 'csv', header => true)`: Delta Live Tables의 내장 함수인 `read_files`를 사용하여 DBFS 경로에 있는 CSV 파일을 읽습니다.
        *   `'dbfs:/delta_lab/covid_data.csv'`: 읽어올 파일의 경로입니다.
        *   `format => 'csv'`: 파일 형식이 CSV임을 지정합니다.
        *   `header => true`: CSV 파일의 첫 번째 줄을 헤더(컬럼 이름)로 사용하도록 지정합니다.
        *   *설명*: `read_files`는 Auto Loader와 유사하게 작동하여 지정된 경로에서 새 파일을 자동으로 감지하고 처리할 수 있습니다. 이 예제에서는 단일 파일을 읽지만, 폴더를 지정하여 여러 파일을 읽을 수도 있습니다.

4.  첫 번째 셀 아래에 **+ Code** 아이콘을 사용하여 새 셀을 추가하고, 이전 테이블의 데이터를 분석 전에 쿼리, 필터링 및 형식화하는 다음 코드를 입력합니다.

    ```sql
    CREATE OR REFRESH LIVE TABLE processed_covid_data(
      CONSTRAINT valid_country_region EXPECT (Country_Region IS NOT NULL) ON VIOLATION FAIL UPDATE
    )
    COMMENT "Formatted and filtered data for analysis."
    AS
    SELECT
        TO_DATE(Last_Update, 'MM/dd/yyyy') as Report_Date,
        Country_Region,
        Confirmed,
        Deaths,
        Recovered
    FROM live.raw_covid_data;
    ```
    *   `CREATE OR REFRESH LIVE TABLE processed_covid_data(...)`: `processed_covid_data`라는 새 `LIVE TABLE`을 정의합니다.
    *   `CONSTRAINT valid_country_region EXPECT (Country_Region IS NOT NULL) ON VIOLATION FAIL UPDATE`: 데이터 품질 제약 조건을 정의합니다.
        *   `EXPECT (Country_Region IS NOT NULL)`: `Country_Region` 컬럼이 `NULL`이 아니어야 한다는 조건을 지정합니다.
        *   `ON VIOLATION FAIL UPDATE`: 이 조건이 위반되면(즉, `Country_Region`이 `NULL`이면) 해당 레코드의 처리가 실패하고 파이프라인 업데이트가 중단됩니다. 다른 옵션으로는 `DROP ROW` (해당 행 삭제) 또는 `QUARANTINE` (나중에 처리하기 위해 격리) 등이 있습니다.
    *   `COMMENT "..."`: 테이블에 대한 설명을 추가합니다.
    *   `AS SELECT ...`: 이 테이블을 채우는 쿼리입니다.
    *   `TO_DATE(Last_Update, 'MM/dd/yyyy') as Report_Date`: `Last_Update` 컬럼(문자열)을 'MM/dd/yyyy' 형식으로 해석하여 날짜 타입으로 변환하고 `Report_Date`라는 이름으로 지정합니다.
    *   `FROM live.raw_covid_data`: 이전 단계에서 정의한 `raw_covid_data` `LIVE TABLE`에서 데이터를 가져옵니다. `live.` 스키마 접두사는 같은 Delta Live Tables 파이프라인 내의 다른 `LIVE TABLE`을 참조할 때 사용됩니다.

5.  세 번째 새 코드 셀에 다음 코드를 입력합니다. 이 코드는 파이프라인이 성공적으로 실행되면 추가 분석을 위해 강화된 데이터 뷰를 만듭니다.

    ```sql
    CREATE OR REFRESH LIVE TABLE aggregated_covid_data
    COMMENT "Aggregated daily data for the US with total counts."
    AS
    SELECT
        Report_Date,
        sum(Confirmed) as Total_Confirmed,
        sum(Deaths) as Total_Deaths,
        sum(Recovered) as Total_Recovered
    FROM live.processed_covid_data
    GROUP BY Report_Date;
    ```
    *   `CREATE OR REFRESH LIVE TABLE aggregated_covid_data`: `aggregated_covid_data`라는 새 `LIVE TABLE`을 정의합니다.
    *   `COMMENT "..."`: 테이블에 대한 설명을 추가합니다.
    *   `AS SELECT ...`: 이 테이블을 채우는 쿼리입니다.
    *   `sum(Confirmed) as Total_Confirmed`, `sum(Deaths) as Total_Deaths`, `sum(Recovered) as Total_Recovered`: 각 `Report_Date`별로 확진자, 사망자, 회복자 수를 합산합니다.
    *   `FROM live.processed_covid_data`: 이전 단계에서 정의한 `processed_covid_data` `LIVE TABLE`에서 데이터를 가져옵니다.
    *   `GROUP BY Report_Date`: `Report_Date`를 기준으로 그룹화하여 일별 집계를 수행합니다.

6.  왼쪽 사이드바에서 **Delta Live Tables**를 선택한 다음 **Create Pipeline**을 선택합니다.
7.  **Create pipeline** 페이지에서 다음 설정으로 새 파이프라인을 만듭니다:
    *   **Pipeline name**: `Covid Pipeline`
    *   **Product edition**: `Advanced`
        *   *설명*: Delta Live Tables에는 `Core`, `Pro`, `Advanced` 세 가지 에디션이 있으며, 각각 제공하는 기능이 다릅니다. `Advanced`는 데이터 품질 제약 조건, 향상된 자동 스케일링 등의 고급 기능을 제공합니다.
    *   **Pipeline mode**: `Triggered`
        *   *설명*: `Triggered` 모드는 파이프라인을 수동으로 또는 예약된 시간에 한 번 실행합니다. `Continuous` 모드는 데이터가 도착하는 대로 지속적으로 실행됩니다.
    *   **Source code**: `Users/user@name` 폴더에서 **Pipeline Notebook** notebook으로 이동하여 선택합니다.
        *   *설명*: Delta Live Tables 파이프라인의 정의가 포함된 notebook 파일의 경로입니다.
    *   **Storage options**: `Hive Metastore`
    *   **Storage location**: `dbfs:/pipelines/delta_lab`
        *   *설명*: 파이프라인 실행과 관련된 모든 데이터(생성된 테이블 데이터, 체크포인트, 로그 등)가 저장될 DBFS 경로입니다.
    *   **Target schema**: `default` 입력
        *   *설명*: 파이프라인에서 생성된 테이블이 Hive Metastore에 등록될 때 사용될 데이터베이스(스키마) 이름입니다. `default`로 지정하면 기본 데이터베이스에 테이블이 생성됩니다.
8.  **Create**를 선택한 다음 **Start**를 선택합니다. 그런 다음 파이프라인이 실행될 때까지 기다립니다 (시간이 다소 걸릴 수 있습니다).
    *   *설명*: 파이프라인이 시작되면 Delta Live Tables는 notebook의 코드를 분석하여 데이터 흐름(DAG)을 구성하고, 필요한 컴퓨팅 리소스를 프로비저닝한 후 데이터 처리를 시작합니다. 진행 상황은 UI에서 시각적으로 확인할 수 있습니다.

9.  파이프라인이 성공적으로 실행된 후, 처음에 만든 **Create a pipeline with Delta Live tables** notebook으로 돌아가서 새 셀에 다음 코드를 실행하여 지정된 저장 위치에 세 개의 새 테이블에 대한 파일이 모두 생성되었는지 확인합니다:

    ```python
    display(dbutils.fs.ls("dbfs:/pipelines/delta_lab/tables"))
    ```
    *   `dbutils.fs.ls("...")`: Databricks 유틸리티 함수로, 지정된 DBFS 경로의 파일 및 디렉토리 목록을 표시합니다. `dbfs:/pipelines/delta_lab/tables`는 Delta Live Tables가 생성한 테이블 데이터가 저장되는 기본 하위 경로입니다.
    *   `display(...)`: 결과를 Databricks notebook에 테이블 형태로 시각화하여 보여줍니다.

10. 다른 코드 셀을 추가하고 다음 코드를 실행하여 테이블이 `default` 데이터베이스에 생성되었는지 확인합니다:

    ```sql
    %sql

    SHOW TABLES
    ```
    *   `%sql`: Python notebook에서 SQL 코드를 실행하기 위한 magic command입니다.
    *   `SHOW TABLES`: 현재 선택된 데이터베이스(여기서는 `default` 스키마)의 모든 테이블 목록을 표시합니다. `raw_covid_data`, `processed_covid_data`, `aggregated_covid_data` 테이블이 보여야 합니다.

**결과 시각화**

테이블을 만든 후에는 DataFrame으로 로드하고 데이터를 시각화할 수 있습니다.

1.  **Create a pipeline with Delta Live tables** notebook에서 새 코드 셀을 추가하고 다음 코드를 실행하여 `aggregated_covid_data`를 DataFrame으로 로드합니다 (실제로는 SQL 쿼리 결과를 직접 사용합니다):

    ```sql
    %sql
       
    SELECT * FROM aggregated_covid_data
    ```
    *   *설명*: 이 SQL 쿼리는 `aggregated_covid_data` 테이블의 모든 데이터를 선택하여 notebook에 테이블 형태로 표시합니다.

2.  결과 테이블 위에서 **+**를 선택한 다음 **Visualization**을 선택하여 시각화 편집기를 열고 다음 옵션을 적용합니다:
    *   **Visualization type**: `Line` (선 그래프)
    *   **X Column**: `Report_Date` (X축)
    *   **Y Column**: 새 열을 추가하고 `Total_Confirmed`를 선택합니다. `Sum` 집계를 적용합니다. (Delta Live Table에서 이미 집계되었으므로, 시각화 설정에서는 'Value' 또는 'None'으로 집계를 설정하거나, Y축에 바로 `Total_Confirmed` 컬럼을 선택합니다.)
        *   *수정 제안*: `aggregated_covid_data` 테이블은 이미 `Report_Date`별로 `Total_Confirmed`가 집계되어 있습니다. 따라서 시각화 설정에서 Y축 컬럼에 `Total_Confirmed`를 선택하고, 추가적인 집계(Aggregation)는 `None` 또는 `Value`로 설정해야 합니다. `Sum`을 다시 적용하면 의도치 않은 결과가 나올 수 있습니다.
3.  시각화를 저장하고 notebook에서 결과 차트를 봅니다.

**정리**

Azure Databricks portal의 **Compute** 페이지에서 cluster를 선택하고 **■ Terminate**를 선택하여 종료합니다.

Azure Databricks 탐색을 마쳤다면, 불필요한 Azure 비용을 피하고 subscription의 용량을 확보하기 위해 생성한 resource를 삭제할 수 있습니다. Delta Live Tables 파이프라인 자체도 필요하지 않으면 **Delta Live Tables** UI에서 삭제할 수 있습니다.

---

