다음은 Delta Live Tables (DLT)를 사용하여 ETL 파이프라인을 구축하는 튜토리얼입니다. 

---

**튜토리얼: DLT로 ETL 파이프라인 구축**

https://learn.microsoft.com/en-us/azure/databricks/getting-started/data-pipeline-get-started

**문서**
2025년 4월 25일

**이 문서의 내용**
*   요구 사항
*   데이터셋 정보
*   1단계: 파이프라인 생성
*   2단계: DLT 파이프라인 개발
*   4개 더 보기

DLT와 Auto Loader를 사용하여 데이터 오케스트레이션을 위한 ETL (extract, transform, and load - 추출, 변환, 로드) 파이프라인을 생성하고 배포하는 방법을 배웁니다. ETL 파이프라인은 소스 시스템에서 데이터를 읽고, 데이터 품질 검사 및 레코드 중복 제거와 같은 요구 사항에 따라 해당 데이터를 변환하고, data warehouse 또는 data lake와 같은 대상 시스템에 데이터를 쓰는 단계를 구현합니다.

이 튜토리얼에서는 DLT와 Auto Loader를 사용하여 다음을 수행합니다:

*   원본 원시 데이터를 대상 테이블로 수집합니다.
*   원본 원시 데이터를 변환하고 변환된 데이터를 두 개의 대상 materialized views에 씁니다.
*   변환된 데이터를 쿼리합니다.
*   Databricks job을 사용하여 ETL 파이프라인을 자동화합니다.

DLT 및 Auto Loader에 대한 자세한 내용은 [DLT](https://docs.databricks.com/delta-live-tables/index.html) 및 [Auto Loader란 무엇인가?](https://docs.databricks.com/ingestion/auto-loader/index.html)를 참조하십시오.

**요구 사항**

이 튜토리얼을 완료하려면 다음 요구 사항을 충족해야 합니다:

*   Azure Databricks workspace에 로그인되어 있어야 합니다.
*   workspace에 Unity Catalog가 활성화되어 있어야 합니다.
    *   *설명*: **Unity Catalog**는 Databricks의 통합 데이터 거버넌스 솔루션으로, 데이터, AI 모델, 노트북 등을 중앙에서 관리하고 보안을 적용할 수 있게 해줍니다.
*   계정에 serverless compute가 활성화되어 있어야 합니다. Serverless DLT 파이프라인은 모든 workspace 지역에서 사용할 수 있는 것은 아닙니다. 사용 가능한 지역은 [지역별 제한된 기능](https://docs.databricks.com/reference/regions.html#features-with-limited-regional-availability)을 참조하십시오.
    *   *설명*: **Serverless compute**는 Databricks가 인프라를 관리하여 사용자가 클러스터 구성 및 관리에 대해 걱정할 필요 없이 쿼리 및 파이프라인을 실행할 수 있도록 하는 기능입니다.
*   compute resource를 생성할 권한 또는 compute resource에 대한 액세스 권한이 있어야 합니다.
*   catalog에 새 schema를 생성할 권한이 있어야 합니다. 필요한 권한은 `ALL PRIVILEGES` 또는 `USE CATALOG` 및 `CREATE SCHEMA`입니다.
*   기존 schema에 새 volume을 생성할 권한이 있어야 합니다. 필요한 권한은 `ALL PRIVILEGES` 또는 `USE SCHEMA` 및 `CREATE VOLUME`입니다.
    *   *설명*: **Volume**은 Unity Catalog에서 관리하는 스토리지 위치로, 테이블이 아닌 데이터(예: CSV, JSON 파일)를 저장하고 액세스하는 데 사용됩니다.

**데이터셋 정보**

이 예제에서 사용되는 데이터셋은 현대 음악 트랙의 feature와 metadata 모음인 [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/)의 하위 집합입니다. 이 데이터셋은 Azure Databricks workspace에 포함된 샘플 데이터셋에서 사용할 수 있습니다.

**1단계: 파이프라인 생성**

먼저 DLT에서 ETL 파이프라인을 만듭니다. DLT는 DLT 구문을 사용하여 notebooks 또는 파일(source code라고 함)에 정의된 종속성을 해결하여 파이프라인을 만듭니다. 각 source code 파일에는 하나의 언어만 포함될 수 있지만 파이프라인에 여러 언어별 notebooks 또는 파일을 추가할 수 있습니다. 자세한 내용은 [DLT](https://docs.databricks.com/delta-live-tables/index.html)를 참조하십시오.

> **중요**
>
> Source code 필드를 비워두면 source code 작성을 위한 notebook이 자동으로 생성되고 구성됩니다.

이 튜토리얼에서는 serverless compute와 Unity Catalog를 사용합니다. 지정되지 않은 모든 구성 옵션에 대해서는 기본 설정을 사용합니다. workspace에서 serverless compute가 활성화되지 않았거나 지원되지 않는 경우 기본 compute 설정을 사용하여 튜토리얼을 작성된 대로 완료할 수 있습니다. 기본 compute 설정을 사용하는 경우, **Create pipeline** UI의 **Destination** 섹션에 있는 **Storage options**에서 Unity Catalog를 수동으로 선택해야 합니다.

DLT에서 새 ETL 파이프라인을 만들려면 다음 단계를 따르십시오:

1.  사이드바에서 **Pipelines**를 클릭합니다.
2.  **Create pipeline**을 클릭하고 **ETL pipeline**을 선택합니다.
3.  **Pipeline name**에 고유한 파이프라인 이름을 입력합니다.
4.  **Serverless** 체크박스를 선택합니다.
5.  **Destination**에서 테이블이 게시될 Unity Catalog 위치를 구성하려면 기존 **Catalog**를 선택하고 **Schema**에 새 이름을 작성하여 catalog에 새 schema를 만듭니다.
    *   *설명*: **Destination**은 DLT 파이프라인이 생성한 테이블이 저장될 위치를 지정합니다. Unity Catalog를 사용하면 catalog와 schema를 지정하여 테이블을 관리할 수 있습니다.
6.  **Create**를 클릭합니다.
    새 파이프라인에 대한 파이프라인 UI가 나타납니다.

**2단계: DLT 파이프라인 개발**

> **중요**
>
> Notebooks는 단일 프로그래밍 언어만 포함할 수 있습니다. 파이프라인 source code notebooks에 Python과 SQL 코드를 혼합하지 마십시오.

이 단계에서는 Databricks Notebooks를 사용하여 DLT 파이프라인의 source code를 대화형으로 개발하고 검증합니다.

이 코드는 증분 데이터 수집을 위해 Auto Loader를 사용합니다. Auto Loader는 클라우드 객체 저장소에 새 파일이 도착하면 자동으로 감지하고 처리합니다. 자세한 내용은 [Auto Loader란 무엇인가?](https://docs.databricks.com/ingestion/auto-loader/index.html)를 참조하십시오.

파이프라인에 대해 빈 source code notebook이 자동으로 생성되고 구성됩니다. 이 notebook은 사용자 디렉토리의 새 디렉토리에 생성됩니다. 새 디렉토리와 파일의 이름은 파이프라인 이름과 일치합니다. 예: `/Users/someone@example.com/my_pipeline/my_pipeline`.

DLT 파이프라인을 개발할 때 Python 또는 SQL을 선택할 수 있습니다. 두 언어 모두에 대한 예제가 포함되어 있습니다. 언어 선택에 따라 기본 notebook 언어를 선택했는지 확인하십시오. DLT 파이프라인 코드 개발을 위한 notebook 지원에 대한 자세한 내용은 [DLT에서 notebook으로 ETL 파이프라인 개발 및 디버그](https://docs.databricks.com/delta-live-tables/delta-live-tables-notebooks.html)를 참조하십시오.

1.  이 notebook에 액세스할 수 있는 링크는 **Pipeline details** 패널의 **Source code** 필드 아래에 있습니다. 다음 단계로 진행하기 전에 링크를 클릭하여 notebook을 엽니다.
2.  오른쪽 상단의 **Connect**를 클릭하여 compute 구성 메뉴를 엽니다.
3.  1단계에서 만든 파이프라인의 이름 위로 마우스를 가져갑니다.
4.  **Connect**를 클릭합니다.
5.  상단에 있는 notebook 제목 옆에서 notebook의 기본 언어(Python 또는 SQL)를 선택합니다.
6.  다음 코드를 notebook의 셀에 복사하여 붙여넣습니다.

**Python**

```python
# 모듈 가져오기
import dlt # Delta Live Tables 모듈
from pyspark.sql.functions import * # PySpark SQL 함수
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType, StructField # PySpark 데이터 타입

# 원본 데이터 경로 정의
file_path = f"/databricks-datasets/songs/data-001/" # Databricks에 내장된 샘플 데이터셋 경로

# volume에서 데이터를 수집하기 위한 스트리밍 테이블 정의
# 데이터의 스키마를 명시적으로 정의합니다.
schema = StructType(
  [
    StructField("artist_id", StringType(), True),
    StructField("artist_lat", DoubleType(), True),
    StructField("artist_long", DoubleType(), True),
    StructField("artist_location", StringType(), True),
    StructField("artist_name", StringType(), True),
    StructField("duration", DoubleType(), True),
    StructField("end_of_fade_in", DoubleType(), True),
    StructField("key", IntegerType(), True),
    StructField("key_confidence", DoubleType(), True),
    StructField("loudness", DoubleType(), True),
    StructField("release", StringType(), True),
    StructField("song_hotnes", DoubleType(), True), # 오타 가능성: 'song_hotness'
    StructField("song_id", StringType(), True),
    StructField("start_of_fade_out", DoubleType(), True),
    StructField("tempo", DoubleType(), True),
    StructField("time_signature", DoubleType(), True), # 스키마는 DoubleType이지만 SQL 예제에서는 INT입니다. 데이터 유형 확인 필요.
    StructField("time_signature_confidence", DoubleType(), True),
    StructField("title", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("partial_sequence", IntegerType(), True)
  ]
)

# @dlt.table 데코레이터는 이 함수가 DLT 테이블을 정의함을 나타냅니다.
@dlt.table(
  comment="Million Song Dataset의 하위 집합에서 가져온 원시 데이터; 현대 음악 트랙의 feature 및 metadata 모음입니다."
)
def songs_raw(): # 이 함수는 'songs_raw'라는 DLT 테이블을 생성합니다.
  return (spark.readStream # 스트리밍 방식으로 데이터를 읽습니다.
    .format("cloudFiles") # Auto Loader를 사용함을 나타냅니다.
    .schema(schema) # 위에서 정의한 스키마를 사용합니다.
    .option("cloudFiles.format", "csv") # Auto Loader가 CSV 파일을 처리하도록 설정합니다.
    .option("sep","\t") # CSV 파일의 구분자가 탭(tab)임을 명시합니다.
    # .option("inferSchema", True) # 스키마를 명시적으로 제공했으므로 이 옵션은 주석 처리하거나 삭제할 수 있습니다.
    .load(file_path)) # 지정된 경로에서 데이터를 로드합니다.

# 데이터를 검증하고 컬럼 이름을 변경하는 materialized view 정의
@dlt.table(
  comment="분석을 위해 데이터가 정리되고 준비된 Million Song Dataset입니다."
)
# @dlt.expect 데코레이터는 데이터 품질 제약 조건을 정의합니다.
@dlt.expect("valid_artist_name", "artist_name IS NOT NULL") # artist_name은 NULL이 아니어야 합니다.
@dlt.expect("valid_title", "song_title IS NOT NULL") # song_title은 NULL이 아니어야 합니다. (다음 단계에서 'title'이 'song_title'로 변경됨)
@dlt.expect("valid_duration", "duration > 0") # duration은 0보다 커야 합니다.
def songs_prepared(): # 이 함수는 'songs_prepared'라는 DLT 테이블(materialized view)을 생성합니다.
  return (
    spark.read.table("songs_raw") # 'songs_raw' 테이블(앞서 정의한 DLT 테이블)에서 데이터를 읽습니다.
      .withColumnRenamed("title", "song_title") # 'title' 컬럼 이름을 'song_title'로 변경합니다.
      .select("artist_id", "artist_name", "duration", "release", "tempo", "time_signature", "song_title", "year") # 필요한 컬럼만 선택합니다.
  )

# 데이터의 필터링, 집계 및 정렬된 뷰를 가진 materialized view 정의
@dlt.table(
  comment="매년 가장 많은 곡을 발표한 아티스트가 발표한 곡의 수를 요약한 테이블입니다."
)
def top_artists_by_year(): # 이 함수는 'top_artists_by_year'라는 DLT 테이블(materialized view)을 생성합니다.
  return (
    spark.read.table("songs_prepared") # 'songs_prepared' 테이블에서 데이터를 읽습니다.
      .filter(expr("year > 0")) # 'year'가 0보다 큰 데이터만 필터링합니다.
      .groupBy("artist_name", "year") # 'artist_name'과 'year'로 그룹화합니다.
      .count().withColumnRenamed("count", "total_number_of_songs") # 각 그룹의 개수를 세고 컬럼 이름을 'total_number_of_songs'로 변경합니다.
      .sort(desc("total_number_of_songs"), desc("year")) # 'total_number_of_songs'와 'year'의 내림차순으로 정렬합니다.
  )
```

**SQL**

```sql
-- volume에서 데이터를 수집하기 위한 스트리밍 테이블 정의
CREATE OR REFRESH STREAMING TABLE songs_raw -- 'songs_raw'라는 스트리밍 테이블을 생성하거나 새로 고칩니다.
( -- 테이블 스키마를 명시적으로 정의합니다.
 artist_id STRING,
 artist_lat DOUBLE,
 artist_long DOUBLE,
 artist_location STRING,
 artist_name STRING,
 duration DOUBLE,
 end_of_fade_in DOUBLE,
 key INT,
 key_confidence DOUBLE,
 loudness DOUBLE,
 release STRING,
 song_hotnes DOUBLE, -- 오타 가능성: 'song_hotness'
 song_id STRING,
 start_of_fade_out DOUBLE,
 tempo DOUBLE,
 time_signature INT, -- Python 예제에서는 DoubleType입니다. 데이터 유형 일관성 확인 필요.
 time_signature_confidence DOUBLE,
 title STRING,
 year INT,
 partial_sequence STRING, -- Python 예제에서는 IntegerType입니다. 데이터 유형 일관성 확인 필요.
 value STRING -- 이 컬럼은 Auto Loader가 CSV 파일의 전체 행을 문자열로 읽을 때 자동으로 추가될 수 있습니다. 또는, schema가 맞지 않을 때 _rescued_data 컬럼에 정보가 들어갈 수 있습니다. 여기서는 명시적으로 정의되어 있습니다. 만약 TSV 파일이고 각 컬럼이 잘 파싱된다면 이 컬럼은 불필요할 수 있습니다.
)
COMMENT "Million Song Dataset의 하위 집합에서 가져온 원시 데이터; 현대 음악 트랙의 feature 및 metadata 모음입니다."
AS SELECT * -- 모든 컬럼을 선택합니다.
FROM STREAM read_files( -- STREAM 키워드는 Auto Loader를 사용하여 스트리밍 방식으로 파일을 읽음을 나타냅니다.
'/databricks-datasets/songs/data-001/', -- 읽어올 파일 또는 폴더 경로입니다.
format => 'csv', -- 파일 형식이 CSV임을 명시합니다.
header => 'true', -- CSV 파일의 첫 줄을 헤더로 사용합니다.
delimiter => '\t' -- 구분자가 탭(tab)임을 명시합니다.
-- inferSchema => 'true' -- 스키마를 명시적으로 정의했으므로 이 옵션은 불필요할 수 있습니다.
);

-- 데이터를 검증하고 컬럼 이름을 변경하는 materialized view 정의
CREATE OR REFRESH MATERIALIZED VIEW songs_prepared( -- 'songs_prepared'라는 materialized view를 생성하거나 새로 고칩니다.
-- CONSTRAINT 키워드를 사용하여 데이터 품질 제약 조건을 정의합니다.
CONSTRAINT valid_artist_name EXPECT (artist_name IS NOT NULL), -- artist_name은 NULL이 아니어야 합니다.
CONSTRAINT valid_title EXPECT (song_title IS NOT NULL), -- song_title은 NULL이 아니어야 합니다.
CONSTRAINT valid_duration EXPECT (duration > 0) -- duration은 0보다 커야 합니다.
)
COMMENT "분석을 위해 데이터가 정리되고 준비된 Million Song Dataset입니다."
AS SELECT artist_id, artist_name, duration, release, tempo, time_signature, title AS song_title, year -- 필요한 컬럼을 선택하고 'title'을 'song_title'로 변경합니다.
FROM LIVE.songs_raw; -- LIVE. 스키마를 사용하여 같은 DLT 파이프라인 내의 'songs_raw' 테이블을 참조합니다.

-- 데이터의 필터링, 집계 및 정렬된 뷰를 가진 materialized view 정의
CREATE OR REFRESH MATERIALIZED VIEW top_artists_by_year -- 'top_artists_by_year'라는 materialized view를 생성하거나 새로 고칩니다.
COMMENT "매년 가장 많은 곡을 발표한 아티스트가 발표한 곡의 수를 요약한 테이블입니다."
AS SELECT
 artist_name,
 year,
 COUNT(*) AS total_number_of_songs -- 각 그룹의 곡 수를 계산하고 'total_number_of_songs'로 명명합니다.
FROM LIVE.songs_prepared -- 'songs_prepared' 테이블에서 데이터를 가져옵니다.
WHERE year > 0 -- 'year'가 0보다 큰 데이터만 필터링합니다.
GROUP BY artist_name, year -- 'artist_name'과 'year'로 그룹화합니다.
ORDER BY total_number_of_songs DESC, year DESC; -- 'total_number_of_songs'와 'year'의 내림차순으로 정렬합니다.
```

7.  **Start**를 클릭하여 연결된 파이프라인에 대한 업데이트를 시작합니다.
    *   *설명*: **Start** 버튼은 DLT 파이프라인 UI에서 파이프라인 실행을 시작하는 버튼입니다. Notebook 내에서 실행하는 것이 아니라, DLT 파이프라인 UI에서 이 notebook을 source code로 지정한 파이프라인을 실행합니다.

**3단계: 변환된 데이터 쿼리**

이 단계에서는 ETL 파이프라인에서 처리된 데이터를 쿼리하여 노래 데이터를 분석합니다. 이 쿼리는 이전 단계에서 만든 준비된 레코드를 사용합니다.

먼저, 1990년 이후 매년 가장 많은 곡을 발표한 아티스트를 찾는 쿼리를 실행합니다.

1.  사이드바에서 **SQL Editor 아이콘 SQL Editor**를 클릭합니다.
2.  **Add 또는 더하기 아이콘 새 탭 아이콘**을 클릭하고 메뉴에서 **Create new query**를 선택합니다.
3.  다음을 입력합니다:

    ```sql
    -- 1990년 또는 그 이후 매년 어떤 아티스트가 가장 많은 곡을 발표했습니까?
    SELECT artist_name, total_number_of_songs, year
    FROM <catalog>.<schema>.top_artists_by_year
    WHERE year >= 1990
    ORDER BY total_number_of_songs DESC, year DESC
    ```
    `<catalog>`와 `<schema>`를 테이블이 있는 catalog 및 schema의 이름으로 바꿉니다. 예: `data_pipelines.songs_data.top_artists_by_year`.
    *   *설명*: 이 쿼리는 2단계에서 DLT 파이프라인으로 생성한 `top_artists_by_year` 테이블을 사용합니다. Unity Catalog를 사용하므로 `catalog_name.schema_name.table_name` 형식으로 테이블을 참조합니다.

4.  **Run selected**를 클릭합니다.

이제 4/4 박자와 춤추기 좋은 템포의 노래를 찾는 다른 쿼리를 실행합니다.

1.  **Add 또는 더하기 아이콘 새 탭 아이콘**을 클릭하고 메뉴에서 **Create new query**를 선택합니다.
2.  다음 코드를 입력합니다:

    ```sql
    -- 4/4 박자와 춤추기 좋은 템포의 노래 찾기
    SELECT artist_name, song_title, tempo
    FROM <catalog>.<schema>.songs_prepared
    WHERE time_signature = 4 AND tempo between 100 and 140;
    ```
    `<catalog>`와 `<schema>`를 테이블이 있는 catalog 및 schema의 이름으로 바꿉니다. 예: `data_pipelines.songs_data.songs_prepared`.
    *   *설명*: 이 쿼리는 `songs_prepared` 테이블을 사용하여 특정 조건(4/4 박자, 템포 100-140)을 만족하는 노래를 찾습니다.

3.  **Run selected**를 클릭합니다.

**4단계: DLT 파이프라인을 실행하는 job 생성**

다음으로, Databricks job을 사용하여 데이터 수집, 처리 및 분석 단계를 자동화하는 workflow를 만듭니다.

1.  workspace의 사이드바에서 **Workflows 아이콘 Workflows**를 클릭하고 **Create job**을 클릭합니다.
2.  작업 제목 상자에서 `New Job <날짜 및 시간>`을 작업 이름으로 바꿉니다. 예: `Songs workflow`.
3.  **Task name**에 첫 번째 작업의 이름을 입력합니다. 예: `ETL_songs_data`.
4.  **Type**에서 **Pipeline**을 선택합니다.
5.  **Pipeline**에서 1단계에서 만든 DLT 파이프라인을 선택합니다.
6.  **Create**를 클릭합니다.
7.  workflow를 실행하려면 **Run Now**를 클릭합니다. 실행에 대한 세부 정보를 보려면 **Runs** 탭을 클릭합니다. 작업을 클릭하여 작업 실행에 대한 세부 정보를 봅니다.
8.  workflow가 완료되었을 때 결과를 보려면 **Go to the latest successful run** 또는 작업 실행의 **Start time**을 클릭합니다. **Output** 페이지가 나타나고 쿼리 결과를 표시합니다.
    *   *설명*: DLT 파이프라인 실행의 출력은 주로 생성된 테이블입니다. 파이프라인 실행 자체의 "Output"은 일반적으로 로그 및 상태 정보를 의미합니다. 쿼리 결과는 SQL Editor에서 별도로 확인합니다.

Databricks Jobs의 모니터링 및 관찰 가능성에 대한 자세한 내용은 [Databricks Jobs 모니터링 및 관찰 가능성](https://docs.databricks.com/workflows/jobs/job-runs.html#monitoring-and-observability-for-databricks-jobs)을 참조하십시오.

**5단계: DLT 파이프라인 작업 예약**

예약에 따라 ETL 파이프라인을 실행하려면 다음 단계를 따르십시오:

1.  사이드바에서 **Workflows 아이콘 Workflows**를 클릭합니다.
2.  **Name** 열에서 작업 이름을 클릭합니다. 사이드 패널에 **Job details**가 표시됩니다.
3.  **Schedules & Triggers** 패널에서 **Add trigger**를 클릭하고 **Trigger type**에서 **Scheduled**를 선택합니다.
4.  기간, 시작 시간 및 시간대를 지정합니다.
5.  **Save**를 클릭합니다.

---

Azure Databricks에서 DLT와 Auto Loader를 사용하여 ETL 파이프라인을 구축하는 과정을 이해하는 데 도움이 되기를 바랍니다!
