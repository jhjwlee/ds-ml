## 핸즈온 실습: Spark ALS를 활용한 영화 추천 시스템 구축 (Unity Catalog 환경)

**소개:**

이 핸즈온 실습에서는 Apache Spark의 MLlib 라이브러리에 포함된 ALS(Alternating Least Squares) 알고리즘을 사용하여 영화 추천 시스템을 구축합니다. MovieLens 최신 소규모 데이터셋을 사용하며, Databricks 환경에서 Unity Catalog 볼륨에 저장된 데이터를 활용합니다. 데이터 전처리, 탐색적 데이터 분석(EDA), 모델 학습, 하이퍼파라미터 튜닝, 학습 곡선 시각화, 그리고 최종 모델 평가 및 추천 생성까지의 전체 과정을 경험하게 됩니다. 이 실습을 통해 분산 컴퓨팅 환경에서의 머신러닝 파이프라인 구축에 대한 실질적인 이해를 높일 수 있습니다.

**학습 목표:**

*   Spark DataFrame을 사용한 데이터 로드 및 전처리 방법 이해
*   Spark SQL 및 DataFrame API를 활용한 탐색적 데이터 분석 수행
*   ALS 알고리즘의 원리 및 Spark MLlib에서의 사용법 이해
*   학습/검증/테스트 데이터셋 분리의 중요성 인지
*   하이퍼파라미터 튜닝을 통한 모델 성능 최적화 과정 경험
*   학습 곡선을 통한 모델 학습 상태 시각화 및 분석
*   학습된 모델을 평가하고, 실제 사용자에게 영화를 추천하는 방법 습득
*   Databricks Unity Catalog 볼륨을 활용한 데이터 관리 이해

**준비물:**

1.  **Databricks 작업 공간:** Unity Catalog가 활성화되어 있어야 합니다.
2.  **클러스터:** Spark 클러스터 (ML Runtime 권장, 예: 13.3 LTS ML)
3.  **데이터셋:** MovieLens 최신 소규모 데이터셋 (다음 파일들이 필요합니다)
    *   `movies.csv`
    *   `ratings.csv`
    *   `links.csv`
    *   `tags.csv`
    **데이터 업로드 위치:** 위의 CSV 파일들을 Databricks Unity Catalog 볼륨의 다음 경로에 업로드해야 합니다.
    *   `/Volumes/dbricks_jhlee/default/movie/`
    *   (만약 `dbricks_jhlee` 카탈로그나 `default` 스키마, `movie` 볼륨이 없다면, 적절히 생성하거나 본인의 환경에 맞게 경로를 수정해야 합니다.)

---

**실습 절차:**

**Phase 1: 환경 설정 및 데이터 로드**

**셀 1: 프로젝트 소개 및 라이브러리 임포트**

```python
# MAGIC %md
# MAGIC ### Spark HW3 Moive Recommendation (Unity Catalog Version)
# MAGIC In this notebook, we will use an Alternating Least Squares (ALS) algorithm with Spark APIs to predict the ratings for the movies in [MovieLens small dataset](https://grouplens.org/datasets/movielens/latest/), loaded from a Unity Catalog Volume.

import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import math
```

*   **설명:**
    *   첫 번째 라인 (`%md ...`)은 이 노트북이 Markdown 셀임을 나타내며, 프로젝트에 대한 설명을 담고 있습니다.
    *   그 아래에는 데이터 분석, 시각화, 수치 연산에 필요한 핵심 Python 라이브러리들을 임포트합니다.
*   **주요 라이브러리 및 함수 설명:**
    *   `numpy` (as np): 과학 계산을 위한 라이브러리로, 다차원 배열 객체와 이를 다루기 위한 도구를 제공합니다.
    *   `pandas` (as pd): 데이터 조작 및 분석을 위한 강력하고 사용하기 쉬운 라이브러리입니다. 주로 Spark DataFrame을 작은 규모로 변환하여 Pandas 기능을 사용하거나, 결과물을 Pandas DataFrame으로 확인할 때 유용합니다.
    *   `seaborn` (as sns): Matplotlib을 기반으로 하는 통계적 데이터 시각화 라이브러리입니다. 더 미려하고 정보 전달력이 높은 그래프를 쉽게 그릴 수 있게 해줍니다.
    *   `matplotlib.pyplot` (as plt): Python에서 가장 널리 사용되는 2D 플로팅 라이브러리입니다. 다양한 종류의 정적, 애니메이션, 인터랙티브 시각화를 생성할 수 있습니다.
    *   `math`: 기본적인 수학 함수(예: `sqrt`, `isfinite`)를 제공하는 내장 모듈입니다.
*   **학생들에게 설명할 내용:**
    *   데이터 분석 프로젝트를 시작할 때 필요한 라이브러리를 먼저 임포트하는 이유 (코드의 재사용성, 기능 확장).
    *   각 라이브러리가 어떤 종류의 작업에 주로 사용되는지에 대한 간략한 소개.
    *   `%md` 매직 커맨드를 사용하여 노트북 내에 설명이나 문서를 추가하는 방법.

**셀 2: SparkSession 생성**

```python
from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("movie_analysis_uc") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
```

*   **설명:**
    *   이 셀은 Apache Spark 애플리케이션을 실행하기 위한 진입점인 `SparkSession` 객체를 생성합니다. Spark 2.0부터 `SparkSession`은 `SQLContext`, `HiveContext`, 그리고 이전의 `SparkContext`의 기능을 통합했습니다.
*   **주요 함수 설명:**
    *   `SparkSession.builder`: `SparkSession` 인스턴스를 구성하기 위한 빌더 패턴을 시작합니다.
    *   `.appName("movie_analysis_uc")`: Spark UI에서 식별할 수 있도록 애플리케이션의 이름을 "movie\_analysis\_uc"로 설정합니다.
    *   `.config("spark.some.config.option", "some-value")`: Spark 애플리케이션에 대한 특정 설정을 구성할 수 있습니다. 이 예제에서는 임의의 설정값을 보여주며, 실제로는 Spark 성능 튜닝이나 기능 활성화를 위해 다양한 옵션을 설정할 수 있습니다. (예: `spark.sql.shuffle.partitions`)
    *   `.getOrCreate()`: 만약 이미 활성화된 `SparkSession`이 있다면 해당 세션을 반환하고, 없다면 새로 생성하여 반환합니다. 이를 통해 중복된 세션 생성을 방지합니다.
*   **학생들에게 설명할 내용:**
    *   Spark 프로그래밍에서 `SparkSession`의 역할과 중요성 (모든 Spark 작업의 시작점).
    *   `appName`을 설정하는 이유 (Spark UI에서 작업 추적 용이).
    *   `config` 옵션을 통해 Spark 동작을 어떻게 커스터마이징할 수 있는지 간략히 소개. (이번 실습에서는 기본 설정으로도 충분함을 언급)

**셀 3: Unity Catalog 볼륨에서 데이터 로드**

```python
# Define the base path for your data in Unity Catalog Volumes
uc_volume_path = "/Volumes/dbricks_jhlee/default/movie"

# Load data from Unity Catalog Volume
try:
    movies = spark.read.load(f"{uc_volume_path}/movies.csv", format='csv', header=True, inferSchema=True)
    ratings = spark.read.load(f"{uc_volume_path}/ratings.csv", format='csv', header=True, inferSchema=True)
    links = spark.read.load(f"{uc_volume_path}/links.csv", format='csv', header=True, inferSchema=True)
    tags = spark.read.load(f"{uc_volume_path}/tags.csv", format='csv', header=True, inferSchema=True)
    print("Data loaded successfully from Unity Catalog Volume.")
except Exception as e:
    print(f"Error loading data from Unity Catalog Volume: {e}")
    print(f"Please ensure CSV files (movies.csv, ratings.csv, links.csv, tags.csv) exist in {uc_volume_path}")
```

*   **설명:**
    *   MovieLens 데이터셋의 각 CSV 파일을 Databricks Unity Catalog 볼륨에서 Spark DataFrame으로 로드합니다. Unity Catalog 볼륨은 테이블 형식이 아닌 파일(예: CSV, JSON, 이미지 등)을 저장, 관리, 접근하는 데 사용됩니다.
*   **주요 함수 및 개념 설명:**
    *   `uc_volume_path`: 데이터 파일들이 저장된 Unity Catalog 볼륨의 경로를 문자열 변수로 지정합니다. **학생들은 이 경로를 자신의 환경에 맞게 수정해야 합니다.**
    *   `spark.read.load(path, format, header, inferSchema)`: 다양한 데이터 소스에서 데이터를 로드하는 Spark의 주요 API입니다.
        *   `path`: 로드할 파일의 경로입니다. f-string을 사용하여 `uc_volume_path`와 파일명을 조합합니다.
        *   `format='csv'`: 로드할 파일의 형식이 CSV임을 명시합니다.
        *   `header=True`: CSV 파일의 첫 번째 줄이 컬럼 이름(헤더)임을 나타냅니다. 이 옵션이 없으면 첫 줄도 데이터로 간주됩니다.
        *   `inferSchema=True`: Spark가 CSV 파일의 내용을 스캔하여 각 컬럼의 데이터 타입을 자동으로 추론하도록 합니다. 이는 편리하지만, 매우 큰 파일의 경우 성능 저하를 유발할 수 있으며, 때로는 부정확한 타입 추론이 발생할 수 있습니다. 대규모 프로덕션 환경에서는 명시적으로 스키마를 정의하는 것이 좋습니다.
    *   `try-except` 블록: 파일 로드 중 발생할 수 있는 오류(예: 파일 경로 오류, 권한 문제)를 처리하여 프로그램이 비정상적으로 종료되는 것을 방지하고 사용자에게 유용한 오류 메시지를 제공합니다.
*   **학생들에게 설명할 내용:**
    *   Unity Catalog 볼륨의 개념과 장점 (데이터 거버넌스 하에 파일 관리).
    *   `spark.read.load` 함수의 주요 옵션들 (`format`, `header`, `inferSchema`)의 의미와 사용법.
    *   `inferSchema`의 편리성과 잠재적인 단점.
    *   실제 프로젝트에서 파일 경로를 변수로 관리하는 것의 이점 (유지보수 용이).
    *   오류 처리를 위한 `try-except` 문의 중요성.
    *   **중요:** 학생들이 `uc_volume_path`를 본인의 환경에 맞게 올바르게 수정했는지 확인하도록 안내.

---

**Phase 2: 탐색적 데이터 분석 (EDA)**

데이터를 로드한 후에는 데이터의 구조, 품질, 주요 통계 등을 파악하기 위한 탐색적 데이터 분석(EDA)을 수행합니다. 이를 통해 데이터에 대한 이해를 높이고, 이후 모델링 단계에서 발생할 수 있는 문제를 미리 예측하거나 데이터 전처리 방향을 설정할 수 있습니다.

**셀 4: `movies` DataFrame 샘플 확인**

```python
movies.show(5)
```

*   **설명:**
    *   로드된 `movies` DataFrame의 처음 5개 행을 콘솔에 출력합니다. 이를 통해 `movies` 데이터가 어떤 컬럼들로 구성되어 있고, 각 컬럼에 어떤 형태의 데이터가 들어있는지 간략하게 파악할 수 있습니다.
*   **주요 함수 설명:**
    *   `.show(n, truncate=True)`: DataFrame의 처음 `n`개 행을 보여주는 액션(action)입니다. `truncate` 옵션은 컬럼 내용이 길 경우 잘라서 보여줄지 여부를 결정합니다 (기본값은 `True`).
*   **학생들에게 설명할 내용:**
    *   `show()` 함수를 사용하여 DataFrame의 내용을 빠르게 확인하는 방법.
    *   `movies` 데이터에는 각 영화의 ID (`movieId`), 제목 (`title`), 장르 (`genres`) 정보가 포함되어 있음을 확인.
    *   장르 정보가 `|` 문자로 구분된 문자열 형태로 저장되어 있음을 주목 (이후 전처리 필요 가능성 암시).

**셀 5: `ratings` DataFrame 샘플 확인**

```python
ratings.show(5)
```

*   **설명:**
    *   로드된 `ratings` DataFrame의 처음 5개 행을 출력합니다. `ratings` 데이터는 추천 시스템 구축에 가장 핵심적인 데이터로, 어떤 사용자가 어떤 영화에 어떤 평점을 매겼는지에 대한 정보를 담고 있습니다.
*   **학생들에게 설명할 내용:**
    *   `ratings` 데이터에는 사용자 ID (`userId`), 영화 ID (`movieId`), 해당 사용자가 영화에 매긴 평점 (`rating`), 평점을 매긴 시간 (`timestamp`) 정보가 포함되어 있음을 확인.
    *   ALS 알고리즘은 주로 `userId`, `movieId`, `rating` 세 가지 컬럼을 사용함을 상기. `timestamp`는 시간의 흐름에 따른 사용자 선호도 변화 분석 등에 활용될 수 있지만, 이 실습에서는 기본 ALS 모델에 직접 사용하지는 않습니다.

**셀 6: 사용자 및 영화별 최소 평점 수 확인**

```python
# Ensure ratings DataFrame is loaded before proceeding
if 'ratings' in locals():
    tmp1 = ratings.groupBy("userID").count().toPandas()['count'].min()
    tmp2 = ratings.groupBy("movieId").count().toPandas()['count'].min()
    print('For the users that rated movies and the movies that were rated:')
    print('Minimum number of ratings per user is {}'.format(tmp1))
    print('Minimum number of ratings per movie is {}'.format(tmp2))
else:
    print("Ratings DataFrame not loaded. Please check the data loading step.")
```

*   **설명:**
    *   이 셀은 각 사용자별로 매긴 평점의 최소 개수와, 각 영화별로 받은 평점의 최소 개수를 계산하여 출력합니다. 이는 데이터의 희소성(sparsity)을 간접적으로 파악하고, 너무 적은 평점을 매긴 사용자나 너무 적은 평점을 받은 영화가 있는지 확인하는 데 도움이 됩니다.
*   **주요 함수 설명:**
    *   `ratings.groupBy("userID")`: `ratings` DataFrame을 `userID` 컬럼 기준으로 그룹화합니다.
    *   `.count()`: 각 그룹별 행의 개수(즉, 사용자별 평점 개수 또는 영화별 평점 개수)를 계산합니다. 결과로 나오는 DataFrame은 `userID`(또는 `movieId`)와 `count` 두 개의 컬럼을 갖습니다.
    *   `.toPandas()`: Spark DataFrame을 Pandas DataFrame으로 변환합니다. `.min()`과 같은 집계 함수를 Pandas에서 더 쉽게 사용하기 위함입니다. **주의: 이 연산은 모든 데이터를 드라이버 노드로 가져오므로, 매우 큰 Spark DataFrame에 대해서는 메모리 부족 오류를 유발할 수 있습니다. 여기서는 그룹화된 결과의 크기가 작을 것으로 예상되므로 사용합니다.**
    *   `['count'].min()`: Pandas DataFrame에서 `count` 컬럼의 최소값을 찾습니다.
*   **학생들에게 설명할 내용:**
    *   콜드 스타트 문제(Cold Start Problem): 신규 사용자나 신규 아이템처럼 평점 데이터가 거의 없는 경우 추천 성능이 저하되는 현상입니다. 이 통계는 그러한 경향이 있는지 대략적으로 파악하는 데 도움을 줄 수 있습니다.
    *   만약 최소 평점 수가 매우 낮다면 (예: 1), 해당 사용자나 영화에 대한 정보가 부족하여 신뢰할 만한 추천을 하기 어려울 수 있음을 설명.
    *   `.toPandas()` 사용 시 주의점 (메모리). 대안으로는 Spark의 집계 함수 (`pyspark.sql.functions.min`)를 직접 사용하는 방법이 있습니다.

**셀 7: 단 한 명의 사용자에게만 평가받은 영화 수 확인**

```python
if 'ratings' in locals():
    tmp1 = sum(ratings.groupBy("movieId").count().toPandas()['count'] == 1)
    tmp2 = ratings.select('movieId').distinct().count()
    print('{} out of {} movies are rated by only one user'.format(tmp1, tmp2))
else:
    print("Ratings DataFrame not loaded.")
```

*   **설명:**
    *   전체 영화 중 오직 한 명의 사용자에게만 평가받은 영화의 수와 그 비율을 계산합니다. 이는 "롱테일(long-tail)" 현상과 관련이 있으며, 소수의 인기 있는 영화에 대부분의 평점이 집중되고 다수의 비인기 영화에는 평점이 거의 없는 데이터 분포를 나타낼 수 있습니다.
*   **주요 함수 설명:**
    *   `ratings.groupBy("movieId").count()`: 영화별 평점 수를 계산합니다.
    *   `.toPandas()['count'] == 1`: Pandas DataFrame으로 변환 후, 평점 수가 1인 영화들을 찾는 불리언 시리즈를 생성합니다.
    *   `sum(...)`: 위 불리언 시리즈에서 `True`의 개수(즉, 평점 수가 1인 영화의 수)를 합산합니다.
    *   `ratings.select('movieId').distinct().count()`: `ratings` DataFrame에 등장하는 고유한 영화 ID의 개수를 계산합니다 (즉, 한 번이라도 평가받은 영화의 총 수).
*   **학생들에게 설명할 내용:**
    *   롱테일 현상이 추천 시스템에 미치는 영향 (비인기 아이템 추천의 어려움).
    *   이러한 영화들은 정보가 매우 제한적이어서 ALS 모델이 해당 영화의 특징을 학습하기 어려울 수 있음을 설명.
    *   데이터 전처리 단계에서 이러한 영화들을 필터링할지 여부를 고려해볼 수 있음 (데이터 양과 추천 목적에 따라 결정).

---

**Phase 3: Spark SQL 및 OLAP 스타일 분석**

Spark SQL이나 DataFrame API를 사용하여 좀 더 구조화된 질의를 통해 데이터를 분석합니다.

**셀 8: 총 사용자 수 계산**

```python
# MAGIC %md ### The number of Users

if 'ratings' in locals():
    tmp_q1 = ratings.select('userid').distinct().count()
    print ('There totally have {} users'.format(tmp_q1))
else:
    print("Ratings DataFrame not loaded.")
```

*   **설명:**
    *   `ratings` DataFrame에 있는 고유한 사용자 ID의 수를 계산하여 전체 사용자 수를 파악합니다.
*   **주요 함수 설명:**
    *   `ratings.select('userid')`: `ratings` DataFrame에서 `userid` 컬럼만 선택합니다.
    *   `.distinct()`: 선택된 컬럼에서 중복된 값을 제거하여 고유한 값들만 남깁니다.
    *   `.count()`: 고유한 값들의 개수를 계산합니다.
*   **학생들에게 설명할 내용:**
    *   데이터셋에 참여한 총 사용자 규모를 파악하는 기본적인 분석.
    *   `select`, `distinct`, `count`와 같은 DataFrame API의 연쇄적인 사용 방법.

**셀 9: 총 영화 수 계산**

```python
# MAGIC %md ### The number of Movies

if 'movies' in locals():
    tmp_q2 = movies.select('movieid').distinct().count()
    print ('There totally have {} movies'.format(tmp_q2))
else:
    print("Movies DataFrame not loaded.")
```

*   **설명:**
    *   `movies` DataFrame에 있는 고유한 영화 ID의 수를 계산하여 전체 영화 수를 파악합니다.
*   **학생들에게 설명할 내용:**
    *   데이터셋에 포함된 총 영화의 규모를 파악.
    *   이전에 계산한 "한 번이라도 평가받은 영화의 총 수"와 이 값을 비교하여, `movies.csv`에는 있지만 한 번도 평가받지 않은 영화가 있는지 유추해볼 수 있음 (다음 셀에서 직접 확인).

**셀 10: 평가받지 않은 영화 수 및 목록 확인**

```python
# MAGIC %md ### How many movies are rated by users? List movies not rated before

from pyspark.sql.functions import col

if 'movies' in locals() and 'ratings' in locals():
    tmp_q3_rated_count = ratings.select('movieid').distinct().count()
    total_movies_count = movies.select('movieid').distinct().count()
    print('{} movies have not been rated'.format(total_movies_count - tmp_q3_rated_count))

    movies_with_ratings = movies.join(ratings, movies.movieId == ratings.movieId, "left_outer")

    unrated_movies = movies_with_ratings.where(ratings.rating.isNull()) \
                                      .select(movies.movieId, movies.title).distinct()
    print("\nList of movies not rated before:")
    unrated_movies.show()
else:
    print("Movies or Ratings DataFrame not loaded.")
```

*   **설명:**
    *   전체 영화 목록(`movies` DataFrame)과 평점 데이터(`ratings` DataFrame)를 비교하여, 한 번도 평점을 받지 않은 영화의 수를 계산하고 그 목록을 출력합니다.
*   **주요 함수 설명:**
    *   `from pyspark.sql.functions import col`: Spark SQL 함수를 사용하기 위해 `col` 함수를 임포트합니다. `col("columnName")`은 특정 컬럼을 참조하는 데 사용됩니다.
    *   `movies.join(ratings, movies.movieId == ratings.movieId, "left_outer")`: `movies` DataFrame을 기준으로 `ratings` DataFrame과 `left_outer` 조인을 수행합니다. 조인 조건은 두 DataFrame의 `movieId`가 같은 경우입니다.
        *   `left_outer` 조인: 왼쪽 DataFrame(`movies`)의 모든 행을 유지하고, 조인 조건에 맞는 오른쪽 DataFrame(`ratings`)의 행을 결합합니다. 만약 오른쪽 DataFrame에 조인 조건에 맞는 행이 없으면, 해당 오른쪽 DataFrame의 컬럼들은 `null` 값으로 채워집니다.
    *   `.where(ratings.rating.isNull())`: 조인된 DataFrame에서 `ratings` 테이블에서 온 `rating` 컬럼이 `null`인 행들만 필터링합니다. 이는 `movies` 목록에는 있지만 `ratings` 데이터에는 없는, 즉 평가받지 않은 영화들을 의미합니다. (컬럼명 중복을 피하기 위해 `ratings.rating`처럼 테이블명을 명시하는 것이 좋습니다. 또는 조인 전에 컬럼명을 변경할 수도 있습니다.)
    *   `.select(movies.movieId, movies.title).distinct()`: 평가받지 않은 영화들의 `movieId`와 `title`만 선택하고, 중복을 제거합니다.
*   **학생들에게 설명할 내용:**
    *   `join` 연산의 개념과 다양한 조인 타입 (`inner`, `left_outer`, `right_outer`, `full_outer`) 소개. 여기서는 `left_outer` 조인이 왜 적합한지 설명.
    *   `isNull()` 함수를 사용하여 `null` 값을 필터링하는 방법.
    *   평가받지 않은 영화는 사용자-아이템 행렬에서 해당 아이템에 대한 정보가 전혀 없는 것이므로, 일반적인 협업 필터링 모델에서는 직접적으로 추천되기 어렵다는 점을 설명. 이러한 아이템들은 다른 방식(예: 콘텐츠 기반 추천, 인기 기반 추천)으로 다루거나, 모델 학습 데이터에서 제외하는 것을 고려할 수 있습니다.

**셀 11: 고유 영화 장르 목록 생성**

```python
# MAGIC %md ### List Movie Genres

import pyspark.sql.functions as f

if 'movies' in locals():
    all_genres_df = movies.withColumn("genre_array", f.split(col("genres"), "\|")) \
                          .select(f.explode(col("genre_array")).alias("genre")) \
                          .distinct()

    distinct_genres_list = [row.genre for row in all_genres_df.collect()]
    hashset = set(distinct_genres_list)

    print("Distinct genres found:")
    print(hashset)
    print("Total number of distinct genres: {}".format(len(hashset)))
else:
    print("Movies DataFrame not loaded.")
```

*   **설명:**
    *   `movies` DataFrame의 `genres` 컬럼은 `Action|Adventure|Sci-Fi`와 같이 `|` 문자로 여러 장르가 결합된 문자열입니다. 이 셀에서는 각 영화의 장르들을 분리하여 데이터셋에 존재하는 모든 고유한 장르의 목록을 만듭니다.
*   **주요 함수 설명:**
    *   `import pyspark.sql.functions as f`: 다양한 내장 SQL 함수들을 `f`라는 별칭으로 사용하기 위해 임포트합니다.
    *   `movies.withColumn("genre_array", f.split(col("genres"), "\|"))`:
        *   `withColumn("newColumnName", columnExpression)`: 기존 DataFrame에 새로운 컬럼을 추가하거나 기존 컬럼을 수정합니다.
        *   `f.split(col("genres"), "\|")`: `genres` 컬럼의 문자열을 `|` (정규 표현식에서 `|`는 특별한 의미를 가지므로 `\|`로 이스케이프)를 기준으로 분리하여 문자열 배열(array)을 생성하고, 이를 `genre_array`라는 새 컬럼에 저장합니다.
    *   `.select(f.explode(col("genre_array")).alias("genre"))`:
        *   `f.explode(col("genre_array"))`: `genre_array` 컬럼(배열 타입)의 각 요소를 별도의 행으로 확장합니다. 예를 들어, 한 영화의 `genre_array`가 `["Action", "Adventure"]`였다면, 이 연산 후에는 "Action"을 값으로 갖는 행과 "Adventure"를 값으로 갖는 행, 이렇게 두 개의 행이 생성됩니다.
        *   `.alias("genre")`: `explode` 연산으로 생성된 새 컬럼의 이름을 `genre`로 지정합니다.
    *   `.distinct()`: 중복된 장르를 제거하여 고유한 장르만 남깁니다.
    *   `[row.genre for row in all_genres_df.collect()]`: `all_genres_df` (고유 장르만 포함된 Spark DataFrame)의 모든 행을 드라이버 노드로 가져와(`collect()`) 각 행의 `genre` 값을 추출하여 Python 리스트를 만듭니다.
    *   `set(...)`: Python 리스트를 `set`으로 변환하여 중복을 최종적으로 제거하고 고유한 장르만 남깁니다 (이론적으로 Spark의 `.distinct()`에서 이미 처리되었어야 하지만, 확인 차원 또는 다른 방식으로 `collect`했을 경우를 대비).
*   **학생들에게 설명할 내용:**
    *   복합적인 문자열 데이터를 다루는 방법 (`split` 함수).
    *   배열 타입의 컬럼을 여러 행으로 확장하는 `explode` 함수의 유용성.
    *   `.collect()` 액션의 의미와 주의점 (드라이버 메모리). 여기서는 고유 장르의 수가 많지 않다고 가정합니다.
    *   데이터셋에 어떤 종류의 영화 장르들이 있는지 파악. 이는 나중에 콘텐츠 기반 필터링이나 추천 결과의 다양성 분석 등에 활용될 수 있습니다.

**셀 12: 각 영화를 카테고리별로 원-핫 인코딩 스타일로 표현 (DataFrame)**

```python
# MAGIC %md ### Movie for Each Category
# MAGIC This part creates a one-hot encoded representation for genres.

from pyspark.sql.functions import expr, when

if 'movies' in locals() and 'hashset' in locals() and len(hashset) > 0:
    q5_base = movies.select("movieid", "title", "genres")

    genre_expressions = [
        when(col("genres").rlike(genre.replace("(", "\\(").replace(")", "\\)")), 1).otherwise(0).alias(genre)
        for genre in hashset if genre != '(no genres listed)'
    ]
    if '(no genres listed)' in hashset:
        genre_expressions.append(
            when(col("genres") == '(no genres listed)', 1).otherwise(0).alias("no_genres_listed")
        )

    if genre_expressions:
        tmp_q5 = q5_base.select(col("movieid"), col("title"), *genre_expressions)
        print("\nMovies with one-hot encoded genres:")
        tmp_q5.show()

        drama_alias = "Drama"
        if drama_alias in tmp_q5.columns:
            tmp_drama = tmp_q5.filter(col(drama_alias) == 1).select("movieid", "title")
            print("\n{} movies are Drama, they are:".format(tmp_drama.count()))
            tmp_drama.show()
        else:
            print(f"\nColumn '{drama_alias}' not found. Available columns: {tmp_q5.columns}")
    else:
        print("\nNo genres found to process for one-hot encoding.")
else:
    print("Movies DataFrame or genre set not available for one-hot encoding.")
```

*   **설명:**
    *   이 셀은 이전 셀에서 얻은 고유 장르 목록(`hashset`)을 사용하여, 각 영화가 어떤 장르에 속하는지를 나타내는 일종의 원-핫 인코딩 형태의 DataFrame을 생성합니다. 각 고유 장르는 새로운 컬럼이 되며, 영화가 해당 장르에 속하면 1, 아니면 0의 값을 갖습니다.
*   **주요 함수 및 로직 설명:**
    *   `genre_expressions = [...]`: Python 리스트 컴프리헨션을 사용하여 각 장르에 대한 Spark SQL `when` 표현식을 생성합니다.
        *   `when(condition, value1).otherwise(value2)`: `condition`이 참이면 `value1`을, 거짓이면 `value2`를 반환하는 SQL의 `CASE WHEN`과 유사한 함수입니다.
        *   `col("genres").rlike(genre_pattern)`: `genres` 컬럼의 값이 정규 표현식 `genre_pattern`과 일치하는지 확인합니다. `genre.replace("(", "\\(").replace(")", "\\)")`는 장르 이름에 포함될 수 있는 괄호와 같은 정규 표현식 특수 문자를 이스케이프 처리하여 정확한 문자열 매칭을 시도합니다. (더 정확하게는, `genres` 문자열이 해당 `genre`를 부분 문자열로 포함하는지 확인합니다.)
        *   `.alias(genre)`: 생성된 표현식으로 만들어질 새 컬럼의 이름을 해당 장르명으로 지정합니다. `(no genres listed)`와 같이 컬럼명으로 부적합할 수 있는 문자열은 `no_genres_listed`와 같이 수정합니다.
    *   `q5_base.select(col("movieid"), col("title"), *genre_expressions)`: `movieid`, `title`과 함께 위에서 생성된 모든 장르 표현식 컬럼들을 선택하여 새로운 DataFrame `tmp_q5`를 만듭니다. `*genre_expressions`는 리스트의 각 요소를 개별 인자로 풀어헤치는 Python의 언패킹(unpacking) 기능입니다.
    *   예시로 "Drama" 장르에 속하는 영화들을 필터링하여 보여줍니다.
*   **학생들에게 설명할 내용:**
    *   범주형 데이터를 수치형으로 변환하는 방법 중 하나인 원-핫 인코딩의 개념 소개. (여기서는 엄밀한 의미의 원-핫 인코딩이라기보다는, 영화가 여러 장르를 가질 수 있으므로 다중 레이블 인코딩에 가깝습니다.)
    *   `when().otherwise()` 구문을 사용한 조건부 컬럼 생성 방법.
    *   `rlike()` 함수를 사용한 정규 표현식 기반 문자열 매칭.
    *   리스트 컴프리헨션과 컬럼 언패킹(`*`)을 사용하여 동적으로 다수의 컬럼을 생성하고 선택하는 고급 DataFrame 조작 기법.
    *   이렇게 변환된 장르 정보는 콘텐츠 기반 추천이나, 추천된 아이템의 장르 다양성 분석 등에 활용될 수 있습니다.

---

**Phase 4: ALS 모델 학습을 위한 데이터 준비**

본격적으로 ALS 모델을 학습시키기 전에, 평점 데이터를 모델이 요구하는 형태로 변환하고 학습, 검증, 테스트 세트로 분할합니다.

**셀 13: `ratings` 데이터를 DataFrame으로 로드 (Serverless 호환)**

```python
# Load ratings data using Spark DataFrame API from Unity Catalog Volume
uc_volume_path = "/Volumes/dbricks_jhlee/default/movie" # 경로가 이미 정의되었지만, 명확성을 위해 다시 언급 가능

ratings_file_path_uc = f"{uc_volume_path}/ratings.csv"
from pyspark.sql.functions import col

try:
    ratings_df_initial = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(ratings_file_path_uc)

    ratings_for_split_df = ratings_df_initial.select(
        col("userId").cast("integer"),
        col("movieId").cast("integer"),
        col("rating").cast("float")
    ).na.drop() # Drop rows where userId, movieId, or rating is null

    print("Ratings data loaded into DataFrame for splitting:")
    ratings_for_split_df.show(5)
    ratings_for_split_df.printSchema()

except Exception as e:
    print(f"Error loading ratings.csv into DataFrame from {ratings_file_path_uc}: {e}")
    ratings_for_split_df = None
```

*   **설명:**
    *   이전에는 EDA를 위해 `ratings` DataFrame을 사용했고, 여기서는 ALS 모델 학습에 직접 사용할 평점 데이터를 준비합니다.
    *   ALS 모델은 기본적으로 `userId`, `movieId`, `rating` 세 가지 컬럼을 필요로 하며, 각각 정수형, 정수형, 실수형(또는 정수형)이어야 합니다.
    *   `inferSchema=True`로 로드했더라도, 명시적으로 타입을 변환하고 불필요한 컬럼을 제거하며, 결측치가 있는 행을 제거하여 데이터 정제 과정을 거칩니다.
*   **주요 함수 설명:**
    *   `.select(...)`: 필요한 컬럼(`userId`, `movieId`, `rating`)만 선택합니다.
    *   `.cast("dataType")`: 컬럼의 데이터 타입을 지정된 타입으로 변환합니다. `integer`는 정수형, `float`은 부동소수점 실수형입니다.
    *   `.na.drop()`: 선택된 컬럼 중 어느 하나라도 `null` 값을 가진 행을 제거합니다. ALS 모델은 `null` 값을 처리할 수 없으므로, 이러한 데이터는 미리 제거해야 합니다.
    *   `.printSchema()`: DataFrame의 스키마(컬럼명과 데이터 타입)를 출력하여 타입 변환이 올바르게 되었는지 확인합니다.
*   **학생들에게 설명할 내용:**
    *   머신러닝 모델에 데이터를 입력하기 전에 데이터 타입을 올바르게 맞춰주는 것의 중요성.
    *   결측치 처리의 필요성과 간단한 처리 방법 (`na.drop()`). (더 고급 결측치 처리 방법도 있음을 언급)
    *   DataFrame API를 연쇄적으로 사용하여 데이터 변환 파이프라인을 구성하는 방법.
    *   이전 실습 코드에서 RDD를 사용했던 부분을 DataFrame API로 완전히 대체하여 Databricks Serverless 환경과의 호환성을 높이고 코드를 현대화했음을 강조.

**셀 14: 데이터를 학습, 검증, 테스트 세트로 분할**

```python
if ratings_for_split_df:
    (train_df, validation_df, test_df) = ratings_for_split_df.randomSplit([0.6, 0.2, 0.2], seed=7856)

    train_df.cache()
    validation_df.cache()
    test_df.cache()

    print("Data split into Training, Validation, and Test DataFrames.")
    print(f"Training DataFrame count: {train_df.count()}")
    print(f"Validation DataFrame count: {validation_df.count()}")
    print(f"Test DataFrame count: {test_df.count()}")
    print("\nSchema of training DataFrame:")
    train_df.printSchema()
    train_df.show(3)
else:
    print("ratings_for_split_df DataFrame not available for splitting.")
    train_df, validation_df, test_df = [spark.createDataFrame([], ratings_for_split_df.schema if ratings_for_split_df else spark.read.format("csv").load(ratings_file_path_uc).schema) for _ in range(3)] # 오류 방지를 위한 빈 DF 생성 (스키마 유지 시도)
```

*   **설명:**
    *   준비된 `ratings_for_split_df`를 학습(Training), 검증(Validation), 테스트(Test) 세트로 무작위 분할합니다.
        *   **학습 세트 (Training Set):** 모델을 직접 학습시키는 데 사용됩니다. 모델은 이 데이터의 패턴을 학습합니다.
        *   **검증 세트 (Validation Set):** 학습 중인 모델의 성능을 평가하고, 최적의 하이퍼파라미터를 선택하는 데 사용됩니다. 모델은 이 데이터를 직접 학습하지는 않습니다.
        *   **테스트 세트 (Test Set):** 최종적으로 학습되고 튜닝된 모델의 일반화 성능을 평가하는 데 사용됩니다. 이 데이터는 모델 학습 및 튜닝 과정에서 전혀 사용되지 않아야 공정한 평가가 가능합니다.
*   **주요 함수 설명:**
    *   `.randomSplit(weights, seed)`: DataFrame을 지정된 비율(`weights`)로 무작위 분할합니다.
        *   `weights = [0.6, 0.2, 0.2]`: 데이터를 약 60% (학습), 20% (검증), 20% (테스트) 비율로 분할하도록 지정합니다. 합이 1이 아니어도 비율에 따라 분할됩니다.
        *   `seed=7856`: 무작위 분할 시 재현성을 보장하기 위한 시드 값입니다. 시드 값을 고정하면 코드를 다시 실행해도 항상 동일한 방식으로 데이터가 분할됩니다.
    *   `.cache()`: 해당 DataFrame을 메모리(또는 디스크)에 캐시합니다. 이후 이 DataFrame에 대한 연산이 반복될 경우, 디스크에서 다시 읽어오는 대신 캐시된 데이터를 사용하여 성능을 향상시킬 수 있습니다. 특히 반복적인 학습 과정에서 유용합니다.
*   **학생들에게 설명할 내용:**
    *   머신러닝에서 데이터를 학습, 검증, 테스트 세트로 분할하는 이유와 각 세트의 역할 (과적합 방지, 일반화 성능 측정, 공정한 모델 평가).
    *   `randomSplit` 함수의 사용법과 `seed` 값의 중요성 (실험 재현성).
    *   `.cache()` (또는 `.persist()`) 연산의 개념과 사용 이유 (성능 최적화). 캐시된 데이터는 클러스터 메모리 상황에 따라 해제될 수 있음을 언급.
    *   분할된 각 DataFrame의 행 수를 출력하여 분할이 의도한 대로 이루어졌는지 확인.

---

**Phase 5: ALS 모델 학습 및 하이퍼파라미터 튜닝**

이제 준비된 데이터를 사용하여 ALS 모델을 학습시키고, 최적의 성능을 내는 하이퍼파라미터를 찾습니다.

**셀 15: `train_ALS_df` 함수 정의 (하이퍼파라미터 튜닝 기능 포함)**

```python
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
import math

def train_ALS_df(train_data, validation_data, num_iters, reg_params_list, ranks_list):
    min_error = float('inf')
    best_rank_val = -1
    best_reg_param_val = 0.0
    best_model_instance = None

    print(f"\n--- Starting Hyperparameter Tuning for ALS ---")
    print(f"Ranks to test: {ranks_list}")
    print(f"Regularization params to test: {reg_params_list}")
    print(f"Number of iterations for each model: {num_iters}")

    if train_data.count() == 0:
        print("Error: Training data is empty. Cannot train model.")
        return None, -1, 0.0
    if validation_data.count() == 0: # 검증 데이터가 비었을 경우 경고
        print("Warning: Validation data is empty. RMSE will be Inf or NaN for all hyperparameter combinations.")

    for rank_val_iter in ranks_list:
        for reg_param_iter in reg_params_list:
            print(f"  Training with Rank: {rank_val_iter}, RegParam: {reg_param_iter}")
            als = ALS(rank=rank_val_iter, maxIter=num_iters, regParam=reg_param_iter,
                      userCol="userId", itemCol="movieId", ratingCol="rating",
                      coldStartStrategy="drop",
                      seed=42)
            try:
                model = als.fit(train_data)
                
                # 검증 데이터가 실제로 행을 가지고 있을 때만 평가 수행
                if validation_data.count() > 0:
                    predictions = model.transform(validation_data)
                    predictions_cleaned = predictions.filter(predictions.prediction.isNotNull())
                    
                    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
                    
                    if predictions_cleaned.count() == 0:
                        rmse_error = float('inf') 
                        print(f"    Warning: No valid predictions for rank={rank_val_iter}, reg={reg_param_iter} on validation set. All predictions were NaN or validation set became empty due to coldStartStrategy.")
                    else:
                        rmse_error = evaluator.evaluate(predictions_cleaned)
                else: # validation_data가 처음부터 비어있는 경우
                    rmse_error = float('inf')
                    print(f"    Warning: Validation data is initially empty. RMSE for rank={rank_val_iter}, reg={reg_param_iter} is Inf.")

                print(f"    Rank = {rank_val_iter}, Regularization = {reg_param_iter}: Validation RMSE = {rmse_error}")
                
                if not math.isinf(rmse_error) and not math.isnan(rmse_error) and rmse_error < min_error:
                    min_error = rmse_error
                    best_rank_val = rank_val_iter
                    best_reg_param_val = reg_param_iter
                    best_model_instance = model
                    print(f"    New best model found! RMSE: {min_error:.4f}, Rank: {best_rank_val}, Reg: {best_reg_param_val}")
            except Exception as e:
                print(f"    Error training ALS with rank={rank_val_iter}, reg={reg_param_iter}: {e}")
                continue

    if best_model_instance:
        print(f"\n--- Hyperparameter Tuning Finished ---")
        print(f"The best model has {best_rank_val} latent factors and regularization = {best_reg_param_val:.4f} with RMSE = {min_error:.4f}")
    else:
        print(f"\n--- Hyperparameter Tuning Finished ---")
        print("Could not find a best model. All training attempts might have failed or produced invalid RMSEs (e.g., Inf or NaN). Check validation data and coldStartStrategy effects.")
    
    return best_model_instance, best_rank_val, best_reg_param_val
```

*   **설명:**
    *   이 함수는 주어진 하이퍼파라미터 조합(잠재 요인 수 `rank`, 규제 파라미터 `regParam`)에 대해 ALS 모델을 학습시키고, 검증 세트에서의 성능(RMSE)을 평가합니다. 가장 낮은 RMSE를 보이는 하이퍼파라미터 조합과 그때 학습된 모델을 찾아 반환합니다. 이것은 간단한 형태의 그리드 서치(Grid Search) 방식의 하이퍼파라미터 튜닝입니다.
*   **주요 라이브러리 및 함수 설명:**
    *   `from pyspark.ml.recommendation import ALS`: Spark MLlib의 ALS 알고리즘 클래스를 임포트합니다.
    *   `from pyspark.ml.evaluation import RegressionEvaluator`: 회귀 모델의 성능을 평가하는 클래스를 임포트합니다. 추천 시스템의 평점 예측은 회귀 문제로 볼 수 있습니다.
    *   `ALS(...)`: ALS 모델 객체를 생성하고 주요 하이퍼파라미터를 설정합니다.
        *   `rank`: 잠재 요인(latent factors)의 수. 사용자-아이템 행렬을 분해할 때 생성되는 사용자 잠재 요인 행렬과 아이템 잠재 요인 행렬의 차원(열의 수)입니다. 너무 작으면 모델이 데이터의 복잡성을 충분히 표현하지 못하고, 너무 크면 과적합의 위험이 있습니다. (튜닝 대상)
        *   `maxIter`: 최적화 알고리즘의 최대 반복 횟수입니다. ALS는 반복적으로 사용자 및 아이템 요인을 업데이트합니다. (이 함수에서는 고정값으로 사용하고, 학습 곡선 함수에서 이 값을 변경하며 테스트합니다.)
        *   `regParam`: 규제 파라미터 (람다 값). 과적합을 방지하기 위해 모델의 복잡도에 페널티를 부과합니다. 값이 클수록 규제가 강해집니다. (튜닝 대상)
        *   `userCol`, `itemCol`, `ratingCol`: 입력 DataFrame에서 사용자 ID, 아이템 ID, 평점 컬럼의 이름을 지정합니다.
        *   `coldStartStrategy="drop"`: 예측 시 학습 데이터에 없던 새로운 사용자나 아이템(콜드 스타트 상황)이 나타났을 때, 해당 예측 결과를 어떻게 처리할지 지정합니다. `"drop"`은 해당 예측 결과를 제거하여 결과 DataFrame에 포함시키지 않습니다. 이렇게 하면 평가 지표 계산 시 `NaN` 값이 발생하는 것을 방지할 수 있습니다. (다른 옵션: `"nan"`)
        *   `seed`: ALS 내부의 일부 무작위 초기화 과정에 사용될 시드 값으로, 모델 학습의 재현성을 높입니다.
    *   `model = als.fit(train_data)`: 설정된 하이퍼파라미터로 `train_data`를 사용하여 ALS 모델을 학습시킵니다. 반환되는 `model`은 학습된 `ALSModel` 객체입니다.
    *   `predictions = model.transform(validation_data)`: 학습된 모델을 사용하여 `validation_data`에 대한 예측을 수행합니다. 결과 DataFrame에는 기존 컬럼과 함께 `prediction`이라는 예측 평점 컬럼이 추가됩니다.
    *   `predictions_cleaned = predictions.filter(predictions.prediction.isNotNull())`: `coldStartStrategy="drop"`으로 인해 `prediction` 컬럼에 `NaN`이 포함될 수 있습니다 (다른 상황에서도 발생 가능). `RegressionEvaluator`는 `NaN` 값을 처리하지 못하므로, 평가 전에 이러한 행을 제거합니다.
    *   `evaluator = RegressionEvaluator(...)`: 평가지표 계산기를 설정합니다.
        *   `metricName="rmse"`: 평가 지표로 RMSE(Root Mean Squared Error, 평균 제곱근 오차)를 사용합니다. RMSE는 실제 평점과 예측 평점 간의 차이를 측정하는 대표적인 지표로, 값이 작을수록 모델 성능이 좋음을 의미합니다.
        *   `labelCol="rating"`: 실제 값(정답)이 있는 컬럼의 이름을 지정합니다.
        *   `predictionCol="prediction"`: 모델이 예측한 값이 있는 컬럼의 이름을 지정합니다.
    *   `rmse_error = evaluator.evaluate(predictions_cleaned)`: 정제된 예측 결과에 대해 RMSE를 계산합니다.
    *   함수는 가장 낮은 RMSE를 기록한 `best_model_instance`, `best_rank_val`, `best_reg_param_val`을 반환합니다.
*   **학생들에게 설명할 내용:**
    *   하이퍼파라미터 튜닝의 개념과 중요성 (모델 성능 극대화).
    *   ALS의 주요 하이퍼파라미터 (`rank`, `maxIter`, `regParam`)의 의미와 역할.
    *   `coldStartStrategy`의 의미와 `"drop"` 옵션을 사용하는 이유.
    *   회귀 문제의 평가지표로서 RMSE의 의미.
    *   그리드 서치 방식의 하이퍼파라미터 튜닝 과정 (모든 조합을 시도하고 검증 세트에서 최적을 찾음). (더 고급 튜닝 방법도 있음을 언급: Random Search, Bayesian Optimization)
    *   함수가 최적 모델과 함께 최적 파라미터 값들을 반환하도록 설계한 이유 (이후 학습 곡선 시각화나 최종 모델 선택에 사용).
    *   검증 데이터가 비어있거나, `coldStartStrategy`로 인해 모든 예측 가능한 샘플이 제거될 경우 RMSE가 `inf` 또는 `NaN`이 될 수 있으며, 이에 대한 처리 로직을 추가했음을 설명.

**셀 16: `train_ALS_df` 함수 호출 및 최적 모델 학습**

```python
# 이전에 train_df, validation_df, test_df가 생성되었다고 가정합니다.
if 'train_df' in locals() and 'validation_df' in locals():
    num_iterations_hyperparam = 10 
    ranks_to_tune = [6, 8, 10, 12]   
    reg_params_to_tune = [0.05, 0.1, 0.2] 
    import time

    print(f"\n--- Calling train_ALS_df for hyperparameter tuning ---")
    start_time = time.time()
    
    final_model, best_rank_found, best_reg_found = train_ALS_df(
        train_df, 
        validation_df, 
        num_iterations_hyperparam, 
        reg_params_to_tune, 
        ranks_to_tune
    )
    
    print('Total Runtime for Hyperparameter Tuning: {:.2f} seconds'.format(time.time() - start_time))

    if final_model: 
        print(f"Best model (final_model) has been trained successfully.")
    else:
        print("Warning: final_model is None. Hyperparameter tuning might have failed to find a valid model.")

else:
    print("Error: train_df or validation_df not found. Please ensure the data splitting cells were executed correctly.")
    final_model = None 
    best_rank_found = -1 
    best_reg_found = -1 
```

*   **설명:**
    *   앞서 정의한 `train_ALS_df` 함수를 호출하여 실제로 하이퍼파라미터 튜닝을 수행하고 최적의 ALS 모델(`final_model`)과 그때 사용된 파라미터(`best_rank_found`, `best_reg_found`)를 얻습니다.
*   **주요 변수 설명:**
    *   `num_iterations_hyperparam`: `train_ALS_df` 내에서 각 모델을 학습시킬 때 사용할 `maxIter` 값입니다. (학습 곡선에서는 이 값을 변경하며 테스트하지만, 여기서는 고정)
    *   `ranks_to_tune`: 테스트할 `rank` 값들의 리스트입니다.
    *   `reg_params_to_tune`: 테스트할 `regParam` 값들의 리스트입니다.
    *   `time.time()`: 코드 실행 시간을 측정하기 위해 사용됩니다.
*   **학생들에게 설명할 내용:**
    *   함수 호출을 통해 하이퍼파라미터 튜닝이 실제로 실행되는 과정.
    *   지정된 `rank`와 `regParam` 조합에 대해 모델이 학습되고 평가되는 로그가 출력됨을 확인.
    *   튜닝 과정에 시간이 소요될 수 있음을 안내. (데이터 크기, 클러스터 사양, 파라미터 조합 수에 따라 달라짐)
    *   최종적으로 어떤 파라미터 조합이 가장 좋은 성능(낮은 RMSE)을 보였는지, 그리고 `final_model` 변수에 최적 모델이 저장되었는지 확인. `best_rank_found`와 `best_reg_found` 변수에는 해당 최적 파라미터 값이 저장됩니다.

---

**Phase 6: 학습 곡선 시각화**

학습 곡선(Learning Curve)은 모델의 학습 반복 횟수(여기서는 `maxIter`)에 따른 성능 변화를 시각화한 것입니다. 이를 통해 모델이 충분히 학습되었는지, 과적합 또는 과소적합 경향이 있는지 등을 파악할 수 있습니다.

**셀 17: `plot_als_learning_curve` 함수 정의**

```python
import matplotlib.pyplot as plt 
# ALS, RegressionEvaluator는 이전 셀에서 이미 임포트되었을 수 있지만, 명확성을 위해 다시 포함 가능
from pyspark.ml.recommendation import ALS 
from pyspark.ml.evaluation import RegressionEvaluator 
import math # isfinite 사용

def plot_als_learning_curve(iter_array, train_data, validation_data, reg, rank_val):
    iter_num_plot, rmse_plot = [], []
    
    print(f"--- Starting plot_als_learning_curve for Rank={rank_val}, RegParam={reg} ---")

    if train_data.count() == 0:
        print("Error: Training data for learning curve is empty.")
        return
    if validation_data.count() == 0:
        print("Warning: Validation data for learning curve is empty. Plot will likely show Inf/NaN RMSE.")

    for iter_val in iter_array:
        print(f"\n  Processing for maxIter: {iter_val}")
        als = ALS(rank=rank_val, maxIter=iter_val, regParam=reg,
                  userCol="userId", itemCol="movieId", ratingCol="rating",
                  coldStartStrategy="drop", seed=42)
        try:
            model = als.fit(train_data)
            
            rmse_error_val = float('nan') # 기본값을 NaN으로 설정
            if validation_data.count() > 0:
                predictions = model.transform(validation_data)
                predictions_cleaned = predictions.filter(predictions.prediction.isNotNull())
                
                evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
                
                if predictions_cleaned.count() == 0:
                    print(f"    Warning: No valid predictions for maxIter={iter_val}. RMSE set to NaN.")
                else:
                    rmse_error_val = evaluator.evaluate(predictions_cleaned)
            else: # 검증 데이터가 처음부터 비어있는 경우
                 print(f"    Warning: Validation data is initially empty for maxIter={iter_val}. RMSE set to NaN.")
            
            print(f'    maxIter = {iter_val}, Rank = {rank_val}, Reg = {reg}: Validation RMSE = {rmse_error_val}')
            
            iter_num_plot.append(iter_val)
            rmse_plot.append(rmse_error_val)
        except Exception as e:
            print(f"    Error training ALS for learning curve with maxIter={iter_val}: {e}")
            iter_num_plot.append(iter_val)
            rmse_plot.append(float('nan'))
            continue

    print("\n  --- Data collected for plotting learning curve ---")
    print(f"  DEBUG: iter_num_plot = {iter_num_plot}")
    print(f"  DEBUG: rmse_plot = {rmse_plot}")

    valid_rmse_exists = any(math.isfinite(x) for x in rmse_plot if isinstance(x, (float, int)))
    if not iter_num_plot or not rmse_plot or not valid_rmse_exists:
        print("  No valid data to plot for learning curve. Plot generation skipped.")
        return

    print("  Attempting to generate learning curve plot...")
    fig, ax = plt.subplots(figsize=(12, 6)) 
    ax.plot(iter_num_plot, rmse_plot, marker='o', linestyle='--')

    ax.set_xlabel("Number of Iterations (maxIter)")
    ax.set_ylabel("Validation RMSE")
    ax.set_title(f"ALS Learning Curve (Rank={rank_val}, RegParam={reg})")
    ax.set_xticks(iter_array) 
    ax.grid(True)
    
    display(fig) # Databricks 노트북에서 플롯 표시
    # plt.show() # display(fig) 사용 시 이 줄은 보통 주석 처리하거나 생략
    print(f"--- Finished plot_als_learning_curve ---")
```

*   **설명:**
    *   이 함수는 주어진 `rank`와 `regParam` 값에 대해, `maxIter` 값을 `iter_array`에 포함된 값들로 변경해가면서 ALS 모델을 학습시키고, 각 `maxIter` 값에서의 검증 세트 RMSE를 계산하여 학습 곡선 데이터를 생성합니다. 마지막으로 이 데이터를 사용하여 Matplotlib으로 학습 곡선을 그리고 `display()` 함수를 통해 노트북에 표시합니다.
*   **주요 로직:**
    *   입력으로 `iter_array`(테스트할 `maxIter` 값들의 리스트), 학습 데이터, 검증 데이터, 그리고 고정된 `reg`와 `rank_val`을 받습니다.
    *   `iter_array`의 각 `maxIter` 값에 대해 ALS 모델을 학습하고 검증 RMSE를 계산합니다.
    *   계산된 `maxIter`와 RMSE 값들을 `iter_num_plot`과 `rmse_plot` 리스트에 저장합니다.
    *   모든 `maxIter`에 대한 RMSE 계산이 끝나면, `matplotlib.pyplot`을 사용하여 x축을 `maxIter`, y축을 RMSE로 하는 꺾은선 그래프를 그립니다.
    *   `display(fig)`: Databricks 환경에서 Matplotlib으로 생성된 그림 객체(`fig`)를 노트북 출력 창에 렌더링하는 특수 함수입니다.
*   **학생들에게 설명할 내용:**
    *   학습 곡선의 의미와 해석 방법:
        *   RMSE가 반복 횟수 증가에 따라 계속 감소하면 모델이 더 학습될 여지가 있음을 의미.
        *   RMSE가 어느 시점부터 수렴하거나 다시 증가하면, 해당 지점이 적절한 학습 반복 횟수이거나 과적합이 시작되는 지점일 수 있음. (이 함수는 검증 RMSE만 그리므로, 학습 RMSE도 함께 그리면 과적합 여부를 더 명확히 판단 가능)
    *   `maxIter` 하이퍼파라미터가 모델 학습에 미치는 영향.
    *   Databricks 노트북에서 Matplotlib 그래프를 표시하기 위해 `display()` 함수를 사용하는 방법.
    *   플롯을 그리기 전에 유효한 데이터(`NaN`이 아닌 RMSE 값)가 있는지 확인하는 로직의 중요성.

**셀 18: 학습 곡선 플롯 생성 및 확인**

```python
# 이전에 final_model, best_rank_found, best_reg_found가 train_ALS_df로부터 반환되었다고 가정
if 'train_df' in locals() and 'validation_df' in locals():
    iter_array_for_plot = [1, 2, 5, 10, 15] 
    
    if final_model is not None and best_rank_found != -1 and best_reg_found != -1 :
        print(f"--- Calling plot_als_learning_curve with best found parameters ---")
        print(f"Plotting learning curve for Rank={best_rank_found}, RegParam={best_reg_found}")
        
        train_count = train_df.count()
        validation_count = validation_df.count()
        print(f"  Train DataFrame count for plotting: {train_count}")
        print(f"  Validation DataFrame count for plotting: {validation_count}")

        if train_count > 0 and validation_count > 0:
            plot_als_learning_curve(iter_array_for_plot, train_df, validation_df, best_reg_found, best_rank_found)
        else:
            print("  Skipping plotting learning curve as train_df or validation_df is empty.")
            
    else: 
        print("final_model is None or best parameters are invalid. Plotting with example parameters (Rank=10, Reg=0.2) as fallback.")
        # 예시 파라미터로 호출 시에도 train_df, validation_df 확인
        train_count = train_df.count()
        validation_count = validation_df.count()
        if train_count > 0 and validation_count > 0:
            plot_als_learning_curve(iter_array_for_plot, train_df, validation_df, 0.2, 10)
        else:
            print("  Skipping plotting learning curve (fallback): train_df or validation_df is empty.")
else:
    print("Training and validation DataFrames (train_df, validation_df) are not available for plotting learning curve.")
```

*   **설명:**
    *   앞서 하이퍼파라미터 튜닝을 통해 찾은 최적의 `rank`(`best_rank_found`)와 `regParam`(`best_reg_found`) 값을 사용하여 `plot_als_learning_curve` 함수를 호출합니다. 이를 통해 최적 모델의 학습 과정을 시각적으로 확인할 수 있습니다.
    *   만약 `final_model`이 성공적으로 학습되지 않았거나 최적 파라미터가 유효하지 않은 경우, 예시 파라미터(Rank=10, Reg=0.2)를 사용하여 학습 곡선을 그리도록 하는 폴백(fallback) 로직이 포함될 수 있습니다 (현재 코드에서는 해당 폴백 호출이 예시로만 언급됨).
*   **학생들에게 설명할 내용:**
    *   함수 호출 시 이전 단계에서 얻은 `best_rank_found`와 `best_reg_found`를 전달하는 것의 의미 (최적 조건에서의 학습 과정 분석).
    *   출력된 학습 곡선 그래프를 함께 해석:
        *   X축은 `maxIter` (반복 횟수), Y축은 검증 RMSE입니다.
        *   `maxIter`가 증가함에 따라 RMSE가 어떻게 변하는지 관찰합니다. 일반적으로 초반에는 RMSE가 빠르게 감소하다가 점차 수렴하는 양상을 보입니다.
        *   `num_iterations_hyperparam` (하이퍼파라미터 튜닝 시 사용한 `maxIter` 값)이 적절했는지, 아니면 더 많은 반복이 필요했는지 등을 추론해볼 수 있습니다.
    *   `train_df` 또는 `validation_df`가 비어있을 경우 플로팅을 건너뛰는 방어적 코딩의 중요성.

---

**Phase 7: 최종 모델 평가 및 추천 생성**

하이퍼파라미터 튜닝과 학습 곡선 분석을 통해 선택된 최종 모델(`final_model`)의 성능을 테스트 세트에서 평가하고, 이를 사용하여 실제 추천을 생성합니다.

**셀 19: 테스트 세트에서 최종 모델 평가**

```python
# MAGIC %md
# MAGIC ### Model testing
# MAGIC And finally, make a prediction and check the testing error using the `final_model`.

if final_model and 'test_df' in locals(): # final_model이 None이 아니고 test_df가 존재하는지 확인
    
    test_count = test_df.count()
    print(f"--- Evaluating final_model on the test set ({test_count} records) ---")

    if test_count > 0:
        predictions_test = final_model.transform(test_df)
        predictions_test_cleaned = predictions_test.filter(predictions_test.prediction.isNotNull())

        evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
        
        if predictions_test_cleaned.count() == 0:
            rmse_test = float('inf')
            print("Warning: No valid predictions on the test set after applying the model. All predictions might be NaN or test set became empty due to coldStartStrategy.")
        else:
            rmse_test = evaluator.evaluate(predictions_test_cleaned)
        
        print(f"Test Set Root-Mean-Square Error (RMSE) = {rmse_test:.4f}")
    else:
        print("Test set is empty. Cannot evaluate model.")

else:
    print("Final model (final_model) or test DataFrame (test_df) is not available for testing.")
    if not final_model:
        print("  Reason: final_model is None.")
    if 'test_df' not in locals():
        print("  Reason: test_df is not defined.")
    elif test_df.count() == 0 :
         print("  Reason: test_df is defined but empty.")
```

*   **설명:**
    *   최종적으로 선택된 `final_model`을 사용하여, 이전에 한 번도 모델 학습이나 튜닝에 사용되지 않은 `test_df`에 대한 예측을 수행하고 RMSE를 계산합니다. 이는 모델의 일반화 성능을 나타내는 중요한 지표입니다.
*   **학생들에게 설명할 내용:**
    *   테스트 세트 평가의 중요성 (모델이 보지 못한 새로운 데이터에 대한 성능 측정, 일반화 능력 평가).
    *   검증 세트 RMSE와 테스트 세트 RMSE를 비교하여 과적합 여부를 판단할 수 있음 (테스트 RMSE가 검증 RMSE보다 현저히 높다면 과적합을 의심).
    *   여기서 얻은 RMSE 값이 이 추천 시스템의 전반적인 예측 정확도를 나타내는 수치임을 설명. (낮을수록 좋음)
    *   `test_df`가 비어있거나 예측 결과가 없는 경우에 대한 예외 처리의 중요성.

**셀 20: 사용자별 영화 추천 및 아이템별 사용자 추천 생성**

```python
if final_model: # final_model이 성공적으로 학습되었는지 확인
    # Generate top 10 movie recommendations for each user
    try:
        userRecs = final_model.recommendForAllUsers(10)
        print("\n--- Top 10 movie recommendations for some users (sample) ---")
        # userRecs.show(5, truncate=False) # 콘솔 출력용
        display(userRecs.limit(5)) # Databricks UI에서 테이블 형태로 보기 좋게 표시 (처음 5명 사용자)
    except Exception as e:
        print(f"Error generating user recommendations: {e}")

    # Generate top 10 user recommendations for each movie
    try:
        movieRecs = final_model.recommendForAllItems(10)
        print("\n--- Top 10 user recommendations for some movies (sample) ---")
        # movieRecs.show(5, truncate=False) # 콘솔 출력용
        display(movieRecs.limit(5)) # Databricks UI에서 테이블 형태로 보기 좋게 표시 (처음 5개 영화)
    except Exception as e:
        print(f"Error generating movie recommendations: {e}")
else:
    print("final_model is not available. Cannot generate recommendations.")
```

*   **설명:**
    *   학습된 `final_model`을 사용하여 다음 두 가지 종류의 추천을 생성합니다:
        1.  `recommendForAllUsers(N)`: 각 사용자에게 가장 높은 예측 평점을 가진 상위 N개의 영화를 추천합니다.
        2.  `recommendForAllItems(N)`: 각 영화에 대해 가장 높은 예측 평점을 가진 상위 N명의 사용자를 추천합니다 (이 기능은 "이 영화를 좋아할 만한 다른 사용자"를 찾는 데 사용될 수 있습니다).
*   **주요 함수 설명:**
    *   `final_model.recommendForAllUsers(numItems)`: 모든 사용자에 대해 각각 `numItems`개의 아이템(영화)을 추천합니다. 결과 DataFrame은 `userId`와 `recommendations` 컬럼을 가지며, `recommendations`는 `(movieId, predicted_rating)` 형태의 배열입니다.
    *   `final_model.recommendForAllItems(numUsers)`: 모든 아이템(영화)에 대해 각각 `numUsers`명의 사용자를 추천합니다. 결과 DataFrame은 `movieId`와 `recommendations` 컬럼을 가지며, `recommendations`는 `(userId, predicted_rating)` 형태의 배열입니다.
    *   `display(DataFrame)`: Databricks 노트북에서 Spark DataFrame을 깔끔한 테이블 형태로 시각화해주는 유용한 함수입니다. `.show()`보다 가독성이 좋습니다. `.limit(5)`를 사용하여 너무 많은 결과를 한 번에 표시하지 않도록 합니다.
*   **학생들에게 설명할 내용:**
    *   학습된 협업 필터링 모델을 활용하여 실제 추천 목록을 생성하는 방법.
    *   `recommendForAllUsers`와 `recommendForAllItems` 함수의 차이점과 각각의 활용 사례.
    *   추천 결과에 나오는 예측 평점의 의미.
    *   Databricks의 `display()` 함수를 사용하여 DataFrame 결과를 효과적으로 탐색하는 방법.
    *   실제 서비스에서는 추천된 영화 목록에서 사용자가 이미 평가했거나 본 영화를 제외하는 후처리 과정이 필요할 수 있음을 언급.

---

**결론 및 추가 논의:**

이 핸즈온 실습을 통해 학생들은 Spark와 ALS를 사용하여 추천 시스템을 구축하는 전체 과정을 경험했습니다. 데이터 로드 및 전처리부터 모델 학습, 튜닝, 평가, 그리고 실제 추천 생성까지의 흐름을 이해하는 것이 중요합니다.

**학생들과 논의해볼 수 있는 추가 주제:**

*   **콜드 스타트 문제 해결 방안:** 콘텐츠 기반 추천, 하이브리드 추천, 사용자/아이템 메타데이터 활용 등.
*   **추천 시스템의 다른 평가지표:** Precision@k, Recall@k, NDCG, MAP 등 (단순 RMSE 외에 실제 추천 목록의 품질을 평가).
*   **실시간 추천 vs 배치 추천:** 사용 사례에 따른 아키텍처 차이.
*   **모델 업데이트 주기:** 사용자 행동 변화에 따라 모델을 얼마나 자주 재학습해야 하는가.
*   **추천 결과의 다양성 및 참신성(Serendipity):** 항상 인기 있는 것만 추천하는 것을 넘어 사용자의 탐색을 돕는 추천.
*   **Spark ML 파이프라인:** 여러 단계의 데이터 변환 및 모델 학습 과정을 하나의 파이프라인으로 묶어 관리하는 방법.
*   **MLflow:** 머신러닝 실험 추적, 모델 저장 및 배포를 위한 도구 (Databricks와 잘 통합됨).

이 가이드가 실습 진행에 도움이 되기를 바랍니다! 각 셀을 실행하면서 나오는 결과와 로그를 함께 분석하고, 궁금한 점에 대해 자유롭게 질문하도록 독려해주세요.
