
## Databricks 핸즈온: 이커머스 고객 데이터 분석 및 연간 지출액 예측

공개된 이커머스 고객 데이터를 사용하여 데이터 분석부터 머신러닝 모델 구축 및 평가까지의 전 과정을 Databricks 환경에서 함께 배워보겠습니다. 
**내용:**

1.  **환경 설정 및 데이터 로딩**: 셸 명령어를 사용하여 데이터를 DBFS에 다운로드하고 Spark DataFrame으로 로딩합니다.
2.  **데이터 변환 및 기본 탐색**: Spark DataFrame을 Pandas DataFrame으로 변환하고 기본 정보를 확인합니다.
3.  **탐색적 데이터 분석 (EDA)**: 결측치 확인, 변수 간 상관관계 분석 등 데이터의 특성을 깊이 있게 파악합니다. (시각화 상세 설명 포함)
4.  **특성 공학 (Feature Engineering)**: 기존 데이터를 가공하여 모델에 유용한 새로운 특성을 만듭니다. (안정적인 이메일 도메인 추출 포함)
5.  **데이터 전처리 및 분할**: 모델 학습을 위해 결측치를 처리하고 데이터를 학습용과 테스트용으로 분할합니다. (X와 y 모두 결측치 처리)
6.  **머신러닝 모델 구축 및 학습**: Scikit-learn 파이프라인을 사용하여 데이터 스케일링, 결측치 대체, 선형 회귀 모델 학습을 수행합니다.
7.  **모델 평가 및 결과 해석**: 학습된 모델의 예측 성능을 다양한 지표로 평가하고 결과를 시각화합니다.

**사전 준비:**

*   Databricks 작업 공간에 로그인합니다.
*   실행 중인 클러스터가 있는지 확인하고, 없다면 새 클러스터를 생성합니다. (예: 1 Driver, DBR 13.3 LTS ML 이상 권장)
*   새로운 Python 노트북을 생성합니다. (예: "ECommerce_Analysis_Full_Guide_KR")

---

### 1단계: 환경 설정 및 데이터 로딩

Databricks 노트북의 셸 명령어 기능을 사용하여 GitHub에서 CSV 파일을 DBFS(Databricks File System)로 직접 다운로드하고, 그 파일을 Spark DataFrame으로 로딩합니다.

#### 1.1 셸 명령어를 사용하여 DBFS에 데이터 파일 다운로드

노트북에 새로운 셸 셀을 추가하고 다음 명령어를 실행합니다.

```sh
%sh
# 기존 폴더/파일 삭제 (오류 방지)
rm -r /dbfs/ecommerce_data
# 새 폴더 생성
mkdir /dbfs/ecommerce_data
# wget으로 파일 다운로드
wget -O /dbfs/ecommerce_data/Ecommerce_Customers.csv https://raw.githubusercontent.com/gaurav-bhatt89/Datasets/main/Ecommerce_Customers.csv
```

**설명:**
*   `%sh`: 셀을 셸 스크립트로 실행합니다.
*   `rm -r /dbfs/ecommerce_data`: 기존에 동일한 이름의 폴더가 있다면 삭제합니다.
*   `mkdir /dbfs/ecommerce_data`: 데이터를 저장할 새 폴더를 DBFS에 만듭니다.
*   `wget -O [저장경로/파일명] [URL]`: 지정된 URL에서 파일을 다운로드하여 지정된 경로에 저장합니다.

**실행 후:** 오류 없이 완료되면 `Ecommerce_Customers.csv` 파일이 DBFS의 `/ecommerce_data/` 폴더에 저장됩니다.

#### 1.2 DBFS 경로를 사용하여 Spark DataFrame으로 로딩

이제 DBFS에 다운로드한 CSV 파일을 Spark DataFrame으로 읽어옵니다. Python 셀에서 다음 코드를 실행합니다.

```python
# DBFS에 다운로드한 파일 경로
dbfs_file_path = "/ecommerce_data/Ecommerce_Customers.csv"

# Spark DataFrame으로 데이터 로딩
try:
    spark_df = spark.read.csv(dbfs_file_path, header=True, inferSchema=True)
    print(f"DBFS 경로 '{dbfs_file_path}'에서 데이터 로드 성공!")

    # 로드된 데이터 확인 (상위 5개 행)
    print("Spark DataFrame 미리보기 (상위 5개 행):")
    display(spark_df.limit(5))

    # 데이터 스키마 확인
    print("\n데이터 스키마:")
    spark_df.printSchema()

except Exception as e:
    print(f"데이터 로드 중 오류 발생: {e}")
    print(f"DBFS 경로 '{dbfs_file_path}' 또는 1.1단계 %sh 명령어 실행을 확인해주세요.")
```

**설명:**
*   `spark.read.csv()`: CSV 파일을 읽는 함수입니다.
    *   `header=True`: 첫 번째 줄을 컬럼명으로 사용합니다.
    *   `inferSchema=True`: Spark가 자동으로 데이터 타입을 추론합니다.
*   `display()`: Databricks에서 DataFrame을 테이블 형태로 보기 좋게 출력합니다.
*   `printSchema()`: DataFrame의 컬럼명과 데이터 타입을 보여줍니다.

**예상 결과:** 데이터 로드 성공 메시지와 함께 DataFrame의 상위 5개 행 및 스키마 정보가 출력됩니다.

---

### 2단계: 데이터 변환 및 기본 탐색

분석 및 모델링에 용이하도록 Spark DataFrame을 Pandas DataFrame으로 변환하고, 필요한 라이브러리를 임포트합니다.

```python
# 필요한 라이브러리 임포트
from pyspark.sql import SparkSession # 이미 spark 객체가 있지만 명시
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import string # 문자열 처리에 사용될 수 있음

# Seaborn 그래프 스타일 설정
sns.set_theme(style='darkgrid', palette='hls')

# Spark DataFrame을 Pandas DataFrame으로 변환
if 'spark_df' in locals() and spark_df is not None:
    df = spark_df.toPandas()
    print("Pandas DataFrame으로 변환 완료!")
    print("\nPandas DataFrame 정보:")
    df.info()
    print("\nPandas DataFrame 기술 통계량:")
    display(df.describe()) # display() 사용 시 더 보기 좋음
else:
    print("오류: spark_df가 정의되지 않았습니다. 1단계를 다시 확인해주세요.")
```

**설명:**
*   **라이브러리**: `numpy` (수치 계산), `pandas` (데이터 분석), `matplotlib` 및 `seaborn` (시각화) 등을 임포트합니다.
*   `spark_df.toPandas()`: Spark DataFrame을 Pandas DataFrame으로 변환합니다. (데이터가 매우 클 경우 드라이버 메모리 주의)
*   `df.info()`: Pandas DataFrame의 컬럼별 정보(데이터 타입, null 값 개수 등)를 보여줍니다.
*   `df.describe()`: 숫자형 컬럼들의 주요 기술 통계량(평균, 표준편차, 최소/최대값 등)을 보여줍니다.

**예상 결과:** 변환 완료 메시지와 함께 `df.info()`, `df.describe()` 결과가 출력됩니다.

---

### 3단계: 탐색적 데이터 분석 (EDA)

데이터의 특성을 파악하기 위해 결측치를 확인하고, 변수 간 상관관계를 시각적으로 분석합니다.

#### 3.1 결측치 시각화

```python
if 'df' in locals() and df is not None:
    plt.figure(figsize=(12, 7))
    sns.heatmap(df.isnull(), yticklabels=False, cbar=False, cmap='viridis')
    #plt.title('결측치 현황 (노란색 선이 결측치를 의미)')
    plt.title('Missing Values Heatmap (Yellow lines indicate missing data)')
    plt.show()
    print(f"\n원본 DataFrame의 컬럼별 결측치 개수:\n{df.isnull().sum()}")
else:
    print("오류: df (Pandas DataFrame)가 생성되지 않았습니다. 2단계를 다시 확인해주세요.")
```

**설명 (결측치 히트맵):**
*   `sns.heatmap(df.isnull(), ...)`: `df.isnull()`은 각 셀이 결측치면 `True`, 아니면 `False`를 반환합니다. 히트맵은 이를 색상으로 표현합니다. `cmap='viridis'`는 결측치를 밝은 노란색 계열로 표시합니다.
*   **해석**: 이 데이터셋은 초기에는 결측치가 없는 것으로 나타날 것입니다. (만약 노란 선이 보인다면 해당 부분에 결측치가 있다는 의미)

#### 3.2 변수 간 상관관계 히트맵

```python
if 'df' in locals() and df is not None:
    plt.figure(figsize=(10,8))
    sns.heatmap(df.corr(), annot=True, cmap='coolwarm', fmt=".2f", linewidths=.5)
    #plt.title('변수 간 상관계수 히트맵')
    plt.title('Correlation Heatmap of Variables')
    plt.show()
else:
    print("오류: df (Pandas DataFrame)가 생성되지 않았습니다. 2단계를 다시 확인해주세요.")
```

**설명 (상관계수 히트맵):**
*   `df.corr()`: 숫자형 컬럼들 간의 피어슨 상관계수를 계산합니다.
*   `sns.heatmap(..., annot=True, cmap='coolwarm')`: 상관계수 매트릭스를 히트맵으로 시각화합니다.
    *   `annot=True`: 각 셀에 상관계수 값을 표시합니다.
    *   `cmap='coolwarm'`: 양의 상관은 따뜻한 색(빨강), 음의 상관은 차가운 색(파랑)으로 표현합니다.
*   **해석**: 'Yearly Amount Spent'와 다른 변수들 간의 관계를 주목합니다. 예를 들어 'Length of Membership'과의 상관계수가 높게 나타날 것입니다 (약 0.81). 이는 회원 유지 기간이 길수록 연간 지출액이 높은 경향을 의미합니다.

#### 3.3 주요 변수 관계 시각화 (Jointplot)

```python
if 'df' in locals() and df is not None:
    # 1. 회원 유지 기간 vs 연간 지출액
    sns.jointplot(x='Length of Membership', y='Yearly Amount Spent', data=df, height=7, kind='scatter')
    plt.suptitle('회원 유지 기간 vs 연간 지출액 (Jointplot)', y=1.02)
    plt.tight_layout()
    plt.show()

    # 2. 앱 사용 시간 vs 연간 지출액 (회귀선 추가)
    sns.jointplot(x='Time on App', y='Yearly Amount Spent', data=df, kind='reg', height=7,
                  joint_kws={'scatter_kws': {'alpha': 0.5, 's': 30}},
                  marginal_kws={'color': 'green'})
    #plt.suptitle('앱 사용 시간 vs 연간 지출액 (Jointplot with Regression)', y=1.02)
    plt.suptitle('Length of Membership vs Yearly Amount Spent (Jointplot)', y=1.02)
    plt.tight_layout()
    plt.show()
else:
    print("오류: df (Pandas DataFrame)가 생성되지 않았습니다. 2단계를 다시 확인해주세요.")
```

**설명 (Jointplot):**
*   `sns.jointplot()`: 두 변수 간의 관계(산점도 등)와 각 변수의 분포(히스토그램 등)를 함께 보여줍니다.
    *   `kind='scatter'`: 일반 산점도를 그립니다.
    *   `kind='reg'`: 산점도에 회귀선과 신뢰구간을 함께 표시합니다.
*   **해석**: 각 변수 쌍의 관계 패턴과 각 변수의 분포를 시각적으로 파악할 수 있습니다.

---

### 4단계: 특성 공학 (Feature Engineering)

이메일 주소에서 도메인 정보를 추출하고, 모델 학습에 불필요한 범주형 컬럼들을 제거합니다.

```python
if 'df' in locals() and df is not None:
    # 이메일 주소에서 도메인 추출하는 함수 (안정적인 버전)
    def extract_email_domain(email_str):
        if not isinstance(email_str, str): return None
        try:
            domain_part = email_str.split('@')
            if len(domain_part) > 1 and domain_part[1]:
                main_domain_part = domain_part[1].split('.')
                if main_domain_part[0]: return main_domain_part[0]
            return None # 기본값 또는 'unknown_domain' 등
        except Exception: return None

    df['Email_Domain'] = df['Email'].apply(extract_email_domain)
    print("이메일 도메인 추출 후 미리보기 (상위 10개 행):")
    display(df[['Email', 'Email_Domain']].head(10))
    print(f"\nEmail_Domain이 추출되지 않은 건수: {df['Email_Domain'].isnull().sum()}")

    # 모델 학습에 사용하지 않을 컬럼 제거
    # Email_Domain은 이 예제에서는 제거하지만, 실제로는 더미 변수화하여 사용할 수 있습니다.
    columns_to_drop = ['Email', 'Address', 'Avatar', 'Email_Domain']
    df.drop(columns=columns_to_drop, axis=1, inplace=True)
    print(f"\n{columns_to_drop} 컬럼 제거 후 DataFrame 정보:")
    df.info()
    print("\n최종 사용될 DataFrame 미리보기 (상위 5개 행):")
    display(df.head())
else:
    print("오류: df (Pandas DataFrame)가 생성되지 않았습니다. 2단계를 다시 확인해주세요.")
```

**설명:**
*   `extract_email_domain()`: 이메일 주소에서 `@` 뒤, 첫 번째 `.` 앞부분을 추출하는 함수입니다. 예외 처리를 포함하여 안정성을 높였습니다.
*   `df.drop()`: 불필요한 컬럼들을 제거합니다. `inplace=True`는 원본 DataFrame을 직접 수정합니다.

**예상 결과**: `Email_Domain` 컬럼이 생성되고, 이후 `Email`, `Address`, `Avatar`, `Email_Domain` 컬럼이 제거된 DataFrame 정보가 출력됩니다.

---

### 5단계: 데이터 전처리 및 분할

모델 학습을 위해 특성(X)과 타겟(y)을 분리하고, 학습용/테스트용으로 나눈 뒤, 결측치를 처리합니다.

```python
if 'df' in locals() and df is not None:
    # 특성(X)과 타겟(y) 변수 분리
    X = df.drop('Yearly Amount Spent', axis=1)
    y = df['Yearly Amount Spent']

    print(f"--- 분리 전 y의 결측치 개수: {y.isnull().sum()} ---")

    # 학습용 데이터와 테스트용 데이터 분리
    from sklearn.model_selection import train_test_split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=101) # test_size 0.3으로 변경

    print(f"\n--- 분리 후 초기 결측치 확인 ---")
    print(f"X_train (처리 전): {X_train.isnull().sum().sum()}개, y_train (처리 전): {y_train.isnull().sum()}개")
    print(f"X_test (처리 전): {X_test.isnull().sum().sum()}개, y_test (처리 전): {y_test.isnull().sum()}개")

    # --- y_train 및 y_test의 결측치 처리 (NaN이 있는 행 제거 및 X 동기화) ---
    print("\n--- y 데이터 결측치 처리 (행 제거 방식) ---")
    # y_train 처리
    y_train_nan_indices = y_train[y_train.isnull()].index
    if not y_train_nan_indices.empty:
        print(f"y_train에서 {len(y_train_nan_indices)}개 NaN 행 제거.")
        y_train = y_train.drop(y_train_nan_indices)
        X_train = X_train.drop(y_train_nan_indices)
    # y_test 처리
    y_test_nan_indices = y_test[y_test.isnull()].index
    if not y_test_nan_indices.empty:
        print(f"y_test에서 {len(y_test_nan_indices)}개 NaN 행 제거.")
        y_test = y_test.drop(y_test_nan_indices)
        X_test = X_test.drop(y_test_nan_indices)
    
    print(f"\n--- 처리 후 최종 결측치 확인 ---")
    print(f"X_train (최종): {X_train.isnull().sum().sum()}개, y_train (최종): {y_train.isnull().sum()}개")
    print(f"X_test (최종): {X_test.isnull().sum().sum()}개, y_test (최종): {y_test.isnull().sum()}개")

    print("\n--- 최종 데이터 크기 ---")
    print(f"X_train: {X_train.shape}, y_train: {y_train.shape}")
    print(f"X_test: {X_test.shape}, y_test: {y_test.shape}")

else:
    print("오류: df (Pandas DataFrame)가 생성되지 않았습니다. 2단계를 다시 확인해주세요.")
```

**설명:**
*   `X = df.drop(...)`, `y = df[...]`: 특성 행렬 `X`와 타겟 벡터 `y`를 분리합니다.
*   `train_test_split()`: 데이터를 학습 세트와 테스트 세트로 분할합니다. `test_size=0.3`은 30%를 테스트용으로 사용합니다. `random_state`는 재현성을 위해 설정합니다.
*   **결측치 처리**:
    *   `y_train`과 `y_test`에서 NaN 값을 가진 행을 식별하고, 해당 행을 `y`와 `X` 양쪽에서 모두 제거하여 데이터의 정합성을 맞춥니다. (원본 데이터에 'Yearly Amount Spent'에 NaN이 있을 경우 이 부분이 동작합니다. 이 데이터셋은 없을 가능성이 높습니다.)
    *   `X_train`과 `X_test`의 결측치는 다음 단계의 파이프라인에서 `SimpleImputer`를 통해 처리될 것입니다. (만약 이 데이터셋에 숫자형 특성에 NaN이 있다면)

**예상 결과**: 각 데이터셋의 크기와 결측치 처리 결과가 출력됩니다. `y` 관련 결측치가 없다면, 제거되는 행은 없을 것입니다.

---

### 6단계: 머신러닝 모델 구축 및 학습

Scikit-learn 파이프라인을 사용하여 데이터 전처리(결측치 대체, 스케일링)와 선형 회귀 모델 학습을 한 번에 처리합니다.

```python
if 'X_train' in locals() and 'y_train' in locals():
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler
    from sklearn.linear_model import LinearRegression
    from sklearn.impute import SimpleImputer # 결측치 처리를 위해

    # 파이프라인 정의:
    # 1. SimpleImputer: 결측치를 각 컬럼의 평균값으로 대체
    # 2. StandardScaler: 특성 스케일링 (평균 0, 표준편차 1)
    # 3. LinearRegression: 선형 회귀 모델
    pipe = Pipeline([
      ('imputer', SimpleImputer(strategy='mean')),
      ('scaler', StandardScaler()),
      ('lin_reg', LinearRegression())
    ])

    # 모델 학습
    try:
        pipe.fit(X_train, y_train)
        print("모델 학습 완료!")
    except Exception as e:
        print(f"모델 학습 중 오류 발생: {e}")
        print("X_train 또는 y_train 데이터를 다시 확인해주세요.")
        print("\nX_train 정보:")
        X_train.info()
        print(f"\nX_train 결측치: {X_train.isnull().sum().sum()}")
        print("\ny_train 정보:")
        print(f"y_train 길이: {len(y_train)}, y_train 결측치: {y_train.isnull().sum()}")
else:
    print("오류: X_train 또는 y_train이 정의되지 않았습니다. 5단계를 다시 확인해주세요.")
```

**설명:**
*   `Pipeline`: 여러 변환 단계와 최종 모델을 하나로 묶어 순차적으로 실행합니다.
    *   `SimpleImputer(strategy='mean')`: 결측치를 해당 특성의 평균값으로 대체합니다.
    *   `StandardScaler()`: 각 특성의 값을 표준화하여 평균 0, 분산 1로 만듭니다. 선형 모델 성능 향상에 도움을 줄 수 있습니다.
    *   `LinearRegression()`: 선형 회귀 모델입니다.
*   `pipe.fit(X_train, y_train)`: 파이프라인에 학습 데이터를 넣어 각 단계를 거쳐 모델을 학습시킵니다.

**예상 결과**: "모델 학습 완료!" 메시지가 출력됩니다. 만약 오류가 발생하면, `except` 블록의 메시지가 출력될 것입니다.

---

### 7단계: 모델 평가 및 결과 해석

학습된 모델을 사용하여 테스트 데이터의 연간 지출액을 예측하고, 실제값과 비교하여 모델의 성능을 평가합니다.

```python
if 'pipe' in locals() and 'X_test' in locals() and 'y_test' in locals():
    # 테스트 데이터로 예측 수행
    predict_lr = pipe.predict(X_test)

    # 예측 결과와 실제 값 비교 (일부만)
    results_df = pd.DataFrame({'Actual': y_test, 'Predicted': predict_lr, 'Difference': y_test - predict_lr})
    print("\n예측 결과 (일부):")
    display(results_df.head())

    # 성능 지표 계산
    from sklearn import metrics
    mae = metrics.mean_absolute_error(y_test, predict_lr)
    mse = metrics.mean_squared_error(y_test, predict_lr)
    rmse = np.sqrt(mse) # MSE에 제곱근을 취한 값
    r2 = metrics.r2_score(y_test, predict_lr)

    print('\n모델 평가 지표:')
    print(f'  Mean Absolute Error (MAE):  {mae:.2f}')
    print(f'  Mean Squared Error (MSE):   {mse:.2f}')
    print(f'  Root Mean Squared Error (RMSE): {rmse:.2f}')
    print(f'  R-squared (R²) Score:       {r2:.4f}')

    # 예측값과 실제값의 산점도 시각화
    plt.figure(figsize=(8,6))
    plt.scatter(y_test, predict_lr, alpha=0.7, edgecolors='w', linewidth=0.5)
    plt.xlabel('Actual Yearly Amount Spent')
    plt.ylabel('Predicted Yearly Amount Spent')
    plt.title('Actual vs. Predicted Yearly Amount Spent')
    # 완벽한 예측선 (y=x) 추가
    min_val = min(y_test.min(), predict_lr.min())
    max_val = max(y_test.max(), predict_lr.max())
    plt.plot([min_val, max_val], [min_val, max_val], 'r--', lw=2) # 빨간 점선
    plt.grid(True)
    plt.show()
else:
    print("오류: pipe, X_test, 또는 y_test가 정의되지 않았습니다. 이전 단계를 다시 확인해주세요.")
```

**설명:**
*   `pipe.predict(X_test)`: 학습된 파이프라인을 사용하여 테스트 데이터의 타겟 변수를 예측합니다.
*   **평가 지표**:
    *   **MAE (Mean Absolute Error)**: 실제값과 예측값 차이의 절대값 평균. 오류의 크기를 직접적으로 나타냅니다.
    *   **MSE (Mean Squared Error)**: 실제값과 예측값 차이의 제곱 평균. 큰 오류에 더 큰 페널티를 줍니다.
    *   **RMSE (Root Mean Squared Error)**: MSE의 제곱근. MAE와 유사하게 오류 크기를 나타내며 단위가 원래 데이터와 동일합니다.
    *   **R² Score (결정 계수)**: 모델이 데이터의 분산을 얼마나 잘 설명하는지를 나타냅니다. 0과 1 사이의 값을 가지며, 1에 가까울수록 모델이 데이터를 잘 설명함을 의미합니다.
*   **시각화**: 실제값과 예측값을 산점도로 그려 모델의 예측 정확도를 시각적으로 확인합니다. 점들이 대각선(y=x 선)에 가까이 분포할수록 예측이 정확함을 의미합니다.

**예상 결과**: 예측 결과 일부, 주요 평가 지표 값들, 그리고 실제값 대비 예측값 산점도 그래프가 출력됩니다. R² 스코어는 0.98 이상으로 매우 높게 나올 것으로 예상됩니다.

---

### 8단계: 결론

이커머스 고객 데이터를 사용하여 다음의 과정을 학습했습니다:
1.  Databricks 환경에서 셸 명령어로 데이터를 DBFS에 로드하고 Spark/Pandas DataFrame으로 변환했습니다.
2.  상세한 EDA를 통해 데이터의 특성과 변수 간 관계를 파악했습니다.
3.  안정적인 특성 공학 기법을 적용하고 불필요한 컬럼을 제거했습니다.
4.  **`X`와 `y` 양쪽 모두의 결측치를 체계적으로 처리**하고 데이터를 학습/테스트용으로 분할했습니다.
5.  `SimpleImputer`, `StandardScaler`, `LinearRegression`을 포함하는 Scikit-learn 파이프라인을 구축하고 모델을 학습시켰습니다.
6.  다양한 지표로 모델의 예측 성능을 평가하고 결과를 시각화하여, 연간 지출액을 매우 정확하게 예측하는 모델을 만들었습니다. 


