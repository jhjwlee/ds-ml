## Databricks 핸즈온: 이커머스 고객 데이터 분석 및 연간 지출액 예측

안녕하세요! 오늘 우리는 공개된 이커머스 고객 데이터를 사용하여 데이터 분석부터 머신러닝 모델 구축 및 평가까지의 과정을 함께 배워보겠습니다. Databricks 환경에서 PySpark와 Python 라이브러리들을 활용하여 실제 데이터 분석 프로젝트의 흐름을 경험해 보세요.

**오늘 우리가 배울 내용:**

1.  **데이터 로딩**: 공개된 CSV 데이터를 Databricks 환경으로 로딩합니다.
2.  **탐색적 데이터 분석 (EDA)**: 데이터의 주요 특징과 패턴을 파악합니다.
3.  **데이터 정제 / 전처리**: 결측치를 확인하고 필요한 경우 처리합니다.
4.  **특성 공학 (Feature Engineering)**: 기존 특성을 변형하거나 새로운 특성을 만듭니다.
5.  **선형 회귀 모델 적용 및 평가**: 데이터를 학습시켜 모델을 만들고, 예측 정확도를 평가합니다.

**원본 자료 출처:**

*   데이터셋: [Ecommerce_Customers.csv](https://github.com/gaurav-bhatt89/Datasets/blob/main/Ecommerce_Customers.csv)
*   GitHub 노트북: [ECommerce_Linear_Regression.ipynb](https://github.com/gaurav-bhatt89/Databricks/blob/main/ECommerce_Linear_Regression.ipynb)

---

### 0단계: Databricks 환경 준비

1.  Databricks 작업 공간에 로그인합니다.
2.  실행 중인 클러스터가 있는지 확인하고, 없다면 새 클러스터를 생성합니다. (예: 1 Driver, DBR 13.3 LTS ML 이상)
3.  새로운 Python 노트북을 생성합니다. (예: "ECommerce_Customer_Analysis_KR")

---

### 1단계: 데이터 로딩

가장 먼저 분석할 데이터를 Databricks 환경으로 가져와야 합니다. 우리는 GitHub에 있는 CSV 파일 URL을 직접 사용하여 Spark DataFrame으로 로딩할 것입니다.

**주요 함수 설명:**

*   `spark.read.format("csv")`: CSV 파일을 읽기 위한 Spark 리더를 지정합니다.
*   `.option("header", "true")`: CSV 파일의 첫 번째 줄을 헤더(컬럼명)로 사용하도록 설정합니다.
*   `.option("inferSchema", "true")`: Spark가 자동으로 각 컬럼의 데이터 타입을 추론하도록 설정합니다. (숫자는 숫자로, 문자는 문자로 등)
*   `.load("URL")`: 지정된 URL로부터 데이터를 로드합니다.
*   `display(dataframe)`: Databricks 노트북에서 DataFrame을 테이블 형태로 시각화하여 보여주는 유용한 함수입니다.
*   `dataframe.printSchema()`: DataFrame의 스키마(컬럼명과 데이터 타입)를 출력합니다.

```python
# 데이터셋 URL
csv_url = "https://raw.githubusercontent.com/gaurav-bhatt89/Datasets/main/Ecommerce_Customers.csv"

# Spark DataFrame으로 데이터 로딩
spark_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(csv_url)

# 로드된 데이터 확인 (상위 5개 행)
print("Spark DataFrame으로 데이터 로드 완료!")
display(spark_df.limit(5))

# 데이터 스키마 확인
print("데이터 스키마:")
spark_df.printSchema()
```

**결과 설명:**
위 코드를 실행하면, 먼저 "Spark DataFrame으로 데이터 로드 완료!"라는 메시지와 함께 데이터의 처음 5개 행이 테이블 형태로 보입니다. 다음으로 각 컬럼의 이름과 추론된 데이터 타입(예: string, double)이 출력됩니다. 이를 통해 데이터가 올바르게 로드되었는지, 각 컬럼의 타입이 적절한지 확인할 수 있습니다.

---

### 2단계: 라이브러리 임포트 및 Pandas DataFrame으로 변환

이제 분석 및 모델링에 필요한 라이브러리들을 가져오고, Spark DataFrame을 Pandas DataFrame으로 변환하겠습니다. Pandas DataFrame은 단일 머신에서 사용하기 편리하며, Scikit-learn, Seaborn 같은 Python 라이브러리와의 호환성이 좋습니다.

**주요 함수 및 라이브러리 설명:**

*   `pyspark.sql.SparkSession`: Spark 애플리케이션의 진입점입니다. Databricks 노트북에서는 `spark`라는 이름으로 이미 생성되어 있습니다.
*   `numpy` (Numerical Python): 수치 계산을 위한 핵심 라이브러리입니다. 배열, 행렬 연산 등에 사용됩니다.
*   `pandas`: 데이터 분석 및 조작을 위한 강력한 라이브러리입니다. DataFrame이라는 자료구조를 제공합니다.
*   `matplotlib.pyplot`: 데이터 시각화를 위한 기본적인 라이브러리입니다.
*   `seaborn`: Matplotlib을 기반으로 더 아름답고 통계적인 그래프를 그릴 수 있게 해주는 라이브러리입니다.
*   `spark_df.toPandas()`: Spark DataFrame을 Pandas DataFrame으로 변환합니다. **주의**: 이 작업은 클러스터의 드라이버 노드로 모든 데이터를 가져오므로, 매우 큰 데이터셋에는 메모리 부족 문제를 일으킬 수 있습니다. 현재 데이터셋은 크기가 작아 괜찮습니다.

```python
# 필요한 라이브러리 임포트
from pyspark.sql import SparkSession # SparkSession은 Databricks에서 기본 제공되지만, 명시적으로 포함하는 것이 좋습니다.
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import string # 문자열 관련 작업을 위해 사용될 수 있습니다. (여기서는 Feature Engineering에서 활용)

# Seaborn 스타일 설정 (더 보기 좋은 그래프를 위해)
sns.set_theme(style='darkgrid', palette='hls')

# Spark DataFrame을 Pandas DataFrame으로 변환
# 원본 노트북에서는 sqlContext.sql(...)을 사용했지만, 우리는 이미 spark_df로 로드했습니다.
df = spark_df.toPandas()

print("Pandas DataFrame으로 변환 완료!")
print("Pandas DataFrame 정보:")
df.info()
```

**결과 설명:**
"Pandas DataFrame으로 변환 완료!" 메시지와 함께 `df.info()`의 결과가 출력됩니다. `df.info()`는 각 컬럼의 이름, null이 아닌 값의 개수, 데이터 타입을 보여줍니다. 이를 통해 데이터의 기본적인 구조와 결측치 유무를 빠르게 파악할 수 있습니다.

---

### 3단계: 탐색적 데이터 분석 (EDA)

데이터를 이해하는 것은 매우 중요합니다. EDA를 통해 데이터의 통계적 특성, 분포, 변수 간의 관계 등을 파악해 봅시다.

**주요 함수 설명:**

*   `df.describe()`: 숫자형 컬럼들의 주요 통계치(개수, 평균, 표준편차, 최소값, 최대값, 사분위수)를 요약해서 보여줍니다.
*   `df.isnull()`: 각 셀이 결측치(NaN)인지 여부를 boolean 값으로 반환합니다.
*   `sns.heatmap()`: 데이터를 색상으로 표현하는 히트맵을 그립니다. 여기서는 결측치와 변수 간 상관계수를 시각화하는 데 사용됩니다.
    *   `yticklabels=False`: y축 레이블을 표시하지 않습니다.
    *   `cbar=False`: 컬러바를 표시하지 않습니다.
    *   `cmap='viridis'`: 사용할 색상 맵을 지정합니다.
    *   `annot=True`: 각 셀에 값을 표시합니다. (상관계수 히트맵에서 사용)
*   `df.corr()`: 숫자형 컬럼들 간의 피어슨 상관계수를 계산합니다.
*   `sns.jointplot()`: 두 변수 간의 관계를 산점도와 함께 각 변수의 분포도(히스토그램 또는 KDE)를 보여줍니다.
    *   `kind='reg'`: 회귀선을 함께 표시합니다.

#### 3.1 데이터 기본 정보 및 결측치 확인

```python
# 숫자형 컬럼들의 기술 통계량 확인
print("데이터 기술 통계량:")
display(df.describe()) # Databricks의 display() 함수를 사용하면 더 보기 좋게 출력됩니다.

# 결측치 시각화
plt.figure(figsize=(10,6)) # 그래프 크기 조절
sns.heatmap(df.isnull(), yticklabels=False, cbar=False, cmap='viridis')
plt.title('결측치가 있다면 노란색 선으로 표시됩니다.')
plt.show() # Databricks 노트북에서는 plt.show()가 없어도 그래프가 표시되지만, 명시적으로 사용하는 것이 좋습니다.
```

**결과 및 해석:**
`df.describe()`를 통해 각 수치형 변수들의 평균, 표준편차, 최소/최대값 등을 확인할 수 있습니다. 예를 들어, 'Yearly Amount Spent'(연간 지출액)의 평균은 약 499달러임을 알 수 있습니다.

결측치 히트맵에서는 노란색 선이 결측치를 나타냅니다. 이 데이터셋에는 결측치가 없는 것으로 보이네요! 만약 결측치가 있다면 해당 부분을 어떻게 처리할지(제거, 대체 등) 고민해야 합니다.

#### 3.2 변수 간 상관관계 분석

어떤 변수들이 'Yearly Amount Spent'(연간 지출액)와 관련이 깊은지 상관계수를 통해 알아봅시다.

```python
# 모든 숫자형 변수 간의 상관계수 히트맵
plt.figure(figsize=(10,8))
sns.heatmap(df.corr(), annot=True, cmap='coolwarm', fmt=".2f") # fmt=".2f"는 소수점 둘째자리까지 표시
plt.title('상관계수 히트맵')
plt.show()
```

**결과 및 해석:**
히트맵은 -1부터 1까지의 상관계수를 색상으로 나타냅니다.
*   **'Yearly Amount Spent'** 행(또는 열)을 보면, **'Length of Membership'** (회원 유지 기간)이 0.81로 매우 강한 양의 상관관계를 보입니다. 즉, 회원 유지 기간이 길수록 연간 지출액이 높은 경향이 있습니다.
*   **'Time on App'** (앱 사용 시간)과 **'Avg. Session Length'** (평균 세션 길이)도 각각 0.50, 0.36으로 양의 상관관계를 보입니다.
*   **'Time on Website'** (웹사이트 사용 시간)은 -0.00으로 거의 상관관계가 없는 것으로 나타납니다.

#### 3.3 주요 변수 관계 시각화

상관관계가 높게 나온 변수들을 중심으로 `jointplot`을 그려 관계를 더 자세히 살펴봅시다.

```python
# 연간 지출액과 회원 유지 기간의 관계
sns.jointplot(x='Yearly Amount Spent', y='Length of Membership', data=df)
plt.suptitle('회원 유지 기간 vs 연간 지출액 상관관계', y=1.02) # suptitle로 전체 그래프 제목 설정
plt.tight_layout() # 레이아웃 자동 조절
plt.show()

# 앱 사용 시간과 회원 유지 기간의 관계 (회귀선 포함)
# 원본 노트북에서는 'Time on App' vs 'Yearly Amount Spent'를 의도한 것 같으나, 코드는 'Time on App' vs 'Length of Membership'으로 되어 있습니다.
# 여기서는 원본 코드대로 'Time on App' vs 'Length of Membership'을 그리겠습니다.
# 만약 'Time on App' vs 'Yearly Amount Spent'를 보고 싶다면 y축 변수를 변경하세요.
sns.jointplot(x='Time on App', y='Length of Membership', data=df, kind='reg')
plt.suptitle('앱 사용 시간 vs 회원 유지 기간 상관관계', y=1.02)
plt.tight_layout()
plt.show()

# 참고: 'Time on App' vs 'Yearly Amount Spent'
sns.jointplot(x='Time on App', y='Yearly Amount Spent', data=df, kind='reg')
plt.suptitle('앱 사용 시간 vs 연간 지출액 상관관계', y=1.02)
plt.tight_layout()
plt.show()
```

**결과 및 해석:**
*   첫 번째 `jointplot`은 회원 유지 기간이 길어질수록 연간 지출액이 증가하는 뚜렷한 양의 상관관계를 시각적으로 보여줍니다.
*   두 번째 `jointplot`은 앱 사용 시간과 회원 유지 기간 간의 관계를 보여줍니다. 약간의 양의 상관관계가 있는 것으로 보입니다.
*   세 번째 `jointplot` (참고용)은 앱 사용 시간이 길수록 연간 지출액이 증가하는 경향을 보여줍니다.

---

### 4단계: 특성 공학 (Feature Engineering)

데이터에서 새로운 의미 있는 특성을 추출하거나 기존 특성을 변형하여 모델의 성능을 향상시킬 수 있습니다. 여기서는 'Email' 주소에서 도메인 정보를 추출해보고, 불필요한 범주형 변수들을 제거하겠습니다.

**주요 함수 및 개념 설명:**

*   `df['Column'].apply(lambda x: ...)`: Pandas Series의 각 요소에 함수를 적용합니다. `lambda`는 간단한 익명 함수를 정의할 때 사용됩니다.
*   `x.split('@')[1]`: 이메일 주소 문자열 `x`를 '@' 기준으로 나누고, 그중 두 번째 요소(도메인 부분)를 가져옵니다.
*   `x.split('.')[0]`: 도메인 문자열 `x`를 '.' 기준으로 나누고, 그중 첫 번째 요소(예: 'gmail' from 'gmail.com')를 가져옵니다.
*   `pd.DataFrame()`: Pandas DataFrame을 생성합니다.
*   `df.drop(['col1', 'col2'], axis=1, inplace=True)`: 지정된 컬럼(들)을 제거합니다.
    *   `axis=1`: 컬럼을 대상으로 작업합니다. (0은 행)
    *   `inplace=True`: 원본 DataFrame을 직접 수정합니다. (False면 복사본을 반환)

```python
# 이메일 주소에서 도메인 추출
# 예: "jared@mays.com" -> "mays.com" -> "mays"
email_domain_series = df['Email'].apply(lambda x: x.split('@')[1].split('.')[0])
df['Email_Domain'] = email_domain_series # 새로운 'Email_Domain' 컬럼으로 추가

print("이메일 도메인 추출 후 상위 5개 행:")
display(df[['Email', 'Email_Domain']].head())

# 원본 노트북에서는 Email 컬럼 자체를 도메인으로 대체했지만,
# 여기서는 새로운 컬럼으로 만들고, 이후 원본 Email 컬럼과 다른 범주형 컬럼들을 제거하겠습니다.

# 모델 학습에 사용하지 않을 범주형 컬럼들 제거
# 선형 회귀는 기본적으로 수치형 입력을 가정합니다. 범주형 변수를 사용하려면 원-핫 인코딩 등이 필요합니다.
# 여기서는 단순화를 위해 Email, Address, Avatar, 그리고 새로 만든 Email_Domain을 제거합니다.
# (Email_Domain을 특성으로 사용하려면 더미 변수화 해야 하지만, 이 예제에서는 제거)
df.drop(['Email', 'Address', 'Avatar', 'Email_Domain'], axis=1, inplace=True)

print("\n불필요한 컬럼 제거 후 DataFrame 정보:")
df.info()

print("\n최종 사용될 DataFrame 상위 5개 행:")
display(df.head())
```

**결과 및 해석:**
'Email' 컬럼에서 도메인 이름만 추출하여 'Email_Domain'이라는 새로운 컬럼을 만들었습니다. 그 후, 선형 회귀 모델에 직접 사용하기 어려운 'Email', 'Address', 'Avatar' 컬럼과, 단순화를 위해 'Email_Domain' 컬럼도 제거했습니다. 최종적으로 숫자형 데이터만 남은 것을 `df.info()`와 `df.head()`를 통해 확인할 수 있습니다.

---

### 5단계: 선형 회귀 모델 적용 및 평가

이제 준비된 데이터를 사용하여 연간 지출액을 예측하는 선형 회귀 모델을 만들고 평가해 보겠습니다.

**주요 라이브러리 및 함수 설명:**

*   `sklearn.model_selection.train_test_split`: 데이터를 학습용(train) 세트와 테스트용(test) 세트로 분리합니다.
    *   `test_size`: 테스트 세트의 비율을 지정합니다 (예: 0.25는 25%).
    *   `random_state`: 재현 가능성을 위해 난수 생성 시드를 고정합니다.
*   `sklearn.pipeline.Pipeline`: 여러 단계를 순차적으로 실행하는 파이프라인을 구성합니다. (예: 스케일링 -> 모델 학습)
*   `sklearn.preprocessing.StandardScaler`: 특성 스케일링을 수행합니다. 각 특성의 평균을 0, 표준편차를 1로 변환하여 모든 특성이 비슷한 범위를 갖도록 합니다. 이는 일부 모델의 성능을 향상시킬 수 있습니다.
*   `sklearn.linear_model.LinearRegression`: 선형 회귀 모델을 구현한 클래스입니다.
*   `pipe.fit(X_train, y_train)`: 파이프라인(스케일러 및 모델)을 학습 데이터로 학습시킵니다.
*   `pipe.predict(X_test)`: 학습된 모델을 사용하여 테스트 데이터의 결과를 예측합니다.
*   `sklearn.metrics`: 모델 평가 지표들을 제공합니다.
    *   `mean_absolute_error (MAE)`: 실제값과 예측값 차이의 절대값 평균. 오류의 크기를 직접적으로 나타냅니다.
    *   `mean_squared_error (MSE)`: 실제값과 예측값 차이의 제곱 평균. 오류에 민감하며, 큰 오류에 더 큰 페널티를 줍니다.
    *   `r2_score (R² Score)`: 결정 계수. 모델이 데이터의 분산을 얼마나 잘 설명하는지를 나타냅니다. 1에 가까울수록 좋습니다.

#### 5.1 데이터 분리 (학습용 및 테스트용)

```python
# 특성(X)과 타겟(y) 변수 분리
X = df.drop('Yearly Amount Spent', axis=1) # 'Yearly Amount Spent'를 제외한 모든 컬럼
y = df['Yearly Amount Spent']              # 'Yearly Amount Spent' 컬럼

# 학습용 데이터와 테스트용 데이터 분리 (75% 학습, 25% 테스트)
from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=101) # random_state로 재현성 확보

print("학습용 특성 데이터 크기:", X_train.shape)
print("테스트용 특성 데이터 크기:", X_test.shape)
```

**결과 및 해석:**
데이터를 독립 변수(특징) `X`와 종속 변수(목표) `y`로 나누었습니다. 그 후 `train_test_split` 함수를 사용하여 전체 데이터를 학습용과 테스트용으로 75:25 비율로 분할했습니다. `random_state`를 설정하여 코드를 다시 실행해도 항상 동일하게 데이터가 분할되도록 합니다.

#### 5.2 파이프라인 구축 및 모델 학습

데이터 스케일링과 선형 회귀 모델 학습을 한 번에 처리할 수 있는 파이프라인을 만듭니다.

```python
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression

# 파이프라인 정의: 1. StandardScaler -> 2. LinearRegression
pipe = Pipeline([
  ('scaler', StandardScaler()),     # 첫 번째 단계: 데이터 스케일링
  ('lin_reg', LinearRegression())   # 두 번째 단계: 선형 회귀 모델
])

# 파이프라인을 사용하여 모델 학습 (스케일링과 학습이 순차적으로 진행됨)
pipe.fit(X_train, y_train)

print("모델 학습 완료!")
```

**결과 및 해석:**
`Pipeline`을 사용하면 전처리 단계와 모델 학습 단계를 하나로 묶어 편리하게 관리할 수 있습니다. `pipe.fit()`을 호출하면 `X_train` 데이터가 먼저 `StandardScaler`를 통해 스케일링된 후, 그 결과가 `LinearRegression` 모델 학습에 사용됩니다.

#### 5.3 예측 및 모델 평가

학습된 모델을 사용하여 테스트 데이터의 연간 지출액을 예측하고, 실제값과 비교하여 모델의 성능을 평가합니다.

```python
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
rmse = np.sqrt(mse) # RMSE는 MSE에 제곱근을 취한 값입니다.
r2 = metrics.r2_score(y_test, predict_lr)

print('\n모델 평가 지표:')
print(f'  Mean Absolute Error (MAE): {mae:.2f}')
print(f'  Mean Squared Error (MSE): {mse:.2f}')
print(f'  Root Mean Squared Error (RMSE): {rmse:.2f}') # 원본 노트북에서는 r2_score를 RMSE로 잘못 표기했었습니다.
print(f'  R-squared (R²) Score: {r2:.4f}')

# (선택 사항) 예측값과 실제값의 산점도
plt.figure(figsize=(8,6))
plt.scatter(y_test, predict_lr, alpha=0.7)
plt.xlabel('Actual Yearly Amount Spent')
plt.ylabel('Predicted Yearly Amount Spent')
plt.title('Actual vs. Predicted Yearly Amount Spent')
# 완벽한 예측을 나타내는 대각선 추가
min_val = min(y_test.min(), predict_lr.min())
max_val = max(y_test.max(), predict_lr.max())
plt.plot([min_val, max_val], [min_val, max_val], 'r--') # 빨간 점선
plt.show()
```

**결과 및 해석:**

*   **MAE (평균 절대 오차)**: 약 7.89달러. 예측값이 실제값과 평균적으로 약 7.89달러 차이가 난다는 의미입니다.
*   **MSE (평균 제곱 오차)**: 약 97.10. 오류의 제곱 평균입니다.
*   **RMSE (평균 제곱근 오차)**: 약 9.85달러. MAE와 유사하게 오류의 크기를 나타내지만, 큰 오류에 더 민감합니다. 단위가 원래 데이터와 동일하여 해석이 용이합니다.
*   **R-squared (R² 결정 계수)**: 약 0.9845. 모델이 데이터의 분산 중 약 98.45%를 설명한다는 의미입니다. 1에 매우 가까우므로 모델이 데이터를 잘 설명하고 있다고 볼 수 있습니다.

산점도에서 점들이 빨간색 대각선 주위에 가깝게 분포할수록 모델의 예측이 정확하다는 것을 의미합니다. 이 경우 점들이 대각선에 매우 가깝게 모여 있어 모델의 성능이 좋음을 시각적으로도 확인할 수 있습니다.

---

### 6단계: 결론

오늘은 이커머스 고객 데이터를 사용하여 다음의 과정들을 진행했습니다:
1.  GitHub에서 CSV 데이터를 Spark DataFrame으로 로드하고 Pandas DataFrame으로 변환했습니다.
2.  EDA를 통해 데이터의 특성과 변수 간의 관계 (특히 'Length of Membership'과 'Yearly Amount Spent' 간의 강한 상관관계)를 파악했습니다.
3.  이메일 주소에서 도메인을 추출하는 간단한 특성 공학을 수행하고, 모델링에 불필요한 컬럼을 제거했습니다.
4.  데이터를 스케일링하고 선형 회귀 모델을 학습시키는 파이프라인을 구축했습니다.
5.  학습된 모델을 평가하여 MAE, MSE, RMSE, R² 값을 확인했고, R² 값이 약 0.98로 매우 높은 예측 성능을 보임을 확인했습니다.

이 핸즈온을 통해 Databricks 환경에서 데이터 분석 및 머신러닝 모델링의 기본적인 흐름을 경험하셨기를 바랍니다. 수고하셨습니다!

---

궁금한 점이나 더 탐구하고 싶은 부분이 있다면 언제든지 질문해주세요!
