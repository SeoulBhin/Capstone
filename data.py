import pandas as pd
import json

def preprocess_fire_data(file_path):
    
    # CSV 파일을 읽고 정제하고 DataFrame으로 변환
    
    # 파일 인코딩 문제 처리
    try:
        df = pd.read_csv(file_path, encoding='utf-8')
    except UnicodeDecodeError:
        df = pd.read_csv(file_path, encoding='cp949')

    # 1. 컬럼명 표준화
    column_rename_map = {
        '화재발생년원일': '화재발생일시',
        '인명피해(명)소계': '인명피해소계',
        '사망': '사망자수',
        '부상': '부상자수'
    }
    df.rename(columns=column_rename_map, inplace=True)

    # 2. 데이터 타입 및 값 정제
    # 날짜/시간 변환
    df['화재발생일시'] = pd.to_datetime(df['화재발생일시'], errors='coerce')

    # 숫자형 컬럼 정제
    numeric_cols = ['인명피해소계', '사망자수', '부상자수', '재산피해소계', '부동산', '동산']
    for col in numeric_cols:
        if col in df.columns:
            # 쉼표 등 비숫자 문자를 제거하는 로직 추가 (필요시)
            if df[col].dtype == 'object':
                 df[col] = df[col].str.replace(',', '', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    # 3. 지역명 표준화
    sido_rename_map = {
        '강원도': '강원특별자치도',
        '전라북도': '전북특별자치도'
    }
    if '시도' in df.columns:
        df['시도'] = df['시도'].replace(sido_rename_map)
        
    # 불필요한 컬럼 제거 및 순서 재정의
    final_columns = [
        '화재발생일시', '시도', '시군구', '화재유형', '발화요인대분류', '발화요인소분류',
        '인명피해소계', '사망자수', '부상자수', '재산피해소계', '부동산', '동산',
        '장소대분류', '장소중분류', '장소소분류'
    ]
    
    # 원본 데이터 없는 컬럼이 있을 경우 예외처리
    final_columns = [col for col in final_columns if col in df.columns]

    return df[final_columns]

# 사용할 파일 경로
file_path = '소방청_화재발생 정보_20241231.csv'

# 데이터 전처리 실행
fire_df = preprocess_fire_data(file_path)

# 결과 확인
print("데이터 전처리 완료. DataFrame 정보:")
<<<<<<< HEAD

=======
>>>>>>> 9fc0bdc75dfb2493a98b342366f85439e19a7455
print(fire_df.info())
print("\n샘플 데이터:")
print(fire_df.head())

def aggregate_causes(series):
    """발화요인대분류 문자열로 변환"""
    counts = series.value_counts()
    return json.dumps(counts.to_dict(), ensure_ascii=False) if not counts.empty else None

# 'year' 컬럼 추가
fire_df['year'] = fire_df['화재발생일시'].dt.year  # 새 컬럼에 전처리 한 연도 정보 추가

# 연도 및 시도별로 데이터 집계
year_stats_df = fire_df.groupby(['year', '시도']).agg(
    incident_count=('화재발생일시', 'size'),
    total_property_damage=('재산피해소계', 'sum'),
    total_casualties=('인명피해소계', 'sum'),
    deaths=('사망자수', 'sum'),
    injuries=('부상자수', 'sum'),
    fire_causes=('발화요인대분류', aggregate_causes)
).reset_index()

# 재산피해액 단위를 천원에서 '원'으로 변경
year_stats_df['total_property_damage'] = year_stats_df['total_property_damage'] * 1000

# 결과 CSV로 저장
print("\n연도별/지역별 집계 데이터:")
print(year_stats_df.head())
year_stats_df.to_csv('fire_stats.csv', index=False, encoding='utf-8-sig')