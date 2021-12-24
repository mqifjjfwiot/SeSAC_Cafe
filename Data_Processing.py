import pandas as pd
import numpy as np
# 카카오 api 위경도 변환용
import geopandas as gpd
import requests; from urllib.parse import urlparse
from shapely.geometry import Point, Polygon, LineString
# 구글 api 용
import pprint
# 좌표계 변환용
from pyproj import Proj, transform

def total_data_processing():
    cafe_data()
    bus_data_processing()
    subway_data_processing()
    traffic_data()
    population_data()
    culture_data()
    area_data()

def bus_data_processing():
    # 원본 데이터 출처
    ## 버스 정류장별 승하차 : https://data.seoul.go.kr/dataList/OA-12913/S/1/datasetView.do
    ## 버스 정류소 위치정보 : https://data.seoul.go.kr/dataList/OA-15067/S/1/datasetView.do

    ## 시간별 승하차인원 전처리
    bus = pd.read_csv('./origin/서울시 버스노선별 정류장별 시간대별 승하차 인원 정보.csv', encoding='cp949')

    # 버스정류장ARS번호 기준으로 groupby를 하기위해 버스정류장 ARS번호 안에들어가있는 불필요한 정보 '~'를 제거하고
    # groupby를 원활하게하기위해 데이터타입을 int64으로 변경 (뒤에있을 좌표데이터의 데이터타입과 동일시 하기위해 int64)
    bus1 = bus[bus.버스정류장ARS번호 != '~']
    bus2 = bus1.astype({'버스정류장ARS번호': 'int64'})

    # 원하는 데이터로 가공
    # 아침 출근시간(07시~10시) , 낮 시간(10시~17시) ,퇴근시간(17시~22시) 의 3단계로 분류 하여 유동인구 체크
    # on은 승차인원수, off는 하차 인원수를 의미.
    bus2['07-10 on avg'] = (bus2['7시승차총승객수'] + bus2['8시승차총승객수'] + bus2['9시승차총승객수']) / 3
    bus2['07-10 off avg'] = (bus2['7시하차총승객수'] + bus2['8시하차총승객수'] + bus2['9시하차총승객수']) / 3
    bus2['10-17 on avg'] = (bus2['10시승차총승객수'] + bus2['11시승차총승객수'] + bus2['12시승차총승객수'] + bus2['13시승차총승객수'] + bus2['14시승차총승객수'] + bus2['15시승차총승객수'] + bus2['16시승차총승객수']) / 7
    bus2['10-17 off avg'] = (bus2['10시하차총승객수'] + bus2['11시하차총승객수'] + bus2['12시하차총승객수'] + bus2['13시하차총승객수'] + bus2['14시하차총승객수'] + bus2['15시하차총승객수'] + bus2['16시하차총승객수']) / 7
    bus2['17-22 on avg'] = (bus2['17시승차총승객수'] + bus2['18시승차총승객수'] + bus2['19시승차총승객수'] + bus2['20시승차총승객수'] + bus2['21시승차총승객수']) / 5
    bus2['17-22 off avg'] = (bus2['17시하차총승객수'] + bus2['18시하차총승객수'] + bus2['19시하차총승객수'] + bus2['20시하차총승객수'] + bus2['21시하차총승객수']) / 5

    # 불필요한 컬럼들 선별, 삭제
    # 1. 사용년월은 3년치 데이터의 평균을 이용할 것이므로 데이터 시간, 날짜에 대한 정보는 불필요
    # 2. 노선번호와 노선명은 데이터군으로 분류하기가 까다롭기 때문에 버스정류장을 기준점으로 유동량 체크

    # bus3 : XX시 승하차 총 승객수, 등록일자 칼럼 삭제
    bus3 = bus2.drop(bus.columns[6:54], axis=1)
    bus3 = bus3.drop('등록일자', axis=1)
    # bus4 : 사용년월, 노선번호, 노선명 ,표준 버스정류장 id 삭제
    bus4 = bus3.drop(bus.columns[:4], axis=1)

    # 버스정류장 번호를 기준으로 그룹화. 평균기준
    bus_gp = bus4.groupby(['버스정류장ARS번호'], as_index=False).mean()

    # 뒤에있을 좌표데이터와 컬럼값을 동일시 하기위해 칼럼명을 'code' 로 변경
    bus_gp.rename(columns={'버스정류장ARS번호': 'code'}, inplace=True)

    ## 좌표 데이터 전처리
    bl = pd.read_csv('./origin/서울특별시 버스정류소 위치정보.csv', encoding='cp949')

    # 승하차 인원 데이터와 합치기 위해 컬럼값 변경
    bl.rename(columns={'정류소번호': 'code', '정류소명': 'station', 'X좌표': 'Y', 'Y좌표': 'X'}, inplace=True)

    ## 승하차 데이터 bus_gp, 정류장 위치 데이터 bl 합치기
    # 머지에 파라미터값을 아우터로 준 이유는 NaN값을 체크하기위해서.
    merge_df = pd.merge(bus_gp, bl, how='outer')

    # 위치좌표데이터는 서울시 정류소 기준으로 설정이 되어있으나 서울시 버스 노선은 서울 주변 지역도 포함되어있기때문에
    # 위치좌표 기준으로 결측되어있는 값들은 버려도 괜찮다.
    merge_df.dropna(axis=0, inplace=True)

    # 결측치 인덱스 행을 비울때, 비워진 인덱스는 공란으로 남게 된다. 자동으로 매꿔지거나 하지 않는다. 이는 나중에 for문 돌릴때 에러를 유발하는 원인이 되므로 리셋해서 재조정해주는게 좋다.
    merge_df.reset_index(drop=True, inplace=True)


    # 칼럼 위치 조정
    merge_df2 = merge_df[
        ['code', 'station', '07-10 on avg', '07-10 off avg', '10-17 on avg', '10-17 off avg', '17-22 on avg', '17-22 off avg', 'X', 'Y']
    ]


    ## 분류 작업 시작
    # 승하차인원중 많은곳만 따로 분류하기
    # 가벼운 카피로 원본 보존
    bus_1 = merge_df2

    ## 정류장별로 의미를 부여하기위해 새로운 칼럼들을 생성

    ## 출퇴근시 주로 이용되는 정류장인지 판별
    # 'commute' : 아침 시간대와 저녁 시간대의 승하차 승객 수의 합
    # 'Total' : 하루종일 해당 버스정류장을 이용한 총 승객 수
    # 'com' : 하루 대비 출퇴근 시간대에 해당 버스 정류장을 이용한 승객의 비율
    # 'com73' : 출퇴근 거점이면 1, 아니면 0 부여.
    bus_1['07-10 totavg'] = (bus_1['07-10 on avg'] + bus_1['07-10 off avg']) / 2  # 07-10 승하차수 평균
    bus_1['10-17 totavg'] = (bus_1['10-17 on avg'] + bus_1['10-17 off avg']) / 2  # 10-17 승하차수 평균
    bus_1['17-22 totavg'] = (bus_1['17-22 on avg'] + bus_1['17-22 off avg']) / 2  # 17-22 승하차수 평균
    bus_1['commute'] = bus_1['07-10 totavg'] + bus_1['17-22 totavg']
    bus_1['Total'] = bus_1['07-10 totavg'] + bus_1['17-22 totavg'] + bus_1['10-17 totavg']
    bus_1['com'] = bus_1['commute'] / bus_1['Total']
    bus_1['coms'] = 0
    bus_1['many75'] = 0
    bus_1['trans'] = 0
    # 백분율 값이 73% 이상이면 1 아니면 0
    bus_ex = []
    for x in bus_1['com']:
        if x >= 0.73:
            bus_ex.append(1)
        else:
            bus_ex.append(0)
    bus_1['coms'] = bus_ex

    ## 하루 이용량이 많은 정류장인지 판별
    # 칼럼 'Total'을 기준으로 상위 75%인 정류장에 부여
    # 1이면 해당, 0이면 해당사항 없음
    # describe() 활용해 얻은 'Total' 칼럼의 상위 75% 값 : 684.730804
    for i in range(len(bus_1['Total'])):
        if bus_1['Total'][i] >= 685:
            bus_1['many75'][i] = 1
        else:
            bus_1['many75'][i] = 0

    ## 환승 목적으로 찾는 이용객이 많은 정류장인지 판별
    # 일상적으로 사용하는 낮 시간대의 승객수를 활용. 승차하는 인원과 하차하는 인원이 비슷하다면 환승을 목적으로 그곳에 하차했을것으로 추정.
    # 칼럼 '10-17'을 활용하여 평균값과 승차 인원수, 하차 인원수의 차이가 20% 이하라면 1 부여, 해당사항 없으면 0.
    for i in range(len(bus_1['10-17 totavg'])):
        if (abs(bus_1['10-17 on avg'][i]-bus_1['10-17 totavg'][i]))/bus_1['10-17 totavg'][i] <= 0.2:
            bus_1['trans'][i] = 1
        else :
            bus_1['trans'][i] = 0

    # 최종 정리된 버스 데이터 저장
    bus_1.to_csv('./after/bus_data.csv', encoding='cp949')


def subway_data_processing():
    # 원본 데이터 출처
    ## 지하철 승하차 역별 시간별 : https://data.seoul.go.kr/dataList/OA-12252/S/1/datasetView.do

    ## 지하철 승하차인원 전처리
    sw = pd.read_csv('./origin/서울시 지하철 호선별 역별 시간대별 승하차 인원 정보.csv', encoding='cp949')

    # 대표값을 설정하기 위해 3년치 데이터를 평균내어 사용. 3년어치 외의 필요없는 기간 칼럼 드랍하기
    sw.loc[(sw['사용월'] == 201901)]
    sw = sw.drop(sw.index[21045:])

    # 원하는 데이터로 가공
    # 아침 출근시간(07시~10시) , 낮 시간(10시~17시) ,퇴근시간(17시~22시) 의 3단계로 분류 하여 유동인구 체크
    # on은 승차인원수, off는 하차 인원수를 의미.
    # 버스 정류장과 동일하게. 나중에 프레임 합칠것
    sw['07-10 on avg'] = (sw['07시-08시 승차인원'] + sw['08시-09시 승차인원'] + sw['09시-10시 승차인원']) / 3
    sw['07-10 off avg'] = (sw['07시-08시 하차인원'] + sw['08시-09시 하차인원'] + sw['09시-10시 하차인원']) / 3
    sw['10-17 on avg'] = (sw['10시-11시 승차인원'] + sw['11시-12시 승차인원'] + sw['12시-13시 승차인원'] + sw['13시-14시 승차인원'] + sw['14시-15시 승차인원'] + sw['15시-16시 승차인원'] + sw['16시-17시 승차인원']) / 7
    sw['10-17 off avg'] = (sw['10시-11시 하차인원'] + sw['11시-12시 하차인원'] + sw['12시-13시 하차인원'] + sw['13시-14시 하차인원'] + sw['14시-15시 하차인원'] + sw['15시-16시 하차인원'] + sw['16시-17시 하차인원']) / 7
    sw['17-22 on avg'] = (sw['17시-18시 승차인원'] + sw['18시-19시 승차인원'] + sw['19시-20시 승차인원'] + sw['20시-21시 승차인원'] + sw['21시-22시 승차인원']) / 5
    sw['17-22 off avg'] = (sw['17시-18시 하차인원'] + sw['18시-19시 하차인원'] + sw['19시-20시 하차인원'] + sw['20시-21시 하차인원'] + sw['21시-22시 하차인원']) / 5

    # 대표값을 제외한 일반 칼럼들 삭제. 기타 미사용 칼럼들 삭제
    sw = sw.drop(sw.columns[3:33], axis=1)
    sw = sw.drop(['사용월', '호선명'], axis=1)

    # groupby를 통해서 지하철역 기준으로 데이터를 병합
    sw1 = sw.groupby(['지하철역'], as_index=False).mean()

    # 승객수 데이터 타입을 실수형에서 정수형으로 변환
    sw1 = sw1.astype({'07-10 on avg': 'int',
                      '07-10 off avg': 'int',
                      '10-17 on avg': 'int',
                      '10-17 off avg': 'int',
                      '17-22 on avg': 'int',
                      '17-22 off avg': 'int'
                      })

    # 데이터 셋 병합을 위해 칼럼명 영문으로 변환
    sw1.rename(columns={'지하철역': 'station'}, inplace=True)


    ## 지하철 좌표 전처리
    sl = pd.read_csv('./origin/지하철 좌표.csv')

    # 중복된값을 제거해서 하나로 정리
    sl1 = sl.drop_duplicates(['지하철역'])

    # 인덱스 순서에 맞게 정렬후 merge하기전에  칼럼값 동일하게 만들기
    sl1.rename(columns={'지하철역': 'station', '호선명': 'line'}, inplace=True)
    sl1.reset_index(drop=False, inplace=True)
    sl1 = sl1.drop(['index'], axis=1)


    # 두 데이터 병합
    # 승하차 인원수 정보 sw1, 좌표정보 sl1
    merge_df = pd.merge(sw1, sl1, how='outer', on='station')

    # 결측치 있는값들 삭제
    merge_df.dropna(axis=0, inplace=True)
    merge_df.reset_index(drop=False, inplace=True)
    merge_df = merge_df.drop('index', axis=1)
    merge_df['code'] = range(30000,30000+len(merge_df['station']))

    # 칼럼 순서 재배치
    merge_df = merge_df[
        ['code', 'station', '07-10 on avg', '07-10 off avg', '10-17 on avg', '10-17 off avg', '17-22 on avg', '17-22 off avg', 'X', 'Y']
    ]

    merge_df2 = merge_df

    ## 역 별로 의미를 부여하기위해 새로운 칼럼들을 생성

    ## 출퇴근시 주로 이용되는 역인지 판별
    # 'commute' : 아침 시간대와 저녁 시간대의 승하차 승객 수의 합
    # 'Total' : 하루종일 해당 버스정류장을 이용한 총 승객 수
    # 'com' : 하루 대비 출퇴근 시간대에 해당 버스 정류장을 이용한 승객의 비율
    # 'coms' : 출퇴근 거점이면 1, 아니면 0 부여.
    merge_df2['07-10 totavg'] = (merge_df2['07-10 on avg'] + merge_df2['07-10 off avg']) / 2  # 07-10 승하차수 평균
    merge_df2['10-17 totavg'] = (merge_df2['10-17 on avg'] + merge_df2['10-17 off avg']) / 2  # 10-17 승하차수 평균
    merge_df2['17-22 totavg'] = (merge_df2['17-22 on avg'] + merge_df2['17-22 off avg']) / 2  # 17-22 승하차수 평균
    merge_df2['commute'] = merge_df2['07-10 totavg'] + merge_df2['17-22 totavg']
    merge_df2['Total'] = merge_df2['07-10 totavg'] + merge_df2['17-22 totavg'] + merge_df2['10-17 totavg']
    merge_df2['com'] = merge_df2['commute'] / merge_df2['Total']
    merge_df2['coms'] = 0
    merge_df2['many75'] = 0
    merge_df2['trans'] = 0
    # 백분율 값이 73% 이상이면 1 아니면 0
    # 버스는 73%, 지하철은 75%
    sub_ex = []
    for x in merge_df2['com']:
        if x >= 0.75:
            sub_ex.append(1)
        else:
            sub_ex.append(0)
    merge_df2['coms'] = sub_ex

    ## 하루 이용량이 많은 정류장인지 판별
    # 칼럼 'Total'을 기준으로 상위 75%인 정류장에 부여
    # 1이면 해당, 0이면 해당사항 없음
    # describe() 활용해 얻은 'Total' 칼럼의 상위 75% 값 : 76054.5
    for i in range(len(merge_df2['Total'])):
        if merge_df2['Total'][i] >= 76055:
            merge_df2['many75'][i] = 1
        else:
            merge_df2['many75'][i] = 0

    ## 환승 목적으로 찾는 이용객이 많은 정류장인지 판별
    # 일상적으로 사용하는 낮 시간대의 승객수를 활용. 승차하는 인원과 하차하는 인원이 비슷하다면 환승을 목적으로 그곳에 하차했을것으로 추정.
    # 칼럼 '10-17'을 활용하여 평균값과 승차 인원수, 하차 인원수의 차이가 10% 이하라면 1 부여, 해당사항 없으면 0.
    # 다만, 이 '하차' 통계에 환승 목적의 하차도 포함되는지가 불명. 큰 의미는 부여하지 않기로. 버스와 달리 20퍼로 할경우 90% 이상이 자격을 부여받아 조건 올림
    for i in range(len(merge_df2['10-17 totavg'])):
        if (abs(merge_df2['10-17 on avg'][i] - merge_df2['10-17 totavg'][i])) / merge_df2['10-17 totavg'][i] <= 0.1:
            merge_df2['trans'][i] = 1
        else:
            merge_df2['trans'][i] = 0

    # 병합한 데이터 별도 파일로 저장
    merge_df2.to_csv('./after/subway_data.csv', encoding='cp949')


def traffic_data():
    # 칼럼이 완전히 동일한 버스, 지하철 데이터셋 읽어 와 DataFrame으로 저장
    bus = pd.read_csv('./after/bus_data.csv', encoding='cp949')
    sub = pd.read_csv('./after/subway_data.csv', encoding='cp949')

    # concat 하여 하나의 데이터프레임으로 병합. 쓰이지 않는 칼럼 최종 삭제
    traffic = pd.concat([bus,sub],ignore_index=True)
    traffic = traffic.drop(['Unnamed: 0'], axis=1)

    # 교통 통합 csv 데이터 저장
    traffic.to_csv('./after/traffic_data.csv', encoding='cp949')


def population_data():
    # 원본 데이터 출처
    ## 서울시 사업체 종사사수 : https://data.seoul.go.kr/dataList/10598/S/2/datasetView.do
    ## 서울시 인구밀도 : https://data.seoul.go.kr/dataList/10584/S/2/datasetView.do
    ## 서울시 주민등록인구 : https://data.seoul.go.kr/dataList/10727/S/2/datasetView.do
    ## 서울시 유동인구 :  https://data.seoul.go.kr/dataVisual/seoul/seoulLivingPopulation.do

    # 모든데이터는 자세하게 '동' 까지나와있으나 필요한 데이터는 '구'이기때문에 직접 수집함.
    # csv파일의 인덱싱이 3중으로 쪼개져 있어 개략적인 구의 데이터만을 얻기 위해 직접 수기로 분류함.
    # 동별 근무자수.xls, 서울시 동별 인구밀도.xls, 서울시 주민등록상 동별 연령별 인구수.xls 의 데이터에서 종류별로 러프하게 분류함.
    # 구별 근무자수.csv, 구별 면적.csv, 유동인구.csv, 주민등록상 구별 인구.csv 의 4가지로 정리.


    ## 수기로 수집한 '주민등록상' 구별 인구 데이터 전처리
    pop = pd.read_csv('./origin/주민등록상 구별 인구.csv', encoding='utf8')
    pop1 = pop

    # 데이터 타입을 바꾸기 위해 , 제거후 타입 변경
    pop1['10-14'] = pop1['10~14세'].str.replace(',', '')
    pop1['15-19'] = pop1['15~19세'].str.replace(',', '')
    pop1['20-24'] = pop1['20~24세'].str.replace(',', '')
    pop1['25-29'] = pop1['25~29세'].str.replace(',', '')
    pop1['30-34'] = pop1['30~34세'].str.replace(',', '')
    pop1['35-39'] = pop1['35~39세'].str.replace(',', '')
    pop1['40-44'] = pop1['40~44세'].str.replace(',', '')
    pop1['45-49'] = pop1['45~49세'].str.replace(',', '')
    pop1['50-54'] = pop1['50~54세'].str.replace(',', '')
    pop1['55-59'] = pop1['55~59세'].str.replace(',', '')
    pop1['60-64'] = pop1['60~64세'].str.replace(',', '')
    pop1['65-69'] = pop1['65~69세'].str.replace(',', '')
    pop1 = pop1.drop(pop1.columns[:13], axis=1)
    pop1 = pop1.astype('int64')

    # 다른 인구 데이터와 칼럼값을 통일시키기위해 세분화된 나이 영역을 통합
    pop1['10-19'] = pop1['10-14'] + pop1['15-19']
    pop1['20-29'] = pop1['20-24'] + pop1['25-29']
    pop1['30-39'] = pop1['30-34'] + pop1['35-39']
    pop1['40-49'] = pop1['40-44'] + pop1['45-49']
    pop1['50-59'] = pop1['50-54'] + pop1['55-59']
    pop1['60-69'] = pop1['60-64'] + pop1['65-69']
    pop1 = pop1.drop(pop1.columns[:12], axis=1)


    ## 수기로 수집한 구별 '유동인구' 데이터 전처리
    pop2 = pd.read_csv('./origin/유동인구.csv')
    # 데이터는 한달치 데이터. 허나 하루치 값을 이용할 것이기때문에 값을 30으로 나눔.
    pop3 = pop2.iloc[:, 1:] / 30
    # 데이터 값을 정수형으로 바꿔 좀 더 깔끔하게 정리
    pop3 = pop3.astype({'10~20M': 'int', '10~20W': 'int',
                        '20~29M': 'int', '20~29W': 'int',
                        '30~39M': 'int', '30~39W': 'int',
                        '40~49M': 'int', '40~49W': 'int',
                        '50~59M': 'int', '50~59W': 'int',
                        '60~69M': 'int', '60~69W': 'int', })


    ## 수기로 수집한 구별 '근무자수' 데이터 전처리
    pop4 = pd.read_csv('./origin/구별 근무자수.csv')


    ## 수기로 수집한 구별 '면적'
    pop5 = pd.read_csv('./origin/구별 면적.csv')


    ## 위의 4개 데이터를 조합해 새로운 정보 생성
    ## 구별 인구 밀도를 구하기 위해 구별 인구수와 면적 데이터 조합하기
    concat_df = pd.concat([pop5, pop1], axis=1)

    # 전체인구수 구하기
    concat_df['Totalpop'] = concat_df.iloc[:, 2:].sum(axis=1)

    # 인구밀도 = 전체인구수/지역면적(단위 km^2)
    concat_df['density'] = (concat_df['Totalpop'] / concat_df['area']).astype('int')
    concat_df = concat_df.drop(concat_df.iloc[:, 2:8], axis=1)


    ## 위의 데이터셋들 규합
    # pop4 = 구별 근무자수 / pop1= 구별 나이별 인구수 / pop3 = 구별 나이별 유동인구
    concat_df2 = pd.concat([pop4, pop1, pop3], axis=1)

    # concat_df2= 구별 근로자수 인구수 유동인구 / concat_df= 인구밀도
    merge_df = pd.merge(concat_df2, concat_df, how='outer', on='gu')

    ## 데이터 저장
    merge_df.to_csv('./after/population_data.csv', encoding='cp949')


def kagoo_address_xy(addr):
    # 위치분석용 라이브러리 geopandas는 프롬프트로 개별 프로세스 거쳐야함
    # 출처 : https://codedragon.tistory.com/9556
    try:
        result = ""
        url = 'https://dapi.kakao.com/v2/local/search/address.json?query=' + addr
        rest_api_key = 'd14be6002a90d442569859af3ed267bf'
        header = {'Authorization': 'KakaoAK ' + rest_api_key}

        r = requests.get(url, headers=header)
        if r.status_code == 200:
            try:
                result_address = r.json()["documents"][0]["address"]
                result = result_address["y"], result_address["x"]
            except:
                # 응답은 왔으나 빈 데이터가 왔을 경우.
                # 구글 api로 한번만 더 돌려보자.
                # google api로 다시 트라이
                apikey = 'AIzaSyA8xeL4urGef52RBv-5blyg-PjI7Uh9Uf0'

                # Local(테스트) 환경 - https 요청이 필요없고, API Key가 따로 필요하지 않지만 횟수에 제약이 있습니다.
                # URL = 'http://maps.googleapis.com/maps/api/geocode/json?sensor=false&language=ko&address={}'.format(location)
                # Production(실제 서비스) 환경 - https 요청이 필수이고, API Key 발급(사용설정) 및 과금 설정이 반드시 필요합니다.
                URL = 'https://maps.googleapis.com/maps/api/geocode/json?key=' + apikey + '&sensor=false&language=ko&address={}'.format(
                    addr)
                # JSON 파싱. 여기서 response에러가 난다면 곧바로 except로.
                try:
                    # URL로 보낸 Requst의 Response를 response 변수에 할당
                    # 약 2000건
                    response = requests.get(URL)
                    data = response.json()
                    # lat, lon 추출
                    lat = data['results'][0]['geometry']['location']['lat']
                    lng = data['results'][0]['geometry']['location']['lng']
                    result = lat, lng
                except:
                    # 약 100여건
                    # 구글 api까지 거른 용자들. 그냥 (0,0)처리하자.
                    result = (0, 0)

        else:
            # 0건
            result = (0, 0)
    except TypeError as e:
        # 약 200여건
        result = (0, 0)

    return result


def cafe_data():
    # 원본 데이터 출처
    ## 서울특별시 휴게음식점 인허가 정보 : https://data.seoul.go.kr/dataList/OA-16095/S/1/datasetView.do

    # 데이터 자체의 카테고리 분류는 신뢰도가 낮아 밑에서 별도의 수작업 분류를 병행하였음

    ### 좌표 재구축을 위한 api 가동이 2~3시간 걸림. 따라서 좌표 재구축을 완료한 시점에서 1차 중간저장을 시행하며, 중간 저장물이 있다면 이후부터 시작, 없다면 처음부터 시작하게한다.
    try :
        df2 = pd.read_csv('./after/seoul_coffee2.csv', encoding='cp949')
    except :
        # 1차 정리 파일이 없어서 에러 - 처음부터 전처리 과정 실행.
        ## 1차 정리 - 불량, 아예 안 쓰일 칼럼 정리
        cafe = pd.read_csv('./origin/서울특별시 휴게음식점 인허가 정보.csv', encoding='cp949')
        cafe1 = cafe.drop([
            '개방자치단체코드', '관리번호', '영업상태코드', '휴업시작일자', '휴업종료일자', '재개업일자', '전화번호', '소재지우편번호', '도로명우편번호', '최종수정일자', '데이터갱신구분',
            '데이터갱신일자', '위생업태명', '남성종사자수', '여성종사자수', '영업장주변구분명', '등급구분명', '급수시설구분명', '총인원', '본사종업원수', '공장사무직종업원수',
            '공장판매직종업원수', '공장생산직종업원수', '건물소유구분명', '보증액', '월세액', '다중이용업소여부', '시설총규모', '전통업소지정번호', '전통업소주된음식', '홈페이지'
        ], axis=1)

        ## 1차 데이터 저장. 확인완료.
        # cafe1.to_csv('./after/cafe_data1.csv', index = False, encoding='cp949')
        df1 = cafe1[cafe1['업태구분명'] == '커피숍']
        df2 = cafe1[cafe1['업태구분명'] == '다방']
        df3 = cafe1[cafe1['업태구분명'] == '기타 휴게음식점']

        # df3에서 잘못분류된 카페 찾기, 데이터 처리
        # 원본 데이터의 업태 분류가 기준없이 뒤죽박죽으로 처리되어있어 그대로 쓰기엔 신뢰도가 너무나 낮다
        # 사업장명에 '커피','카페','cafe'이 포함된 데이터 체크
        c1 = df3[df3['사업장명'].str.contains('커피')]
        c2 = df3[df3['사업장명'].str.contains('카페')]
        c3 = df3[df3['사업장명'].str.contains('cafe')]
        # 사업장명에 카페 프렌차이즈명이 포함되어 있는 데이터 체크. 항목별로 0~20개 남짓의 데이터 확인됨.
        c4 = df3[df3['사업장명'].str.contains('스타벅스')]
        c5 = df3[df3['사업장명'].str.contains('투썸')]
        c6 = df3[df3['사업장명'].str.contains('메가엠지씨')]
        c7 = df3[df3['사업장명'].str.contains('메가MGC')]
        c8 = df3[df3['사업장명'].str.contains('이디야')]
        c9 = df3[df3['사업장명'].str.contains('빽다방')]
        c10 = df3[df3['사업장명'].str.contains('폴바셋')]
        c11 = df3[df3['사업장명'].str.contains('할리스')]
        c12 = df3[df3['사업장명'].str.contains('커피빈')]
        c13 = df3[df3['사업장명'].str.contains('엔제리너스')]
        c14 = df3[df3['사업장명'].str.contains('파스쿠찌')]
        c15 = df3[df3['사업장명'].str.contains('커피나무')]
        c16 = df3[df3['사업장명'].str.contains('커피베이')]
        c17 = df3[df3['사업장명'].str.contains('탐앤탐스')]
        c18 = df3[df3['사업장명'].str.contains('카페베네')]
        c19 = df3[df3['사업장명'].str.contains('더착한커피')]
        c20 = df3[df3['사업장명'].str.contains('만랩커피')]
        c21 = df3[df3['사업장명'].str.contains('달콤커피')]
        c22 = df3[df3['사업장명'].str.contains('커피에반하다')]
        c23 = df3[df3['사업장명'].str.contains('셀렉토')]
        c24 = df3[df3['사업장명'].str.contains('매머드')]
        c25 = df3[df3['사업장명'].str.contains('드롭탑')]
        c26 = df3[df3['사업장명'].str.contains('명가커피')]
        c27 = df3[df3['사업장명'].str.contains('커피스미스')]
        c28 = df3[df3['사업장명'].str.contains('커피마마')]
        c29 = df3[df3['사업장명'].str.contains('토프레소')]
        c30 = df3[df3['사업장명'].str.contains('전광수커피')]
        c31 = df3[df3['사업장명'].str.contains('빈스빈스')]
        c32 = df3[df3['사업장명'].str.contains('더카페')]
        c33 = df3[df3['사업장명'].str.contains('그라찌에')]
        c34 = df3[df3['사업장명'].str.contains('카페보니또')]

        # 위의 뽑아놓은 행들 전부 일괄 수취
        d1 = pd.concat(
            [c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24,
             c25, c26, c27, c28, c29, c30, c31, c32, c33, c34], axis=0)

        # 중복된 열 제거
        d1 = d1.drop_duplicates()

        # 업태구분명(커피숍+다방+기타휴게음식점) 합치기
        # 약 48000여개 행.
        df = pd.concat([df1, df2, d1], axis=0)

        ## 중간중간, 불량 데이터 필터링하는 함수 정의. 후 사용
        # blacklist.txt에 필터링할 어휘들 저장.
        f = open("blacklist.txt", 'r', encoding='utf8')
        lines = f.readlines()
        for line in lines:
            line = line.strip()
            select_index = df[df['사업장명'].str.contains(line)].index
            df = df.drop(index=select_index)
        f.close()

        # 지번 주소를 베이스로 카카오 주소 API를 활용, 기존의 좌표 데이터를 위경도 데이터로 변환.
        # 관계없는 업종명으로 정리된 데이터 복사해 원본 보존.
        df2 = df
        long_li = []
        lat_li = []

        for i in df2['지번주소']:
            li_li = kagoo_address_xy(i)
            long_li.append(li_li[1])
            lat_li.append(li_li[0])

        # 기존의 수정 되느라 뒤섞인 인덱스 리셋, 정리.
        df2.reset_index(drop=True, inplace=True)

        # 좌표군 리스트를 시리즈화 시키고 열에 삽입.
        df2['long'] = pd.Series(long_li)
        df2['lat'] = pd.Series(lat_li)

        # 변환 거친 뒤 기존의 좌표정보 칼럼 삭제
        df2.drop(['좌표정보(X)'], axis=1, inplace=True)
        df2.drop(['좌표정보(Y)'], axis=1, inplace=True)


        ## 여기까지 좌표 변환 완료
        df2.to_csv('./after/seoul_coffee2.csv', index=False, encoding='cp949')

    finally :
        # 1차 처리 전처리 파일 오픈 성공.
        # 주소 변환후 좌표가 (0,0)인 행들 결측치 처리. 삭제
        df_drop = df2[df2['long'] == 0]
        df2 = df2.drop(index=df_drop.index)

        ## 프렌차이즈 작업 전 데이터셋 정리
        # 상세영업상태코드를 영업1 폐업0으로 변환
        df2['상세영업상태코드'] = df2['상세영업상태코드'].map({2: 0, 1: 1})

        df2.rename(columns={'상세영업상태코드': '영업상태코드'}, inplace=True)

        # 폐업일자 빈칸인곳에 NaN
        df2['폐업일자'] = df2['폐업일자'].replace(np.nan, 0)

        # 도로명 주소 제거. 지번주소만 남김
        df2.drop(['도로명주소'], axis=1, inplace=True)

        # 인허가 취소 칼럼 삭제
        df2.drop(['인허가취소일자'], axis=1, inplace=True)

        # 비슷한 역할 하는 영업상태명 관련 칼럼 정리
        df2.drop(['영업상태명', '상세영업상태명'], axis=1, inplace=True)

        ## 프렌차이즈 확인 칼럼 생성
        # 기본 메뉴인 아메리카노 한잔의 가격을 기준으로, 3500이상이면 고가 프렌차이즈, 미만이면 저가 프렌차이즈로 구분.
        # 조사 대상이 된 프렌차이즈는 한국 30대 프렌차이즈를 대상으로 함.
        # 최종적으로 0 : 저가형 프렌차이즈, 1 : 고가형 프렌차이즈, 2 : 기타, 개인카페 으로 라벨링.
        fran_high = ['스타벅스', '투썸', '폴바셋', '할리스', '커피빈', '엔제리너스', '파스쿠찌', '탐앤탐스', '카페베네', '달콤커피', '드롭탑', '명가커피', '커피스미스',
                      '전광수커피', '빈스빈스', '카페보니또']
        fran_low = ['메가MGC', '메가엠지씨', '이디야', '빽다방','커피나무', '커피베이', '더착한커피', '만랩커피', '커피에반하다', '셀렉토', '메머드', '커피마마',
                     '토프레소', '더카페', '그라찌에']

        # 상호일치 여부 검색용 스트링 지정
        high = '|'.join(fran_high)
        low = '|'.join(fran_low)
        df2['fhigh'] = df2['사업장명'].str.contains(high)
        df2['flow'] = df2['사업장명'].str.contains(low)

        df2['fhigh'] = df2['fhigh'].map({True: 2, False: 0}, na_action=None)
        df2['flow'] = df2['flow'].map({True: 1, False: 0}, na_action=None)
        df2['franchise'] = df2['fhigh'] + df2['flow']
        # 남아있던 카페는 기타로 분류, 저가 1은 0으로 고가 2는 1로 조정.
        df2['franchise'] = df2['franchise'].map({0: 2, 2: 1, 1: 0})

        # 임시로 썼던 칼럼 제거
        df2.drop(['fhigh'], axis=1, inplace=True)
        df2.drop(['flow'], axis=1, inplace=True)

        # 카페 면적은 분포도 50%인 52.5와 면적 2000이상의 이상치를 제거한 평균값 69.7818 중 분포도 중간값을 기준으로 빈칸 정리.
        df2[df2['소재지면적'] < 0.1 ] =  52.5
        df2[df2['소재지면적'] > 2000] = 52.5
        df2['소재지면적'] = df2['소재지면적'].fillna('52.5')

        df2.to_csv('./after/cafe_data.csv', index=False, encoding='cp949')


def culture_data():
    #
    # 원 데이터셋 출처
    # http://data.seoul.go.kr/dataList/OA-15487/S/1/datasetView.do
    df = pd.read_csv('./origin/서울시 문화공간 정보.csv', encoding='cp949')

    df = df.drop("번호", axis=1)
    df = df.drop("전화번호", axis=1)
    df = df.drop("팩스번호", axis=1)
    df = df.drop("홈페이지", axis=1)
    df = df.drop("관람시간", axis=1)
    df = df.drop("관람료", axis=1)
    df = df.drop("휴관일", axis=1)
    df = df.drop("개관일자", axis=1)
    df = df.drop("객석수", axis=1)
    df = df.drop("대표이미지", axis=1)
    df = df.drop("기타사항", axis=1)
    df = df.drop("시설소개", axis=1)
    df = df.drop("무료구분", axis=1)
    df = df.drop("지하철", axis=1)
    df = df.drop("버스정거장", axis=1)
    df = df.drop("YELLOW", axis=1)
    df = df.drop("GREEN", axis=1)
    df = df.drop("BLUE", axis=1)
    df = df.drop("RED", axis=1)
    df = df.drop("공항버스", axis=1)

    # 기타          187
    # 공연장        162
    # 미술관        122
    # 박물관/기념관  115
    # 도서관        108
    # 문화원        35
    # 문화예술회관   31
    df.to_csv('./after/culture_data.csv', index=False, encoding='cp949')


def xy_trans(x, y):
    # 좌표체계 바꾸는 함수.
    # 좌표계 종류가 다르면 수정 혹은 재지정 해줘야 한다.
    # 중부원점(Bessel): 서울 등 중부지역 EPSG:5181
    proj_1 = Proj(init='epsg:5181')

    # WGS84 경위도: GPS가 사용하는 좌표계 EPSG:4326
    proj_2 = Proj(init='epsg:4326')

    x_, y_ = transform(proj_1, proj_2, x, y)
    return (x_, y_)


def area_data():
    # 원본 데이터 출처
    # https://data.seoul.go.kr/dataList/OA-15560/S/1/datasetView.doa
    df = pd.read_csv('./origin/서울시 우리마을가게 상권분석서비스(상권영역).csv', encoding='cp949')

    # 데이터 프레임 복사
    DataFrame = df.copy()

    # 새 좌표 넣을 빈 리스트 생성
    x_list = []
    y_list = []

    # 변환 시작
    for idx, row in DataFrame.iterrows():
        x, y = row['엑스좌표_값'], row['와이좌표_값']
        x_, y_ = xy_trans(x, y)
        x_list.append(x_)
        y_list.append(y_)

    df['lon'] = x_list
    df['lat'] = y_list

    df = df.drop("기준_년월_코드", axis=1)
    df = df.drop("엑스좌표_값", axis=1)
    df = df.drop("와이좌표_값", axis=1)
    df = df.drop("시군구_코드", axis=1)
    df = df.drop("행정동_코드", axis=1)
    df = df.drop("형태정보", axis=1)


    # 상권_구분_코드,	상권_구분_코드_명,	상권_코드,  상권_코드_명,    lon,   lat

    df.to_csv('./after/area_data.csv', index=False, encoding='cp949')
