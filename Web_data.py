import pandas as pd
import Functions

#웹 서비스 구현을 위한 db저장 전용 데이터셋 생성

# df_cafe = pd.read_csv('./after/cafe_data.csv', encoding='cp949')
# df_traffic = pd.read_csv('./after/traffic_data.csv', encoding='cp949')
# df_population = pd.read_csv('./after/population_data.csv', encoding='cp949')
# df_area = pd.read_csv('./after/area_data.csv', encoding='cp949')
# df_culture = pd.read_csv('./after/culture_data.csv', encoding='cp949')
# df_main = pd.read_csv('./after/main_data.csv.csv', encoding='cp949')

def near_cafe_db(long, lat):
    # near_cafe

    # 반경 200미터 내에 존재하는 카페의 수를 value값으로 리턴
    df_main = pd.read_csv('./after/cafe_data.csv', encoding='cp949')
    df = df_main[['long', 'lat','영업상태코드']]
    df = df[df['영업상태코드'] == 1]

    count = 0
    stdlong = long
    stdlat = lat

    df2 = df.iloc[:][df['long'] > stdlong - 0.0018]
    df2 = df2.iloc[:][df['long'] < stdlong + 0.0018]
    df2 = df2.iloc[:][df['lat'] > stdlat - 0.0018]
    df2 = df2.iloc[:][df['lat'] < stdlat + 0.0018]
    df3 = df2
    df3.reset_index(drop=True, inplace=True)

    for i in range(len(df3.index)):
        objlong = df3['long'][i]
        objlat = df3['lat'][i]
        dis = Functions.cal_distance(stdlong, stdlat, objlong, objlat)
        if dis < 200:
            count = count + 1

    return count



def near_subway_db(long, lat):
    # near_subway100
    # near_subway600
    # near_subway_commute
    # near_subway_transfer
    # near_subway_many

    # 반경 600미터 내에 존재하는 역의 수를 value값으로 리턴
    df_traffic = pd.read_csv('./after/traffic_data.csv', encoding='cp949')
    df = df_traffic[['long', 'lat', 'coms', 'many75', 'trans']]
    # 지하철역 한정
    df = df[df['code'] >= 30000]
    count600 = 0
    count100 = 0
    com = 0
    trans = 0
    many = 0

    stdlong = long
    stdlat = lat
    df2 = df.iloc[:][df['long'] > stdlong - 0.0054]
    df2 = df2.iloc[:][df['long'] < stdlong + 0.0054]
    df2 = df2.iloc[:][df['lat'] > stdlat - 0.0054]
    df2 = df2.iloc[:][df['lat'] < stdlat + 0.0054]
    df3 = df2
    df3.reset_index(drop=True, inplace=True)

    for i in range(len(df2.index)):
        objlong = df3['long'][i]
        objlat = df3['lat'][i]
        dis = Functions.cal_distance(stdlong, stdlat, objlong, objlat)
        if dis < 600:
            count600 = count600 + 1
            com = com + df3['coms'][i]
            trans = trans + df3['trans'][i]
            many = many + df3['many75'][i]
            if dis < 100 :
                count100 = count100 + 1

    return count100, count600, com, trans, many



def near_bus_db(long, lat):
    # near_bus
    # near_bus_commute
    # near_bus_transfer
    # near_bus_many

    # 반경 200미터 내에 존재하는 정류장의 수를 value값으로 리턴
    df_traffic = pd.read_csv('./after/traffic_data.csv', encoding='cp949')
    df = df_traffic[['long', 'lat', 'coms', 'many75', 'trans']]
    # 버스 정류장 한정
    df = df[df['code'] < 30000]
    count = 0
    com = 0
    trans = 0
    many = 0

    stdlong = long
    stdlat = lat
    df2 = df.iloc[:][df['long'] > stdlong - 0.0018]
    df2 = df2.iloc[:][df['long'] < stdlong + 0.0018]
    df2 = df2.iloc[:][df['lat'] > stdlat - 0.0018]
    df2 = df2.iloc[:][df['lat'] < stdlat + 0.0018]
    df3 = df2
    df3.reset_index(drop=True, inplace=True)

    for i in range(len(df2.index)):
        objlong = df3['long'][i]
        objlat = df3['lat'][i]
        dis = Functions.cal_distance(stdlong, stdlat, objlong, objlat)
        if dis < 200:
            count = count + 1
            com = com + df3['coms'][i]
            trans = trans + df3['trans'][i]
            many = many + df3['many75'][i]

    return count, com, trans, many



def gu_work_db(long, lat):
    # gu_work
    # gu_rate_2030 : 인구 중 20, 30대 비율
    # gu_rate_405060
    # gu_density

    # 받아온 좌표를 베이스로 소속 구 이름 받아오기
    gu = Functions.gu_finder(long, lat)
    df_population = pd.read_csv('./after/population_data.csv', encoding='cp949')
    # 해당 구의 이름과 일치하는 행 불러오기
    df = df_population[['gu', 'total worker', 'density', '20-29', '30-39', '40-49', '50-59', '60-69', 'Totalpop']]
    df1 = df[df['gu'] == gu]

    workers = df1['total worker'].values[0]
    rate20 = (df1['20-29'].values[0] + df1['30-39'].values[0]) / df1['Totalpop'].values[0]
    rate40 = (df1['40-49'].values[0] + df1['50-59'].values[0] + df1['60-69'].values[0]) / df1['Totalpop'].values[0]
    dens = df1['density'].values[0]

    return workers, rate20, rate40, dens



def near_culture_db(long, lat):
    # near_culture

    # 반경 1000미터 내에 존재하는 문화시설 수를 value값으로 리턴
    df_culture = pd.read_csv('./after/culture_data.csv', encoding='cp949')
    df = df_culture[['long', 'lat']]
    count = 0

    stdlong = long
    stdlat = lat
    df2 = df.iloc[:][df['long'] > stdlong - 0.009]
    df2 = df2.iloc[:][df['long'] < stdlong + 0.009]
    df2 = df2.iloc[:][df['lat'] > stdlat - 0.009]
    df2 = df2.iloc[:][df['lat'] < stdlat + 0.009]
    df3 = df2
    df3.reset_index(drop=True, inplace=True)

    for i in range(len(df2.index)):
        objlong = df3['long'][i]
        objlat = df3['lat'][i]
        dis = Functions.cal_distance(stdlong, stdlat, objlong, objlat)
        if dis < 1000:
            count = count + 1

    return count


def area_cafe_db(long, lat):
    # area_cafe
    # area_avgTake
    # area_avgCustomer
    # area_count
    df_area = pd.read_csv('./after/area_data.csv', encoding='cp949')

    # 기본 변수 설정
    temp_dis = 300
    bal = 0
    gol = 0
    gi = 0
    no = 0
    area_count = 0

    # 입력받은 좌표를 기준점으로 설정
    stdlong = long
    stdlat = lat

    # 250m 정사각형 영역 내의 데이터 추출
    df2 = df_area.iloc[:][df_area['lon'] > stdlong - 0.00225]
    df2 = df2.iloc[:][df_area['lon'] < stdlong + 0.00225]
    df2 = df2.iloc[:][df_area['lat'] > stdlat - 0.00225]
    df2 = df2.iloc[:][df_area['lat'] < stdlat + 0.00225]
    df3 = df2
    df3.reset_index(drop=True, inplace=True)

    # 영역 내의 점포들 대상으로 거리 계산&비교
    for i in range(len(df3.index)):
        objlong = df3['lon'][i]
        objlat = df3['lat'][i]

        dis = Functions.cal_distance(stdlong, stdlat, objlong, objlat)
        # 거리를 받아올때마다 250m 이내인지 비교
        if dis < 250:
            # 해당되는 표본이 들어올때마다 비교하여 최소값 갱신. 최소값일때의 코드값 저장하여 상권정보 저장.
            area_count = area_count + 1
                
            # 마지막에 남은 상권정보가 점포에서 가장 가까운 상권.
            if dis < temp_dis:
                temp_dis = dis
                temp_code = df3['code'][i]

    # temp_code = 가장 가까운 상권의 코드

    if area_count == 0 :
        # 변동없음 = 250m이내의 상권이 하나도 없었음.
        no = 1
    else:
        # 검색된 상권 있음
        if df3[df3['code'] == temp_code]['area_type'].values[0] == '발달상권':
            bal = 1
        elif df3[df3['code'] == temp_code]['area_type'].values[0] == '골목상권':
            gol = 1
        else:
            gi = 1

    area_cafe = df3[df3['code'] == temp_code]['area_store'].values[0]
    area_avgTake = df3[df3['code'] == temp_code]['avg_take'].values[0]
    area_avgCustomer = df3[df3['code'] == temp_code]['tot_customer'].values[0]

    # 모든 탐색, 비교가 끝나고 거리가 가장 가까운 1개 상권의 코드가 temp_code에 저장됨

    # 4번째 기능은 API로 긁어오는걸로 하고 분석용 정보는 다른 데이터셋과 동일하게 처리한다

    return bal, gol, gi, no, area_cafe, area_avgTake, area_avgCustomer, area_count, temp_code

