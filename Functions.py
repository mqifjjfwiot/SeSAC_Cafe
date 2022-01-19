import requests; from urllib.parse import urlparse
# 위도 경도 거리 계산용
from haversine import haversine
# 카카오 api 위경도 변환용
import geopandas as gpd

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


def gu_finder(x, y):
    # GPS 좌표계 좌표를 입력하면 그곳이 어느 구인지 출력해주는 공공 API 함수.
    # 좌표는 숫자가 아닌 str로 입력해줘야 해서 URL입력시 두 변수를 str으로 형변환 시켜준다.
    # https://dev.vworld.kr/dev/v4dv_2ddataguide2_s002.do?svcIde=adsigg
    apikey = 'C86E1B98-0AD1-3040-BA11-E81EA4BC70BA'
    URL = 'http://api.vworld.kr/req/data?service=data&request=GetFeature&data=LT_C_ADSIGG_INFO&key=' + apikey + '&geomFilter=point(' + str(x) + ' ' + str(y) + ')&crs=EPSG:4326'
    r = requests.get(URL)
    if r.status_code == 200:
        data = r.json()
        # 시 까지 포함해서 출력하려면 ['sig_kor_nm'] 대신 ['full_nm']
        result = data['response']['result']['featureCollection']['features'][0]['properties']['sig_kor_nm']
    else :
        result = '가동 에러'
    return result


def cal_distance(x1,y1,x2,y2):
    ## WGS84 경위도 = EPSG:4326 하에서 좌표 0.0054 차이 = 약 600m
    ## 0.0018 차이 = 약 200m
    ## 0.0009 차이 = 약 100m

    ## 거리를 계산할때 전부 하면 미친놈이기 때문에, 반경과 같은 길이를 갖는 정사각형 영역내의 점들만 계산에 동원한다.
    # ex) 반경 200m내의 조사를 하고 싶다면 기준 좌표 + - 0.0018 내에 존재하는 좌표 소유자만 계산에 동원하는 방식.
    standard = (x1, y1)
    object = (x2, y2)

    distance = haversine(standard, object) * 1000
    return distance



def area_info(long, lat):
    pass
