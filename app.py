from flask import Flask, g, request
from py2neo import Graph, Node, Relationship
from flask_cors import CORS
import os

import requests
import json


API_KEY = "MKhOJFCcCXA2VYK5BiaJceZ18wCDtOHjzufAuNTT"

# 데이터베이스를 연결하는 부분
url = os.getenv("NEO4J_URI", "bolt://localhost:7687")
username = os.getenv("NEO4J_USER", "neo4j")
password = os.getenv("NEO4J_PASSWORD", "edacom")
neo4jVersion = os.getenv("NEO4J_VERSION", "4")
database = os.getenv("NEO4J_DATABASE", "edacom")

gdb = Graph(url, auth=("neo4j", password))

## solve the Cross-Origin Resource Sharing(CORS)
app = Flask(__name__)
cors = CORS()
CORS(app, resources={r'/*': {'origins': '*'}})

def get_db():
    if not hasattr(g, 'neo4j_db'):
        if neo4jVersion.startswith("4"):
            g.neo4j_db = gdb
        else:
            g.neo4j_db = gdb
    return g.neo4j_db

@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route("/api/pwatts")
def get_pwatts():
    # 웹페이지 요청에 의해서 전달된 파라미터를 입력하는 부분임
    lat = request.args.get('lat')
    lon = request.args.get('lon')
    system_capacity = request.args.get('system_capacity')
    azimuth = request.args.get('azimuth')
    tilt = request.args.get('tilt')
    array_type = request.args.get('array_type')
    module_type = request.args.get('module_type')
    losses = request.args.get('losses')

    if lat is not None:
        print(lat)

    f = open('cities.json', 'rt', encoding='UTF8')
    cities = json.load(f)

    URL_PVWATTS6 = f"https://developer.nrel.gov/api/pvwatts/v6.json?"
    for c in cities:
        params = {'api_key': API_KEY,
                  # 'address': , The address to use. Required if lat/lon or file_id not specified.
                  # climate dataset: NREL Physical Solar Model (PSM) TMY from the NREL National Solar Radiation Database (NSRDB)
                  # 'address': 'KWANGJU, REPUBLIC OF KOREA',
                  'lat': c['lat'],
                  'lon': c['lng'],
                  'dataset': 'intl',
                  'system_capacity': 4,  # Nameplate capacity (kW). 지정된 위치의 태양관 판의 크기라고 이야기할 수 있음
                  'azimuth': 180,
                  'tilt': 40,
                  'array_type': 1,
                  # 0: Fixed - Open rack, 1: Fixed - Roof Mounted 2: 1-Axis, 3: 1-Axis Backtracking 4: 2-Axis
                  'module_type': 1,  # Module type. 0: Standard, 1: Premium, 2: Thin film
                  'losses': 10  # System losses (percent).
                  }

        response = requests.get(URL_PVWATTS6, params=params)
        json_obj = response.json()

        # print(json.dumps(json_obj, indent=4, sort_keys=True))
        # 데이터베이스에 저장하면 될듯
        put_graph1(json_obj, c['name'], c['country'])

    return json.dumps(json_obj['outputs'], indent=4)

def put_graph1(json_obj, city, country):
    conn = get_db()

    ## 트랙젝션 생성(한번에 저장했다가 삽입)
    tx = conn.begin()

    # 노드에 대한 변수 정의
    STATION = "Station"
    PVSYSTEM = "PVSystem"
    ENERGY_ESTIMATE = "EnergyEstimate"
    COUNTRY = "Country"
    CITY = "City"

    # 관계에 대한 정의
    HAS_SYSTEM = Relationship.type("HAS_SYSTEM")
    HAS_ENERGY_ESTIMATE = Relationship.type("HAS_ENERGY_ESTIMATE")
    HAS_CITY = Relationship.type("HAS_CITY")
    HAS_STATION = Relationship.type("HAS_STATION")

    # 스테이션에 대한 정보 생성
    json_st = json_obj['inputs']
    n_country = Node(COUNTRY, name=country)
    tx.merge(n_country, COUNTRY, "name")
    n_city = Node(CITY, name=city)
    tx.merge(n_city, CITY, "name")
    tx.merge(HAS_CITY(n_country, n_city), CITY, "name")

    n_station = Node(STATION, name=f"{country}-{city} station", lat=json_st['lat'], lon=json_st['lon'])
    tx.merge(HAS_STATION(n_city, n_station), STATION, "name")
    n_pvsystem = Node(PVSYSTEM, name=f"{city} PV system", array_type=json_st['array_type'], azimuth=json_st['azimuth'], losses=json_st['losses'], module_type=json_st['module_type'], system_capacity=json_st['system_capacity'], tilt=json_st['tilt'])
    # 트랙젝션에 노드 와 관련된 관계를 추가
    tx.merge(n_station, STATION, "name" )
    tx.merge(n_pvsystem, PVSYSTEM, "name")
    tx.merge(HAS_SYSTEM(n_station, n_pvsystem), STATION, "name")

    # 통계치에 대한 정보 생성
    try:
        json_est = json_obj['outputs']
        n_energy_estimate = Node(ENERGY_ESTIMATE, name=f"{country}{city} estimates", ac_annual=json_est['ac_annual'], ac_monthly=json_est['ac_monthly'], capacity_factor=json_est['capacity_factor'], dc_monthly=json_est['dc_monthly'], poa_monthly=json_est['poa_monthly'], solrad_annual=json_est['solrad_annual'], solrad_monthly=json_est['solrad_annual'])
        tx.create(HAS_ENERGY_ESTIMATE(n_station, n_energy_estimate))
    except:
        pass

    # 트렉젝션을 그래프 데이터베이스에 반영
    tx.commit()
    print(f"{city} in {country} is added")

if __name__ == '__main__':
    app.run()
