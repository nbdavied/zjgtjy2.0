import json
import requests
import time
from bs4 import BeautifulSoup
import mysql.connector

YDLX = {
    "GYYD":"工业用地",
    "JYXYD":"经营性用地",
    "SYYD":"商业用地",
    "SZYD":"商住用地",
    "ZZYD":"住宅用地",
    "GSYYD":"工、商业用地",
    "ZHYD":"综合用地",
    "CCYD":"仓储用地",
    "QTYD":"其他用地",
}

LIST_URL = 'https://td.zjgtjy.cn:8553/devops/landBidding/queryLandBidding?pageSize=%d&pageNumber=%d&GGFBSJ_END=%d&sortField=BZJJZSJ&sortWay=DESC&regionCode=%s&date=%d'
RESOURCE_URL = 'https://td.zjgtjy.cn:8553/devops/landHangoutBidding/getResource?resourceId=%s&date=%d'
REGION_CODE = '330200,330201,330203,330205,330211,330206,330212,330283,330281,330282,330226,330225'
PAGE_SIZE = 50
CONFIG = {}
def loadConfig():
    with open('config.txt', encoding='utf-8') as configFile:
        configs = configFile.readlines()
        for config in configs:
            config = config.strip()
            if(config.strip[0] == '#'):
                continue
            if(config[-1] == '\n'):
                config = config[:-1]
            key_value = config.split('=')
            key = key_value[0].strip()
            value = key_value[1].strip()
            CONFIG[key] = value
def getDbConnection():
    return mysql.connector.connect(user=CONFIG['db_user'],
                                    password=CONFIG['db_passwd'],
                                    host=CONFIG['db_host'],
                                    database=CONFIG['db_database'])
def getLandList(pageNumber):
    ts = int(round(time.time() * 1000))
    url = LIST_URL % (PAGE_SIZE, pageNumber, ts, REGION_CODE, int(ts / 1000) * 1000)
    print(url)
    response = requests.get(url)
    text = response.text
    resObj = json.loads(text)
    return resObj['data']


def getResourceInfo(resourceId):
    ts = int(time.time()) * 1000
    url = RESOURCE_URL % (resourceId, ts)
    response = requests.get(url)
    resObj = json.loads(response.text)
    return resObj['data']

def readResourceInfo(data):
    pass



if __name__ == '__main__':
    loadConfig()
    for i in range(1, int(CONFIG['end_page']) + 1):
        landList = getLandList(i)
        for land in landList:
            print("土地编号:", land['ZYBH'])
            landInfo = getResourceInfo(land['ZYID'])
            

    
