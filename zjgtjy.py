import json
import requests
import time
from bs4 import BeautifulSoup
import mysql.connector
from datetime import datetime
from pytz import timezone

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
RESOURCE_AUCTION_URL = 'https://td.zjgtjy.cn:8553/devops/landAuctionBidding/getResource?resourceId=%s&date=%d'
REGION_CODE = '330200,330201,330203,330205,330211,330206,330212,330283,330281,330282,330226,330225'
PAGE_SIZE = 50
CONFIG = {}
def loadConfig():
    with open('config.txt', encoding='utf-8') as configFile:
        configs = configFile.readlines()
        for config in configs:
            config = config.strip()
            if(config[0] == '#'):
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


def getGpResourceInfo(resourceId):
    ts = int(time.time()) * 1000
    url = RESOURCE_URL % (resourceId, ts)
    print(url)
    response = requests.get(url)
    resObj = json.loads(response.text)
    return resObj['data']

def getPmResourceInfo(resourceId):
    ts = int(time.time()) * 1000
    url = RESOURCE_AUCTION_URL % (resourceId, ts)
    print(url)
    response = requests.get(url)
    resObj = json.loads(response.text)
    return resObj['data']
def getFromDist(data, key):
    if key in data:
        return data[key]
    else:
        return ''
def readResourceInfo(data):
    land = {}
    # 资源id
    land['zyid'] = getFromDist(data, 'ZYID')
    # 资源编号
    land['zybh'] = getFromDist(data,'ZYBH')
    # 发布时间
    land['ggfbsj'] = localizeTime(getFromDist(data,'GGFBSJ'))
    # 结束时间
    land['ggjssj'] = localizeTime(getFromDist(data,'GGJSSJ'))
    # 挂牌开始时间
    land['gpkssj'] = localizeTime(getFromDist(data,'GPKSSJ'))
    # 挂牌结束时间
    land['gpjssj'] = localizeTime(getFromDist(data,'GPJSSJ'))
    # 保证金到帐截止时间
    land['bzjjzsj'] = localizeTime(getFromDist(data,'BZJJZSJ'))
    # 报名截止时间
    land['bmjssj'] = localizeTime(getFromDist(data,'BMJSSJ'))
    # 报名开始时间
    land['bmkssj'] = localizeTime(getFromDist(data,'BMKSSJ'))
    # 土地位置
    land['zywz'] = getFromDist(data,'ZYWZ')
    # 规划用途
    land['ghyt'] = parseGhyt(data['GHYT'])
    # 建筑面积
    land['jzmj'] = getFromDist(data,'JZMJ')
    # 行政区编号
    land['xzqbm'] = getFromDist(data,'XZQBM')
    # 地区编码
    land['district'] = getFromDist(data,'DISTRICT')
    # 用地类型
    land['ydlx'] = getFromDist(data,'YDLX')
    # 出让年限
    land['crnx'] = getFromDist(data,'CRNX')
    # 出让面积
    land['crmj'] = getFromDist(data,'CRMJ')
    # 出让面积（亩）
    land['crmjm'] = getFromDist(data,'CRMJM')
    # 起始价
    land['qsj'] = getFromDist(data,'QSJ')
    # 保证金
    land['bzj'] = getFromDist(data,'BZJ')
    # 增价幅度
    land['zjfd'] = getFromDist(data,'ZJFD')

    land['sffb'] = getFromDist(data,'SFFB')
    land['sfyxlhjm'] = getFromDist(data,'SFYXLHJM')
    land['sfyxnclxgs'] = getFromDist(data,'SFYXNCLXGS')
    # 建筑密度
    jzmd = parseRange(data['JZMD'], 'JZMD')
    land['jzmd_x'] = jzmd[0]
    land['jzmd_s'] = jzmd[1]
    land['sfytsyq'] = getFromDist(data,'SFYTSYQ')
    land['tsyqnr'] = getFromDist(data,'TSYQNR')
    land['cjfs'] = getFromDist(data,'CJFS')
    land['crxzfs'] = getFromDist(data,'CRXZFS')
    land['zyjd'] = getFromDist(data,'ZYJD')
    land['zyzt'] = getFromDist(data,'ZYZT')
    land['fbsj'] = getFromDist(data,'FBSJ')
    # 竞得单位
    land['jddw'] = getFromDist(data,'JDDW')
    # 结束时间
    land['jssj'] = localizeTime(getFromDist(data,'JSSJ'))
    # 成交价
    land['cjj'] = getFromDist(data,'CJJ')
    # 成交明细
    land['cjmx'] = getFromDist(data,'CJMX')
    # 容积率
    rjl = parseRange(data['RJL'], 'RJL')
    land['rjl_x'] = rjl[0]
    land['rjl_s'] = rjl[1]
    # 是否有底价
    land['sfydj'] = getFromDist(data,'SFYDJ')
    # 底价
    land['dj'] = getFromDist(data,'DJ')
    # 未成交原因
    land['jyjgms'] = getFromDist(data,'JYJGMS')
    # 绿化率
    lhl = parseRange(data['LHL'], 'LHL')
    land['lhl_x'] = lhl[0]
    land['lhl_s'] = lhl[1]
    # 限高
    xg = parseRange(data['XG'], 'XG')
    land['xg_x'] = xg[0]
    land['xg_s'] = xg[1]
    # 是否租赁
    land['sfzl'] = getFromDist(data,'SFZL')
    # 资源名称
    land['zymc'] = getFromDist(data,'ZYMC')
    land['tdytms'] = getFromDist(data,'TDYTMS')
    for key in land.keys():
        if(land[key] == None):
            land[key] = ''
    return land

def insertZjgtjy(conn, landInfo):
    columns = ['zyid','zybh','ggfbsj','ggjssj','gpkssj','gpjssj','bzjjzsj','bmjssj','bmkssj',
    'zywz','ghyt','jzmj','xzqbm','district','ydlx','crnx','crmj','crmjm','qsj','bzj','zjfd',
    'sffb','sfyxlhjm','sfyxnclxgs','jzmd_x','jzmd_s','sfytsyq','tsyqnr','cjfs','crxzfs','zyjd',
    'zyzt','fbsj','jddw','jssj','cjj','cjmx','rjl_x','rjl_s','sfydj','dj','jyjgms','lhl_x',
    'lhl_s','xg_x','xg_s','sfzl','zymc','tdytms']
    sql = "insert into zjgtjy ("
    for i in range(len(columns)):
        sql = sql + columns[i]
        if i != len(columns) - 1:
            sql = sql + ','
    sql = sql + ") values ("
    for i in range(len(columns)):
        value = landInfo[columns[i]]
        if not isinstance(value, str):
            value = str(value)
        if value == '':
            sql = sql + " null "
        else:
            sql = sql + "'" + value + "'"
        if i != len(columns) - 1:
            sql = sql + ','
    sql = sql + ")"
    print(sql)
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    cursor.close()

def parseGhyt(ghyt):
    ghytList = json.loads(ghyt)
    if len(ghytList) > 0:
        return ghytList[0]['NAME_']
    else:
        return ""

def parseRange(data, key):
    obj = json.loads(data)
    x = ''
    s = ''
    if(key + '_X' in obj):
        x = obj[key + '_X']
    if(key + '_S' in obj):
        s = obj[key + '_S']
    return (x,s)

def ifExistLand(conn, zyid):
    cursor = conn.cursor()
    sql = "select * from zjgtjy where zyid = '%s'" % zyid
    cursor.execute(sql)
    exist = False
    if cursor.fetchall():
        exist = True
    cursor.close()
    return exist

def localizeTime(utcTime):
    if utcTime == None or utcTime == '':
        return ''
    d = datetime.strptime(utcTime[:19],  '%Y-%m-%dT%H:%M:%S')
    utc = timezone('UTC')
    utc_d = utc.localize(d)
    loc_d = utc_d.astimezone(timezone('Asia/Shanghai'))
    s = str(loc_d)
    return s[:19]

if __name__ == '__main__':
    loadConfig()
    conn = getDbConnection()
    for i in range(1, int(CONFIG['end_page']) + 1):
        landList = getLandList(i)
        for land in landList:
            print("土地编号:", land['ZYBH'])
            if not ifExistLand(conn, land['ZYID']):
                print(land['ZYID'])
                landData = None
                if(land['JYFS'] == 'GP'):
                    landData = getGpResourceInfo(land['ZYID'])
                else:
                    landData = getPmResourceInfo(land['ZYID'])
                landInfo = readResourceInfo(landData)
                print(landInfo)
                insertZjgtjy(conn, landInfo)
    conn.close()

            

    
