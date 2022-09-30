import json
import requests
import time
from bs4 import BeautifulSoup
import mysql.connector
from datetime import datetime
from pytz import timezone
import base

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
    # 拍卖开始时间
    land['pmkssj'] = localizeTime(getFromDist(data,'PMKSSJ'))
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
    'lhl_s','xg_x','xg_s','sfzl','zymc','tdytms', 'jyfs','pmkssj']
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
    # print(sql)
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
def deleteByZjid(conn, zyid):
    sql = "delete from zjgtjy where zyid = '%s'" % zyid
    cursor = conn.cursor()
    cursor.execute(sql)
    cursor.close()

def refreshLandInfo(conn):
    ### refresh lands that are not finished
    sql = "select zyid, zyjd, jyfs from zjgtjy z where zyjd  in ('GGQ','GPQ', 'PMGGQ', 'PMXWQ', 'PMJJQ','XCYH')"
    cursor = conn.cursor()
    cursor.execute(sql)
    for row in cursor.fetchall():
        # print(row)
        zyid = row[0]
        jyfs = row[-1]
        landData = None
        if jyfs == 'GP':
            landData = getGpResourceInfo(zyid)
        else:
            landData = getPmResourceInfo(zyid)
        if(landData['ZYJD'] != row[1]):
            print(zyid, ' status updated')
            landInfo = readResourceInfo(landData)
            landInfo['jyfs'] = jyfs
            deleteByZjid(conn,zyid)
            insertZjgtjy(conn, landInfo)

def generateFinalData(conn):
    sql = "drop table zjgtjy_dist"
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    sql = r"""create table zjgtjy_dist
select case when z.district  in ('330203',
'330205',
'330206',
'330211',
'330212','330283', '330213','330201') then '1' else '0' END as '市六区',
ghyt as '用途',
zt.ztmc as '状态',zyid, 
d.name as '行政区划',
zymc as '资源名称',  
zybh as '资源编号', 
zywz as '位置' ,  date_format(ggfbsj, '%Y-%m-%d') as '公告发布时间',  
case when jyfs = 'GP' then date_format(gpjssj, '%Y-%m-%d') else date_format(pmkssj, '%Y-%m-%d')  end as '交易时间', 
crmj as '出让面积',rjl_s as '容积率', crmj * rjl_s as '建筑面积',  bzj as '保证金',qsj as '起始价',   round(qsj / ( crmj * rjl_s) * 10000, 2) as '起始单价',cjj as '成交价',round(cjj/(crmj * rjl_s )*10000,2) as '成交单价', 
jddw as '竞得单位',concat(round((cjj-qsj)/qsj *100,1), '%')  as '溢价率', date_format(jssj , '%Y-%m-%d')  as '成交时间'  from zjgtjy z
left join district d on z.xzqbm  = d.code 
left join zjzt zt on z.zyjd = zt.zyjd"""
    cursor.execute(sql)
    conn.commit()
    cursor.close()

def run(config):
    conn = base.getDbConnection(config)
    refreshLandInfo(conn)
    for i in range(int(config['start_page']), int(config['end_page']) + 1):
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
                landInfo['jyfs'] = land['JYFS']
                # print(landInfo)
                insertZjgtjy(conn, landInfo)
    generateFinalData(conn)
    conn.close()

if __name__ == '__main__':
    config = base.loadConfig()
    run(config)

            

    
