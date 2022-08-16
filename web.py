import tornado.ioloop
import tornado.web
import mysql.connector
import base
import json

CONFIG = {}
class MainHandler(tornado.web.RequestHandler):
    def get(self):
        conn = base.getDbConnection(CONFIG)
        sql = r"""SELECT zyid, zybh, date_format(ggfbsj,'%Y-%m-%d %H:%i:%S') as ggfbsj, date_format(ggjssj,'%Y-%m-%d %H:%i:%S') as ggjssj,
date_format(gpkssj,'%Y-%m-%d %H:%i:%S') as gpkssj, date_format(gpjssj,'%Y-%m-%d %H:%i:%S') as gpjssj,
date_format(bzjjzsj,'%Y-%m-%d %H:%i:%S') as bzjjzsj, date_format(bmjssj,'%Y-%m-%d %H:%i:%S') as bmjssj,
date_format(bmkssj,'%Y-%m-%d %H:%i:%S') as bmkssj,
zywz, ghyt, jzmj, 
xzqbm, district, ydlx, crnx, crmj, crmjm, qsj, bzj, zjfd, sffb, sfyxlhjm, sfyxnclxgs, jzmd_x, 
jzmd_s, sfytsyq, tsyqnr, cjfs, crxzfs, zyjd, zyzt, fbsj, jddw, 
date_format(jssj,'%Y-%m-%d %H:%i:%S') as jssj,
cjj, cjmx, rjl_x, rjl_s, 
sfydj, dj, jyjgms, lhl_x, lhl_s, xg_x, xg_s, sfzl, zymc, tdytms
FROM zjgtjy.zjgtjy;"""
        cursor = conn.cursor()
        cursor.execute(sql)
        r = []
        for row in cursor.fetchall():
            d = dict((cursor.description[i][0], value) for i, value in enumerate(row))
            r.append(d)
        
        cursor.close()
        conn.close()
        data = json.dumps(r, ensure_ascii=False)
        # print(data)
        self.write(data)

def make_app():
    return tornado.web.Application([
            (r"/", MainHandler),
        ]
    )

if __name__ == "__main__":
    CONFIG = base.loadConfig()
    app = make_app()
    app.listen(8888)
    tornado.ioloop.IOLoop.current().start()
