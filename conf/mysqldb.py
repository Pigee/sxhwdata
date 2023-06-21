import pymysql

def Callconn():
    db_quarkcalldb = pymysql.connect( # 创建数据库连接
    host='192.168.199.100', # 要连接的数据库所在主机ip
    #host='192.168.30.83', # 要连接的数据库所在主机ip
    port=3306, # 数据库端口
    database='quarkcalldb54',
    user='admin', # 数据库登录用户名
    password='xxxxxxxxx', # 登录用户密码
    charset='utf8mb4' # 编码，注意不能写成utf-8
    #cursorclass=cursors.DictCursor
    )
    return db_quarkcalldb

def Csconn():
    db_cs = pymysql.connect( # 创建数据库连接
    host='192.168.30.71', # 要连接的数据库所在主机ip
    port=3306, # 数据库端口
    #database='cs_y_run_nxsw_liupanshan',
    user='admin', # 数据库登录用户名
    password='xxxxxxxxx', # 登录用户密码
    charset='utf8mb4' # 编码，注意不能写成utf-8
    #cursorclass=cursors.DictCursor
    )
    return db_cs
