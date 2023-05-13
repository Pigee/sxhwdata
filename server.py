from sanic import Sanic
from sanic.response import text
from sanic.response import json
import pymysql

csdb = pymysql.connect( # 创建数据库连接
    host='192.168.1.11', # 要连接的数据库所在主机ip
    port=3306, # 数据库端口
    database='cs_y_run_nxsw',
    user='sxadmin', # 数据库登录用户名
    password='sx@123', # 登录用户密码
    charset='utf8mb4' # 编码，注意不能写成utf-8
    #cursorclass=cursors.DictCursor
)

cur = csdb.cursor(pymysql.cursors.DictCursor)

app = Sanic("sxhwdata")

@app.get("/")
async def hello_world(request):
    return text("Hello, world.")

# get 请求
@app.get("/j")
async def get_json(request):
    sql = "select id,concat(create_date,'')create_date,time_accept,repair,time_accept,time_receiver,time_handle,remark,gd_result,appraise,dz_address from w_1592374411638 where create_date >= '2023-03-01 00:00:00' and create_date <= '2023-04-25 23:59:59' and time_handle is null"
    cur.execute(sql)
    all_obj = cur.fetchall()
    return json(all_obj,ensure_ascii=False)

# post 请求
@app.post("/p")
async def post_json(request):
    print(request.json["start_date"])
    sql = "select id,concat(create_date,'')create_date,time_accept,repair,time_accept,time_receiver,time_handle,remark,gd_result,appraise,dz_address from w_1592374411638 where create_date >= %s and create_date <= %s and time_handle is null"
    cur.execute(sql,(request.json["start_date"],request.json["end_date"]))
    all_obj = cur.fetchall()
    return json(all_obj,ensure_ascii=False)

# 调用存储过程
@app.post("/proc")
async def proc_json(request):
    print(request.json["name"])
    sql = "call proc_t(%s)"
    cur.execute(sql,(request.json["name"]))
    all_obj = cur.fetchall()
    return json(all_obj,ensure_ascii=False)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=1337, dev=True)
