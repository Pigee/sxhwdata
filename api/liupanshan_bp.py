from sanic.response import json
import pymysql
from sanic import Blueprint

lpsbp = Blueprint("liupanshan_bp", url_prefix="/liupanshan")

db_liupanshan = pymysql.connect( # 创建数据库连接
    host='192.168.1.11', # 要连接的数据库所在主机ip
    #host='192.168.199.100', # 要连接的数据库所在主机ip
    port=3306, # 数据库端口
    database='cs_y_run_nxsw',
    user='sxadmin', # 数据库登录用户名
    password='sx@123', # 登录用户密码
    charset='utf8mb4' # 编码，注意不能写成utf-8
    #cursorclass=cursors.DictCursor
)

db_quarkcalldb54 = pymysql.connect( # 创建数据库连接
    host='192.168.1.11', # 要连接的数据库所在主机ip
    #host='192.168.199.100', # 要连接的数据库所在主机ip
    port=3306, # 数据库端口
    database='quarkcalldb54',
    user='sxadmin', # 数据库登录用户名
    password='sx@123', # 登录用户密码
    charset='utf8mb4' # 编码，注意不能写成utf-8
    #cursorclass=cursors.DictCursor
)


cur_quarkcalldb = db_quarkcalldb54.cursor(pymysql.cursors.DictCursor)
cur_liupanshan = db_liupanshan.cursor(pymysql.cursors.DictCursor)
#cur_quarkcalldb = db_quarkcalldb.cursor()

@lpsbp.get("/test")
async def test(request):
    return json({"greeting":"Hello, sx world."})

# 服务热线受理情况
@lpsbp.post("/callstatus")
async def lps_callstatus(request):
    sql = "call proc_callstatus(%s,%s)"
    cur_liupanshan.execute(sql,(request.json["start_date"],request.json    ["end_date"]))
    all_obj = cur_liupanshan.fetchone()

    sql = "select GroupNo WorkerNo,count(1) OutsideCallTotalCnt from tbcallcdr where calltime >= %s and calltime <= %s and CallInOut=1 and GroupNo = %s"
    cur_quarkcalldb.execute(sql,(request.json["start_date"],request.json["end_date"],request.json["group_no"]))
    fd_obj = cur_quarkcalldb.fetchone()
    all_obj["共受理热线来电"] = fd_obj["OutsideCallTotalCnt"]
    all_obj["热线解释处理"] = fd_obj["OutsideCallTotalCnt"] - all_obj["派单处理"]
    all_obj["处理执行率"] = str(round((all_obj["派单处理"]-all_obj["接单超时"])/all_obj["派单处理"]*100,2))+'%'
    all_obj["处理及时率"] = str(round((all_obj["派单处理"]-all_obj["维修超时"]-all_obj["未完结"])/all_obj["派单处理"]*100,2))+'%'
    all_obj["回访满意率"] = str(round((all_obj["派单处理"]-all_obj["不满意"])/all_obj["派单处理"]*100,2))+'%'
    return json(all_obj,ensure_ascii=False)

# 各单位派单处理情况
@lpsbp.post("/depstatus")
async def lps_depstatus(request):
    sql = "call proc_depstatus(%s,%s)"
    cur_liupanshan.execute(sql,(request.json["start_date"],request.json    ["end_date"]))
    all_obj = cur_liupanshan.fetchall()
    return json(all_obj,ensure_ascii=False)

# 接单超时事件
@lpsbp.post("/jdcs")
async def lps_jecs(request):
    sql = """
select ROW_NUMBER() over (order by t.create_date) id ,t.department,t.bz from (
select id,department,create_date,concat(create_date,',',dz_address,',',remark,',',time_accept,'派单',time_receiver,'接单') bz
 from w_1592374411638 where create_date >= %s and create_date <= %s  and timestampdiff(minute,time_accept,time_receiver) >= 20
union
select id,department,create_date,concat(create_date,',',dz_address,',',remark,',',time_accept,'派单',time_receiver,'接单') bz from nxsw_1609840221906 where  create_date >= %s and create_date <= %s and timestampdiff(minute,time_accept,time_receiver)>= 20) t"""
    cur_liupanshan.execute(sql,(request.json["start_date"],request.json    ["end_date"],request.json["start_date"],request.json["end_date"]))
    all_obj = cur_liupanshan.fetchall()
    return json(all_obj,ensure_ascii=False)

# 处理回复超时事件
@lpsbp.post("/clhfcs")
async def lps_clhfcs(request):
    sql = """select ROW_NUMBER() over (order by t.create_date) id ,t.department,t.bz from (
select id,department,create_date,concat(create_date,',',dz_address,',',remark,',',time_accept,'派单',time_receiver,'接单;','小修',person_handle,time_handle,'处理:',gd_result) bz from w_1592374411638 where create_date >= %s and create_date <= %s and
repair = '小修'  and round((round(UNIX_TIMESTAMP(time_handle))-round(UNIX_TIMESTAMP(time_receiver)))/3600) > 8
union
select id,department,create_date,concat(create_date,',',dz_address,',',remark,',',time_accept,'派单',time_receiver,'接单;','小修',person_handle,time_handle,'处理:',gd_result) bz from w_1592374411638 where create_date >= %s and create_date <= %s and repair = '大修' and  round((round(UNIX_TIMESTAMP(time_handle))-round(UNIX_TIMESTAMP(time_receiver)))/3600)>24 ) t"""
    cur_liupanshan.execute(sql,(request.json["start_date"],request.json    ["end_date"],request.json["start_date"],request.json["end_date"]))
    all_obj = cur_liupanshan.fetchall()
    return json(all_obj,ensure_ascii=False)

# 接单后未处理回复事件 
@lpsbp.post("/jdwcl")
async def lps_jdwcl(request):
    sql = """select ROW_NUMBER() over (order by t.create_date) id ,t.department,t.bz from (
select id,department,create_date,concat(create_date,',',dz_address,',',remark,',',time_accept,'派单',time_receiver,'接单;',person_receiver1) bz from w_1592374411638 where create_date >= %s and create_date <= %s  and time_handle is null) t """
    cur_liupanshan.execute(sql,(request.json["start_date"],request.json    ["end_date"]))
    all_obj = cur_liupanshan.fetchall()
    return json(all_obj,ensure_ascii=False)

# 不满意事件 
@lpsbp.post("/bmy")
async def lps_bmy(request):
    sql = """
       select ROW_NUMBER() over (order by t.create_date) id ,t.department,t.bz from (
select id,department,create_date,concat(create_date,',',dz_address,',',remark,',',time_accept,'派单',time_receiver,'接单;',person_handle,time_handle,'处理:',gd_result,time_appraise,'回访;',remark_appraise,',不满意') bz from w_1592374411638 where create_date >= %s and create_date <= %s  and appraise = '不满意'
union
select id,department,create_date,concat(create_date,',',dz_address,',',remark,',',time_accept,'派单',time_receiver,'接单;',person_handle,time_handle,'处理:',gd_result,time_appraise,'回访;',remark_appraise,',不满意') bz from nxsw_1609840221906 where create_date >= %s and create_date <= %s and appraise = '不满意') t
 """
    cur_liupanshan.execute(sql,(request.json["start_date"],request.json    ["end_date"],request.json["start_date"],request.json["end_date"]))
    all_obj = cur_liupanshan.fetchall()
    return json(all_obj,ensure_ascii=False)
