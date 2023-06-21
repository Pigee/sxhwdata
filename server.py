from sanic import Sanic
from sanic.response import json
import pymysql
from conf.mysqldb import Lpsconn
from conf.mysqldb import Callconn

'''
db_liupanshan = pymysql.connect( # 创建数据库连接
    #host='192.168.1.11', # 要连接的数据库所在主机ip
    host='192.168.199.100', # 要连接的数据库所在主机ip
    port=3306, # 数据库端口
    database='cs_y_run_nxsw',
    user='admin', # 数据库登录用户名
    password='123123', # 登录用户密码
    charset='utf8mb4' # 编码，注意不能写成utf-8
    #cursorclass=cursors.DictCursor
)

db_quarkcalldb = pymysql.connect( # 创建数据库连接
    #host='192.168.1.11', # 要连接的数据库所在主机ip
    host='192.168.199.100', # 要连接的数据库所在主机ip
    port=3306, # 数据库端口
    database='quarkcalldb54',
    user='admin', # 数据库登录用户名
    password='123123', # 登录用户密码
    charset='utf8mb4' # 编码，注意不能写成utf-8
    #cursorclass=cursors.DictCursor
)
'''
calldb = Callconn()
lpsdb = Lpsconn()
cur_quarkcalldb = calldb.cursor(pymysql.cursors.DictCursor)
cur_liupanshan = lpsdb.cursor(pymysql.cursors.DictCursor)
#cur_quarkcalldb = db_quarkcalldb.cursor()

app = Sanic("sxhwdata")

@app.get("/test")
async def hello_world(request):
    return text("Hello, sx world.")

# 服务热线受理情况
@app.post("/liupanshan/callstatus")
async def lps_callstatus(request):
    sql = """
    select t1.sl pdsl,
round((t1.sl-ifnull(t2.sl,0))/t1.sl*100,2) zxl,
round((t1.sl-ifnull(t3.sl,0)-ifnull(t10.sl,0))/t1.sl*100,2) jsl,
round((t1.sl-ifnull(t5.sl,0))/t1.sl*100,2) hfmyl,
t6.sl gzbx,round(t8.sl/t6.sl*100,2) gzbxwjl,
t7.sl ywzx,round(t9.sl/t7.sl*100,2) ywzxwjl from (
select 1 id,count(a.id) sl from (select id from w_1592374411638 where create_date >= %(start_date)s and create_date <= %(end_date)s union
select id from  nxsw_1609840221906 where create_date >= %(start_date)s and create_date <= %(end_date)s )a) t1
join
 (select 1 id, count(a.id) sl from (select id from w_1592374411638 where
create_date >= %(start_date)s and create_date <= %(end_date)s and ROUND((UNIX_TIMESTAMP(time_receiver)-UNIX_TIMESTAMP(time_accept))/60) > 20
  union
select id from nxsw_1609840221906 where
create_date >= %(start_date)s and create_date <= %(end_date)s and ROUND((UNIX_TIMESTAMP(time_receiver)-UNIX_TIMESTAMP(time_accept))/60) > 20) a
) t2
join
(select count(a.id) sl from (
select id from w_1592374411638 where create_date >= %(start_date)s and create_date <= %(end_date)s and repair = '小修'  and round((round(UNIX_TIMESTAMP(time_handle))-round(UNIX_TIMESTAMP(time_receiver)))/3600) > 8
union
select id from w_1592374411638 where create_date >= %(start_date)s and create_date <= %(end_date)s and repair = '大修'  and  round((round(UNIX_TIMESTAMP(time_handle))-round(UNIX_TIMESTAMP(time_receiver)))/3600)>24 )a ) t3
join
(select count(1) sl from w_1592374411638 where  create_date >= %(start_date)s and create_date <= %(end_date)s and time_handle is null ) t4
join
(select count(1) sl from w_1592374411638 where
create_date >= %(start_date)s and create_date <= %(end_date)s and appraise ='不满意'
) t5
join
(select count(1) sl from w_1592374411638 where
create_date >= %(start_date)s and create_date <= %(end_date)s ) t6
join
(select count(1) sl from nxsw_1609840221906 where
create_date >= %(start_date)s and create_date <= %(end_date)s ) t7
join
(select  count(id) sl from w_1592374411638 where create_date >= %(start_date)s and create_date <= %(end_date)s and time_appraise is not null )t8
join
(select count(id) sl from nxsw_1609840221906 where create_date >= %(start_date)s and create_date <= %(end_date)s and time_appraise is not null
) t9
join
( select count(1) sl from (select  id from w_1592374411638 where create_date >= %(start_date)s and create_date <= %(end_date)s and time_appraise is null union select id from nxsw_1609840221906 where create_date >= %(start_date)s and create_date <= %(end_date)s and time_appraise is null)a )t10
    """
    cur_liupanshan.execute(sql,{"start_date":request.json["start_date"],"end_date":request.json["end_date"]})
    all_obj = cur_liupanshan.fetchone()

    sql = "select GroupNo WorkerNo,count(1) OutsideCallTotalCnt from tbcallcdr where calltime >= %s and calltime <= %s and CallInOut=1 and GroupNo = %s"
    cur_quarkcalldb.execute(sql,(request.json["start_date"],request.json["end_date"],request.json["group_no"]))
    fd_obj = cur_quarkcalldb.fetchone()
    all_obj["gzlld"] = fd_obj["OutsideCallTotalCnt"]
    all_obj["jscl"] = fd_obj["OutsideCallTotalCnt"] - all_obj["pdsl"] 
    all_obj["jsl"] = str(all_obj["jsl"]) + "%"
    all_obj["zxl"] = str(all_obj["zxl"]) + "%"
    all_obj["hfmyl"] = str(all_obj["hfmyl"]) + "%"
    all_obj["gzbxwjl"] = str(all_obj["gzbxwjl"]) + "%"
    all_obj["ywzxwjl"] = str(all_obj["ywzxwjl"]) + "%"
    #all_obj["执行率"] = str(round((all_obj["派单处理"]-all_obj["接单超时"])/all_obj["派单处理"]*100,2))+'%'
    #all_obj["处理及时率"] = str(round((all_obj["派单处理"]-all_obj["维修超时"]-all_obj["未完结"])/all_obj["派单处理"]*100,2))+'%'
    #all_obj["回访满意率"] = str(round((all_obj["派单处理"]-all_obj["不满意"])/all_obj["派单处理"]*100,2))+'%'
    return json(all_obj,ensure_ascii=False)

# 各单位派单处理情况
@app.post("/liupanshan/depstatus")
async def lps_depstatus(request):
    sql = """
    select t1.department ,t1.sl hj ,t2.sl wc,
round((t1.sl -ifnull(t4.sl,0))/t1.sl * 100,2) zxl ,
round((t1.sl -ifnull(t5.sl,0))/t1.sl * 100,2) cll,
round(ifnull(t6.sl,0)/t1.sl * 100,2) hfl,
ifnull(t3.sl,0) bmy,
round((ifnull(t2.sl,0)-ifnull(t3.sl,0))/t1.sl * 100,2) hfmyl
from (
select a.department,count(id) sl from 
(select department,id from w_1592374411638 where 
create_date >= %(start_date)s and create_date <= %(end_date)s 
union
select department,id from  nxsw_1609840221906 where 
create_date >= %(start_date)s and create_date <= %(end_date)s) a GROUP BY a.department) t1
left join 
(select a.department,count(id) sl from (select department,id from w_1592374411638 where 
create_date >= %(start_date)s and create_date <= %(end_date)s  and time_handle is not null
union
select department,id from  nxsw_1609840221906 where 
create_date >= %(start_date)s and create_date <= %(end_date)s and time_handle is not null) a GROUP BY a.department ) t2 on t1.department = t2.department
left join 
(select a.department,count(id) sl from (select department,id from w_1592374411638 where 
create_date >= %(start_date)s and create_date <= %(end_date)s  and appraise ='不满意'
union
select department,id from  nxsw_1609840221906 where 
create_date >= %(start_date)s and create_date <= %(end_date)s and appraise ='不满意') a GROUP BY a.department ) t3 on t1.department = t3.department
left join 
(select a.department,count(id) sl from (select department,id from w_1592374411638 where 
create_date >= %(start_date)s and create_date <= %(end_date)s and ROUND((UNIX_TIMESTAMP(time_receiver)-UNIX_TIMESTAMP(time_accept))/60) > 20
union
select department,id from  nxsw_1609840221906 where 
create_date >= %(start_date)s and create_date <= %(end_date)s and ROUND((UNIX_TIMESTAMP(time_receiver)-UNIX_TIMESTAMP(time_accept))/60) > 20) a GROUP BY a.department ) t4 on t1.department = t4.department
left join 
(select a.department,count(id) sl  from (
select department,id from w_1592374411638 where create_date >= %(start_date)s and create_date <= %(end_date)s and repair = '小修'  and round((round(UNIX_TIMESTAMP(time_handle))-round(UNIX_TIMESTAMP(time_receiver)))/3600) > 8
union 
select department,id from w_1592374411638 where create_date >= %(start_date)s and create_date <= %(end_date)s and repair = '大修'  and  round((round(UNIX_TIMESTAMP(time_handle))-round(UNIX_TIMESTAMP(time_receiver)))/3600)>24 )a GROUP BY a.department ) t5 on t1.department = t5.department
left join 
(select a.department,count(id) sl  from (
select department,id from w_1592374411638 where create_date >= %(start_date)s and create_date <= %(end_date)s and time_appraise is not null
union
select department,id from nxsw_1609840221906 where create_date >= %(start_date)s and create_date <= %(end_date)s and time_appraise is not null
)a GROUP BY a.department ) t6 on t1.department = t6.department
    """
    cur_liupanshan.execute(sql,{"start_date":request.json["start_date"],"end_date":request.json["end_date"]})
    all_obj = cur_liupanshan.fetchall()
    return json(all_obj,ensure_ascii=False)

# 接单超时事件
@app.post("/liupanshan/jdcs")
async def lps_jecs(request):
    sql = """
select ROW_NUMBER() over (order by t.create_date) id ,t.department,t.bz from (
select id,department,create_date,concat(create_date,',',dz_address,',',remark,',',time_accept,'派单',time_receiver,'接单') bz
 from w_1592374411638 where create_date >= %s and create_date <= %s  and timestampdiff(minute,time_accept,time_receiver) >= 20
union
select id,department,create_date,concat(create_date,',',dz_address,',',remark,',',time_accept,'派单',time_receiver,'接单') bz from nxsw_1609840221906 where  create_date >= %s and create_date <= %s and timestampdiff(minute,time_accept,time_receiver)>= 20) t"""
    cur_liupanshan.execute(sql,(request.json["start_date"],request.json["end_date"],request.json["start_date"],request.json["end_date"]))
    all_obj = cur_liupanshan.fetchall()
    return json(all_obj,ensure_ascii=False)

# 处理回复超时事件
@app.post("/liupanshan/clhfcs")
async def lps_clhfcs(request):
    sql = """select ROW_NUMBER() over (order by t.create_date) id ,t.department,t.bz from (
select id,department,create_date,concat(create_date,',',dz_address,',',remark,',',time_accept,'派单',time_receiver,'接单;','小修',person_handle,time_handle,'处理:',gd_result) bz from w_1592374411638 where create_date >= %s and create_date <= %s and
repair = '小修'  and round((round(UNIX_TIMESTAMP(time_handle))-round(UNIX_TIMESTAMP(time_receiver)))/3600) > 8
union
select id,department,create_date,concat(create_date,',',dz_address,',',remark,',',time_accept,'派单',time_receiver,'接单;','小修',person_handle,time_handle,'处理:',gd_result) bz from w_1592374411638 where create_date >= %s and create_date <= %s and repair = '大修' and  round((round(UNIX_TIMESTAMP(time_handle))-round(UNIX_TIMESTAMP(time_receiver)))/3600)>24 ) t"""
    cur_liupanshan.execute(sql,(request.json["start_date"],request.json["end_date"],request.json["start_date"],request.json["end_date"]))
    all_obj = cur_liupanshan.fetchall()
    return json(all_obj,ensure_ascii=False)

# 接单后未处理回复事件 
@app.post("/liupanshan/jdwcl")
async def lps_jdwcl(request):
    sql = """select ROW_NUMBER() over (order by t.create_date) id ,t.department,t.bz from (
select id,department,create_date,concat(create_date,',',dz_address,',',remark,',',time_accept,'派单',time_receiver,'接单;',person_receiver1) bz from w_1592374411638 where create_date >= %s and create_date <= %s  and time_handle is null) t """
    cur_liupanshan.execute(sql,(request.json["start_date"],request.json["end_date"]))
    all_obj = cur_liupanshan.fetchall()
    return json(all_obj,ensure_ascii=False)

# 不满意事件 
@app.post("/liupanshan/bmy")
async def lps_bmy(request):
    sql = """
       select ROW_NUMBER() over (order by t.create_date) id ,t.department,t.bz from (
select id,department,create_date,concat(create_date,',',dz_address,',',remark,',',time_accept,'派单',time_receiver,'接单;',person_handle,time_handle,'处理:',gd_result,time_appraise,'回访;',remark_appraise,',不满意') bz from w_1592374411638 where create_date >= %(start_date)s and create_date <= %(end_date)s  and appraise = '不满意'
union
select id,department,create_date,concat(create_date,',',dz_address,',',remark,',',time_accept,'派单',time_receiver,'接单;',person_handle,time_handle,'处理:',gd_result,time_appraise,'回访;',remark_appraise,',不满意') bz from nxsw_1609840221906 where create_date >= %(start_date)s and create_date <= %(end_date)s and appraise = '不满意') t
 """
    cur_liupanshan.execute(sql,{'start_date':request.json["start_date"],'end_date':request.json["end_date"]})
    all_obj = cur_liupanshan.fetchall()
    return json(all_obj,ensure_ascii=False)


# post 请求
@app.post("/p")
async def post_json(request):
    print(request.json["start_date"])
    sql = "select id,concat(create_date,'')create_date,time_accept,repair,time_accept,time_receiver,time_handle,remark,gd_result,appraise,dz_address from w_1592374411638 where create_date >= %s and create_date <= %s and time_handle is null"
    cur_liupanshan.execute(sql,(request.json["start_date"],request.json["end_date"]))
    all_obj = cur_liupanshan.fetchall()
    return json(all_obj,ensure_ascii=False)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=1337, dev=True)
