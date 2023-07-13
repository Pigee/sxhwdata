from sanic.response import json
import pymysql
from sanic import Blueprint
from conf.mysqldb import Csconn
from conf.mysqldb import Callconn

hongsibaobp = Blueprint("hongsibao_bp", url_prefix="/hongsibao")

#calldb = Callconn()
csdb = Csconn()
csdb.select_db("cs_y_run_nxsw_hongsibao")
#cur_quarkcalldb = calldb.cursor(pymysql.cursors.DictCursor)
cur_hongsibao = csdb.cursor(pymysql.cursors.DictCursor)
cur_quarkcalldb = Callconn()

@hongsibaobp.get("/test")
async def test(request):
    return json({"greeting":"Hello, sx world."})

# 服务热线受理情况
@hongsibaobp.post("/callstatus")
async def lps_callstatus(request):
    sql = """
select t1.sl pdsl,
ifnull(round((t1.sl-ifnull(t2.sl,0))/t1.sl*100,2),0) zxl,
ifnull(round((t1.sl-ifnull(t3.sl,0)-ifnull(t10.sl,0))/t1.sl*100,2),0) jsl,
ifnull(round((t1.sl-ifnull(t5.sl,0))/t1.sl*100,2),0) hfmyl,
ifnull(t6.sl,0) gzbx,ifnull(round(t8.sl/t6.sl*100,2),0) gzbxwjl,
ifnull(t7.sl,0) ywzx,ifnull(round(t9.sl/t7.sl*100,2),0) ywzxwjl from (
select 1 id,count(a.id) sl from (select id from w_1592374411638 where create_date >= %(start_date)s and create_date <= %(end_date)s
union
select id from  nxsw_1612146673154 where create_date >= %(start_date)s and create_date <= %(end_date)s )a) t1
join
 (select 1 id, count(a.id) sl from (select id from w_1592374411638 where
create_date >= %(start_date)s and create_date <= %(end_date)s and ROUND((UNIX_TIMESTAMP(time_receiver)-UNIX_TIMESTAMP(time_accept))/60) > 20
  union
select id from nxsw_1612146673154 where
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
(select count(1) sl from nxsw_1612146673154 where
create_date >= %(start_date)s and create_date <= %(end_date)s ) t7
join
(select  count(id) sl from w_1592374411638 where create_date >= %(start_date)s and create_date <= %(end_date)s and time_appraise is not null )t8
join
(select count(id) sl from nxsw_1612146673154 where create_date >= %(start_date)s and create_date <= %(end_date)s and time_appraise is not null
) t9
join
( select count(1) sl from (select  id from w_1592374411638 where create_date >= %(start_date)s and create_date <= %(end_date)s and time_appraise is null union select id from nxsw_1612146673154 where create_date >= %(start_date)s and create_date <= %(end_date)s and time_appraise is null)a )t10
    """
    cur_hongsibao.execute(sql,{"start_date":request.json["start_date"],"end_date":request.json["end_date"]})
    all_obj = cur_hongsibao.fetchone()

    sql = "select GroupNo WorkerNo,count(1) OutsideCallTotalCnt from tbcallcdr where calltime >= %s and calltime <= %s and CallInOut=1 and GroupNo = %s"
    cur_quarkcalldb.execute(sql,(request.json["start_date"],request.json["end_date"],request.json["group_no"]))
    fd_obj = cur_quarkcalldb.fetchone()
    all_obj["gslld"] = fd_obj["OutsideCallTotalCnt"]
    all_obj["jscl"] = fd_obj["OutsideCallTotalCnt"] - all_obj["pdsl"]
    all_obj["jsl"] = str(all_obj["jsl"]) + "%"
    all_obj["zxl"] = str(all_obj["zxl"]) + "%"
    all_obj["hfmyl"] = str(all_obj["hfmyl"]) + "%"
    all_obj["gzbxwjl"] = str(all_obj["gzbxwjl"]) + "%"
    all_obj["ywzxwjl"] = str(all_obj["ywzxwjl"]) + "%"
    all_obj["jbjy"] = 0
    all_obj["jbjywjl"] = "0%"
    all_obj["tssl"] = 0
    all_obj["tsslwjl"] = "0%"
    all_obj["ysxh"] = 0
    all_obj["ysxhwjl"] = "0%"
    all_obj["bzsq"] = 0
    all_obj["bzsqwjl"] = "0%"
    return json(all_obj,ensure_ascii=False)

# 各单位派单处理情况
@hongsibaobp.post("/depstatus")
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
select department,id from  nxsw_1612146673154 where 
create_date >= %(start_date)s and create_date <= %(end_date)s) a GROUP BY a.department) t1
left join 
(select a.department,count(id) sl from (select department,id from w_1592374411638 where 
create_date >= %(start_date)s and create_date <= %(end_date)s  and time_handle is not null
union
select department,id from  nxsw_1612146673154 where 
create_date >= %(start_date)s and create_date <= %(end_date)s and time_handle is not null) a GROUP BY a.department ) t2 on t1.department = t2.department
left join 
(select a.department,count(id) sl from (select department,id from w_1592374411638 where 
create_date >= %(start_date)s and create_date <= %(end_date)s  and appraise ='不满意'
union
select department,id from  nxsw_1612146673154 where 
create_date >= %(start_date)s and create_date <= %(end_date)s and appraise ='不满意') a GROUP BY a.department ) t3 on t1.department = t3.department
left join 
(select a.department,count(id) sl from (select department,id from w_1592374411638 where 
create_date >= %(start_date)s and create_date <= %(end_date)s and ROUND((UNIX_TIMESTAMP(time_receiver)-UNIX_TIMESTAMP(time_accept))/60) > 20
union
select department,id from  nxsw_1612146673154 where 
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
select department,id from nxsw_1612146673154 where create_date >= %(start_date)s and create_date <= %(end_date)s and time_appraise is not null
)a GROUP BY a.department ) t6 on t1.department = t6.department
    """
    cur_hongsibao.execute(sql,{"start_date":request.json["start_date"],"end_date":request.json["end_date"]})
    all_obj = cur_hongsibao.fetchall()
    return json(all_obj,ensure_ascii=False)

# 接单超时事件
@hongsibaobp.post("/jdcs")
async def lps_jecs(request):
    sql = """
select ROW_NUMBER() over (order by t.create_date) id ,t.department,t.bz from (
select id,department,create_date,concat(create_date,',',dz_address,',',remark,',',time_accept,'派单',time_receiver,'接单') bz
 from w_1592374411638 where create_date >= %s and create_date <= %s  and timestampdiff(minute,time_accept,time_receiver) >= 20
union
select id,department,create_date,concat(create_date,',',dz_address,',',remark,',',time_accept,'派单',time_receiver,'接单') bz from nxsw_1612146673154 where  create_date >= %s and create_date <= %s and timestampdiff(minute,time_accept,time_receiver)>= 20) t"""
    cur_hongsibao.execute(sql,(request.json["start_date"],request.json["end_date"],request.json["start_date"],request.json["end_date"]))
    all_obj = cur_hongsibao.fetchall()
    return json(all_obj,ensure_ascii=False)

# 处理回复超时事件
@hongsibaobp.post("/clhfcs")
async def lps_clhfcs(request):
    sql = """select ROW_NUMBER() over (order by t.create_date) id ,t.department,t.bz from (
select id,department,create_date,concat(create_date,',',dz_address,',',remark,',',time_accept,'派单',time_receiver,'接单;','小修',person_handle,time_handle,'处理:',gd_result) bz from w_1592374411638 where create_date >= %s and create_date <= %s and
repair = '小修'  and round((round(UNIX_TIMESTAMP(time_handle))-round(UNIX_TIMESTAMP(time_receiver)))/3600) > 8
union
select id,department,create_date,concat(create_date,',',dz_address,',',remark,',',time_accept,'派单',time_receiver,'接单;','大修',person_handle,time_handle,'处理:',gd_result) bz from w_1592374411638 where create_date >= %s and create_date <= %s and repair = '大修' and  round((round(UNIX_TIMESTAMP(time_handle))-round(UNIX_TIMESTAMP(time_receiver)))/3600)>24 ) t"""
    cur_hongsibao.execute(sql,(request.json["start_date"],request.json["end_date"],request.json["start_date"],request.json["end_date"]))
    all_obj = cur_hongsibao.fetchall()
    return json(all_obj,ensure_ascii=False)

# 接单后未处理回复事件 
@hongsibaobp.post("/jdwcl")
async def lps_jdwcl(request):
    sql = """select ROW_NUMBER() over (order by t.create_date) id ,t.department,t.bz from (
select id,department,create_date,concat(create_date,',',dz_address,',',remark,',',time_accept,'派单',time_receiver,'接单;',person_receiver) bz from w_1592374411638 where create_date >= %s and create_date <= %s  and time_handle is null) t """
    cur_hongsibao.execute(sql,(request.json["start_date"],request.json["end_date"]))
    all_obj = cur_hongsibao.fetchall()
    return json(all_obj,ensure_ascii=False)

# 不满意事件 
@hongsibaobp.post("/bmy")
async def lps_bmy(request):
    sql = """
       select ROW_NUMBER() over (order by t.create_date) id ,t.department,t.bz from (
select id,department,create_date,concat(create_date,',',dz_address,',',remark,',',time_accept,'派单',time_receiver,'接单;',person_handle,time_handle,'处理:',gd_result,time_appraise,'回访;',remark_appraise,',不满意') bz from w_1592374411638 where create_date >= %s and create_date <= %s  and appraise = '不满意'
union
select id,department,create_date,concat(create_date,',',dz_address,',',remark,',',time_accept,'派单',time_receiver,'接单;',person_handle,time_handle,'处理:',gd_result,time_appraise,'回访;',remark_appraise,',不满意') bz from nxsw_1612146673154 where create_date >= %s and create_date <= %s and appraise = '不满意') t
 """
    cur_hongsibao.execute(sql,(request.json["start_date"],request.json["end_date"],request.json["start_date"],request.json["end_date"]))
    all_obj = cur_hongsibao.fetchall()
    return json(all_obj,ensure_ascii=False)

# 单位派单日报 
@hongsibaobp.post("/pdday")
async def lps_pdday(request):
    sql = """
 select t1.department ,t1.sl pd ,t1.sl - ifnull(t4.sl,0) asjd,ifnull(t4.sl,0) wasjd,
 ifnull(t2.sl,0) clhf,t1.sl-ifnull(t2.sl,0) whf,
 t6.sl hfs,t1.sl-ifnull(t3.sl,0) my, ifnull(t3.sl,0) bmy
from (
select a.department,count(id) sl from 
(select department,id from w_1592374411638 where 
create_date >= %(start_date)s and create_date <= %(end_date)s 
union
select department,id from  nxsw_1612146673154 where 
create_date >= %(start_date)s and create_date <= %(end_date)s) a GROUP BY a.department) t1
left join 
(select a.department,count(id) sl from (select department,id from w_1592374411638 where 
create_date >= %(start_date)s and create_date <= %(end_date)s  and time_handle is not null
union
select department,id from  nxsw_1612146673154 where 
create_date >= %(start_date)s and create_date <= %(end_date)s and time_handle is not null) a GROUP BY a.department ) t2 on t1.department = t2.department
left join 
(select a.department,count(id) sl from (select department,id from w_1592374411638 where 
create_date >= %(start_date)s and create_date <= %(end_date)s  and appraise ='不满意'
union
select department,id from  nxsw_1612146673154 where 
create_date >= %(start_date)s and create_date <= %(end_date)s and appraise ='不满意') a GROUP BY a.department ) t3 on t1.department = t3.department
left join 
(select a.department,count(id) sl from (select department,id from w_1592374411638 where 
create_date >= %(start_date)s and create_date <= %(end_date)s and ROUND((UNIX_TIMESTAMP(time_receiver)-UNIX_TIMESTAMP(time_accept))/60) > 20
union
select department,id from  nxsw_1612146673154 where 
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
select department,id from nxsw_1612146673154 where create_date >= %(start_date)s and create_date <= %(end_date)s and time_appraise is not null
)a GROUP BY a.department ) t6 on t1.department = t6.department

 """
    cur_hongsibao.execute(sql,{"start_date":request.json["start_date"],"end_date":request.json["end_date"]})
    all_obj = cur_hongsibao.fetchall()
    return json(all_obj,ensure_ascii=False)


# 具体情况日报 
@hongsibaobp.post("/dtday")
async def lps_dtday(request):
   sql = """
       select work_no,'故障报修' wtype,concat(create_date,'')create_date,hm,phone,dz_address,remark,time_accept,person_accept,department,time_receiver,person_receiver,time_handle,person_handle,gd_result,time_appraise,person_appraise,remark_appraise,appraise,`repair` from w_1592374411638 where create_date >= %(start_date)s and create_date <= %(end_date)s 
union
select work_no,'信息查询' wtype,concat(create_date,'')create_date,hm,phone,dz_address,remark,time_accept,person_accept,department,time_receiver,person_receiver,time_handle,person_handle,gd_result,time_appraise,person_appraise,remark_appraise,appraise,'无修' from  nxsw_1612146673154 where  create_date >= %(start_date)s and create_date <= %(end_date)s order by create_date
     """
   cur_hongsibao.execute(sql,{"start_date":request.json["start_date"],"end_date":request.json["end_date"]})
   all_obj = cur_hongsibao.fetchall()
   return json(all_obj,ensure_ascii=False)

# 个人报表 返回部门用户列表
@hongsibaobp.get("/deptuser")
async def lps_deptuser(request):
   sql = """
     select  t2.userid,t2.name username,t3.name deptname from cs_base_depart_user t1
join cs_base_user t2 on t1.userid = t2.userid
join cs_base_department t3 on t1.departmentid = t3.departmentid
where t3.parentid = '1'
order by deptname
     """
   cur_hongsibao.execute(sql)
   all_obj = cur_hongsibao.fetchall()
   return json(all_obj,ensure_ascii=False)

# 个人报表
@hongsibaobp.post("/usergdzsl")
async def lps_usergdzsl(request):
   sql = """
       SELECT count(1) gdzsl FROM wf_hist_task where  operator = %(userid)s and create_Time >= %(start_date)s and create_Time < %(end_date)s
     """
   cur_hongsibao.execute(sql,{"userid":request.json["userid"],"start_date":request.json["start_date"],"end_date":request.json["end_date"]})
   all_obj = cur_hongsibao.fetchall()
   return json(all_obj,ensure_ascii=False)
