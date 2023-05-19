-- 派单处理
select count(t.id) sl from (select id from w_1592374411638 where 
create_date >= '2023-04-25 00:00:00' and create_date <= '2023-04-25 23:59:59' 
union
select id from  nxsw_1609840221906 where 
create_date >= '2023-04-25 00:00:00' and create_date <= '2023-04-25 23:59:59') t;

-- 接单超时
select count(t.id) sl from (select id,dz_address,`repair`,position,create_date,time_accept,time_receiver,time_handle,remark,gd_result,appraise from w_1592374411638 where 
create_date >= '2023-04-25 00:00:00' and create_date <= '2023-04-25 23:59:59'  and timestampdiff(minute,time_accept,time_receiver) >= 20
  union
select id,dz_address,'' ,'' ,create_date,time_accept,time_receiver,time_handle,remark,gd_result,appraise from nxsw_1609840221906 where 
create_date >= '2023-04-25 00:00:00' and create_date <= '2023-04-25 23:59:59' and timestampdiff(minute,time_accept,time_receiver)>= 20) t;
-- 维修超时
select count(t.id) sl from (select id,dz_address,repair,position,create_date,time_accept,time_receiver,time_handle,remark,gd_result,appraise from w_1592374411638 where 
create_date >= '2023-04-25 00:00:00' and create_date <= '2023-04-25 23:59:59' and repair = '小修'  and timestampdiff(hour,time_receiver,time_handle) > 8
union 
select id,dz_address,repair,position,create_date,time_accept,time_receiver,time_handle,remark,gd_result,appraise from w_1592374411638 where 
create_date >= '2023-04-25 00:00:00' and create_date <= '2023-04-25 23:59:59' and repair = '大修'  and timestampdiff(hour,time_receiver,time_handle) > 24) t;

-- 不满意事件
select id,dz_address,repair,position,create_date,time_accept,time_receiver,time_handle,remark,gd_result,appraise from w_1592374411638 where 
create_date >= '2023-03-01 00:00:00' and create_date <= '2023-04-25 23:59:59' and appraise ='不满意';

-- 接单后未处理回复事件
select id,dz_address,repair,position,create_date,time_accept,time_receiver,time_handle,remark,gd_result,appraise from w_1592374411638 where  create_date >= '2023-03-01 00:00:00' and create_date <= '2023-04-25 23:59:59' and time_handle is null;
-- 故障报修 未完结
select id,dz_address,repair,position,create_date,time_accept,time_receiver,time_handle,remark,gd_result,appraise from w_1592374411638 where  create_date >= '2023-03-01 00:00:00' and create_date <= '2023-04-25 23:59:59' and time_handle is null;

-- 故障报修 未完结
select id,dz_address,create_date,time_accept,time_receiver,time_handle,remark,gd_result,appraise from nxsw_1609840221906 where  create_date >= '2023-03-01 00:00:00' and create_date <= '2023-04-25 23:59:59' and time_handle is null;

-- 共受理热线来电
select GroupNo WorkerNo,hour(calltime) tm,count(1) OutsideCallTotalCnt from tbcallcdr where calltime >= '2023-04-22' and calltime <= '2023-04-26 23:59:59' and length(CallerNo) > 4 and GroupNo = '1';

	select t1.sl 派单处理,t2.sl 接单超时,t3.sl 维修超时,t4.sl 不满意 from (
select 1 id,count(a.id) sl from (select id from w_1592374411638 where 
create_date >= '2023-03-01 00:00:00' and create_date <= '2023-04-25 23:59:59' 
union
select id from  nxsw_1609840221906 where 
create_date >= '2023-03-01 00:00:00' and create_date <= '2023-04-25 23:59:59') a) t1
join
 (
select 1 id, count(a.id) sl from (select id from w_1592374411638 where 
create_date >= '2023-03-01 00:00:00' and create_date <= '2023-04-25 23:59:59' and ROUND((UNIX_TIMESTAMP(time_receiver)-UNIX_TIMESTAMP(time_accept))/60) > 20
  union
select id from nxsw_1609840221906 where 
create_date >= '2023-03-01 00:00:00' and create_date <= '2023-04-25 23:59:59' and ROUND((UNIX_TIMESTAMP(time_receiver)-UNIX_TIMESTAMP(time_accept))/60) > 20) a
) t2 
join
(select count(a.id) sl from (
select id from w_1592374411638 where create_date >= '2023-03-01 00:00:00' and create_date <= '2023-04-25 23:59:59' and repair = '小修'  and round((round(UNIX_TIMESTAMP(time_handle))-round(UNIX_TIMESTAMP(time_receiver)))/3600) > 8
union 
select id from w_1592374411638 where create_date >= '2023-03-01 00:00:00' and create_date <= '2023-04-25 23:59:59' and repair = '大修'  and  round((round(UNIX_TIMESTAMP(time_handle))-round(UNIX_TIMESTAMP(time_receiver)))/3600)>24 )a ) t3
join
(
  select count(1) sl from w_1592374411638 where 
create_date >= '2023-03-01 00:00:00' and create_date <= '2023-04-25 23:59:59' and appraise ='不满意'
) t4 ;
####################################
select t1.sl pdsl,
concat(round((t1.sl-ifnull(t2.sl,0))/t1.sl*100,2),'%') zxl, 
concat(round((t1.sl-ifnull(t3.sl,0)-ifnull(t10.sl,0))/t1.sl*100,2),'%') jsl,
concat(round((t1.sl-ifnull(t5.sl,0))/t1.sl*100,2),'%') hfmyl,
t6.sl gzbx,concat(round(t8.sl/t6.sl*100,2),'%') gzbxwjl, 
t7.sl ywzx,concat(round(t9.sl/t7.sl*100,2),'%') ywzxwjl from (
select 1 id,count(a.id) sl from (select id from w_1592374411638 where
create_date >= '2023-03-19 00:00:00' and create_date <= '2023-04-10 23:59:59'
union
select id from  nxsw_1609840221906 where
create_date >= '2023-03-19 00:00:00' and create_date <= '2023-04-10 23:59:59') a) t1
join
 (select 1 id, count(a.id) sl from (select id from w_1592374411638 where
create_date >= '2023-03-19 00:00:00' and create_date <= '2023-04-10 23:59:59' and ROUND((UNIX_TIMESTAMP(time_receiver)-UNIX_TIMESTAMP(time_accept))/60) > 20
  union
select id from nxsw_1609840221906 where
create_date >= '2023-03-19 00:00:00' and create_date <= '2023-04-10 23:59:59' and ROUND((UNIX_TIMESTAMP(time_receiver)-UNIX_TIMESTAMP(time_accept))/60) > 20) a
) t2
join
(select count(a.id) sl from (
select id from w_1592374411638 where create_date >= '2023-03-19 00:00:00' and create_date <= '2023-04-10 23:59:59' and repair = '小修'  and round((round(UNIX_TIMESTAMP(time_handle))-round(UNIX_TIMESTAMP(time_receiver)))/3600) > 8
union
select id from w_1592374411638 where create_date >= '2023-03-19 00:00:00' and create_date <= '2023-04-10 23:59:59' and repair = '大修'  and  round((round(UNIX_TIMESTAMP(time_handle))-round(UNIX_TIMESTAMP(time_receiver)))/3600)>24 )a ) t3
join
(select count(1) sl from w_1592374411638 where  create_date >= '2023-03-19 00:00:00' and create_date <= '2023-04-10 23:59:59' and time_handle is null ) t4
join 
(select count(1) sl from w_1592374411638 where
create_date >= '2023-03-19 00:00:00' and create_date <= '2023-04-10 23:59:59' and appraise ='不满意'
) t5 
join 
(select count(1) sl from w_1592374411638 where
create_date >= '2023-03-19 00:00:00' and create_date <= '2023-04-10 23:59:59' 
) t6 
join 
(select count(1) sl from nxsw_1609840221906 where
create_date >= '2023-03-19 00:00:00' and create_date <= '2023-04-10 23:59:59' 
) t7 
join 
(select  count(id) sl from w_1592374411638 where create_date >= '2023-03-19 00:00:00' and create_date <= '2023-04-10 23:59:59' and time_appraise is not null )t8 
join
(select count(id) sl from nxsw_1609840221906 where create_date >= '2023-03-19 00:00:00' and create_date <= '2023-04-10 23:59:59' and time_appraise is not null
) t9
join 
( select count(1) sl from (
select  id from w_1592374411638 where create_date >= '2023-03-19 00:00:00' and create_date <= '2023-04-10 23:59:59' and time_appraise is null union select id from nxsw_1609840221906 where create_date >= '2023-03-19 00:00:00' and create_date <= '2023-04-10 23:59:59' and time_appraise is  null)a )t10 
;

