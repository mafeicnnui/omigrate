1.生产库列表查询
select concat(b.line_username,'@',a.db_purpose,'-',a.db_type,'-',a.ip,'/',a.port) as "prod_db" ,
       a.id as prod_inst_id ,
       b.id as prod_config_id
from db_inst a,db_config b
 where a.id=b.db_inst_id
 and a.db_type='oracle'
 and a.ip in('10.120.12.41');

2.通过用户名获取user_id
SQL>  select user_id,username from dba_users@dblink_clic where username='CLIC';

   USER_ID USERNAME
---------- ------------------------------
       300 CLIC


3.生成分区语句，有bug需要修改，不能通过分区名来获取分区所在月份，从数据中动态获取。
select 'insert into db_mgt_tab_part_config(db_mgt_tab_config_id,part_name,part_month,flag,creator) values(''2'',' ||
       chr(39) || partition_name || chr(39) || ',' || chr(39) ||
       substr(partition_name, 5) || chr(39) || ',''Y'',''dba''); '
  from dba_tab_partitions
 where table_owner = 'CLIC'
   and table_name = 'TC_LOG'
 order by partition_name;
 
#修改后的代码如下：这段代码必须在check()方法返回值为0才能正确获取分区名,xhx分区表存在一个月多个分区情况
declare
 v_part_month varchar2(50);
 v_part_month_sql varchar2(1000);
 v_ins_sql    varchar2(2000);
 v_owner      varchar2(50):='CLIC';
 v_tab        varchar2(50):='TC_LOG';
 
 function get_source_part_col(v_owner varchar2,v_tab varchar2) return varchar2 is
    v_ret varchar2(50);
 begin   
      select column_name
        into v_ret
        from DBA_PART_KEY_COLUMNS
       where owner = upper(v_owner)
         and name = upper(v_tab);
       return v_ret;
 end ; 
 
 function get_source_part_col_type(v_owner varchar2,v_tab varchar2,v_col varchar2) return varchar2 is
    v_ret varchar2(50);
 begin   
     select data_type
       into v_ret
       from dba_tab_columns
      where owner = upper(v_owner)
        and table_name = upper(v_tab)
        and column_name = upper(v_col);
       return v_ret;
 end ; 
begin
  for i in(select partition_name from dba_tab_partitions  
          where table_owner =upper(v_owner) and table_name =upper(v_tab) order by partition_name) loop      
     if get_source_part_col_type(v_owner,v_tab,get_source_part_col(v_owner, v_tab))='DATE' then       
       
	       v_part_month_sql:='select to_char('||get_source_part_col(v_owner,v_tab)||',''yyyymm'') from '
	                          ||v_owner||'.'||v_tab||' partition('||i.partition_name||') where rownum=1';
	       
	       begin                  
	         execute immediate v_part_month_sql into  v_part_month;  
	       exception
	        when others then 
	           v_part_month:=null;
	       end; 
     end if;
     
     v_ins_sql:= 'insert into db_mgt_tab_part_config(db_mgt_tab_config_id,part_name,part_month,flag,creator) values(''2'',' 
                  ||chr(39) || i.partition_name || chr(39) || ',' 
                  ||chr(39) || v_part_month || chr(39) || ',''Y'',''dba''); ';
     if v_part_month is not null then
        dbms_output.put_line(v_ins_sql);
     end if;      
  end loop;
end;
/


#建立历史库操作账号dbant专用账号
select * from dba_sys_privs where grantee='PUPPET'
select * from dba_role_privs where grantee='PUPPET'
select * from dba_tab_privs where grantee='PUPPET'

create user PUPPET  identified by "U2h7FliaezODyW18bbTo" default tablespace USERS  temporary tablespace TEMP   profile DEFAULT;
grant connect to PUPPET;
grant exp_full_database to PUPPET;
grant resource to PUPPET;
grant select_catalog_role to PUPPET;
grant create session to PUPPET;
grant execute any procedure to PUPPET;
grant select any dictionary to PUPPET;
grant unlimited tablespace to PUPPET;
grant CREATE DATABASE LINK to PUPPET;
grant create any directory to puppet;
grant imp_full_database  to puppet;

#查询数据库链路
conn puppet
set lines 200
col host format a30
col owner format a14
col db_link format a30
select * from dba_db_links;
create  database link dblink_clic connect to puppet identified by "U2h7FliaezODyW18bbTo" using '10.120.12.41:1521/clic' ;
create  database link dblink_tcsbns connect to puppet identified by "U2h7FliaezODyW18bbTo" using '10.120.12.21:1521/tcsbns' ;
create  database link dblink_tcsvbs connect to puppet identified by "U2h7FliaezODyW18bbTo" using '10.120.12.21:1521/tcsvbs' ;

#查询目录名
conn puppet
set lines 200
col owner format a12
col directory_path format a80
select * from dba_directories;
create directory clic_dump as '/logic_dbbk/10.120.12.41/clic';
create directory tcsbns_dump as '/logic_dbbk/10.120.12.21/tcsbns';
create directory tcsvbs_dump as '/logic_dbbk/10.120.12.21/tcsvbs';

#建立测试账号
create user clic_test identified by clic_test;
grant connect,resource to clic_test;


5.程序中用到的查询

#连接数据库
sqlplus clic/'c43!cl76ic#)'@10.120.12.41:1521/clic


#查询要迁移的分区
select a.part_name from db_mgt_tab_part_config a ,db_mgt_tab_config b
  where a.db_mgt_tab_id=b.id
    and a.part_month<date_format(date_add(curdate(),interval -1 month), '%Y%m')
    and b.table_name='TC_SYS_MESSAGE_BOX'
    and a.flag='N' ;
   

#从dbant中获取任务运行参数--生产库
select  a.ip               as "prod_ip",
		    a.port             as "prod_port",
		    b.line_username    as "prod_user",
		    b.line_password    as "prod_pass",
		    b.db_service_name  as "prod_service",
		    concat(a.ip,':',a.port,'/',b.db_service_name) as "prod_db",
		    d.table_name    as "prod_tab"
	 from db_inst a,db_config b, db_mgt_config c, db_mgt_tab_config d 
	 where  a.id=b.db_inst_id
	   and  a.id =c.prod_inst_id
	   and  b.id =c.prod_config_id
	   and  c.id=d.db_mgt_id
	   and  c.id=1\G
                
 
#从dbant中获取任务运行参数--历史库
select  a.ip               as "hist_ip",
		    a.port             as "hist_port",
		    b.line_username    as "hist_user",
		    b.line_password    as "hist_pass",
		    c.hist_inst_name   as "hist_inst",
		    concat(a.ip,':',a.port,'/',b.db_service_name) as "hist_db",		    
		    c.dump_dir         as "dump_dir",
		    c.dblink_name      as "dblink_name"
	 from db_inst a,db_config b, db_mgt_config c, db_mgt_tab_config d 
	 where  a.id=b.db_inst_id
	   and  a.id =c.hist_inst_id
	   and  b.id =c.hist_config_id
	   and  c.id=d.db_mgt_id
	   and  c.id=1\G


#检测历史库链路是否存在
select count(0) 
           from dba_db_links 
           where username=upper('system') 
             and db_link=upper('dblink_clic')
             
             
#检测当天，同一配置任务是否已运行过任务
select count(0) 
  from db_mgt_task 
  where config_id=1 
  and date_format(creation_time, '%Y%m%d')=date_format(curdate(),'%Y%m%d')
  
#检测生产库某表使用哪些表空间
select tablespace_name
from dba_segments
where owner = 'CLIC'
 and segment_name = upper('TC_SYS_MESSAGE_BOX')
union 
select tablespace_name
  from dba_segments
 where owner = 'CLIC'
   and segment_name in
       (select segment_name
          from dba_lobs
         where owner = 'CLIC'
           and table_name = upper('TC_SYS_MESSAGE_BOX'))
union 
select tablespace_name
  from dba_segments
 where owner = 'CLIC'
   and segment_name in
       (select index_name
          from dba_ind_partitions
         where (index_owner, index_name) in
               (select owner, index_name
                  from dba_indexes
                 where owner = 'CLIC'
                   and table_name = 'TC_SYS_MESSAGE_BOX'))
union 
select tablespace_name
  from dba_segments
 where owner = 'CLIC'
   and segment_name in
       (select index_name
                  from dba_indexes
                 where owner = 'CLIC'
                   and table_name = 'TC_SYS_MESSAGE_BOX')
                   
                   
select max(id)
from db_mgt_log 
where task_id=1
  and tab_id=1
  and date_format(creation_time,'%Y%m%d')=date_format(curdate(),'%Y%m%d')
  and status='exp';
  
select a.part_name 
from db_mgt_tab_part_config a ,db_mgt_tab_config b
where a.db_mgt_tab_id=b.id
 and a.part_month<date_format(date_add(curdate(),interval -1 month), '%Y%m')
 and b.table_name='TC_SYS_MESSAGE_BOX'
 and a.flag='N'




#添加模板配置至字典中
dict['exp_tab_stru_path']='/ops/python/script/exp_tab_stru.par'
dict['exp_tab_data_path']='/ops/python/script/exp_tab_data.par'
dict['imp_tab_stru_path']='/ops/python/script/imp_tab_stru.par'
dict['imp_tab_data_path']='/ops/python/script/imp_tab_data.par' 

rm /logic_dbbk/10.120.12.41/clic/clic_TC_SYS_MESSAGE_BOX_20180312*


drop table if exists db_mgt_log_ext;
drop table if exists db_mgt_log;
drop table if exists db_mgt_task;
drop table if exists db_mgt_tab_part_config;
drop table if exists db_mgt_tab_config;
drop table if exists db_mgt_config;

select a.ip                            as "prod_ip",
       a.port                          as "prod_port",
       b.line_username                 as "prod_user",
       from_base64(b.line_password)    as "prod_pass", 
       b.db_service_name  as "prod_service",
       concat(a.ip,':',a.port,'/',b.db_service_name) as "prod_db",
       d.table_name       as "prod_tab"
from db_inst a,db_config b, db_mgt_config c, db_mgt_tab_config d 
where a.id=b.db_inst_id
 and  b.id =c.db_config_id_source
 and  c.id=d.db_mgt_config_id
 and  c.id=1;
 
select a.ip                             as "hist_ip",
	     a.port                           as "hist_port",
	     b.line_username                  as "hist_user",
	     from_base64(b.line_password)     as "hist_pass",
	     b.db_service_name                as "hist_inst",
	     concat(a.ip,':',a.port,'/',b.db_service_name) as "hist_db",
	     c.dump_dir                       as "dump_dir",
	     c.dblink_name                    as "dblink_name"
	from db_inst a,db_config b, db_mgt_config c, db_mgt_tab_config d 
	where  a.id=b.db_inst_id
   and  b.id =c.db_config_id_dest
   and  c.id=d.db_mgt_config_id
   and  c.id=1\G
 
 
#历史库中是否存在源库中的表空间
select distinct tablespace_name as tablespace_name
  from dba_segments
 where owner = 'CLIC'
   and segment_name = 'TC_SYS_MESSAGE_BOX';

#检测生产库中的表是否为分区表
select count(0)
  from dba_tab_partitions
 where table_owner = 'CLIC'
   and table_name = 'TC_SYS_MESSAGE_BOX';

   
#检测生产库中分区表的索引是否全部为本地索引   
select count(0)
  from DBA_PART_INDEXES
 where owner = 'CLIC'
   and table_name = 'TC_SYS_MESSAGE_BOX'
   and locality!='LOCAL' ;

#查询日志ID   
select max(id)
from db_mgt_log 
where db_mgt_task_id=11
  and date_format(create_time,'%Y%m%d')=date_format(curdate(),'%Y%m%d')
  and status='3';
  
#检测目标库中的表是分区名是否存在
select count(0)
  from dba_tab_partitions
 where table_owner = 'CLIC'
   and table_name = 'TC_SYS_MESSAGE_BOX'
   and partition_name='PAR_201109';


#检测导出的分区中是否存在活动事务
select count(0)
  from gv$lock
 where id1 = (select object_id
                from dba_objects
               where owner = 'CLIC'
                 and object_name = 'TC_SYS_MESSAGE_BOX'
                 and subobject_name = 'PAR_201801');
 
#检测导出文件是否存在 
select concat(path,'/',filename) 
from db_mgt_log_ext 
where db_mgt_log_id=(select max(id) from db_mgt_log
                     where db_mgt_task_id=16
                       and date_format(create_time,'%Y%m%d')=date_format(curdate(),'%Y%m%d')
                       and status='3'
                       and message='success!');
                       
#启动任务
select b.id as "db_mgt_tab_config_id"
from db_mgt_config a,db_mgt_tab_config b
where a.id=b.db_mgt_config_id
and a.run_day=1
order by b.db_mgt_config_id,b.id;

select b.db_mgt_config_id 
from db_mgt_config a,db_mgt_tab_config b
where a.id=b.db_mgt_config_id
and a.run_day={0}"""datetime.datetime.now().day;



#查询配置文件
select a.*
from db_mgt_tab_part_config a ,db_mgt_tab_config b
where a.db_mgt_tab_config_id=b.id
 and a.part_month<date_format(date_add(curdate(),interval -1 month), '%Y%m')
 and b.table_name ='TC_SYS_MESSAGE_BOX'
 and a.flag='N';
 
select a.*
from db_mgt_tab_part_config a ,db_mgt_tab_config b
where a.db_mgt_tab_config_id=b.id
 and a.part_month<date_format(date_add(curdate(),interval -3 month), '%Y%m')
 and b.table_name ='TC_LOG'
 and a.flag='N';
 
select a.*
from db_mgt_tab_part_config a ,db_mgt_tab_config b
where a.db_mgt_tab_config_id=b.id
 and a.part_month<date_format(date_add(curdate(),interval -3 month), '%Y%m')
 and b.table_name ='TEST_PART_MULTI_MONTH'
 and a.flag='N'; 
  
 
select a.*
from db_mgt_tab_part_config a ,db_mgt_tab_config b
where a.db_mgt_tab_config_id=b.id
 and b.table_name in('TC_SYS_MESSAGE_BOX','TC_LOG')
 and a.flag='Y';

#测试，修改配置文件为未迁移
#dbant
delete from db_mgt_log_ext;
delete from db_mgt_log;
delete from db_mgt_task; 

alter table tcsbns.BNSACCOPROC_TGJS  truncate partition PAR_201712_1;
alter table tcsbns.BNSACCOPROC_TGJS  truncate partition PAR_201712_2;
alter table tcsbns.BNSACCOPROC_TGJS  truncate partition PAR_201712_3;
alter table tcsbns.BNSACCOPROC_TGJS  truncate partition PAR_201712_4;

select count(0) from tcsbns.BNSACCOLIST_TGJS partition(PAR_201712_1);
select count(0) from TCSBNS.BNSACCOLIST_TGJS partition(PAR_201712_2);
select count(0) from TCSBNS.BNSACCOLIST_TGJS partition(PAR_201712_3);
select count(0) from TCSBNS.BNSACCOLIST_TGJS partition(PAR_201712_4);

select count(0) from tcsbns.BNSACCOLIST partition(PAR_201712_1);
select count(0) from TCSBNS.BNSACCOLIST partition(PAR_201712_2);
select count(0) from TCSBNS.BNSACCOLIST partition(PAR_201712_3);
select count(0) from TCSBNS.BNSACCOLIST partition(PAR_201712_4);

delete from db_mgt_config;
delete from db_mgt_tab_config;
delete from db_mgt_tab_part_config;

#回置表db_mgt_tab_part_config中的flag标志，用于测试
update db_mgt_tab_part_config set flag='Y'
  where db_mgt_tab_config_id='13' and part_name in('PAR_201712_1','PAR_201712_2','PAR_201712_3','PAR_201712_4');

#oracle
drop table TC_LOG purge;
drop table TC_SYS_MESSAGE_BOX purge;
drop table TC_LCSP_RATE purge;

#验证数据
select substr(create_date,1,7),count(0) 
from clic_test.TC_SYS_MESSAGE_BOX 
group by  substr(create_date,1,7);
having count(0)>0;

select to_char(OP_DATE,'yyyy.mm'),count(0) 
from clic_test.TC_LOG 
group by to_char(OP_DATE,'yyyy.mm')
having count(0)>0;

#查询有多个少子任务
select b.id as "db_mgt_tab_config_id"
from db_mgt_config a,db_mgt_tab_config b
where a.id=b.db_mgt_config_id
 and a.run_day=1
order by b.db_mgt_config_id,b.id

#检测目标库索引状态是否有效
select count(0)
  from (select status
          from dba_indexes
         where owner = 'TCSBNS'
           and table_name = 'BNSLISTDETAIL_TGJS'
           and status = 'UNUSABLE'
        union
        select distinct status
          from dba_ind_partitions
         where (index_owner, index_name) in
               (select owner, index_name
                  from dba_indexes
                 where owner = 'TCSBNS'
                   and table_name = 'BNSLISTDETAIL_TGJS')
           and status = 'UNUSABLE');

           
#计算迁移分区的大小
select sum(bytes) / 1024 / 1024 
  from (
select bytes
  from dba_segments
 where owner = 'TCSBNS'
   and segment_name = 'BNSACCOPROC_TGJS'
   and partition_name in
       ('PAR_201712_1', 'PAR_201712_2', 'PAR_201712_3', 'PAR_201712_4')
union all       
select bytes
  from dba_segments
 where (owner, segment_name) in
       (select owner, segment_name
          from dba_lobs
         where owner = 'TCSBNS'
           and table_name = upper('BNSACCOPROC_TGJS'))
   and partition_name in
       ('PAR_201712_1', 'PAR_201712_2', 'PAR_201712_3', 'PAR_201712_4')
union all                  
 select bytes
   from dba_segments
  where (owner,segment_name) in
         (select owner,index_name
           from dba_indexes
          where owner = 'TCSBNS'
            and table_name = 'BNSACCOPROC_TGJS')
    and partition_name in
        ('PAR_201712_1', 'PAR_201712_2', 'PAR_201712_3', 'PAR_201712_4')
);

#当前任务不包含当天已执行成功的任务
select b.id as "db_mgt_tab_config_id"
from db_mgt_config a,db_mgt_tab_config b
where a.id=b.db_mgt_config_id
 and a.run_day=2           
 and b.id not in(select db_mgt_tab_config_id 
                 from db_mgt_task
                 where date_format(create_time,'%Y%m%d')=date_format(curdate(),'%Y%m%d')
                   and status='8');   
                   
                   
#db4-253清空分区重新导入
alter table clic.TC_SYS_MESSAGE_BOX     truncate partition (PAR_201801) ;
alter table clic.TC_SYS_MESSAGE_BOX     truncate partition (PAR_201802) ;
alter table clic.TC_LOG                 truncate partition (PAR_201711) ;
alter table clic.TC_LOG                 truncate partition (PAR_201712) ;
alter table clic.TC_LCSP_RATE           truncate partition (PAR_201801) ;
alter table clic.TC_LCSP_RATE           truncate partition (PAR_201802) ;
alter table clic.TC_CREDIT_GRADE_DETAIL truncate partition (PAR_201801) ;
alter table clic.TC_CREDIT_GRADE_DETAIL truncate partition (PAR_201802) ;
alter table clic.TC_PBOC_C_LOAN_SUMMARY truncate partition (PAR_201711) ;
alter table clic.TC_PBOC_C_LOAN_SUMMARY truncate partition (PAR_201712) ;
alter table clic.TC_PBOC_C_LOAN_DETAIL  truncate partition (PAR_201711) ;
alter table clic.TC_PBOC_C_LOAN_DETAIL  truncate partition (PAR_201712) ;
alter table clic.TC_PBOC_C_QUERY_HIS    truncate partition (PAR_201711) ;
alter table clic.TC_PBOC_C_QUERY_HIS    truncate partition (PAR_201712) ;

#xhx-tcsbns清空分区重新导入
alter table tcsbns.BNSACCOLIST         truncate partition PAR_201712_1; 
alter table tcsbns.BNSACCOLIST         truncate partition PAR_201712_2;
alter table tcsbns.BNSACCOLIST         truncate partition PAR_201712_3;
alter table tcsbns.BNSACCOLIST         truncate partition PAR_201712_4;
alter table tcsbns.BNSACCOPROC         truncate partition PAR_201712_1;
alter table tcsbns.BNSACCOPROC         truncate partition PAR_201712_2;
alter table tcsbns.BNSACCOPROC         truncate partition PAR_201712_3;
alter table tcsbns.BNSACCOPROC         truncate partition PAR_201712_4;
alter table tcsbns.BNSLISTDETAIL       truncate partition PAR_201712_1;
alter table tcsbns.BNSLISTDETAIL       truncate partition PAR_201712_2;
alter table tcsbns.BNSLISTDETAIL       truncate partition PAR_201712_3;
alter table tcsbns.BNSLISTDETAIL       truncate partition PAR_201712_4;
alter table tcsbns.BNSACCOLIST_TGJS    truncate partition PAR_201712_1;
alter table tcsbns.BNSACCOLIST_TGJS    truncate partition PAR_201712_2;
alter table tcsbns.BNSACCOLIST_TGJS    truncate partition PAR_201712_3;
alter table tcsbns.BNSACCOLIST_TGJS    truncate partition PAR_201712_4;
alter table tcsbns.BNSACCOPROC_TGJS    truncate partition PAR_201712_1;  
alter table tcsbns.BNSACCOPROC_TGJS    truncate partition PAR_201712_2;  
alter table tcsbns.BNSACCOPROC_TGJS    truncate partition PAR_201712_3;  
alter table tcsbns.BNSACCOPROC_TGJS    truncate partition PAR_201712_4;
alter table tcsbns.BNSLISTDETAIL_TGJS  truncate partition PAR_201712_1 update global indexes;
alter table tcsbns.BNSLISTDETAIL_TGJS  truncate partition PAR_201712_2 update global indexes;
alter table tcsbns.BNSLISTDETAIL_TGJS  truncate partition PAR_201712_3 update global indexes;
alter table tcsbns.BNSLISTDETAIL_TGJS  truncate partition PAR_201712_4 update global indexes;


#xhx-tcsvbs清空分区重新导入
