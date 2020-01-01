#测试测试数据，测试以下内容
1.源库分区表分区键内容是否可以转为合法日期类型
2.源库分区表中某一分区是否存在多月数据

create table clic.test_part_multi_month (
 id        int primary key,
 create_date   date
)
partition by range (create_date)
(  
  partition PAR1     values less than (TO_DATE(' 2017-04-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')) ,
  partition PAR2     values less than (TO_DATE(' 2017-08-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')) ,
  partition PAR3     values less than (TO_DATE(' 2017-11-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')) ,
  partition PAR_MAX  values less than (MAXVALUE) 
);

insert into clic.test_part_multi_month(id,create_date) values(1,to_date('20170101','yyyymmdd'));
insert into clic.test_part_multi_month(id,create_date) values(2,to_date('20170201','yyyymmdd'));
insert into clic.test_part_multi_month(id,create_date) values(3,to_date('20170301','yyyymmdd'));
insert into clic.test_part_multi_month(id,create_date) values(4,to_date('20170401','yyyymmdd'));
insert into clic.test_part_multi_month(id,create_date) values(5,to_date('20170501','yyyymmdd'));
insert into clic.test_part_multi_month(id,create_date) values(6,to_date('20170601','yyyymmdd'));
insert into clic.test_part_multi_month(id,create_date) values(7,to_date('20170701','yyyymmdd'));
insert into clic.test_part_multi_month(id,create_date) values(8,to_date('20170801','yyyymmdd'));
insert into clic.test_part_multi_month(id,create_date) values(9,to_date('20170901','yyyymmdd'));
insert into clic.test_part_multi_month(id,create_date) values(10,to_date('20171001','yyyymmdd'));
insert into clic.test_part_multi_month(id,create_date) values(11,to_date('20171101','yyyymmdd'));
insert into clic.test_part_multi_month(id,create_date) values(12,to_date('20171201','yyyymmdd'));
commit;

set serveroutput on 
declare
  v_sql varchar2(200);
  n_rec int;
begin
  for i in (select table_owner,table_name,partition_name
            from dba_tab_partitions
           where table_owner = 'CLIC'
             and table_name = 'TEST_PART_MULTI_MONTH' order by partition_name) loop
      v_sql:='select count(0) from '||i.table_owner||'.'||i.table_name||' partition('||i.partition_name||')';
      execute immediate v_sql  into n_rec ;
      dbms_output.put_line(i.table_owner||'.'||i.table_name||'.'||i.partition_name||':'||n_rec);
   end loop;
end;
/
CLIC.TEST_PART_MULTI_MONTH.PAR1:3
CLIC.TEST_PART_MULTI_MONTH.PAR2:4
CLIC.TEST_PART_MULTI_MONTH.PAR3:3
CLIC.TEST_PART_MULTI_MONTH.PAR_MAX:2
PL/SQL procedure successfully completed

#增加检测函数：源库分区表是否存在授权引用的用户
select count(0) from dba_tab_privs
 where owner='CLIC' and table_name='TC_SYS_MESSAGE_BOX';
 
 
--20180416准备数据，测试不规范日期，字符或数值转日期时出错的情况
create table CLIC.TEST_PART_MULTI_MONTHV
(
  id          INTEGER not null primary key,
  create_date varchar2(20)
)
partition by range (CREATE_DATE)
(
  partition PAR1 values less than ('2017-04-01 00:00:00') ,
  partition PAR2 values less than ('2017-08-01 00:00:00') ,
  partition PAR3 values less than ('2017-11-01 00:00:00') , 
  partition PAR_MAX values less than (MAXVALUE) 
);

insert into CLIC.TEST_PART_MULTI_MONTHV(id,create_date) values(1,'2017-03-10');
insert into CLIC.TEST_PART_MULTI_MONTHV(id,create_date) values(2,'2017-05-32');
insert into CLIC.TEST_PART_MULTI_MONTHV(id,create_date) values(3,'2017-11-44');
insert into CLIC.TEST_PART_MULTI_MONTHV(id,create_date) values(4,'2018-01-44');
commit;

create table CLIC.TEST_PART_MULTI_MONTHI
(
  id          INTEGER not null primary key,
  create_date number
)
partition by range (CREATE_DATE)
(
  partition PAR1 values less than (201701),
  partition PAR2 values less than (201704),
  partition PAR3 values less than (201708), 
  partition PAR_MAX values less than (MAXVALUE) 
);

insert into CLIC.TEST_PART_MULTI_MONTHI(id,create_date) values(1,201611);
insert into CLIC.TEST_PART_MULTI_MONTHI(id,create_date) values(2,201713);
insert into CLIC.TEST_PART_MULTI_MONTHI(id,create_date) values(3,201723);
insert into CLIC.TEST_PART_MULTI_MONTHI(id,create_date) values(4,201801);
commit;


--测试个月多分区
insert into db_mgt_tab_config(db_mgt_config_id,table_name,keep_time_source,keep_time_dest,creator)
values(1,'TEST_PART_MULTI_MONTH',1,2,'dba');

--数据分区数据法合法，字符无法转为日期
insert into db_mgt_tab_config(db_mgt_config_id,table_name,keep_time_source,keep_time_dest,creator)
values(1,'TEST_PART_MULTI_MONTHV',1,0,'dba');

--数据分区数据法合法，数值无法转为日期
insert into db_mgt_tab_config(db_mgt_config_id,table_name,keep_time_source,keep_time_dest,creator)
values(1,'TEST_PART_MULTI_MONTHI',1,0,'dba');


--个月多分区测试数据
insert into db_mgt_tab_part_config(db_mgt_tab_config_id,part_name,part_month,flag,creator) values('3','PAR1','201701','N','dba'); 
insert into db_mgt_tab_part_config(db_mgt_tab_config_id,part_name,part_month,flag,creator) values('3','PAR2','201704','N','dba'); 
insert into db_mgt_tab_part_config(db_mgt_tab_config_id,part_name,part_month,flag,creator) values('3','PAR3','201709','N','dba'); 
insert into db_mgt_tab_part_config(db_mgt_tab_config_id,part_name,part_month,flag,creator) values('3','PAR_MAX','201712','N','dba'); 


--分区数据不合法测试数据-字符型
insert into db_mgt_tab_part_config(db_mgt_tab_config_id,part_name,part_month,flag,creator) values('4','PAR1','201703','N','dba'); 
insert into db_mgt_tab_part_config(db_mgt_tab_config_id,part_name,part_month,flag,creator) values('4','PAR2','201705','N','dba'); 
insert into db_mgt_tab_part_config(db_mgt_tab_config_id,part_name,part_month,flag,creator) values('4','PAR3','201711','N','dba'); 
insert into db_mgt_tab_part_config(db_mgt_tab_config_id,part_name,part_month,flag,creator) values('4','PAR_MAX','201801','N','dba'); 

--分区数据不合法测试数据-数值型
insert into db_mgt_tab_part_config(db_mgt_tab_config_id,part_name,part_month,flag,creator) values('5','PAR1','201611','N','dba'); 
insert into db_mgt_tab_part_config(db_mgt_tab_config_id,part_name,part_month,flag,creator) values('5','PAR2','201713','N','dba'); 
insert into db_mgt_tab_part_config(db_mgt_tab_config_id,part_name,part_month,flag,creator) values('5','PAR3','201723','N','dba'); 
insert into db_mgt_tab_part_config(db_mgt_tab_config_id,part_name,part_month,flag,creator) values('5','PAR_MAX','201801','N','dba'); 


#删除测试数据
delete from db_mgt_tab_part_config where db_mgt_tab_config_id in('3','4','5');
delete from db_mgt_tab_config where  id in('3','4','5');