#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
  功能：从源库将分区表中满足保留策略的分区迁移至目标库
  说明：只支持Oracle数据库分区表进行迁移，具体限制详见check()方法

'''
import cx_Oracle
import traceback
import pymysql
import os
import sys
import datetime

'''
  功能：根据不同类型返回数据库连接对象
  入口：dict  :字典配置
        v_type:数据源类型
  出口：数据库连接对象
'''
def get_db(dict,v_type):
    if v_type=="dbant":
       return pymysql.connect(host="10.120.12.253", port=3306, user="dbant", passwd="dbant", db="dbant")
    elif v_type=="source":
       return cx_Oracle.connect(dict['source_user'],
                                dict['source_pass'],
                                dict['source_ip']+':'+dict['source_port']+'/'+dict['source_service'])
    elif v_type=="source_mgr":
       return cx_Oracle.connect(dict['source_mgr_user'],
                                dict['source_mgr_pass'],
                                dict['source_ip']+':'+dict['source_port']+'/'+dict['source_service'])
    elif v_type=="dest":
       return cx_Oracle.connect(dict['dest_user'],
                                dict['dest_pass'],
                                dict['dest_ip']+':'+dict['dest_port']+'/'+dict['dest_inst'])
    elif v_type=="dest_mgr":
       return cx_Oracle.connect(dict['dest_mgr_user'],
                                dict['dest_mgr_pass'],
                                dict['dest_ip']+':'+dict['dest_port']+'/'+dict['dest_inst'])



'''
  功能：根据db_mgt_tab_config.id 返回当前任务配置文件
  入口：p_id  :db_mgt_tab_config.id 
  出口：字典配置文件
'''
def read_para(p_id):
    v_sql_source="""
           select a.ip                            as "source_ip",
                  a.port                          as "source_port",
                  b.line_username                 as "source_user",
                  cast(from_base64(b.line_password) as char)  as "source_pass",
                  "puppet"                        as "source_mgr_user",
                  "U2h7FliaezODyW18bbTo"          as "source_mgr_pass", 
                  b.db_service_name               as "source_service",
                  concat(a.ip,':',a.port,'/',b.db_service_name) as "source_db",
                  d.table_name                    as "source_tab",
                  d.keep_time_source              as "source_keep_time"
           from db_inst a,db_config b,db_mgt_config c, db_mgt_tab_config d 
           where a.id=b.db_inst_id
            and  b.id =c.db_config_id_source
            and  c.id=d.db_mgt_config_id
            and  d.id={0} 
          """.format(p_id)

    v_sql_dest="""
           select a.ip                              as "dest_ip",
                  a.port                            as "dest_port",
                  b.line_username                   as "dest_user",
                  cast(from_base64(b.line_password) as char)  as "dest_pass",
                  "puppet"                          as "dest_mgr_user",
                  "U2h7FliaezODyW18bbTo"            as "dest_mgr_pass",
                  b.db_service_name                 as "dest_inst",
                  d.keep_time_dest                  as "dest_keep_time",
                  concat(a.ip,':',a.port,'/',b.db_service_name) as "dest_db",
                  c.dump_dir                        as "dump_dir",
                  c.dblink_name                     as "dblink_name",
                  d.id                              as "db_mgt_tab_config_id"
          from db_inst a,db_config b, db_mgt_config c, db_mgt_tab_config d 
          where  a.id=b.db_inst_id
            and  b.id =c.db_config_id_dest
            and  c.id=d.db_mgt_config_id
            and  d.id={0}  
          """.format(p_id)

    conn=get_db({},'dbant')
    dict={}
    cr= conn.cursor()
    cr.execute(v_sql_source)
    desc=cr.description
    rs=cr.fetchone()
    for i in range(len(rs)):
        dict[desc[i][0]]=rs[i] 
    
    cr.execute(v_sql_dest)
    desc=cr.description
    rs=cr.fetchone()
    for i in range(len(rs)):
        dict[desc[i][0]]=rs[i]
    cr.close()

    #添加数据库连接对象至字典中
    dict['db_dbant']     =get_db(dict,'dbant')
    dict['db_source']    =get_db(dict,'source')
    dict['db_source_mgr']=get_db(dict,'source_mgr')
    dict['db_dest']      =get_db(dict,'dest')
    dict['db_dest_mgr']  =get_db(dict,'dest_mgr')
    
    #添加导出导入参数配置至字典中
    dict['exp_cfg_file']='./script/exp.par'
    dict['imp_cfg_file']='./script/imp.par'
    
    #添加分区信息至字典中
    dict['source_part_key']      =get_source_part_col(dict)
    dict['source_part_key_type'] =get_source_part_col_type(dict)
    dict['source_part_names']    =get_part_names(dict)
    dict['source_part_names_fmt']=get_part_names_fmt(dict)
    dict['source_part_size']     =get_part_size(dict)
    return dict


'''
  功能：打印当前任务配置文件
  入口：dict:当前任务字典配置文件 
  出口：无
'''
def print_para(dict):
    os.system('clear')
    print("")
    print("print task_id:{0} parameter from dbant...".format(dict['db_mgt_task_id']))
    print('-'.ljust(85,'-'))
    print(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    print('-'.ljust(85,'-'))
    for key in dict:
      print(' '.ljust(3,' ')+key.ljust(20,' ')+'=',dict[key])
    print('-'.ljust(85,'-'))


'''
  功能：获取当前任务当前日志ID
  入口：p_dict:字典配置文件
        status:当前日志状态
  出口：当前 db_mgt_log.id
'''
def get_logid(p_dict,status):
    db = p_dict['db_dbant']
    sql ="""select max(id)
            from db_mgt_log 
            where db_mgt_task_id={0}
              and date_format(create_time,'%Y%m%d')=date_format(curdate(),'%Y%m%d')
              and status='{1}'""".format(p_dict['db_mgt_task_id'],status)
    cr=db.cursor()
    cr.execute(sql)
    rs=cr.fetchone()
    return rs[0]


'''
  功能：写任务日志
  入口：p_dict    :字典配置文件
        status    :当前日志状态
        message   :日志内容
        begin_time:日志开始时间
        end_time  :日志结束时间
  出口：当前日志ID
'''
def write_log(p_dict,status,message,begin_time,end_time):
    db = p_dict['db_dbant']
    cr = db.cursor()
    n_row=cr.execute("""insert into db_mgt_log(db_mgt_task_id,message,status,begin_time,end_time) 
                          values('{0}','{1}','{2}','{3}','{4}')
                     """.format(p_dict['db_mgt_task_id'],message,status,begin_time,end_time))
    id = int(cr.lastrowid)
    db.commit()
    cr.close()
    return id


'''
  功能：检测最近2天内当前任务是否存在
  入口：p_dict:字典配置文件
  出口：0:不存在,1:存在
'''
def check_task_exist(p_dict):
    db = p_dict['db_dbant']
    sql ="""select count(0)
            from db_mgt_task 
            where db_mgt_tab_config_id={0} 
              and date_format(create_time,'%Y%m%d')>=date_format(curdate()-1,'%Y%m%d')
              and date_format(create_time,'%Y%m%d')<=date_format(curdate(),'%Y%m%d')
         """.format(p_dict['db_mgt_tab_config_id'])
    cr=db.cursor()
    cr.execute(sql) 
    rs=cr.fetchone()
    return rs[0]


'''
  功能：获取当前任务ID
  入口：p_dict:字典配置文件
  出口：当前db_mgt_task.id
'''
def get_task_id(p_dict):
    db = p_dict['db_dbant']
    sql ="""select id 
            from db_mgt_task 
            where db_mgt_tab_config_id={0} 
              and date_format(create_time,'%Y%m%d')=date_format(curdate(),'%Y%m%d') limit 1
         """.format(p_dict['db_mgt_tab_config_id'])
    cr=db.cursor()
    cr.execute(sql)
    rs=cr.fetchone()
    return rs[0]


'''
  功能：写任务表
  入口：p_dict:字典配置文件
        status:当前日志状态
  出口：0:成功，1：失败
'''
def write_task(p_dict,status):
    db = p_dict['db_dbant']
    begin_time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    try:
       if (check_task_exist(p_dict)==0):
          sql="""insert into db_mgt_task(db_mgt_tab_config_id,status) 
                       values('{0}','{1}')""".format(p_dict['db_mgt_tab_config_id'],status)
          cr = db.cursor()
          cr.execute(sql)
          id = int(cr.lastrowid)
          db.commit()
          end_time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
          p_dict['db_mgt_task_id']=get_task_id(p_dict)
          write_log(p_dict,'1','success!',begin_time,end_time)
       else:
          p_dict['db_mgt_task_id']=get_task_id(p_dict)
          sql="update db_mgt_task set last_update_time=now(),status='{0}' where id={1}".format(status,p_dict['db_mgt_task_id'])
          cr = db.cursor()
          cr.execute(sql)
          db.commit()
       return 0
    except:
       end_time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
       write_log(p_dict,'1',traceback.format_exc(),begin_time,end_time)
       return -1

'''
  功能：获取当前任务源库的表空间列表
  入口：p_dict:当前任务字典配置文件
  出口：逗号分隔的源库表空间名称
'''
def get_source_tablespace(p_dict):
    sql="""select distinct tablespace_name as tablespace_name
           from dba_segments
           where owner =upper('{0}')
           and segment_name =upper('{1}')
        """.format(p_dict['source_user'],p_dict['source_tab'])
    db=p_dict['db_source_mgr']
    cr=db.cursor()
    cr.execute(sql)
    rs=cr.fetchall()
    tmp=''
    for i in rs:
      for j in range(len(i)):
         tmp=tmp+str(i[j])+','
    return tmp[0:-1]


'''
  功能：检测当前任务源库表空间对应的目标库表空间是否存在
  入口：p_dict:当前任务字典配置文件
  出口：逗号分隔的目标库表空间名称
'''
def check_dest_tablespace(p_dict):
    db=p_dict['db_dest_mgr']
    cr=db.cursor()
    for tbs in get_source_tablespace(p_dict).split(','):
      sql="""select count(0) 
             from dba_tablespaces 
             where tablespace_name=upper('{0}')""".format(tbs)
      cr.execute(sql)
      rs=cr.fetchone()
      if rs[0]==0:
        p_dict['dest_tablespace'] =tbs
        return 0
    return 1


'''
  功能：检测目标库用户是否存在
  入口：p_dict:当前任务字典配置文件
  出口：1:存在，0:不存在
'''
def check_dest_user_exist(p_dict):
    sql="""select count(0) 
           from dba_users 
           where username=upper('{0}')""".format(p_dict['dest_user'])
    db=p_dict['db_dest_mgr']
    cr=db.cursor()
    cr.execute(sql)
    rs=cr.fetchone()
    return rs[0]


'''
  功能：检测目标库表是否存在
  入口：p_dict:当前任务字典配置文件
  出口：1:存在，0:不存在
'''
def check_dest_table_exist(p_dict):
    sql="""select count(0) 
           from dba_tables 
           where owner=upper('{0}')
             and table_name=upper('{1}')""".format(p_dict['dest_user'],p_dict['source_tab'])
    db=p_dict['db_dest_mgr']
    cr=db.cursor()
    cr.execute(sql)
    rs=cr.fetchone()
    return rs[0]

'''
  功能：检测源库中的表是否存在
  入口：p_dict:当前任务字典配置文件
  出口：1:存在，0:不存在
'''
def check_source_tab_exist(p_dict):
    sql="""select count(0) 
           from dba_tables 
           where owner=upper('{0}')
             and table_name=upper('{1}')""".format(p_dict['source_user'],p_dict['source_tab'])
    db=p_dict['db_source_mgr']
    cr=db.cursor()
    cr.execute(sql)
    rs=cr.fetchone()
    return rs[0]


'''
  功能：检测源库中的表是否为分区表
  入口：p_dict:当前任务字典配置文件
  出口：>0:分区表，0:非分区表
'''
def check_source_tab_part(p_dict):
    sql="""select count(0)
           from dba_tab_partitions
           where table_owner=upper('{0}')
             and table_name =upper('{1}')""".format(p_dict['source_user'],p_dict['source_tab'])
    db=p_dict['db_source_mgr']
    cr=db.cursor()
    cr.execute(sql)
    rs=cr.fetchone()
    return rs[0]


'''
  功能：检测目标库puppet用户下是否存在相应数据库链路
  入口：p_dict:当前任务字典配置文件
  出口：>0:存在，0:不存在
'''
def check_dest_dblink(p_dict):
    sql="""select count(0) 
           from dba_db_links 
           where owner=upper('{0}')
             and username=upper('{1}')
             and db_link=upper('{2}')
        """.format(p_dict['dest_mgr_user'],p_dict['dest_mgr_user'],p_dict['dblink_name'])
    db=p_dict['db_dest_mgr']
    cr=db.cursor()
    cr.execute(sql)
    rs=cr.fetchone()
    return rs[0]



'''
  功能：检测目标库puppet用户下是否存在相应目录对象
  入口：p_dict:当前任务字典配置文件
  出口：>0:存在，0:不存在
'''
def check_dest_dumpdir(p_dict):
    sql="""select count(0) 
           from dba_directories 
           where owner='SYS' and directory_name=upper('{0}')""".format(p_dict['dump_dir'])
    db=p_dict['db_dest_mgr']
    cr=db.cursor()
    cr.execute(sql)
    rs=cr.fetchone()
    return rs[0]


'''
  功能：检测目标库puppet用户下是的目录对象指向的路径是否有效
  入口：p_dict:当前任务字典配置文件
  出口：0:有效，1:无效
'''
def check_dumpdir_exist(p_dict):
    if os.system("ls "+get_real_path(p_dict)+" >/dev/null 2>&1")==0:
       return 0
    else:
       return 1


'''
  功能：检测目标库业务用户中的分区表中特定分区中是否存在数据
  入口：p_dict:当前任务字典配置文件
  出口：0:存在，1:不存在
'''
def check_dest_tab_part(p_dict):
    db=p_dict['db_dest_mgr']
    cr=db.cursor()
    part_names=get_part_names(p_dict)
    for part_name in part_names.split(','):
      sql="""select count(0)
              from dba_tab_partitions
              where table_owner =upper('{0}')
                and table_name = upper('{1}')
                and partition_name=upper('{2}')
          """.format(p_dict['dest_user'],p_dict['source_tab'],part_name)
      cr.execute(sql)
      rs=cr.fetchone()
      if rs[0]==1:
        cr.execute("select count(0) from {0}.{1} partition({2})".format(p_dict['dest_user'],p_dict['source_tab'],part_name))
        rs=cr.fetchone()
        if rs[0]>0:
          p_dict['dest_part_name']=part_name
          return 0
    return 1


'''
  功能：获取子任务中的分区表中的所有索引名列表，以逗号分隔
  入口：p_dict  ：当前任务配置文件
  出口：逗号分隔的索引名称
'''
def get_source_index_names(p_dict):
    db=p_dict['db_source_mgr']
    cr=db.cursor()
    sql="""select index_name from dba_indexes where owner = upper('{0}') and table_name = upper('{1}')
        """.format(p_dict['source_user'],p_dict['source_tab'])
    cr.execute(sql)
    rs=cr.fetchall()
    tmp=''
    for i in rs:
      for j in range(len(i)):
         tmp=tmp+str(i[j])+','
    return tmp[0:-1]
     

'''
  功能：检测源库分区表中的索引是否全部为本地索引
  入口：p_dict:当前任务字典配置文件
  出口：0:否，1:是
'''
def check_source_local_index(p_dict):
    db=p_dict['db_source_mgr']
    cr=db.cursor()
    for index_name in get_source_index_names(p_dict).split(','): 
        sql="""select count(0) from DBA_IND_PARTITIONS where index_owner =upper('{0}') and index_name =upper('{1}')            
            """.format(p_dict['source_user'],index_name)
        cr.execute(sql)
        rs=cr.fetchone()
        if rs[0]==0:
           return 0
    return 1   


'''
  功能：检测源库分区表中本次迁移的分区上是否存在活动事务
  入口：p_dict:当前任务字典配置文件
  出口：0:存在，1:不存在
'''
def check_source_active_trans(p_dict):
    db=p_dict['db_source_mgr']
    cr=db.cursor()
    part_names=get_part_names(p_dict)
    for part_name in part_names.split(','):
      sql="""select count(0)
             from gv$lock
             where id1 = (select object_id
                          from dba_objects
                          where owner =upper('{0}')
                            and object_name =upper('{1}')
                            and subobject_name =upper('{2}'))
          """.format(p_dict['source_user'],p_dict['source_tab'],part_name)
      cr.execute(sql)
      rs=cr.fetchone()
      if rs[0]>0:
         return 0
    return 1

'''
  功能：检测目标库分区表中本次迁移的分区上是否存在活动事务
  入口：p_dict:当前任务字典配置文件
  出口：0:存在，1:不存在
'''
def check_dest_active_trans(p_dict):
    db=p_dict['db_dest_mgr']
    cr=db.cursor()
    part_names=get_dest_part_names(p_dict)
    for part_name in part_names.split(','):
      sql="""select count(0)
             from gv$lock
             where id1 = (select object_id
                          from dba_objects
                          where owner =upper('{0}')
                            and object_name =upper('{1}')
                            and subobject_name =upper('{2}'))
          """.format(p_dict['dest_user'],p_dict['source_tab'],part_name)
      cr.execute(sql)
      rs=cr.fetchone()
      if rs[0]>0:
         return 0
    return 1


'''
  功能：检测源库分区表是否授于其它用户对象权限
  入口：p_dict:当前任务字典配置文件
  出口：0:否，1:是
'''
def check_source_part_grants(p_dict):
    db=p_dict['db_source_mgr']
    cr=db.cursor()
    sql="""select count(0)
           from DBA_TAB_PRIVS
           where owner =upper('{0}')
             and table_name =upper('{1}')
        """.format(p_dict['source_user'],p_dict['source_tab'])
    cr.execute(sql)
    rs=cr.fetchone()
    return rs[0]


'''
  功能：获取源库字符集
  入口：p_dict:当前任务字典配置文件
  出口：字符集
'''
def get_source_charset(p_dict):
    db=p_dict['db_source_mgr']
    cr=db.cursor()
    sql="select property_value from database_properties where property_name='NLS_CHARACTERSET'"
    cr.execute(sql)
    rs=cr.fetchone()
    return rs[0]


'''
  功能：获取目标库字符集
  入口：p_dict:当前任务字典配置文件
  出口：字符集
'''
def get_dest_charset(p_dict):
    db=p_dict['db_dest_mgr']
    cr=db.cursor()
    sql="select property_value from database_properties where property_name='NLS_CHARACTERSET'"
    cr.execute(sql)
    rs=cr.fetchone()
    return rs[0]


'''
  功能：检测源库字符与目标库字符集是否一致
  入口：p_dict:当前任务字典配置文件
  出口：0:一致，1:不一致
'''
def check_source_dest_charset(p_dict):
    if get_source_charset(p_dict)==get_dest_charset(p_dict):
       return 0
    else:
       return 1


'''
  功能：获取目标库当前任务中的分区表索引是否有效
  入口：p_dict:当前任务字典配置文件
  出口：字符集
'''
def check_dest_index_status(p_dict):
    db=p_dict['db_dest_mgr']
    cr=db.cursor()
    sql="""select count(0)
             from (select status
                    from dba_indexes
                        where owner=upper('{0}')
                          and table_name=upper('{1}')
                          and status = 'UNUSABLE'
                  union
                   select distinct status
                    from dba_ind_partitions
                        where (index_owner, index_name) in
                           (select owner, index_name
                            from dba_indexes
                            where owner=upper('{2}')
                              and table_name=upper('{3}'))
                              and status = 'UNUSABLE')
        """.format(p_dict['source_user'],p_dict['source_tab'],p_dict['source_user'],p_dict['source_tab'])
    cr.execute(sql)
    rs=cr.fetchone()
    return rs[0]


'''
  功能：检测主函数
  入口：p_dict:当前任务字典配置文件
  出口：0:成功，1:失败
'''
def check(p_dict):
    n_status=0
    message=""
    v_message=""
    begin_time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    if get_part_names(p_dict)=='':
       message="""\nsource:''{0}'' table: ''{1}'' no partitions that need to be migrated!
               """.format(p_dict['source_db'],p_dict['source_tab'])
       v_message=v_message+message
       end_time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
       write_log(p_dict,'2',v_message,begin_time,end_time)
       write_task(p_dict,'2')
       return 1
   
    if check_source_dest_charset(p_dict)==1:
       message="""\nsource:''{0}'' charset is: ''{1}'',dest:''{2}'' charset is:''{3}'' not same!""" \
               .format(p_dict['source_db'],get_source_charset(p_dict),p_dict['dest_db'],get_dest_charset(p_dict))
       v_message=v_message+message
       n_status=1
 
    if check_source_tab_exist(p_dict)==0:
       message="\nsource:''{0}'' table: ''{1}'' not exist!".format(p_dict['source_db'],p_dict['source_tab'])
       v_message=v_message+message
       n_status=1

    if check_source_tab_part(p_dict)==0:
       message="\nsource:''{0}'' table: ''{1}'' not partition table!".format(p_dict['source_db'],p_dict['source_tab'])
       v_message=v_message+message
       n_status=1
    ''' 
    if check_source_local_index(p_dict)==0:
       message="\nsource:''{0}'' table: ''{1}'' exist no local index!".format(p_dict['source_db'],p_dict['source_tab'])
       v_message=v_message+message
       n_status=1
    '''
    
    if check_source_active_trans(p_dict)==0:
       message="\nsource:''{0}'' table: ''{1}'' exist active transaction!".format(p_dict['source_db'],p_dict['source_tab'])
       v_message=v_message+message
       n_status=1

    if check_source_part_valid(p_dict)==1:
       message="""\nsource:''{0}'' table: ''{1}'' partition: {2} key:{3} cannot convert to date type!
               """.format(p_dict['source_db'],p_dict['source_tab'],p_dict['source_part_name'],p_dict['source_part_key'])
       v_message=v_message+message
       n_status=1
    elif check_source_part_multi_month(p_dict)==1:
       message="""\nsource:''{0}'' table: ''{1}'' partition: {2} exists {3} months data!
               """.format(p_dict['source_db'],p_dict['source_tab'],p_dict['source_part_name'],p_dict['source_part_record'])
       v_message=v_message+message
       n_status=1

    if check_dest_tablespace(p_dict)==0:
       message="\ndest:''{0}'' tablespace ''{1}'' not exists!".format(p_dict['dest_db'],p_dict['dest_tablespace'])
       v_message=v_message+message
       n_status=1
    
    if check_dest_user_exist(p_dict)==0:
       message="\ndest:''{0}'' user ''{1}'' not exists!".format(p_dict['dest_db'],p_dict['dest_user'])
       v_message=v_message+message
       n_status=1  
   
    if check_dest_dblink(p_dict)==0:
       message="\ndest:{0}  dblink ''{1}'' not exists!".format(p_dict['dest_db'],p_dict['dblink_name'])
       v_message=v_message+message
       n_status=1

    if check_dest_dumpdir(p_dict)==0:
       message="\ndest:''{0}'' dumpdir ''{1}'' not exists!".format(p_dict['dest_db'],p_dict['dump_dir'])
       v_message=v_message+message
       n_status=1
  
    if check_dumpdir_exist(p_dict)==1:
       message="""\ndest:''{0}'' dumpdir: ''{1}'' path: ''{2}'' is invalid!""" \
       .format(p_dict['dest_db'],p_dict['dump_dir'],get_real_path(p_dict))
       v_message=v_message+message
       n_status=1

    if check_dest_tab_part(p_dict)==0:
       message="""\ndest:''{0}'' table: ''{1}'' partition ''{2}'' data already exists!""" \
       .format(p_dict['dest_db'],p_dict['source_tab'],p_dict['dest_part_name'])
       v_message=v_message+message
       n_status=1  

    if check_dest_index_status(p_dict)==1:
       message="\ndest:''{0}'' table: ''{1}'' exists unusable index!".format(p_dict['dest_db'],p_dict['source_tab'])
       v_message=v_message+message
       n_status=1 
 
    end_time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if n_status==0:
       write_log(p_dict,'2','success!',begin_time,end_time)
       write_task(p_dict,'2')
       return 0
    else:
       write_log(p_dict,'2',v_message,begin_time,end_time)
       write_task(p_dict,'2')
       return 1

'''
  功能：获取当前日期
  入口：无
  出口：字符日期,如:20180410
'''
def current_rq():
    year =str(datetime.datetime.now().year)
    month=str(datetime.datetime.now().month).rjust(2,'0')
    day  =str(datetime.datetime.now().day).rjust(2,'0')
    return year+month+day


'''
  功能：获取服务器当前天数
  入口：无
  出口: 当天日期,如:10
'''
def curren_day(bz):
    if bz=="test":
      return 3
    day  =str(datetime.datetime.now().day)
    return day


'''
  功能：获取导出导入参数文件中键对应的值
  入口：v_file：参数文件的文件名
        v_key : 参数文件中的键名
  出口：参数文件中键对应的值
'''
def read_exp_cfg(v_file,v_key):
    file_name=str(v_file)
    file_handle = open(file_name,'r')
    line = file_handle.readline()
    ret=''
    while line:
      val=line.split("=")
      if val[0].replace(" ","")==v_key:
         ret=val[1].replace("\n","")
         break
      line = file_handle.readline()
    return ret


'''
  功能：获取导出导入日志明细
  入口：p_dict  ：当前任务配置文件
        logfile : 导出或导入dmp文件名
  出口：导出或导入文件内容
'''
def read_exp_content(p_dict,logfile):
    logfile = get_real_path(p_dict)+'/'+logfile
    file_handle = open(logfile,'r')
    line = file_handle.readline()
    lines=''
    while line:
      lines=lines+line
      line = file_handle.readline() 
    lines=lines+line
    return lines


'''
  功能：获取目标库中目录对象指向的路径名
  入口：p_dict  ：当前任务配置文件
  出口：路径名

'''
def get_real_path(p_dict):
    sql="""select directory_path
           from dba_directories 
           where owner='SYS' 
             and directory_name=upper('{0}')""".format(p_dict['dump_dir']) 
    db=p_dict['db_dest_mgr']
    cr=db.cursor()
    cr.execute(sql)
    rs=cr.fetchone()
    return rs[0]


'''
  功能：写任务的扩展日志信息
  入口：p_dict  ：当前任务配置文件
        status  : 当前任务的状态
  出口：无
'''
def write_log_ext(p_dict,status):
    db = p_dict['db_dbant']
    cr = db.cursor()
    cfg=''
    if status==3:
      cfg=p_dict['exp_cfg_file']
    else:
      cfg=p_dict['imp_cfg_file']
    path        = get_real_path(p_dict)
    filename    = read_exp_cfg(cfg,'dumpfile')
    logfile     = read_exp_cfg(cfg,'logfile')
    log_content = read_exp_content(p_dict,logfile).replace("'","''''")
    log_id      = get_logid(p_dict,status)
    v_sql="""insert into db_mgt_log_ext(db_mgt_log_id,op_type,path,filename,logfile,log_content)
                values('{0}','{1}','{2}','{3}','{4}','{5}')""".format(log_id,status,path,filename,logfile,log_content)
    n_row=cr.execute(v_sql)
    db.commit()
    cr.close()


'''
  功能：获取子任务中需要导出的分区名称，以逗号分隔
  入口：p_dict  ：当前任务配置文件
  出口：逗号分隔的分区名称
'''
def get_part_names(p_dict):
    sql="""select a.part_name 
           from db_mgt_tab_part_config a ,db_mgt_tab_config b
           where a.db_mgt_tab_config_id=b.id
             and a.part_month<date_format(date_add(curdate(),interval -{0} month), '%Y%m')
             and b.table_name='{1}'
             and a.flag='N'""".format(p_dict['source_keep_time'],p_dict['source_tab'])
    db=p_dict['db_dbant']
    cr=db.cursor()
    cr.execute(sql)
    rs=cr.fetchall()
    tmp=''
    for i in rs:
      for j in range(len(i)):
         tmp=tmp+str(i[j])+','
    return tmp[0:-1]



'''
  功能：获取子任务中需要导出的分区名称
  入口：p_dict  ：当前任务配置文件
  出口：用单引号包褒且以逗号分隔的分区名称
'''
def get_part_names_fmt(p_dict):
    sql="""select a.part_name 
           from db_mgt_tab_part_config a ,db_mgt_tab_config b
           where a.db_mgt_tab_config_id=b.id
             and a.part_month<date_format(date_add(curdate(),interval -{0} month), '%Y%m')
             and b.table_name='{1}'
             and a.flag='N'""".format(p_dict['source_keep_time'],p_dict['source_tab'])
    db=p_dict['db_dbant']
    cr=db.cursor()
    cr.execute(sql)
    rs=cr.fetchall()
    tmp=''
    for i in rs:
      for j in range(len(i)):
         tmp=tmp+"'"+str(i[j])+"',"
    return tmp[0:-1]


'''
  功能：获取子任务中需要导出的所有子分区占用的空间,单位MB
  入口：p_dict  ：当前任务配置文件
  出口：分区大小
'''
def get_part_size(p_dict):
    sql="""
        select sum(bytes)/1024/1024||'M' 
        from (select bytes
              from dba_segments
              where owner =upper('{0}')
                and segment_name =upper('{1}')
                and partition_name in ({2})
              union all       
              select bytes
               from dba_segments
               where (owner, segment_name) in
               (select owner, segment_name
                from dba_lobs
                where owner = '{0}'
                 and table_name = upper('{1}'))
                 and partition_name in({2})
              union all                  
               select bytes
                from dba_segments
                where (owner,segment_name) in
                (select owner,index_name
                 from dba_indexes
                 where owner =upper('{0}')
                   and table_name = '{1}')
                    and partition_name in({2}))
        """.format(p_dict['source_user'],p_dict['source_tab'],p_dict['source_part_names_fmt'], \
                   p_dict['source_user'],p_dict['source_tab'],p_dict['source_part_names_fmt'], \
                   p_dict['source_user'],p_dict['source_tab'],p_dict['source_part_names_fmt'])

    if p_dict['source_part_names_fmt']=='':
       return 0
    db=p_dict['db_source_mgr']
    cr=db.cursor()
    cr.execute(sql)
    rs=cr.fetchone()
    return rs[0]


'''
  功能：获取当前任务分区表中已迁移完成的分区数
  入口：p_dict  ：当前任务配置文件
  出口：分区数量
'''
def get_part_records(p_dict):
    sql="""select count(0) 
           from db_mgt_tab_part_config a ,db_mgt_tab_config b
           where a.db_mgt_tab_config_id=b.id
             and b.table_name='{0}'
             and a.flag='Y'
        """.format(p_dict['source_tab'])
    db=p_dict['db_dbant']
    cr=db.cursor()
    cr.execute(sql)
    rs=cr.fetchone()
    return rs[0]


'''
  功能：获取当前任务目标库中已迁移完成的最大分区月份
  入口：p_dict  ：当前任务配置文件
  出口：最大分区月份
'''
def get_dest_part_max_month(p_dict):
    sql="""select concat(max(a.part_month),'01')
           from db_mgt_tab_part_config a ,db_mgt_tab_config b
           where a.db_mgt_tab_config_id=b.id
             and b.table_name='{0}'
             and a.flag='Y'                
        """.format(p_dict['source_tab'])
    db=p_dict['db_dbant']
    cr=db.cursor()
    cr.execute(sql)
    rs=cr.fetchone()
    return rs[0]


'''
  功能：获取当前任务目标库中待清理的分区数量
  入口：p_dict  ：当前任务配置文件
  出口：分区数量
'''
def get_dest_part_records(p_dict):
    if p_dict['dest_keep_time']==0:
       return 0
    else:
       sql="""select count(0) 
              from db_mgt_tab_part_config a ,db_mgt_tab_config b
              where a.db_mgt_tab_config_id=b.id
                and b.table_name='{0}'
                and a.part_month<=date_format(date_add({1},interval -{2} month), '%Y%m')
                and a.flag='Y'                 
              order by a.part_name,a.part_month
           """.format(p_dict['source_tab'],get_dest_part_max_month(p_dict),p_dict['dest_keep_time'])
       db=p_dict['db_dbant']
       cr=db.cursor()
       cr.execute(sql)
       rs=cr.fetchone()
       return rs[0]


'''
  功能：获取当前任务目标库中待清理的分区列表，以逗号分隔
  入口：p_dict  ：当前任务配置文件
  出口：逗号分隔的分区名称
'''
def get_dest_part_names(p_dict):
    if p_dict['dest_keep_time']>0:
       sql="""select a.part_name 
              from db_mgt_tab_part_config a ,db_mgt_tab_config b
              where a.db_mgt_tab_config_id=b.id
                and b.table_name='{0}'
                and a.part_month<=date_format(date_add({1},interval -{2} month), '%Y%m')
                and a.flag='Y'
                order by a.part_name,a.part_month
           """.format(p_dict['source_tab'],get_dest_part_max_month(p_dict),p_dict['dest_keep_time'])
       db=p_dict['db_dbant']
       cr=db.cursor()
       cr.execute(sql)
       rs=cr.fetchall()
       tmp=''
       for i in rs:
         for j in range(len(i)):
           tmp=tmp+str(i[j])+','
       return tmp[0:-1]
    return '' 



'''
  功能：获取待导出的分区名列表
  入口：p_dict  ：当前任务配置文件
  出口：返回适合exp.par文件中格式的分区名列表
'''
def get_exp_part_names(p_dict):
    sql="""select a.part_name 
           from db_mgt_tab_part_config a ,db_mgt_tab_config b
           where a.db_mgt_tab_config_id=b.id
             and a.part_month<date_format(date_add(curdate(),interval -{0} month), '%Y%m')
             and b.table_name='{1}'
             and a.flag='N'""".format(p_dict['source_keep_time'],p_dict['source_tab'])
    db=p_dict['db_dbant']
    cr=db.cursor()
    cr.execute(sql)
    rs=cr.fetchall()
    tmp=''
    for i in rs:
      for j in range(len(i)):
         tmp=tmp+p_dict['source_user']+'.'+p_dict['source_tab']+':'+str(i[j])+','
    return tmp[0:-1]


'''
  功能：获取待导出的分区月范围
  入口：p_dict  ：当前任务配置文件
  出口：返回适合exp.par文件中格式的分区月范围名
'''
def get_exp_part_months(p_dict):
    sql="""select concat(min(a.part_month),'~',max(a.part_month)) 
           from db_mgt_tab_part_config a ,db_mgt_tab_config b
           where a.db_mgt_tab_config_id=b.id
             and a.part_month<date_format(date_add(curdate(),interval -{0} month), '%Y%m')
             and b.table_name='{1}'
             and a.flag='N'""".format(p_dict['source_keep_time'],p_dict['source_tab'])
    db=p_dict['db_dbant']
    cr=db.cursor()
    cr.execute(sql)
    rs=cr.fetchone()
    return rs[0]



'''
  功能：检测某一天某一子任务是否已导出数据
  入口：p_dict  ：当前任务配置文件
  出口：0：未导出，>0：已导出
'''
def check_exp_file_exist(p_dict):
    db  = p_dict['db_dbant']
    cr=db.cursor()
    sql ="""select count(0)
            from db_mgt_log_ext 
            where db_mgt_log_id=(select max(id) from db_mgt_log
                                 where db_mgt_task_id={0}
                                   and date_format(create_time,'%Y%m%d')=date_format(curdate(),'%Y%m%d')
                                   and status='3'
                                   and message='success!')
         """.format(p_dict['db_mgt_task_id'])
    cr.execute(sql)
    rs=cr.fetchone()
    if rs[0]==0:
       return 1

    sql ="""select concat(path,'/',filename) 
            from db_mgt_log_ext 
            where db_mgt_log_id=(select max(id) from db_mgt_log
                                 where db_mgt_task_id={0}
                                   and date_format(create_time,'%Y%m%d')=date_format(curdate(),'%Y%m%d')
                                   and status='3'
                                   and message='success!')
         """.format(p_dict['db_mgt_task_id'])
    cr.execute(sql)
    rs=cr.fetchone()
    exp_file=rs[0]
    if os.system("ls "+exp_file+" >/dev/null 2>&1")==0:
       return 0
    else:
       return 1
    


'''
  功能: 从源库导出特定分区数据至目标库
  入口：p_dict  ：当前任务配置文件
  出口：0：成功，1：失败
'''
def exp(p_dict):
    if check_exp_file_exist(p_dict)==0:
       return 0
    rq=current_rq()
    begin_time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    part_name=get_exp_part_names(p_dict)
    userid=p_dict['source_mgr_user']+'\/'+p_dict['source_mgr_pass']
    os.system("\cp -rf ./template/exp.sh       ./script/exp.sh")
    os.system("\cp -rf ./template/exp.par      ./script/exp.par")
    os.system("""sed -i 's/@USER/"{0}"'/g      ./script/exp.par""".format(userid))
    os.system("sed -i 's/@DUMP/{0}'/g          ./script/exp.par".format(p_dict['dump_dir']))
    os.system("sed -i 's/@DBLINK/{0}'/g        ./script/exp.par".format(p_dict['dblink_name']))
    os.system("sed -i 's/@OWNER/{0}'/g         ./script/exp.par".format(p_dict['source_user']))
    os.system("sed -i 's/@TABLE/{0}'/g         ./script/exp.par".format(p_dict['source_tab']))
    os.system("sed -i 's/@EXP_TAB_PART/{0}'/g  ./script/exp.par".format(part_name))
    os.system("sed -i 's/@DATE/{0}'/g          ./script/exp.par".format(rq))
    os.system("sed -i 's/@MONTHS/{0}'/g        ./script/exp.par".format(get_exp_part_months(p_dict)))
    os.system("sed -i 's/@SID/{0}'/g           ./script/exp.sh".format(p_dict['dest_inst']))
    os.system("sed -i 's/@PATH/{0}'/g          ./script/exp.sh".format(os.getcwd().replace("/","\/")))
    exp_cmd="sh ./script/exp.sh  >/dev/null 2>&1"
    ret=os.system(exp_cmd)
    end_time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if (ret == 0):
        write_log(p_dict,3,"success!",begin_time,end_time)
        write_log_ext(p_dict,3)
        write_task(p_dict,3)
        return 0
    else:      
        write_log(p_dict,3,"failure!",begin_time,end_time)
        write_log_ext(p_dict,3)
        write_task(p_dict,3)
        return -1


'''
  功能: 将DMP文件导入目标库中
  入口：p_dict  ：当前任务配置文件
  出口：0：成功，1：失败
'''
def imp(p_dict):
    rq=current_rq() 
    begin_time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    userid=p_dict['dest_mgr_user']+'\/'+p_dict['dest_mgr_pass']
    os.system("\cp -rf ./template/imp.sh      ./script/imp.sh")
    os.system("\cp -rf ./template/imp.par     ./script/imp.par")
    os.system("""sed -i 's/@USER/"{0}"'/g     ./script/imp.par""".format(userid))
    if check_source_part_grants(p_dict)==0:
       os.system("""sed -i 's/@SKIP/{0}'/g    ./script/imp.par""".format('IGNORE=Y'))
    else:
       os.system("""sed -i 's/@SKIP/{0}'/g    ./script/imp.par""".format('EXCLUDE=GRANT'))
    os.system("sed -i 's/@DUMP/{0}'/g         ./script/imp.par".format(p_dict['dump_dir']))
    os.system("sed -i 's/@OWNER/{0}'/g        ./script/imp.par".format(p_dict['source_user']))
    os.system("sed -i 's/@REMAPSCHEMA/{0}'/g  ./script/imp.par".format(p_dict['source_user']+':'+p_dict['dest_user']))
    os.system("sed -i 's/@TABLE/{0}'/g        ./script/imp.par".format(p_dict['source_tab']))
    os.system("sed -i 's/@DATE/{0}'/g         ./script/imp.par".format(rq))
    os.system("sed -i 's/@MONTHS/{0}'/g       ./script/imp.par".format(get_exp_part_months(p_dict)))
    os.system("sed -i 's/@SID/{0}'/g          ./script/imp.sh".format(p_dict['dest_inst']))
    os.system("sed -i 's/@PATH/{0}'/g         ./script/imp.sh".format(os.getcwd().replace("/","\/")))
    imp_cmd="sh ./script/imp.sh >/dev/null 2>&1"
    ret=os.system(imp_cmd)
    end_time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if (ret == 0):
       write_log(p_dict,4,"success!",begin_time,end_time)
       write_log_ext(p_dict,4)
       write_task(p_dict,4)
       return 0
    else:
       write_log(p_dict,4,"failure! "+msg,begin_time,end_time)
       write_log_ext(p_dict,4)
       write_task(p_dict,4)
       return -1


'''
  功能: 验证源库目标库分区记录数
  入口：p_dict  ：当前任务配置文件
  出口：0：成功，1：失败
'''
def validate(p_dict):
    db_source=p_dict['db_source_mgr']
    db_dest=p_dict['db_dest_mgr']
    cr_source=db_source.cursor()
    cr_dest=db_dest.cursor()
    d_source={}
    d_dest={}
    part_names=get_part_names(p_dict)
    begin_time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for part_name in part_names.split(','):
      cr_source.execute("select count(0) from {0}.{1} partition({2})".format(p_dict['source_user'],p_dict['source_tab'],part_name))
      cr_dest.execute("select count(0) from {0}.{1} partition({2})".format(p_dict['dest_user'],p_dict['source_tab'],part_name))
      rs_source=cr_source.fetchone()
      rs_dest=cr_dest.fetchone()
      d_source[part_name]=rs_source[0]
      d_dest[part_name]=rs_dest[0]      
    end_time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for part_name in part_names.split(','):
      errmsg="""\nsource:''{0}'' TABLE: ''{1}'' PARTITION: ''{2}'' records not same! \nsource records:{3}, dest records:{4}
             """.format(p_dict['source_db'],p_dict['source_tab'],part_name,str(d_source[part_name]),str(d_dest[part_name]))
      if d_source[part_name]!=d_dest[part_name]:
        write_log(p_dict,5,errmsg,begin_time,end_time)
        write_task(p_dict,5)
        return 1
    write_log(p_dict,5,"success!",begin_time,end_time)
    write_task(p_dict,5)
    return 0 


'''
  功能: 清除源库业务用户下已导入目标库中的分区数据
  入口：p_dict  ：当前任务配置文件
  出口：0：成功，1：失败
'''
def truncate_source_part(p_dict):
    db=p_dict['db_source_mgr']
    cr=db.cursor()        
    begin_time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if check_source_active_trans(p_dict)==0:
       end_time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
       message="\nsource:''{0}'' table: ''{1}'' exist active transaction!".format(p_dict['source_db'],p_dict['source_tab'])
       write_log(p_dict,'6',v_message,begin_time,end_time)
       write_task(p_dict,'6')
       return 1
    
    part_names=get_part_names(p_dict)
    for part_name in part_names.split(','):
        sql=''
        if check_source_local_index(p_dict)==0:
           sql="alter table {0}.{1}  truncate partition {2} update global indexes".format(p_dict['source_user'],p_dict['source_tab'],part_name)
        else:
           sql="alter table {0}.{1}  truncate partition {2}".format(p_dict['source_user'],p_dict['source_tab'],part_name)
        print("source: ",sql)
        cr.execute(sql)

    end_time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    write_log(p_dict,'6','success!',begin_time,end_time)
    write_task(p_dict,'6')
    return 0


'''
  功能: 清除目标库业务用户下分区数据
  入口：p_dict  ：当前任务配置文件
  出口：0：成功，1：失败
'''
def truncate_dest_part(p_dict):
    db=p_dict['db_dest_mgr']
    cr=db.cursor()
    begin_time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if check_dest_active_trans(p_dict)==0:
       end_time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
       message="\ndest:''{0}'' table: ''{1}'' exist active transaction!".format(p_dict['dest_db'],p_dict['source_tab'])
       write_log(p_dict,'8',v_message,begin_time,end_time)
       write_task(p_dict,'8')
       return 1

    if get_dest_part_records(p_dict)>0:
      part_names=get_dest_part_names(p_dict)
      for part_name in part_names.split(','):
          sql=''
          if check_source_local_index(p_dict)==0:
            sql="alter table {0}.{1}  truncate partition {2} update global indexes".format(p_dict['dest_user'],p_dict['source_tab'],part_name)
          else:
            sql="alter table {0}.{1}  truncate partition {2}".format(p_dict['dest_user'],p_dict['source_tab'],part_name)
          print("dest: ",sql)
          cr.execute(sql)

    end_time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    write_log(p_dict,'8','success!',begin_time,end_time)
    write_task(p_dict,'8')
    return 0
    

'''
  功能: 更新数据迁移配置表信息
  入口：p_dict  ：当前任务配置文件
  出口：0：成功，1：失败
'''
def upd_dbant_cfg(p_dict):
    db = p_dict['db_dbant']
    sql="""update db_mgt_tab_part_config a 
              set flag='Y' 
           where a.db_mgt_tab_config_id={0} 
             and a.part_name='{1}'
             and a.flag='N'"""
    begin_time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    try:
       begin_time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
       for part_name in get_part_names(p_dict).split(','):
           cr=db.cursor()
           cr.execute(sql.format(p_dict['db_mgt_tab_config_id'],part_name))
       db.commit()
       end_time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
       write_log(p_dict,'7','success!',begin_time,end_time)
       write_task(p_dict,'7')
       return 0 
    except:
       end_time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
       write_log(p_dict,'7',traceback.format_exc(),begin_time,end_time)
       write_task(p_dict,'7')
       return 1


'''
  功能: 根据数据迁移配置生成待迁移子任务列表
  入口：p_dict  ：当前任务配置文件
  出口：db_mgt_tab_config.id列表
'''
def get_configs(n_day):
    conn=get_db({},'dbant')
    cr= conn.cursor()
    sql="""select b.id as "db_mgt_tab_config_id"
           from db_mgt_config a,db_mgt_tab_config b
           where a.id=b.db_mgt_config_id
             and a.run_day={0}
             and b.id not in(select db_mgt_tab_config_id 
                             from db_mgt_task
                            where date_format(create_time,'%Y%m%d')=date_format(curdate(),'%Y%m%d')
                              and status='8')
           order by b.db_mgt_config_id,b.id""".format(n_day)
    cr.execute(sql)
    rs=cr.fetchall() 
    return rs   


'''
  功能: 返回源库当前任务中分区表的分区列名
  入口：p_dict  ：当前任务配置文件
  出口：分区列名
'''
def get_source_part_col(p_dict):      
    sql="""select column_name from DBA_PART_KEY_COLUMNS  
           where owner =upper('{0}') 
             and name = upper('{1}')
        """.format(p_dict['source_user'],p_dict['source_tab'])
    db=p_dict['db_source_mgr']
    cr=db.cursor()
    cr.execute(sql)
    rs=cr.fetchone()
    return rs[0]    


'''
  功能: 返回源库当前任务中分区表的分区列类型
  入口：p_dict  ：当前任务配置文件
  出口：分区列类型
'''
def get_source_part_col_type(p_dict): 
   sql="""select data_type from dba_tab_columns 
          where owner =upper('{0}')
            and table_name =upper('{1}')

            and column_name=upper('{2}')
       """.format(p_dict['source_user'],p_dict['source_tab'],get_source_part_col(p_dict))
   db=p_dict['db_source_mgr']
   cr=db.cursor()
   cr.execute(sql)
   rs=cr.fetchone()
   return rs[0]



'''
  功能: 返回源库当前任务分区表所有分区名列表
  入口：p_dict  ：当前任务配置文件
  出口：分区名列表列
'''
def get_source_tab_part_list(p_dict):
    sql="""select partition_name
                from dba_tab_partitions
                where table_owner =upper('{0}')
                  and table_name = upper('{1}') order by partition_name
        """.format(p_dict['source_user'],p_dict['source_tab'])
    db=p_dict['db_source_mgr']
    cr=db.cursor() 
    cr.execute(sql)
    rs=cr.fetchall() 
    return rs


'''
  功能: 检测源库分区表中某一个分区中是否存在多个月数据
  入口：p_dict:当前任务字典配置文件
  出口：0:通过，1:分区中存在多月数据
'''
def check_source_part_multi_month(p_dict):
    db=p_dict['db_source_mgr']
    cr=db.cursor()   
    if p_dict['source_part_key_type']=='DATE':
       for i in get_source_tab_part_list(p_dict):
         for j in range(len(i)):
            sql="""select count(distinct trunc({0},'mm')) from  {1}.{2} partition({3})
                """.format(p_dict['source_part_key'],p_dict['source_user'],p_dict['source_tab'],i[j])
            cr.execute(sql)
            rs=cr.fetchone() 
            if rs[0]>1:     
               p_dict['source_part_name']=i[j]
               p_dict['source_part_record']=rs[0]
               return 1
       return 0

    elif p_dict['source_part_key_type']=='NUMBER':
       for i in get_source_tab_part_list(p_dict):
         for j in range(len(i)):      
             sql="""select count(distinct to_date({0},'yyyymm')) from  {1}.{2} partition({3})
                 """.format(p_dict['source_part_key'],p_dict['source_user'],p_dict['source_tab'],i[j])
             cr.execute(sql)
             rs=cr.fetchone() 
             if rs[0]>1:     
                p_dict['source_part_name']=i[j]
                p_dict['source_part_record']=rs[0]
                return 1
       return 0
    elif p_dict['source_part_key_type'] in('VARCHAR','VARCHAR2'):   
       for i in get_source_tab_part_list(p_dict):
         for j in range(len(i)) :
             
             status1=0
             status2=0
             status3=0
             status4=0
             
             sql1="""select count(distinct to_date(substr({0},1,6),'yyyymm'))  from  {1}.{2} partition({3})
                  """.format(p_dict['source_part_key'],p_dict['source_user'],p_dict['source_tab'],i[j])

             sql2="""select count(distinct to_date(substr({0},1,7),'yyyy.mm')) from  {1}.{2} partition({3})
                  """.format(p_dict['source_part_key'],p_dict['source_user'],p_dict['source_tab'],i[j])

             sql3="""select count(distinct to_date(substr({0},1,7),'yyyy-mm')) from  {1}.{2} partition({3})
                  """.format(p_dict['source_part_key'],p_dict['source_user'],p_dict['source_tab'],i[j])

             sql4="""select count(distinct to_date(substr({0},1,7),'yyyy/mm')) from  {1}.{2} partition({3})
                  """.format(p_dict['source_part_key'],p_dict['source_user'],p_dict['source_tab'],i[j])
              
             try:
               cr.execute(sql1)  
               rs=cr.fetchone()   
               if rs[0]>1:                
                 status1=1
             except:
               status1=2
                            
             try:
               cr.execute(sql2)  
               rs=cr.fetchone()   
               if rs[0]>1:
                 status1=1
             except:
               status2=2
               
             try:
               cr.execute(sql3)  
               rs=cr.fetchone()   
               if rs[0]>1:
                  status3=1
             except:
               status3=2
               
             try:
               cr.execute(sql4)  
               rs=cr.fetchone()   
               if rs[0]>1:
                  status4=1
             except:
               status4=2
                    
             if status1==1  or status2==1 or status3==1  or status4==1:  
                p_dict['source_part_name']=i[j]
                p_dict['source_part_record']=rs[0]    
                return 1 
                
       return 0

'''
  功能: 检测源库分区表中分区当键值为字符和数值类型时时否可以转为有效日期
  入口：p_dict:当前任务字典配置文件
  出口：0:通过，1：分区键值内容不合法
'''
def check_source_part_valid(p_dict):
    db=p_dict['db_source_mgr']
    cr=db.cursor()   
    if p_dict['source_part_key_type']=='DATE':     
       return 0
    elif p_dict['source_part_key_type']=='NUMBER':
       for i in get_source_tab_part_list(p_dict):
         for j in range(len(i)): 
             status1=0
             status2=0     
             sql1="""select to_date({0},'yyyymmdd') from  {1}.{2} partition({3})
                  """.format(p_dict['source_part_key'],p_dict['source_user'],p_dict['source_tab'],i[j])
 
             sql2="""select to_date({0},'yyyymm') from  {1}.{2} partition({3})
                  """.format(p_dict['source_part_key'],p_dict['source_user'],p_dict['source_tab'],i[j])
             try:    
               cr.execute(sql1)
               rs=cr.fetchall()              
             except:
               status1=1  

             try:    
               cr.execute(sql2)
               rs=cr.fetchall()              
             except:
               status2=1  
 
             if status1==1 and status2==1:
                p_dict['source_part_name']=i[j]
                return 1
       return 0
    elif p_dict['source_part_key_type'] in('VARCHAR','VARCHAR2'):   
       for i in get_source_tab_part_list(p_dict):
         for j in range(len(i)) :
             
             status1=0
             status2=0
             status3=0
             status4=0
             
             sql1="""select count(distinct to_date(substr({0},1,8),'yyyymmdd'))  from  {1}.{2} partition({3})
                  """.format(p_dict['source_part_key'],p_dict['source_user'],p_dict['source_tab'],i[j])

             sql2="""select count(distinct to_date(substr({0},1,10),'yyyy.mm.dd')) from  {1}.{2} partition({3})
                  """.format(p_dict['source_part_key'],p_dict['source_user'],p_dict['source_tab'],i[j])

             sql3="""select count(distinct to_date(substr({0},1,10),'yyyy-mm-dd')) from  {1}.{2} partition({3})
                  """.format(p_dict['source_part_key'],p_dict['source_user'],p_dict['source_tab'],i[j])

             sql4="""select count(distinct to_date(substr({0},1,10),'yyyy/mm/dd')) from  {1}.{2} partition({3})
                  """.format(p_dict['source_part_key'],p_dict['source_user'],p_dict['source_tab'],i[j])
              
             try:
               cr.execute(sql1)
               rs=cr.fetchone() 
             except:              
               status1=1
                            
             try:
               cr.execute(sql2)
               rs=cr.fetchone() 
             except:            
               status2=1
               
             try:
               cr.execute(sql3)
               rs=cr.fetchone()  
             except:              
               status3=1
               
             try:
               cr.execute(sql4)
               rs=cr.fetchone()
             except:              
               status4=1
                
             if status1==1  and  status2==1 and  status3==1  and status4==1:  
                p_dict['source_part_name']=i[j]               
                return 1 
       return 0

'''
  功能: 数据迁移处理主方法
  入口：p_dict  ：当前任务配置文件
  出口：无
'''
def start_migration(p_dict):

    #初始化任务和日志表
    if write_task(p_dict,'1')!=0:
       return
    
    #输出参数，用于调试
    print_para(p_dict)
 
    #数据迁移前检测
    if check(p_dict)!=0:
       return
    
    #从生产库中导出数至历史库
    if exp(p_dict)!=0:
       return
   
    #将分区导入历史库
    if imp(p_dict)!=0:
       return 
    
    #生产库与历史库分区记录数核对
    if validate(p_dict)!=0:
       return

    #根据保留策略清除源库已迁移成功的分区数据
    if truncate_source_part(p_dict)!=0:
       return 1

    #更新dbant配置信息
    if upd_dbant_cfg(p_dict)!=0:
       return 1 

    #根据保留策略清除目标库已迁移成的分区数据
    if truncate_dest_part(p_dict)!=0:
       return 1

    #任务完成后，发送邮件,短信通知


'''
  功能: 数据迁移主函数
  入口：无
  出口：无

'''
def main():
    
    #循环处理每一个子任务,正式上线时需要改为：get_configs(curren_day())
    for i in get_configs(curren_day('test')):
      
      for j in range(len(i)):
 
         #从dbant中获取运行参数
         p_dict = read_para(i[j])    
         
         #启动任务
         start_migration(p_dict)


if __name__ == '__main__':
   main()
