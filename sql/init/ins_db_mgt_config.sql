delete from db_mgt_config where id=1;
insert into db_mgt_config(id,db_config_id_source,db_config_id_dest,run_day,dump_dir,dblink_name,creator)
values(1,'4e4c3c9d-9829-4fc1-82a8-87bc6fbf6b58','00211e04-4449-11e8-ad47-90e2ba6e527a',1,'clic_dump','dblink_clic','dba');

delete from db_mgt_config where id=2;
insert into db_mgt_config(id,db_config_id_source,db_config_id_dest,run_day,dump_dir,dblink_name,creator)
values(2,'21e0ee1b-3b67-499b-a5d6-c9b94c43c62d','003d4601-4449-11e8-ad47-90e2ba6e527a',2,'tcsbns_dump','dblink_tcsbns','dba');

delete from db_mgt_config where id=3;
insert into db_mgt_config(id,db_config_id_source,db_config_id_dest,run_day,dump_dir,dblink_name,creator)
values(3,'680d1dd0-9dea-402f-87ad-3dfdd40d9363','002ccb5b-4449-11e8-ad47-90e2ba6e527a',3,'tcsvbs_dump','dblink_tcsvbs','dba');
