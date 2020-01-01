#Ô´¿â
delete from  db_config where db_inst_id=209;
delete from  db_config where db_inst_id=221;
insert into db_config(id,db_inst_id,line_username,line_password,create_time,db_name,dept_id,db_service_name,query_limit)  values ('4e4c3c9d-9829-4fc1-82a8-87bc6fbf6b58',209,'clic','YzQzIWNsNzZpYyMp\n','2017-08-10 18:46:16','clic',1,'cedb',120);
insert into db_config(id,db_inst_id,line_username,line_password,create_time,db_name,dept_id,db_service_name,query_limit)  values ('21e0ee1b-3b67-499b-a5d6-c9b94c43c62d',221,'tcsbns','VWpmKlBjdzQ1eHN3cVE=','2017-08-10 18:46:16','tcsbns',1,'cedb',120);
insert into db_config(id,db_inst_id,line_username,line_password,create_time,db_name,dept_id,db_service_name,query_limit)  values ('680d1dd0-9dea-402f-87ad-3dfdd40d9363',221,'tcsvbs','WiMsa28wbGpGXkRu\n','2017-08-10 18:46:16','tcsvbs',1,'cedb',120);

#Ä¿±ê¿â
delete from db_config where db_inst_id=210;
insert into db_config(id,db_inst_id,line_username,line_password,create_time,db_name,db_service_name)  values('00211e04-4449-11e8-ad47-90e2ba6e527a',210,'clic',to_base64('c43!cl76ic#)'),now(),'','hist1');
insert into db_config(id,db_inst_id,line_username,line_password,create_time,db_name,db_service_name)  values('003d4601-4449-11e8-ad47-90e2ba6e527a',210,'tcsbns',to_base64('Ujf*Pcw45xswqQ'),now(),'','hist2');
insert into db_config(id,db_inst_id,line_username,line_password,create_time,db_name,db_service_name)  values('002ccb5b-4449-11e8-ad47-90e2ba6e527a',210,'tcsvbs',to_base64('Z#,ko0ljF^Dn'),now(),'','hist2');