delete from  db_inst where id=209;
insert into db_inst values (209,'10.120.12.41','1521','oracle','','',NULL,NULL,'2222','master','10.120.12.42','58cddd75-469b-11e7-a0f9-001b21bf50f6','production',0,0,NULL);

delete from  db_inst where id=221;
insert into db_inst values(221,'10.120.12.21','1521','oracle','','',NULL,NULL,'2222','master','10.120.12.22','1583ca5d-469b-11e7-a0f9-001b21bf50f6','production',0,0,NULL);

delete from db_inst where id=210;
insert into db_inst(id,ip,port,db_type,db_purpose,user,password) values(210,'10.120.12.253','1522','oracle','production',' ',' '); 


