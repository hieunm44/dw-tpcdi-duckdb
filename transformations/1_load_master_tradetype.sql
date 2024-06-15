-- tradetype
delete from master.tradetype;
insert into master.tradetype
	select * from staging.tradetype;