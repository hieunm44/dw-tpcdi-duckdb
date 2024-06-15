-- statustype
delete from master.statustype;
insert into master.statustype
	select * from staging.statustype;