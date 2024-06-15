-- taxrate
delete from master.taxrate;
insert into master.taxrate
	select * from staging.taxrate;