-- industry
delete from master.industry;
insert into master.industry
	select * from staging.industry;