-- dimdate
delete from master.dimdate;
insert into master.dimdate
	select
	  sk_dateid
	, datevalue::date
	, datedesc
	, calendaryearid
	, calendaryeardesc
	, calendarqtrid
	, calendarqtrdesc
	, calendarmonthid
	, calendarmonthdesc
	, calendarweekid
	, calendarweekdesc	
	, dayofweeknum
	, dayofweekdesc
	, fiscalyearid
	, fiscalyeardesc
	, fiscalqtrid
	, fiscalqtrdesc
	, holidayflag
	from staging.date;