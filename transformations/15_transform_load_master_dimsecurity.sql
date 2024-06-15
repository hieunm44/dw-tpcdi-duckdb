-- master.dimsecurity transform and load
truncate table master.dimsecurity;
insert into master.dimsecurity 
	select 
		row_number() over() as sk_securityid,
		symbol,
		issuetype as issue,
		s.st_name as status,
		f.name,
		exid as exchangeid,
		c.sk_companyid as sk_companyid,
		shout::numeric(12) as sharesoutstanding,
		CAST(SUBSTRING(left(firsttradedate, 8), 1, 4) || '-' || SUBSTRING(left(firsttradedate, 8), 5, 2) || '-' || SUBSTRING(left(firsttradedate, 8), 7, 2) AS DATE),
		CAST(SUBSTRING(left(firsttradeexchg, 8), 1, 4) || '-' || SUBSTRING(left(firsttradeexchg, 8), 5, 2) || '-' || SUBSTRING(left(firsttradeexchg, 8), 7, 2) AS DATE),
		dividend::numeric(10,2),
		case 
			when lead( (select batchdate from staging.batchdate) ) over ( partition by symbol order by pts asc ) is null 
			then true 
			else false 
			end as iscurrent,
		1 as batchid,
		CAST(SUBSTRING(left(f.pts, 8), 1, 4) || '-' || SUBSTRING(left(f.pts, 8), 5, 2) || '-' || SUBSTRING(left(f.pts, 8), 7, 2) AS DATE),
		'9999-12-31'::date as enddate 
	from staging.finwire_sec f,
		staging.statustype s,
		master.dimcompany c
	where f.status = s.st_id
	and ((ltrim(f.conameorcik, '0') = c.companyid::varchar) 
		or (f.conameorcik = c.name))
	and CAST(SUBSTRING(left(f.pts, 8), 1, 4) || '-' || SUBSTRING(left(f.pts, 8), 5, 2) || '-' || SUBSTRING(left(f.pts, 8), 7, 2) AS DATE) >= c.effectivedate
	and CAST(SUBSTRING(left(f.pts, 8), 1, 4) || '-' || SUBSTRING(left(f.pts, 8), 5, 2) || '-' || SUBSTRING(left(f.pts, 8), 7, 2) AS DATE) < c.enddate;