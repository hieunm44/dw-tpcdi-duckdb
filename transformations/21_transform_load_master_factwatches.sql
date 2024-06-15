-- factwatches
delete from master.factwatches;
insert into master.factwatches
	with watches as (
		select w1.w_c_id, 
		TRIM(w1.w_s_symb) as w_s_symb, 
		w1.w_dts::date as dateplaced, 
		w2.w_dts::date as dateremoved
		from staging.watchhistory w1,
			staging.watchhistory w2
		where w1.w_c_id = w2.w_c_id
		and w1.w_s_symb = w2.w_s_symb
		and w1.w_action = 'ACTV'
		and w2.w_action = 'CNCL'
	) 

	select 
		c.sk_customerid as sk_customerid,
		s.sk_securityid as sk_securityid,
		CAST(EXTRACT(YEAR FROM w.dateplaced) * 10000 + EXTRACT(MONTH FROM w.dateplaced) * 100 + EXTRACT(DAY FROM w.dateplaced) AS NUMERIC) as sk_dateid_dateplaced,
		CAST(EXTRACT(YEAR FROM w.dateremoved) * 10000 + EXTRACT(MONTH FROM w.dateremoved) * 100 + EXTRACT(DAY FROM w.dateremoved) AS NUMERIC) as sk_dateid_dateremoved,
		1 as batchid
	from watches w,
		master.dimcustomer c,
		master.dimsecurity s,
		master.dimdate d1,
		master.dimdate d2
	where w.w_c_id = c.customerid
	and w.w_s_symb = s.symbol
	and w.dateplaced = d1.datevalue
	and w.dateremoved = d2.datevalue;