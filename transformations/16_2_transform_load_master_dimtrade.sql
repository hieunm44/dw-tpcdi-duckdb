delete from master.dimtrade;
insert into master.dimtrade (sk_createdateid, sk_createtimeid, sk_closedateid, sk_closetimeid, tradeid, cashflag, 
        quantity, bidprice, executedby, tradeprice, fee, commission, tax, status, type, sk_securityid, sk_companyid,
        sk_accountid, sk_customerid, sk_brokerid, batchid)
SELECT 
Case When (sth.th_st_id = 'SBMT' and (sth.t_tt_id = 'TMB' or sth.t_tt_id ='TMS')) or (sth.th_st_id = 'PNDG') 
        then D.sk_dateid else NULL
end,
Case When (sth.th_st_id = 'SBMT' and (sth.t_tt_id = 'TMB' or sth.t_tt_id ='TMS')) or (sth.th_st_id = 'PNDG') 
        then T.sk_timeid else NULL
end,
case when (sth.th_st_id = 'CMPT' or sth.th_st_id = 'CNCL') then D.sk_dateid else NULL
end,
case when (sth.th_st_id = 'CMPT' or sth.th_st_id = 'CNCL') then T.sk_timeid else NULL
end,
sth.t_id, sth.t_is_cash, sth.t_qty, sth.t_bid_price, sth.t_exec_name, sth.t_trade_price, sth.t_chrg,
sth.t_comm, sth.t_tax, st.st_name, tt.tt_name, 
case when (CAST(sth.th_dts as DATE) >= ds.EffectiveDate) and (CAST(sth.th_dts as DATE) <= ds.EndDate) 
    then ds.SK_SecurityID END,
case when (CAST(sth.th_dts as DATE) >= ds.EffectiveDate) and (CAST(sth.th_dts as DATE) <= ds.EndDate) 
    then ds.SK_CompanyID END,
case when (CAST(sth.th_dts as DATE) >= ds.EffectiveDate) and (CAST(sth.th_dts as DATE) <= ds.EndDate) 
    then da.SK_AccountID END,
case when (CAST(sth.th_dts as DATE) >= ds.EffectiveDate) and (CAST(sth.th_dts as DATE) <= ds.EndDate) 
    then da.SK_CustomerID END,
case when (CAST(sth.th_dts as DATE) >= ds.EffectiveDate) and (CAST(sth.th_dts as DATE) <= ds.EndDate) 
    then da.SK_BrokerID END,
1
from staging.trade_joined sth
join staging.statustype st on sth.t_st_id = st.st_id
join staging.tradetype tt on sth.t_tt_id = tt.tt_id
join master.dimsecurity ds on sth.t_s_symb = ds.symbol
join master.dimaccount da on sth.t_ca_id = da.accountid
-- need check
JOIN master.dimdate D on CAST(sth.th_dts as DATE) = D.datevalue
-- need check
join master.dimtime T on CAST(sth.th_dts AS TIME) = T.timevalue

-- ON DUPLICATE KEY UPDATE  sk_closedateid = D.sk_dateid, sk_closetimeid = T.sk_timeid;