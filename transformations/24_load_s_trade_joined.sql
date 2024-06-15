insert into staging.trade_joined
select * from staging.trade t inner join staging.tradehistory th on t.t_id = th.th_t_id;