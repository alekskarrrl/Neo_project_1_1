-- CALL ds.fill_account_turnover_f(to_date('2018-01-10','yyyy-mm-dd'), 'Airflow some message')
--CALL dm.fill_f101_round_f(to_date('2018-01-11','yyyy-mm-dd'));
--CALL dm.looped_fill_turnover(to_date('2018-01-31','yyyy-mm-dd'))

create or replace procedure ds.fill_account_turnover_f (
   i_OnDate date,
	info_msg varchar
)
language plpgsql    
as $$
declare
	v_RowCount int;
	curr_seq_log_id bigint;
begin
	
	curr_seq_log_id := (SELECT last_value FROM logs.seq_log_id);
		   
	call logs.write_log_info('[BEGIN] fill_account_turnover_f(i_OnDate => date ''' 
         || to_char(i_OnDate, 'yyyy-mm-dd') 
         || ''');', 1, curr_seq_log_id, 'call_procedure ds.fill_account_turnover_f', Null, info_msg);
		 
	call logs.write_log_info('delete from dm_account_turnover_f on_date = ' 
         || to_char(i_OnDate, 'yyyy-mm-dd'), 1, curr_seq_log_id, 'call_procedure ds.fill_account_turnover_f', Null, info_msg);
	   
    delete
      from dm.dm_account_turnover_f f
     where f.on_date = i_OnDate;
	
	call logs.write_log_info('insert', 1, curr_seq_log_id, 'call_procedure ds.fill_account_turnover_f', Null, info_msg);
	
    insert
      into dm.dm_account_turnover_f
           ( on_date
           , account_rk
           , credit_amount
           , credit_amount_rub
           , debet_amount
           , debet_amount_rub
           )
    with wt_turn as
    ( select p.credit_account_rk                  as account_rk
           , p.credit_amount                      as credit_amount
           , p.credit_amount * COALESCE(er.reduced_cource, 1)         as credit_amount_rub 			-- Замена NULLIF на COALESCE
           , cast(null as numeric)                 as debet_amount
           , cast(null as numeric)                 as debet_amount_rub
        from ds.ft_posting_f p
        join ds.md_account_d a
          on a.account_rk = p.credit_account_rk
        left
        join ds.md_exchange_rate_d er
          on er.currency_rk = a.currency_rk
         and i_OnDate between er.data_actual_date   and er.data_actual_end_date
       where p.oper_date = i_OnDate
         and i_OnDate           between a.data_actual_date    and a.data_actual_end_date
         and a.data_actual_date between date_trunc('month', i_OnDate) and (date_trunc('MONTH', i_OnDate) + INTERVAL '1 MONTH - 1 day')
       union all
      select p.debet_account_rk                   as account_rk
           , cast(null as numeric)                 as credit_amount
           , cast(null as numeric)                 as credit_amount_rub
           , p.debet_amount                       as debet_amount
           , p.debet_amount * COALESCE(er.reduced_cource, 1)          as debet_amount_rub 			-- Замена NULLIF на COALESCE
        from ds.ft_posting_f p
        join ds.md_account_d a
          on a.account_rk = p.debet_account_rk
        left 
        join ds.md_exchange_rate_d er
          on er.currency_rk = a.currency_rk
         and i_OnDate between er.data_actual_date and er.data_actual_end_date
       where p.oper_date = i_OnDate
         and i_OnDate           between a.data_actual_date and a.data_actual_end_date
         and a.data_actual_date between date_trunc('month', i_OnDate) and (date_trunc('MONTH', i_OnDate) + INTERVAL '1 MONTH - 1 day')
    )
    select i_OnDate                               as on_date
         , t.account_rk
         , COALESCE(sum(t.credit_amount), 0)                   as credit_amount
         , COALESCE(sum(t.credit_amount_rub), 0)               as credit_amount_rub
         , COALESCE(sum(t.debet_amount), 0)                    as debet_amount
         , COALESCE(sum(t.debet_amount_rub), 0)                as debet_amount_rub
      from wt_turn t
     group by t.account_rk;
	 
	GET DIAGNOSTICS v_RowCount = ROW_COUNT;
   	
	call logs.write_log_info('[END] inserted to dm_account_turnover_f ' || to_char(v_RowCount,'FM99999999') || ' rows.', 1, 
							 curr_seq_log_id, 'call_procedure ds.fill_account_turnover_f', v_RowCount, info_msg);

    commit;
	
end;$$



