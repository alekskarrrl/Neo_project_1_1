--CALL dm.fill_f101_round_f(to_date('2018-01-31','yyyy-mm-dd'), 'some message');

create or replace procedure dm.fill_f101_round_f ( 
  i_OnDate  date,
  info_msg varchar
)
language plpgsql    
as $$
declare
	v_RowCount int;
	curr_seq_log_id bigint;
begin

	curr_seq_log_id := (SELECT last_value FROM logs.seq_log_id);
    call logs.write_log_info( '[BEGIN] fill_f101_round_f(i_OnDate => date ''' 
         || to_char(i_OnDate, 'yyyy-mm-dd') 
         || ''');', 1, curr_seq_log_id, 'call_procedure dm.fill_f101_round_f', Null, info_msg
       );
	    
    call logs.write_log_info('delete from dm_f101_round_f on_date = ' 
         || to_char(i_OnDate, 'yyyy-mm-dd'), 1, curr_seq_log_id, 'call_procedure dm.fill_f101_round_f', Null, info_msg
       );

    delete
      from dm.DM_F101_ROUND_F f
     where from_date = date_trunc('month', i_OnDate)  
       and to_date = (date_trunc('MONTH', i_OnDate) + INTERVAL '1 MONTH - 1 day');
   
    call logs.write_log_info('insert', 1, curr_seq_log_id, 'call_procedure dm.fill_f101_round_f', Null, info_msg);
   
    insert 
      into dm.dm_f101_round_f
           ( from_date         
           , to_date           
           , chapter           
           , ledger_account    
           , characteristic    
           , balance_in_rub    
           , balance_in_val    
           , balance_in_total  
           , turn_deb_rub      
           , turn_deb_val      
           , turn_deb_total    
           , turn_cre_rub      
           , turn_cre_val      
           , turn_cre_total    
           , balance_out_rub  
           , balance_out_val   
           , balance_out_total 
           )
	with t1 as (
    select  date_trunc('month', i_OnDate)        as from_date,
           (date_trunc('MONTH', i_OnDate) + INTERVAL '1 MONTH - 1 day')  as to_date,
           s.chapter                             as chapter,
           substr(acc_d.account_number, 1, 5)    as ledger_account,
           acc_d.char_type                       as characteristic,
           -- RUB balance
           sum( case 
                  when cur.currency_code in ('643', '810')
                  then b.balance_out
                  else 0
                 end
              )                                  as balance_in_rub,
          -- VAL balance converted to rub
          sum( case 
                 when cur.currency_code not in ('643', '810')
                 then b.balance_out * exch_r.reduced_cource
                 else 0
                end
             )                                   as balance_in_val,
          -- Total: RUB balance + VAL converted to rub
          sum(  case 
                 when cur.currency_code in ('643', '810')
                 then b.balance_out
                 else b.balance_out * exch_r.reduced_cource
               end
             )                                   as balance_in_total  ,
           -- RUB debet turnover					 				-- Избавиться от Null в оборотах после Left Join с dm.dm_account_turnover_f
           COALESCE(sum(case 
                 when cur.currency_code in ('643', '810')
                 then at.debet_amount_rub
                 else 0
               end
           ), 0)                                     as turn_deb_rub,
           -- VAL debet turnover converted
           COALESCE(sum(case 
                 when cur.currency_code not in ('643', '810')
                 then at.debet_amount_rub								
                 else 0
               end
           ), 0)                                     as turn_deb_val,
           -- SUM = RUB debet turnover + VAL debet turnover converted
           COALESCE(sum(at.debet_amount_rub), 0)              as turn_deb_total,
           -- RUB credit turnover
           COALESCE(sum(case 
                 when cur.currency_code in ('643', '810')
                 then at.credit_amount_rub
                 else 0
               end
              ), 0)                                  as turn_cre_rub,
           -- VAL credit turnover converted
           COALESCE(sum(case 
                 when cur.currency_code not in ('643', '810') 
                 then at.credit_amount_rub						
                 else 0
               end
              ), 0)                                  as turn_cre_val,
           -- SUM = RUB credit turnover + VAL credit turnover converted
           COALESCE(sum(at.credit_amount_rub), 0)             as turn_cre_total,
           
		   cast(null as numeric) 				  as balance_out_rub,
           cast(null as numeric)                  as balance_out_val,
           cast(null as numeric)                  as balance_out_total 
      from ds.md_ledger_account_s s
      join ds.md_account_d acc_d
        on substr(acc_d.account_number, 1, 5) = to_char(s.ledger_account, 'FM99999999')
      join ds.md_currency_d cur
        on cur.currency_rk = acc_d.currency_rk
      left 
      join ds.ft_balance_f b
        on b.account_rk = acc_d.account_rk
       and b.on_date  = (date_trunc('month', i_OnDate) - INTERVAL '1 day')
      left 
      join ds.md_exchange_rate_d exch_r
        on exch_r.currency_rk = acc_d.currency_rk
       and i_OnDate between exch_r.data_actual_date and exch_r.data_actual_end_date
      left 
      join dm.dm_account_turnover_f at
        on at.account_rk = acc_d.account_rk
       and at.on_date between date_trunc('month', i_OnDate) and (date_trunc('MONTH', i_OnDate) + INTERVAL '1 MONTH - 1 day')
     where i_OnDate between s.start_date and s.end_date
       and i_OnDate between acc_d.data_actual_date and acc_d.data_actual_end_date
       and i_OnDate between cur.data_actual_date and cur.data_actual_end_date
     group by s.chapter,
           substr(acc_d.account_number, 1, 5),
           acc_d.char_type
		)
	SELECT from_date         
           , to_date           
           , chapter           
           , ledger_account    
           , characteristic    
           , balance_in_rub    
           , balance_in_val    
           , balance_in_total  
           , turn_deb_rub      
           , turn_deb_val      
           , turn_deb_total    
           , turn_cre_rub     
           , turn_cre_val      
           , turn_cre_total    
           , 
		   CASE
		       WHEN characteristic = 'A' THEN balance_in_rub -turn_cre_rub + turn_deb_rub
			   WHEN characteristic = 'P' THEN balance_in_rub + turn_cre_rub - turn_deb_rub
		   END as balance_out_rub
		     
           , 
		   CASE
		       WHEN characteristic = 'A' THEN balance_in_val - turn_cre_val + turn_deb_val
			   WHEN characteristic = 'P' THEN balance_in_val + turn_cre_val - turn_deb_val
		   END as balance_out_val
		     
           , 
		    CASE
		       WHEN characteristic = 'A' THEN (balance_in_rub - turn_cre_rub + turn_deb_rub) + 
			   									(balance_in_val - turn_cre_val + turn_deb_val)
			   WHEN characteristic = 'P' THEN (balance_in_rub + turn_cre_rub - turn_deb_rub) + 
			   									(balance_in_val + turn_cre_val - turn_deb_val)
		   END as balance_out_total 
		   
	FROM t1;
	
	GET DIAGNOSTICS v_RowCount = ROW_COUNT;
    call logs.write_log_info('[END] inserted to dm_f101_round_f ' ||  to_char(v_RowCount,'FM99999999') || ' rows.', 1, 
							curr_seq_log_id, 'call_procedure dm.fill_f101_round_f', v_RowCount, info_msg);

    commit;
    
  end;$$

